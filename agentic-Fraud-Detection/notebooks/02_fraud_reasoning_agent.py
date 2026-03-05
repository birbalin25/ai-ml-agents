# Databricks notebook source
# MAGIC %md
# MAGIC # Fraud Reasoning Agent
# MAGIC
# MAGIC AI Agent that analyzes transaction metadata and provides:
# MAGIC 1. **Risk Score** (0-100) combining rule-based and AI reasoning
# MAGIC 2. **Plain-English Explanation** for regulatory compliance (GDPR/CCPA)
# MAGIC 3. **Recommended Action**: BLOCK, YELLOW_FLAG, or ALLOW
# MAGIC
# MAGIC Uses Foundation Model API for reasoning and embeds fraud signature matching.

# COMMAND ----------

# MAGIC %pip install mlflow databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

import mlflow
import json
from mlflow.pyfunc import PythonModel
from databricks.sdk import WorkspaceClient

CATALOG = spark.conf.get("spark.databricks.unityCatalog.defaultCatalog", "serverless_bir_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Fraud Reasoning Agent

# COMMAND ----------

class FraudReasoningAgent(PythonModel):
    """
    AI Agent that analyzes transaction metadata and provides explainable risk assessments.
    Uses Foundation Model API for natural language reasoning.
    """

    def load_context(self, context):
        """Initialize the agent with Databricks SDK client."""
        from databricks.sdk import WorkspaceClient
        self.w = WorkspaceClient()

    def _call_llm(self, prompt: str) -> str:
        """Call Foundation Model API for reasoning."""
        import requests
        import os

        response = self.w.serving_endpoints.query(
            name="databricks-claude-sonnet-4-5",
            messages=[
                {"role": "system", "content": """You are a senior fraud analyst AI at a major bank.
Your job is to analyze transaction metadata and provide risk assessments.
You must ALWAYS respond in valid JSON format with exactly these keys:
- risk_score: integer 0-100
- explanation: string (2-3 sentences, plain English, suitable for regulatory review)
- action: one of "BLOCK", "YELLOW_FLAG", "ALLOW"
- risk_factors: list of strings identifying key risk signals"""},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500,
            temperature=0.1
        )
        return response.choices[0].message.content

    def _build_analysis_prompt(self, txn: dict) -> str:
        """Build the analysis prompt from transaction metadata."""
        return f"""Analyze this banking transaction for fraud risk:

TRANSACTION:
- Amount: ${txn.get('amount', 0):,.2f} ({txn.get('txn_type', 'unknown')})
- Channel: {txn.get('txn_channel', 'unknown')}
- Merchant: {txn.get('merchant_name', 'unknown')} ({txn.get('merchant_category', 'unknown')})
- International: {txn.get('is_international', False)}

SESSION CONTEXT:
- IP Changed Recently: {txn.get('ip_change_flag', 0) == 1}
- MFA Changed in Session: {txn.get('mfa_change_flag', False)}
- Typing Cadence Score: {txn.get('typing_cadence_score', 'N/A')} (normal: 0.5-1.0, suspicious: <0.45)
- Bot Signature Detected: {txn.get('is_bot_signature', False)}
- Geo Distance from Previous Login: {txn.get('geo_distance_miles', 0):.0f} miles
- Time Since Previous Login: {txn.get('time_since_prev_login_min', 'N/A')} minutes

USER PROFILE:
- Account Age: {txn.get('account_age_days', 0)} days
- Average Monthly Transaction: ${txn.get('avg_monthly_txn', 0):,.2f}
- Home City: {txn.get('home_city', 'unknown')}
- Current Risk Tier: {txn.get('risk_tier', 'unknown')}

COMPUTED RISK SIGNALS:
- Impossible Travel Detected: {txn.get('impossible_travel', False)}
- MFA Change + High Value: {txn.get('mfa_change_high_value', False)}
- High Value Wire After IP Change: {txn.get('high_value_wire_after_ip_change', False)}
- Abnormal Typing Pattern: {txn.get('abnormal_typing', False)}
- Amount Anomaly (>5x avg): {txn.get('amount_anomaly', False)}
- Rule-Based Risk Score: {txn.get('rule_based_risk_score', 0)}

Provide your risk assessment in JSON format."""

    def predict(self, context, model_input):
        """Score one or more transactions."""
        import pandas as pd

        if isinstance(model_input, pd.DataFrame):
            results = []
            for _, row in model_input.iterrows():
                txn = row.to_dict()
                result = self._analyze_single(txn)
                results.append(result)
            return pd.DataFrame(results)
        else:
            return self._analyze_single(model_input)

    def _analyze_single(self, txn: dict) -> dict:
        """Analyze a single transaction."""
        prompt = self._build_analysis_prompt(txn)

        try:
            llm_response = self._call_llm(prompt)
            # Parse JSON from response
            # Handle potential markdown code blocks
            clean = llm_response.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]
            assessment = json.loads(clean)
        except Exception as e:
            # Fallback to rule-based score if LLM fails
            rule_score = txn.get("rule_based_risk_score", 0)
            assessment = {
                "risk_score": rule_score,
                "explanation": f"Rule-based assessment (LLM unavailable): Score {rule_score} based on detected risk signals.",
                "action": "BLOCK" if rule_score >= 80 else ("YELLOW_FLAG" if rule_score >= 50 else "ALLOW"),
                "risk_factors": [k for k in ["impossible_travel", "mfa_change_high_value",
                    "high_value_wire_after_ip_change", "abnormal_typing", "amount_anomaly"]
                    if txn.get(k, False)]
            }

        return {
            "transaction_id": txn.get("transaction_id", ""),
            "risk_score": assessment.get("risk_score", 0),
            "explanation": assessment.get("explanation", ""),
            "action": assessment.get("action", "ALLOW"),
            "risk_factors": json.dumps(assessment.get("risk_factors", [])),
            "rule_based_score": txn.get("rule_based_risk_score", 0),
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Agent with MLflow

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="fraud_reasoning_agent_v1") as run:
    mlflow.pyfunc.log_model(
        artifact_path="fraud_reasoning_agent",
        python_model=FraudReasoningAgent(),
        registered_model_name=f"{CATALOG}.fraud_operations.fraud_reasoning_agent",
        pip_requirements=["mlflow", "databricks-sdk"],
    )
    print(f"Model logged: run_id={run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Agent

# COMMAND ----------

# Load and test with a sample high-risk transaction
sample_txn = spark.sql(f"""
    SELECT * FROM {CATALOG}.fraud_detection.silver_enriched_transactions
    WHERE rule_based_risk_score >= 50
    ORDER BY rule_based_risk_score DESC
    LIMIT 5
""").toPandas()

print(f"Testing with {len(sample_txn)} high-risk transactions...")

# Load model and test
model = FraudReasoningAgent()
model.load_context(None)

for i, row in sample_txn.iterrows():
    result = model._analyze_single(row.to_dict())
    print(f"\n--- Transaction {result['transaction_id']} ---")
    print(f"  Rule Score: {result['rule_based_score']} | AI Score: {result['risk_score']}")
    print(f"  Action: {result['action']}")
    print(f"  Explanation: {result['explanation']}")
    print(f"  Risk Factors: {result['risk_factors']}")
