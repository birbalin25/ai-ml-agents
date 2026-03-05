"""
Live Fraud Queue - Databricks App
Allows analysts to review and release yellow-flagged transactions in real time.

Dual data source:
  - Lakebase (Postgres): Operational queries (stats, queue, decisions, user profiles)
  - SQL Warehouse (Delta): Analytical queries (KPIs, patterns, enrichment data)
"""

import os
import time
import json
import logging
import subprocess
import uuid
from pathlib import Path

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from databricks.sdk import WorkspaceClient

app = FastAPI(title="Live Fraud Queue", version="2.0.0")
logger = logging.getLogger("fraud-queue")

# ── Config ─────────────────────────────────────────────
CATALOG = os.getenv("CATALOG", "serverless_bir_catalog")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "19be9738b181575a")
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST", "instance-2af707c9-c7d7-4bc9-91b0-632db640ccb4.database.cloud.databricks.com")
LAKEBASE_DB = os.getenv("LAKEBASE_DB", "fraud_ops")
LAKEBASE_INSTANCE = os.getenv("LAKEBASE_INSTANCE", "fraud-triage-ops")

_client = None
_pg_conn = None
_pg_token = None
_pg_token_expiry = 0
_pg_user = None


def get_client():
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


# ── Lakebase (Postgres) connection ─────────────────────

def _generate_db_credential():
    """Generate a Lakebase database credential via the Provisioned Database API."""
    w = get_client()
    resp = w.api_client.do(
        "POST",
        "/api/2.0/database/credentials",
        body={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
    )
    return resp["token"], resp.get("expiration_time", "")


def _extract_pg_user(token):
    """Extract the 'sub' claim from the JWT to use as the Postgres username."""
    import base64
    payload = token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    data = json.loads(base64.b64decode(payload))
    return data.get("sub", "databricks")


def get_pg_connection():
    """Get or refresh a psycopg2 connection to Lakebase with OAuth token caching."""
    global _pg_conn, _pg_token, _pg_token_expiry, _pg_user

    now = time.time()
    # Refresh token 5 minutes before expiry, or if no connection exists
    needs_refresh = (_pg_conn is None or _pg_conn.closed
                     or now >= _pg_token_expiry - 300)

    if needs_refresh:
        if _pg_conn and not _pg_conn.closed:
            try:
                _pg_conn.close()
            except Exception:
                pass

        # Generate a Lakebase-specific database credential
        _pg_token, expire_time = _generate_db_credential()
        _pg_user = _extract_pg_user(_pg_token)
        # Tokens are valid for 1 hour; refresh 5 min before
        _pg_token_expiry = now + 3600

        _pg_conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            port=5432,
            dbname=LAKEBASE_DB,
            user=_pg_user,
            password=_pg_token,
            sslmode="require",
        )
        _pg_conn.autocommit = True
        logger.info(f"Lakebase connection established as {_pg_user}")

    return _pg_conn


def run_pg_query(sql, params=None):
    """Execute a SELECT query against Lakebase Postgres. Returns list of dicts."""
    try:
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except psycopg2.OperationalError:
        # Connection may have been dropped; reset and retry once
        global _pg_conn
        _pg_conn = None
        conn = get_pg_connection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Postgres query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def run_pg_execute(sql, params=None):
    """Execute a DML statement (INSERT/UPDATE/DELETE) against Lakebase Postgres."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except psycopg2.OperationalError:
        global _pg_conn
        _pg_conn = None
        conn = get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(sql, params)
    except Exception as e:
        logger.error(f"Postgres execute error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── SQL Warehouse (Delta) connection ───────────────────

def run_warehouse_query(sql):
    """Execute SQL via Databricks SDK Statement Execution API (for analytical queries)."""
    try:
        w = get_client()
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql,
            wait_timeout="30s",
        )
        state = response.status.state.value if response.status and response.status.state else "UNKNOWN"
        if state == "SUCCEEDED":
            columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
            rows = response.result.data_array if response.result and response.result.data_array else []
            return [dict(zip(columns, row)) for row in rows]
        else:
            error_msg = str(response.status.error) if response.status and response.status.error else "Unknown SQL error"
            logger.error(f"SQL failed: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Warehouse query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── API Endpoints (Operational - Lakebase) ─────────────

@app.get("/api/stats")
def get_stats():
    """Dashboard statistics for the last 24 hours."""
    results = run_pg_query("""
        SELECT
            COUNT(*) FILTER (WHERE risk_category = 'RED') as blocked,
            COUNT(*) FILTER (WHERE risk_category = 'YELLOW' AND analyst_decision IS NULL) as pending_review,
            COUNT(*) FILTER (WHERE risk_category = 'YELLOW' AND analyst_decision IS NOT NULL) as reviewed,
            COUNT(*) FILTER (WHERE risk_category = 'GREEN') as allowed,
            ROUND(AVG(ttl_decision_ms)::numeric, 0) as avg_latency_ms,
            ROUND(SUM(CASE WHEN risk_category IN ('RED','YELLOW') THEN amount ELSE 0 END)::numeric, 2) as amount_at_risk,
            COUNT(*) as total_transactions
        FROM real_time_fraud_triage
    """)
    return results[0] if results else {}


@app.get("/api/queue")
def get_fraud_queue(
    status: str = Query("YELLOW", description="Risk category: RED, YELLOW, or ALL"),
    limit: int = Query(50, le=200),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("risk_score", description="Sort field"),
    sort_dir: str = Query("DESC", description="ASC or DESC"),
):
    """Fetch transactions for analyst review."""
    conditions = []
    params = {}

    if status == "YELLOW":
        conditions.append("risk_category = 'YELLOW' AND analyst_decision IS NULL")
    elif status == "RED":
        conditions.append("risk_category = 'RED'")
    elif status == "ALL_FLAGGED":
        conditions.append("risk_category IN ('RED', 'YELLOW')")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    allowed_sorts = {"risk_score", "amount", "created_at", "ttl_decision_ms"}
    sort_field = sort_by if sort_by in allowed_sorts else "risk_score"
    direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"

    # sort_field and direction are validated above, safe to interpolate
    results = run_pg_query(f"""
        SELECT transaction_id, user_id, ROUND(amount::numeric, 2) as amount, txn_type,
               risk_score, risk_category, automated_action, explanation,
               risk_factors, analyst_decision, analyst_notes,
               created_at, reviewed_at, ttl_decision_ms
        FROM real_time_fraud_triage
        {where_clause}
        ORDER BY {sort_field} {direction}
        LIMIT %(limit)s OFFSET %(offset)s
    """, {"limit": limit, "offset": offset})
    return results


@app.post("/api/decision")
def submit_decision(txn_id: str, decision: str, analyst_notes: str = ""):
    """Analyst submits Block/Release decision on a flagged transaction."""
    if decision not in ("BLOCK", "RELEASE", "ESCALATE"):
        raise HTTPException(status_code=400, detail="Decision must be BLOCK, RELEASE, or ESCALATE")

    if decision == "BLOCK":
        new_category = "RED"
        new_action = "BLOCK"
    elif decision == "RELEASE":
        new_category = "GREEN"
        new_action = "ALLOW"
    else:
        new_category = "YELLOW"
        new_action = "ESCALATE"

    run_pg_execute("""
        UPDATE real_time_fraud_triage
        SET analyst_decision = %(decision)s,
            analyst_notes = %(notes)s,
            risk_category = %(category)s,
            automated_action = %(action)s,
            reviewed_at = NOW()
        WHERE transaction_id = %(txn_id)s
    """, {
        "decision": decision,
        "notes": analyst_notes,
        "category": new_category,
        "action": new_action,
        "txn_id": txn_id,
    })
    return {"status": "updated", "transaction_id": txn_id, "decision": decision}


@app.get("/api/user/{user_id}")
def get_user_risk_profile(user_id: str):
    """Get risk profile for a specific user."""
    results = run_pg_query("""
        SELECT transaction_id, ROUND(amount::numeric, 2) as amount, txn_type, risk_score,
               risk_category, automated_action, LEFT(explanation, 200) as explanation,
               created_at
        FROM real_time_fraud_triage
        WHERE user_id = %(user_id)s
        ORDER BY risk_score DESC
    """, {"user_id": user_id})
    return results


@app.get("/api/transaction/{txn_id}")
def get_transaction_detail(txn_id: str):
    """Get full details for a specific transaction (hybrid: Lakebase + SQL Warehouse)."""
    # Triage data from Lakebase (fast, low-latency)
    triage = run_pg_query("""
        SELECT * FROM real_time_fraud_triage
        WHERE transaction_id = %(txn_id)s
    """, {"txn_id": txn_id})

    if not triage:
        raise HTTPException(status_code=404, detail="Transaction not found")

    result = triage[0]

    # Enrichment data from SQL Warehouse (Silver table)
    enrichment = run_warehouse_query(f"""
        SELECT login_ip, login_geo_lat, login_geo_lon,
               device_fingerprint, typing_cadence_score, is_bot_signature,
               geo_distance_miles, time_since_prev_login_min,
               impossible_travel, mfa_change_high_value,
               high_value_wire_after_ip_change, abnormal_typing,
               amount_anomaly, home_city, account_age_days, avg_monthly_txn
        FROM {CATALOG}.fraud_detection.silver_enriched_transactions
        WHERE transaction_id = '{txn_id}'
    """)

    if enrichment:
        result.update(enrichment[0])

    return result


# ── API Endpoints (Analytical - SQL Warehouse) ─────────

@app.get("/api/kpis")
def get_kpis():
    """Fraud detection KPIs."""
    results = run_warehouse_query(f"""
        SELECT report_date, total_transactions, red_flagged, yellow_flagged,
               actual_fraud_count, false_positive_ratio_pct, fraud_detection_rate_pct,
               avg_risk_score, ROUND(amount_at_risk, 2) as amount_at_risk
        FROM {CATALOG}.fraud_operations.gold_fraud_kpis
        ORDER BY report_date DESC LIMIT 30
    """)
    return results


@app.get("/api/patterns")
def get_fraud_patterns():
    """Fraud by attack pattern type."""
    results = run_warehouse_query(f"""
        SELECT pattern_type, txn_count, ROUND(total_amount, 2) as total_amount,
               avg_amount, avg_risk_score, unique_users
        FROM {CATALOG}.fraud_operations.gold_fraud_by_pattern
        ORDER BY txn_count DESC
    """)
    return results


# ── Frontend ───────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    """Serve the Live Fraud Queue UI."""
    return FRONTEND_HTML


FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Live Fraud Queue</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }

        .header { background: #1a1d27; padding: 16px 24px; border-bottom: 1px solid #2a2d3a; display: flex; align-items: center; justify-content: space-between; }
        .header h1 { font-size: 20px; color: #fff; }
        .header .subtitle { font-size: 13px; color: #888; }

        .stats-bar { display: flex; gap: 16px; padding: 16px 24px; background: #13151d; border-bottom: 1px solid #2a2d3a; flex-wrap: wrap; }
        .stat-card { background: #1a1d27; border-radius: 8px; padding: 12px 20px; min-width: 140px; border: 1px solid #2a2d3a; }
        .stat-card .label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
        .stat-card .value { font-size: 24px; font-weight: 700; margin-top: 4px; }
        .stat-card.red .value { color: #ff4d4f; }
        .stat-card.yellow .value { color: #faad14; }
        .stat-card.green .value { color: #52c41a; }
        .stat-card.blue .value { color: #1890ff; }

        .controls { padding: 12px 24px; display: flex; gap: 8px; align-items: center; background: #13151d; }
        .tab { padding: 8px 16px; border-radius: 6px; border: 1px solid #2a2d3a; background: #1a1d27; color: #888; cursor: pointer; font-size: 13px; }
        .tab.active { background: #2a2d3a; color: #fff; border-color: #3a3d4a; }
        .tab:hover { border-color: #3a3d4a; color: #bbb; }
        .refresh-btn { margin-left: auto; padding: 8px 16px; border-radius: 6px; border: 1px solid #1890ff; background: transparent; color: #1890ff; cursor: pointer; font-size: 13px; }
        .refresh-btn:hover { background: #1890ff22; }

        .queue { padding: 0 24px 24px; }
        .txn-card { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 8px; margin-top: 12px; overflow: hidden; transition: border-color 0.2s; }
        .txn-card:hover { border-color: #3a3d4a; }
        .txn-card.red { border-left: 4px solid #ff4d4f; }
        .txn-card.yellow { border-left: 4px solid #faad14; }

        .txn-header { display: flex; align-items: center; padding: 14px 18px; gap: 16px; flex-wrap: wrap; }
        .txn-id { font-family: monospace; font-size: 13px; color: #1890ff; }
        .txn-amount { font-size: 18px; font-weight: 700; color: #fff; }
        .txn-type { font-size: 12px; padding: 3px 8px; border-radius: 4px; background: #2a2d3a; color: #bbb; }
        .txn-score { font-size: 14px; font-weight: 600; padding: 4px 10px; border-radius: 4px; }
        .txn-score.high { background: #ff4d4f22; color: #ff4d4f; }
        .txn-score.medium { background: #faad1422; color: #faad14; }
        .txn-time { font-size: 12px; color: #666; margin-left: auto; }

        .txn-explanation { padding: 0 18px 14px; font-size: 13px; line-height: 1.6; color: #aaa; }
        .txn-explanation strong { color: #e0e0e0; }

        .txn-actions { display: flex; gap: 8px; padding: 0 18px 14px; }
        .btn { padding: 8px 16px; border-radius: 6px; border: none; cursor: pointer; font-size: 13px; font-weight: 500; transition: all 0.2s; }
        .btn-block { background: #ff4d4f; color: white; }
        .btn-block:hover { background: #ff7875; }
        .btn-release { background: #52c41a; color: white; }
        .btn-release:hover { background: #73d13d; }
        .btn-escalate { background: #faad14; color: #000; }
        .btn-escalate:hover { background: #ffc53d; }
        .btn-detail { background: #2a2d3a; color: #bbb; }
        .btn-detail:hover { background: #3a3d4a; }

        .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
        .badge-red { background: #ff4d4f22; color: #ff4d4f; }
        .badge-yellow { background: #faad1422; color: #faad14; }
        .badge-green { background: #52c41a22; color: #52c41a; }

        .detail-modal { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); z-index: 100; justify-content: center; align-items: center; }
        .detail-modal.active { display: flex; }
        .detail-content { background: #1a1d27; border: 1px solid #2a2d3a; border-radius: 12px; max-width: 700px; width: 90%; max-height: 80vh; overflow-y: auto; padding: 24px; }
        .detail-content h2 { margin-bottom: 16px; font-size: 18px; }
        .detail-row { display: flex; padding: 8px 0; border-bottom: 1px solid #2a2d3a; }
        .detail-label { width: 180px; color: #888; font-size: 13px; }
        .detail-value { flex: 1; font-size: 13px; }
        .close-btn { float: right; font-size: 20px; cursor: pointer; color: #888; background: none; border: none; }
        .close-btn:hover { color: #fff; }

        .loading { text-align: center; padding: 40px; color: #666; }
        .empty { text-align: center; padding: 60px; color: #555; }
        .empty h3 { font-size: 18px; margin-bottom: 8px; color: #888; }

        .risk-factors { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; padding: 0 18px 14px; }
        .risk-tag { font-size: 11px; padding: 2px 8px; border-radius: 4px; background: #2a2d3a; color: #faad14; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>Live Fraud Queue</h1>
            <div class="subtitle">Agentic Fraud Triage System</div>
        </div>
        <div id="latency" style="font-size:12px; color:#666;">Avg Latency: --ms</div>
    </div>

    <div class="stats-bar" id="stats-bar">
        <div class="stat-card red"><div class="label">Blocked (RED)</div><div class="value" id="stat-blocked">--</div></div>
        <div class="stat-card yellow"><div class="label">Pending Review</div><div class="value" id="stat-pending">--</div></div>
        <div class="stat-card green"><div class="label">Allowed</div><div class="value" id="stat-allowed">--</div></div>
        <div class="stat-card blue"><div class="label">Amount at Risk</div><div class="value" id="stat-amount">--</div></div>
        <div class="stat-card"><div class="label">Total Transactions</div><div class="value" id="stat-total">--</div></div>
    </div>

    <div class="controls">
        <div class="tab active" data-status="YELLOW" onclick="switchTab(this)">Pending Review</div>
        <div class="tab" data-status="RED" onclick="switchTab(this)">Blocked</div>
        <div class="tab" data-status="ALL_FLAGGED" onclick="switchTab(this)">All Flagged</div>
        <button class="refresh-btn" onclick="loadQueue()">Refresh</button>
    </div>

    <div class="queue" id="queue">
        <div class="loading">Loading transactions...</div>
    </div>

    <div class="detail-modal" id="detail-modal">
        <div class="detail-content" id="detail-content">
            <button class="close-btn" onclick="closeDetail()">&times;</button>
            <h2>Transaction Detail</h2>
            <div id="detail-body"></div>
        </div>
    </div>

    <script>
        let currentStatus = 'YELLOW';

        function formatMoney(val) {
            if (val == null) return '$0.00';
            return '$' + Number(val).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
        }

        function formatNumber(val) {
            if (val == null) return '0';
            return Number(val).toLocaleString();
        }

        async function loadStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('stat-blocked').textContent = formatNumber(data.blocked);
                document.getElementById('stat-pending').textContent = formatNumber(data.pending_review);
                document.getElementById('stat-allowed').textContent = formatNumber(data.allowed);
                document.getElementById('stat-amount').textContent = formatMoney(data.amount_at_risk);
                document.getElementById('stat-total').textContent = formatNumber(data.total_transactions);
                document.getElementById('latency').textContent = 'Avg Latency: ' + Math.round(data.avg_latency_ms || 0) + 'ms';
            } catch(e) { console.error('Stats error:', e); }
        }

        async function loadQueue() {
            const queue = document.getElementById('queue');
            queue.innerHTML = '<div class="loading">Loading...</div>';
            try {
                const res = await fetch('/api/queue?status=' + currentStatus + '&limit=50');
                const data = await res.json();
                if (!data.length) {
                    queue.innerHTML = '<div class="empty"><h3>Queue Empty</h3><p>No transactions matching this filter.</p></div>';
                    return;
                }
                queue.innerHTML = data.map(txn => renderCard(txn)).join('');
            } catch(e) {
                queue.innerHTML = '<div class="empty"><h3>Error Loading Queue</h3><p>' + e.message + '</p></div>';
            }
        }

        function renderCard(txn) {
            const colorClass = txn.risk_category === 'RED' ? 'red' : 'yellow';
            const scoreClass = txn.risk_score >= 80 ? 'high' : 'medium';
            let factors = [];
            try { factors = JSON.parse(txn.risk_factors || '[]'); } catch(e) {}
            const isReviewed = txn.analyst_decision != null;
            const reviewBadge = isReviewed
                ? '<span class="badge ' + (txn.analyst_decision === 'BLOCK' ? 'badge-red' : 'badge-green') + '">' + txn.analyst_decision + '</span>'
                : '';

            return '<div class="txn-card ' + colorClass + '">'
                + '<div class="txn-header">'
                + '<span class="txn-id">' + txn.transaction_id + '</span>'
                + '<span class="txn-amount">' + formatMoney(txn.amount) + '</span>'
                + '<span class="txn-type">' + (txn.txn_type || '').replace('_', ' ') + '</span>'
                + '<span class="txn-score ' + scoreClass + '">Score: ' + txn.risk_score + '</span>'
                + reviewBadge
                + '<span class="txn-time">' + (txn.created_at || '') + '</span>'
                + '</div>'
                + '<div class="txn-explanation">' + (txn.explanation || 'No explanation available.') + '</div>'
                + (factors.length ? '<div class="risk-factors">' + factors.map(f => '<span class="risk-tag">' + f + '</span>').join('') + '</div>' : '')
                + (isReviewed ? '' : '<div class="txn-actions">'
                    + '<button class="btn btn-block" onclick="submitDecision(\\'' + txn.transaction_id + '\\', \\'BLOCK\\')">Confirm Block</button>'
                    + '<button class="btn btn-release" onclick="submitDecision(\\'' + txn.transaction_id + '\\', \\'RELEASE\\')">Release</button>'
                    + '<button class="btn btn-escalate" onclick="submitDecision(\\'' + txn.transaction_id + '\\', \\'ESCALATE\\')">Escalate</button>'
                    + '<button class="btn btn-detail" onclick="showDetail(\\'' + txn.transaction_id + '\\')">Details</button>'
                    + '</div>')
                + '</div>';
        }

        async function submitDecision(txnId, decision) {
            const notes = decision === 'ESCALATE' ? prompt('Add notes for escalation:', '') || '' : '';
            try {
                await fetch('/api/decision?txn_id=' + txnId + '&decision=' + decision + '&analyst_notes=' + encodeURIComponent(notes), {method: 'POST'});
                loadQueue();
                loadStats();
            } catch(e) { alert('Error: ' + e.message); }
        }

        async function showDetail(txnId) {
            const modal = document.getElementById('detail-modal');
            const body = document.getElementById('detail-body');
            body.innerHTML = '<div class="loading">Loading details...</div>';
            modal.classList.add('active');
            try {
                const res = await fetch('/api/transaction/' + txnId);
                const data = await res.json();
                const fields = [
                    ['Transaction ID', data.transaction_id],
                    ['User ID', data.user_id],
                    ['Amount', formatMoney(data.amount)],
                    ['Type', data.txn_type],
                    ['Risk Score', data.risk_score],
                    ['Category', data.risk_category],
                    ['Action', data.automated_action],
                    ['Device', data.device_fingerprint],
                    ['Login IP', data.login_ip],
                    ['Geo Distance', Math.round(data.geo_distance_miles || 0) + ' miles'],
                    ['Time Since Prev Login', Math.round(data.time_since_prev_login_min || 0) + ' min'],
                    ['Typing Cadence', data.typing_cadence_score],
                    ['Bot Signature', data.is_bot_signature],
                    ['Impossible Travel', data.impossible_travel],
                    ['MFA Change + High Value', data.mfa_change_high_value],
                    ['Home City', data.home_city],
                    ['Account Age', (data.account_age_days || 0) + ' days'],
                    ['Avg Monthly Txn', formatMoney(data.avg_monthly_txn)],
                    ['Explanation', data.explanation],
                    ['Latency', (data.ttl_decision_ms || 0) + ' ms'],
                ];
                body.innerHTML = fields.map(([label, value]) =>
                    '<div class="detail-row"><div class="detail-label">' + label + '</div><div class="detail-value">' + (value || 'N/A') + '</div></div>'
                ).join('');
            } catch(e) { body.innerHTML = '<p>Error loading details</p>'; }
        }

        function closeDetail() {
            document.getElementById('detail-modal').classList.remove('active');
        }

        function switchTab(el) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
            currentStatus = el.dataset.status;
            loadQueue();
        }

        // Initial load
        loadStats();
        loadQueue();

        // Auto-refresh every 30 seconds
        setInterval(() => { loadStats(); loadQueue(); }, 30000);
    </script>
</body>
</html>"""
