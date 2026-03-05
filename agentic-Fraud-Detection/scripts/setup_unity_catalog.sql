-- ============================================================
-- Unity Catalog Setup for Agentic Fraud Triage
-- ============================================================

-- 1. Create the Financial_Security catalog
CREATE CATALOG IF NOT EXISTS financial_security
COMMENT 'Fraud Triage Agent - Banking security analytics catalog';

USE CATALOG financial_security;

-- 2. Create schemas
CREATE SCHEMA IF NOT EXISTS fraud_detection
COMMENT 'Raw and enriched fraud detection data';

CREATE SCHEMA IF NOT EXISTS fraud_investigation
COMMENT 'Investigation tools and Genie Space data';

CREATE SCHEMA IF NOT EXISTS fraud_operations
COMMENT 'Operational data, KPIs, and model artifacts';

-- 3. Create Volume for mock source files
CREATE VOLUME IF NOT EXISTS fraud_detection.source_files
COMMENT 'Volume for mock banking source CSV/JSON files';
