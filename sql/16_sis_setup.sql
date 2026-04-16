-- =============================================================================
-- SignalFlowAI — Streamlit in Snowflake (SiS) Setup
-- Run as ACCOUNTADMIN. Execute each STEP block sequentially.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SIGNALFLOWAI_PROD_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE SIGNALFLOWAI_WH;


-- =============================================================================
-- STEP 1: Network Rules — allow outbound HTTPS to OpenAI and Groq
-- =============================================================================

CREATE OR REPLACE NETWORK RULE OPENAI_NETWORK_RULE
    MODE       = EGRESS
    TYPE       = HOST_PORT
    VALUE_LIST = ('api.openai.com:443');

CREATE OR REPLACE NETWORK RULE GROQ_NETWORK_RULE
    MODE       = EGRESS
    TYPE       = HOST_PORT
    VALUE_LIST = ('api.groq.com:443');

SHOW NETWORK RULES;


-- =============================================================================
-- STEP 2: Secrets — store API keys inside Snowflake
-- REPLACE the placeholder strings with your actual keys before running.
-- =============================================================================

CREATE OR REPLACE SECRET SIGNALFLOWAI_OPENAI_KEY
    TYPE          = GENERIC_STRING
    SECRET_STRING = 'sk-your-openai-api-key-here';   -- ← REPLACE THIS

CREATE OR REPLACE SECRET SIGNALFLOWAI_GROQ_KEY
    TYPE          = GENERIC_STRING
    SECRET_STRING = 'gsk_your-groq-api-key-here';    -- ← REPLACE THIS

SHOW SECRETS;  -- confirms creation without revealing values


-- =============================================================================
-- STEP 3: External Access Integration
-- Links the network rules + secrets so the app can call OpenAI and Groq.
-- =============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SIGNALFLOWAI_EAI
    ALLOWED_NETWORK_RULES          = (OPENAI_NETWORK_RULE, GROQ_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (SIGNALFLOWAI_OPENAI_KEY, SIGNALFLOWAI_GROQ_KEY)
    ENABLED                        = TRUE;

GRANT USAGE ON INTEGRATION SIGNALFLOWAI_EAI TO ROLE SYSADMIN;

SHOW EXTERNAL ACCESS INTEGRATIONS;


-- =============================================================================
-- STEP 4: Stage — holds all Python source files
-- =============================================================================

CREATE OR REPLACE STAGE SIGNALFLOWAI_APP_STAGE
    COMMENT = 'SignalFlowAI Streamlit app files for SiS deployment';

SHOW STAGES;


-- =============================================================================
-- STEP 5: Upload files using PUT commands
-- Run these in SnowSQL (terminal) from your project root directory:
--   cd C:\Ranjithnathk\Projects\SignalFlowAI
--   snowsql -a <your_account> -u <your_user>
--
-- Then paste the commands below one section at a time.
-- AUTO_COMPRESS=FALSE is required — Streamlit files must not be gzipped.
-- =============================================================================

/*
-- ── Main app entry point ─────────────────────────────────────────────────────
PUT file://src/app/app_sis.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/app_sis.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- ── Package init files (Python needs these to treat dirs as packages) ────────
PUT file://src/__init__.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/__init__.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/__init__.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/__init__.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/retrieval/__init__.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/retrieval/__init__.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/reasoning/__init__.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/reasoning/__init__.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- ── Agent modules ─────────────────────────────────────────────────────────────
PUT file://src/agents/state.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/state.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/graph.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/graph.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/query_agent.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/query_agent.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/retrieval_agent.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/retrieval_agent.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/reasoning_agent.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/reasoning_agent.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://src/agents/verifier_agent.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/agents/verifier_agent.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- ── Retrieval module — upload the SiS version AS snowflake_retriever.py ──────
-- IMPORTANT: upload snowflake_retriever_sis.py but name it snowflake_retriever.py
-- so the existing import (from src.retrieval.snowflake_retriever import ...) works.
PUT file://src/retrieval/snowflake_retriever_sis.py
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/src/retrieval/snowflake_retriever.py
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- ── Requirements file ─────────────────────────────────────────────────────────
PUT file://requirements_sis.txt
    @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE/requirements.txt
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
*/

-- Verify all files are uploaded:
LIST @SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE;

-- Expected output — you should see these paths:
--   app_sis.py
--   src/__init__.py
--   src/agents/__init__.py
--   src/agents/graph.py
--   src/agents/query_agent.py
--   src/agents/reasoning_agent.py
--   src/agents/retrieval_agent.py
--   src/agents/state.py
--   src/agents/verifier_agent.py
--   src/retrieval/__init__.py
--   src/retrieval/snowflake_retriever.py   ← this is the SiS version
--   src/reasoning/__init__.py
--   requirements.txt


-- =============================================================================
-- STEP 6: Create the Streamlit app object
-- =============================================================================

CREATE OR REPLACE STREAMLIT SIGNALFLOWAI_APP
    ROOT_LOCATION                = '@SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE'
    MAIN_FILE                    = '/app_sis.py'
    QUERY_WAREHOUSE              = 'SIGNALFLOWAI_WH'
    EXTERNAL_ACCESS_INTEGRATIONS = (SIGNALFLOWAI_EAI)
    SECRETS = (
        'openai_api_key' = SIGNALFLOWAI_OPENAI_KEY,
        'groq_api_key'   = SIGNALFLOWAI_GROQ_KEY
    )
    COMMENT = 'SignalFlowAI — Operational Decision Intelligence';

SHOW STREAMLITS;


-- =============================================================================
-- STEP 7: Get the live URL + grant access
-- =============================================================================

-- Get the live URL (copy this and open in browser)
SELECT SYSTEM$GET_STREAMLIT_URL('SIGNALFLOWAI_APP');

-- Let SYSADMIN role manage the app
GRANT USAGE ON STREAMLIT SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP TO ROLE SYSADMIN;

-- Optional: make it accessible to all users in the account (open demo)
-- GRANT USAGE ON STREAMLIT SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP TO ROLE PUBLIC;


-- =============================================================================
-- UPDATING THE APP after code changes
-- Re-PUT only the changed file(s), then run:
-- =============================================================================
-- ALTER STREAMLIT SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP
--     SET MAIN_FILE = '/app_sis.py';
-- The app picks up the new stage files on next load — no recreation needed.


-- =============================================================================
-- TEAR DOWN
-- =============================================================================
-- DROP STREAMLIT  SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP;
-- DROP STAGE      SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_APP_STAGE;
-- DROP INTEGRATION SIGNALFLOWAI_EAI;
-- DROP SECRET     SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_OPENAI_KEY;
-- DROP SECRET     SIGNALFLOWAI_PROD_DB.PUBLIC.SIGNALFLOWAI_GROQ_KEY;
-- DROP NETWORK RULE OPENAI_NETWORK_RULE;
-- DROP NETWORK RULE GROQ_NETWORK_RULE;
