"""
SignalFlowAI — SiS Deployment Script

Deploys the Streamlit app to Snowflake Streamlit in Snowflake (SiS)
using the existing RSA key pair connection from .env.

No SnowSQL needed. Run this once from the project root:
    python scripts/deploy_to_sis.py

What it does:
  1. Connects to Snowflake using your existing RSA key pair (.env)
  2. Creates Network Rules for OpenAI + Groq API access
  3. Creates Secret objects for OPENAI_API_KEY and GROQ_API_KEY
     (reads values from your .env — same keys, stored in Snowflake now)
  4. Creates the External Access Integration
  5. Creates the internal stage
  6. Uploads all app source files to the stage
  7. Creates (or replaces) the STREAMLIT object
  8. Prints the live URL

Prerequisites:
  - Your .env must have OPENAI_API_KEY and GROQ_API_KEY set
  - Your Snowflake role must have CREATE INTEGRATION privilege
    (ACCOUNTADMIN or SYSADMIN with appropriate grants)
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

SNOWFLAKE_CONN = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
    private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
)

DATABASE      = os.getenv("SNOWFLAKE_DATABASE", "SIGNALFLOWAI_PROD_DB")
WAREHOUSE     = os.getenv("SNOWFLAKE_WAREHOUSE", "SIGNALFLOWAI_WH")
WORKING_ROLE  = os.getenv("SNOWFLAKE_ROLE", "TRAINING_ROLE")   # your normal role
ADMIN_ROLE    = "ACCOUNTADMIN"                                  # needed for CREATE INTEGRATION
STAGE         = f"{DATABASE}.PUBLIC.SIGNALFLOWAI_APP_STAGE"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")

# Files to upload: (local_relative_path, stage_path)
# IMPORTANT: snowflake_retriever_sis.py is uploaded AS snowflake_retriever.py
# so the existing import (from src.retrieval.snowflake_retriever import ...) works.
FILES_TO_UPLOAD = [
    ("src/app/app_sis.py",                          "app_sis.py"),
    ("src/__init__.py",                             "src/__init__.py"),
    ("src/agents/__init__.py",                      "src/agents/__init__.py"),
    ("src/agents/state.py",                         "src/agents/state.py"),
    ("src/agents/graph.py",                         "src/agents/graph.py"),
    ("src/agents/query_agent.py",                   "src/agents/query_agent.py"),
    ("src/agents/retrieval_agent.py",               "src/agents/retrieval_agent.py"),
    ("src/agents/reasoning_agent.py",               "src/agents/reasoning_agent.py"),
    ("src/agents/verifier_agent.py",                "src/agents/verifier_agent.py"),
    ("src/retrieval/__init__.py",                   "src/retrieval/__init__.py"),
    ("src/retrieval/snowflake_retriever_sis.py",    "src/retrieval/snowflake_retriever.py"),
    ("src/reasoning/__init__.py",                   "src/reasoning/__init__.py"),
    ("requirements_sis.txt",                        "requirements.txt"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run(cur, sql: str, label: str = "") -> None:
    tag = f"  [{label}]" if label else ""
    try:
        cur.execute(sql)
        print(f"  ✅{tag} done")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower():
            print(f"  ⚠️{tag} already exists — skipped")
        else:
            print(f"  ❌{tag} FAILED: {msg}")
            raise


def put_file(cur, local_rel: str, stage_path: str) -> None:
    local_abs = PROJECT_ROOT / local_rel
    if not local_abs.exists():
        print(f"  ❌ LOCAL FILE NOT FOUND: {local_abs}")
        return
    # Windows paths need forward slashes and triple-slash for absolute paths
    local_uri = "file:///" + str(local_abs).replace("\\", "/")
    sql = (
        f"PUT {local_uri} "
        f"@{STAGE}/{stage_path} "
        f"OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
    )
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        status = rows[0][6] if rows else "unknown"
        print(f"  ✅ {local_rel:55s} → {stage_path}  [{status}]")
    except Exception as e:
        print(f"  ❌ {local_rel} → {stage_path}  FAILED: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # ── Pre-flight checks ────────────────────────────────────────────────────
    missing = [k for k, v in SNOWFLAKE_CONN.items() if not v]
    if missing:
        print(f"❌ Missing .env values: {missing}")
        sys.exit(1)

    if not OPENAI_API_KEY:
        print("❌ OPENAI_API_KEY not set in .env")
        sys.exit(1)

    if not GROQ_API_KEY:
        print("❌ GROQ_API_KEY not set in .env")
        sys.exit(1)

    # ── Connect ──────────────────────────────────────────────────────────────
    print("\n── Connecting to Snowflake ─────────────────────────────────────")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cur  = conn.cursor()
    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
    print(f"  ✅ Connected as {os.getenv('SNOWFLAKE_USER')} "
          f"(role: {WORKING_ROLE})")

    try:
        # ── Step 1: Network Rules (needs ACCOUNTADMIN) ────────────────────────
        print(f"\n── Step 1: Network Rules  [switching to {ADMIN_ROLE}] ──────────")
        cur.execute(f"USE ROLE {ADMIN_ROLE}")
        run(cur, """
            CREATE OR REPLACE NETWORK RULE OPENAI_NETWORK_RULE
                MODE = EGRESS TYPE = HOST_PORT
                VALUE_LIST = ('api.openai.com:443')
        """, "OpenAI rule")

        run(cur, """
            CREATE OR REPLACE NETWORK RULE GROQ_NETWORK_RULE
                MODE = EGRESS TYPE = HOST_PORT
                VALUE_LIST = ('api.groq.com:443')
        """, "Groq rule")

        # ── Step 2: Secrets (read from .env) ─────────────────────────────────
        print("\n── Step 2: Secrets (values from your .env) ─────────────────────")
        print("  ℹ️  .env keys are for local dev. Snowflake secrets are for the")
        print("     SiS app running on Snowflake's cloud — it can't read .env.")
        run(cur, f"""
            CREATE OR REPLACE SECRET SIGNALFLOWAI_OPENAI_KEY
                TYPE = GENERIC_STRING
                SECRET_STRING = '{OPENAI_API_KEY}'
        """, "OpenAI secret")

        run(cur, f"""
            CREATE OR REPLACE SECRET SIGNALFLOWAI_GROQ_KEY
                TYPE = GENERIC_STRING
                SECRET_STRING = '{GROQ_API_KEY}'
        """, "Groq secret")

        # ── Step 3: External Access Integration (needs ACCOUNTADMIN) ─────────
        print("\n── Step 3: External Access Integration ─────────────────────────")
        run(cur, """
            CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SIGNALFLOWAI_EAI
                ALLOWED_NETWORK_RULES          = (OPENAI_NETWORK_RULE, GROQ_NETWORK_RULE)
                ALLOWED_AUTHENTICATION_SECRETS = (SIGNALFLOWAI_OPENAI_KEY, SIGNALFLOWAI_GROQ_KEY)
                ENABLED = TRUE
        """, "EAI")

        # ── Step 4: Stage (switch back to working role) ───────────────────────
        print(f"\n── Step 4: Stage  [switching back to {WORKING_ROLE}] ───────────")
        cur.execute(f"USE ROLE {WORKING_ROLE}")
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute("USE SCHEMA PUBLIC")
        run(cur, f"""
            CREATE OR REPLACE STAGE {STAGE}
                COMMENT = 'SignalFlowAI Streamlit SiS app files'
        """, "stage")

        # ── Step 5: Upload files ──────────────────────────────────────────────
        print(f"\n── Step 5: Uploading {len(FILES_TO_UPLOAD)} files ───────────────────────────────")
        for local_rel, stage_path in FILES_TO_UPLOAD:
            put_file(cur, local_rel, stage_path)

        # Verify
        cur.execute(f"LIST @{STAGE}")
        staged = cur.fetchall()
        print(f"\n  📦 {len(staged)} files now in stage")
        if len(staged) < len(FILES_TO_UPLOAD):
            print("  ⚠️  Some files may have failed to upload — check errors above")

        # ── Step 6: Create Streamlit object ───────────────────────────────────
        print("\n── Step 6: Create Streamlit App ────────────────────────────────")
        run(cur, f"""
            CREATE OR REPLACE STREAMLIT {DATABASE}.PUBLIC.SIGNALFLOWAI_APP
                ROOT_LOCATION                = '@{STAGE}'
                MAIN_FILE                    = '/app_sis.py'
                QUERY_WAREHOUSE              = '{WAREHOUSE}'
                EXTERNAL_ACCESS_INTEGRATIONS = (SIGNALFLOWAI_EAI)
                SECRETS = (
                    'openai_api_key' = SIGNALFLOWAI_OPENAI_KEY,
                    'groq_api_key'   = SIGNALFLOWAI_GROQ_KEY
                )
                COMMENT = 'SignalFlowAI — Operational Decision Intelligence'
        """, "STREAMLIT")

        # ── Step 7: Get URL ────────────────────────────────────────────────────
        print("\n── Step 7: Live URL ─────────────────────────────────────────────")
        cur.execute(f"SELECT SYSTEM$GET_STREAMLIT_URL('{DATABASE}.PUBLIC.SIGNALFLOWAI_APP')")
        row = cur.fetchone()
        url = row[0] if row else "Could not retrieve URL"
        print(f"\n  🌐 LIVE URL: {url}\n")
        print("  Open this in your browser to use the deployed app.")
        print("  Share it with anyone who has a Snowflake login to your account.")

    finally:
        cur.close()
        conn.close()
        print("\n── Connection closed ───────────────────────────────────────────\n")


if __name__ == "__main__":
    main()
