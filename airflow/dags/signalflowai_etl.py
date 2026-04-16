"""
SignalFlowAI — DAG 2: ETL (S3 → Snowflake → dbt → Cortex Search)

Runs daily at 3 AM. Short-circuits if no new Parquet files are in S3 today,
so the pipeline is a no-op on days when no new data was ingested.

  Task 0: check_new_s3_data    → ShortCircuitOperator (skip if nothing new)
  Task 1: load_to_snowflake    → COPY INTO RAW.REVIEWS + RAW.METADATA
  Task 2: dbt_run              → dbt run (CLEAN → CURATED → RAG schemas)
  Task 3: dbt_test             → dbt test (data quality gates)
  Task 4: refresh_cortex_search → SUSPEND + RESUME Cortex Search index

Duplication safety:
  Snowflake COPY INTO tracks every file it has loaded in an internal metadata
  table. Re-running on the same S3 files is safe — they are silently skipped.

Connections required in Airflow UI:
  snowflake_default : Snowflake connection (account, user, keypair auth)

Environment variables:
  PROJECT_ROOT, PYTHON_BIN, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_PRIVATE_KEY_PATH,
  SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, SNOWFLAKE_ROLE,
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (or IAM role)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/signalflowai_dbt"
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "SIGNALFLOWAI_PROD_DB")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "SIGNALFLOWAI_WH")

DEFAULT_ARGS = {
    "owner": "signalflowai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Helper: check if today's S3 partition has new data
# ---------------------------------------------------------------------------
def check_new_s3_data(**context) -> bool:
    """
    Returns True if today's S3 Parquet partition exists (i.e. DAG 1 has run today).
    Short-circuits all downstream tasks if there is nothing new to load.

    In production this is the standard pattern — ingest DAG writes data,
    ETL DAG checks before doing expensive Snowflake + dbt work.
    """
    import boto3
    from datetime import date

    bucket = "signalflowai-s3"
    prefix = (
        f"landing/parquet/category=electronics/"
        f"ingest_dt={date.today().isoformat()}/"
    )

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    has_data = response.get("KeyCount", 0) > 0

    if not has_data:
        print(
            f"No new data at s3://{bucket}/{prefix} — "
            "short-circuiting ETL pipeline for today."
        )
    return has_data


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="signalflowai_etl",
    description="Daily ETL: S3 Parquet → Snowflake RAW → dbt (CLEAN/CURATED/RAG) → Cortex Search",
    schedule_interval="0 3 * * *",   # 3 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["signalflowai", "etl", "snowflake", "dbt"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 0: Short-circuit if no new S3 data today
    # -----------------------------------------------------------------------
    check_new_data = ShortCircuitOperator(
        task_id="check_new_s3_data",
        python_callable=check_new_s3_data,
        doc_md=(
            "Checks S3 for today's Parquet partition. "
            "If nothing new, skips the entire ETL run to avoid reprocessing."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 1: COPY INTO Snowflake RAW tables from S3 external stage
    # -----------------------------------------------------------------------
    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE WAREHOUSE {{ params.warehouse }};
            USE DATABASE {{ params.database }};

            COPY INTO RAW.REVIEWS
            FROM @RAW.SIGNALFLOWAI_S3_STAGE/landing/parquet/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE;

            COPY INTO RAW.METADATA
            FROM @RAW.SIGNALFLOWAI_S3_STAGE/landing/parquet/meta/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE;
        """,
        params={
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
        },
        doc_md=(
            "Loads new Parquet files from S3 external stage into RAW.REVIEWS and RAW.METADATA. "
            "COPY INTO is idempotent — previously loaded files are tracked and skipped automatically."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2: dbt run — CLEAN → CURATED → RAG
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROJECT_DIR} --target prod"
        ),
        doc_md=(
            "Runs the full dbt pipeline: "
            "CLEAN (normalise/deduplicate) → "
            "CURATED (signal scoring, complaint classification) → "
            "RAG (review_documents with embeddings for Cortex Search)."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 3: dbt test — data quality gates
    # -----------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROJECT_DIR} --target prod"
        ),
        doc_md=(
            "Runs dbt schema tests: not_null, unique, accepted_values on complaint_type. "
            "If tests fail, the DAG halts here — Cortex Search is never updated with bad data."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 4: Refresh Snowflake Cortex Search index
    # -----------------------------------------------------------------------
    refresh_cortex_search = SnowflakeOperator(
        task_id="refresh_cortex_search",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            USE DATABASE SIGNALFLOWAI_PROD_DB;
            USE SCHEMA RAG;
            ALTER CORTEX SEARCH SERVICE REVIEW_DOCUMENTS_SEARCH SUSPEND;
            ALTER CORTEX SEARCH SERVICE REVIEW_DOCUMENTS_SEARCH RESUME;
        """,
        doc_md=(
            "Suspends and resumes the Cortex Search service to force an index rebuild "
            "after new complaint documents are loaded into RAG.REVIEW_DOCUMENTS."
        ),
    )

    # -----------------------------------------------------------------------
    # DAG flow
    # -----------------------------------------------------------------------
    (
        check_new_data
        >> load_to_snowflake
        >> dbt_run
        >> dbt_test
        >> refresh_cortex_search
    )
