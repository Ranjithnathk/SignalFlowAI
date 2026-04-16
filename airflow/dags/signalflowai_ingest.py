"""
SignalFlowAI - DAG 1: Ingest (UCSD → S3 → Parquet)

Runs on-demand / manually triggered - not on a schedule.
We already have the full dataset in S3. This DAG only needs to run
if the UCSD source is refreshed or new categories are added.

  Task 1: ingest_to_s3         → fetch_ucsd_to_s3.py  (UCSD API → S3 JSON.gz)
  Task 2: transform_to_parquet → transform_to_parquet.py (S3 JSON.gz → Parquet)

Duplication safety:
  Snowflake COPY INTO (DAG 2) tracks all previously loaded files in its internal
  metadata table, so re-uploading the same Parquet files to S3 will NOT cause
  duplicate rows in Snowflake - the COPY step simply skips files it has seen before.

Connections required:
  None (AWS credentials resolved from the Airflow worker's IAM role or env vars)

Environment variables:
  PROJECT_ROOT, PYTHON_BIN, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (or IAM role)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

DEFAULT_ARGS = {
    "owner": "signalflowai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="signalflowai_ingest",
    description="On-demand ingest: UCSD Amazon review data → S3 (JSON.gz) → Parquet",
    schedule_interval=None,          # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["signalflowai", "ingest", "s3"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Stream UCSD review data → S3 as JSON.gz
    # -----------------------------------------------------------------------
    ingest_to_s3 = BashOperator(
        task_id="ingest_to_s3",
        bash_command=f"{PYTHON_BIN} {PROJECT_ROOT}/scripts/fetch_ucsd_to_s3.py",
        doc_md=(
            "Streams raw UCSD Amazon review + metadata JSON.gz files into S3 "
            "under s3://signalflowai-s3/landing/raw/ with Hive-style partitioning "
            "(category=electronics/, category=home_kitchen/)."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2: Convert S3 JSON.gz → Parquet
    # -----------------------------------------------------------------------
    transform_to_parquet = BashOperator(
        task_id="transform_to_parquet",
        bash_command=f"{PYTHON_BIN} {PROJECT_ROOT}/scripts/transform_to_parquet.py",
        doc_md=(
            "Reads S3 JSON.gz files, applies column normalisation and type casting, "
            "and writes Parquet to s3://signalflowai-s3/landing/parquet/ "
            "for efficient Snowflake COPY INTO loading."
        ),
    )

    # -----------------------------------------------------------------------
    # DAG flow
    # -----------------------------------------------------------------------
    ingest_to_s3 >> transform_to_parquet
