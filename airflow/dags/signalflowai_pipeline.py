"""
SignalFlowAI - Airflow Pipeline DAG  [SUPERSEDED]

This single monolithic DAG has been split into three focused DAGs.
This file is kept for reference only - it is NOT registered with Airflow.

New DAGs (use these instead):
  signalflowai_ingest.py  - DAG 1: UCSD → S3 → Parquet  (manual/on-demand)
  signalflowai_etl.py     - DAG 2: S3 → Snowflake → dbt → Cortex Search  (daily 3 AM)
  signalflowai_eval.py    - DAG 3: Evaluation benchmark  (weekly Mondays 4 AM)

Why split?
  - Ingest only needs to run when UCSD data is refreshed (very rare)
  - ETL should run daily to pick up any new data loaded to S3
  - Evaluation is a weekly quality check, independent of the data pipeline
  - Separate DAGs = independent retry, scheduling, and monitoring

See demo.md §Design Decisions for the full rationale.
"""

# This file is intentionally left without a DAG definition so Airflow does
# not register it. The three replacement files above contain all the logic.

from __future__ import annotations
