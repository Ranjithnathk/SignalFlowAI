"""
SignalFlowAI - DAG 3: Evaluation Benchmark (Weekly, Mondays)

Runs every Monday at 4 AM. Executes the Phase 9 evaluation pipeline:
  - Generates 10 benchmark Q&A pairs (OpenAI GPT-4o-mini)
  - Runs each question through the LangGraph 4-agent pipeline
  - Scores 4 RAGalyst metrics using Groq Llama-3.3-70b as independent judge:
      Retrieval Relevance, Answerability, Answer Correctness, Faithfulness
  - Writes results to src/evaluation/data/eval_details.csv

Model separation (by design):
  OpenAI  → Q&A generation + LangGraph pipeline (reasoning agents)
  Groq    → LLM-as-Judge only (independent, never touches the pipeline)

This separation ensures the judge is architecturally independent from the
system under evaluation - a core principle of the RAGalyst evaluation framework.

Connections required:
  None (API keys resolved from Airflow Variables or worker environment)

Environment variables:
  PROJECT_ROOT, PYTHON_BIN, OPENAI_API_KEY, GROQ_API_KEY,
  SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE,
  SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
  SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
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
    "retries": 0,              # evaluation failures should not auto-retry
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="signalflowai_eval",
    description="Weekly evaluation benchmark: RAGalyst 4-metric scoring via Groq LLM-as-Judge",
    schedule_interval="0 4 * * 1",   # 4 AM every Monday
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["signalflowai", "evaluation", "rag", "groq"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1: Generate Q&A benchmark (OpenAI GPT-4o-mini)
    # -----------------------------------------------------------------------
    generate_qa = BashOperator(
        task_id="generate_qa_benchmark",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_ROOT}/src/evaluation/qa_generator.py"
        ),
        doc_md=(
            "Generates 10 benchmark questions (1 per category × complaint_type combination) "
            "using OpenAI GPT-4o-mini. Writes to src/evaluation/data/eval_questions.json. "
            "Questions are grounded in the actual RAG.REVIEW_DOCUMENTS dataset."
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2: Run evaluation pipeline + score all 4 metrics
    # -----------------------------------------------------------------------
    run_evaluation = BashOperator(
        task_id="run_evaluation",
        bash_command=(
            f"{PYTHON_BIN} {PROJECT_ROOT}/src/evaluation/run_eval.py"
        ),
        doc_md=(
            "Runs each benchmark question through the LangGraph 4-agent pipeline, "
            "then scores 4 RAGalyst metrics using Groq Llama-3.3-70b as independent judge:\n"
            "  1. Retrieval Relevance  - continuous 0–1\n"
            "  2. Answerability        - binary 0/1\n"
            "  3. Answer Correctness   - continuous 0–1\n"
            "  4. Faithfulness         - continuous 0–1\n"
            "Results written to src/evaluation/data/eval_details.csv.\n"
            "Inter-metric sleeps (5s) and inter-question sleeps (10s) handle "
            "Groq free-tier rate limits."
        ),
    )

    # -----------------------------------------------------------------------
    # DAG flow
    # -----------------------------------------------------------------------
    generate_qa >> run_evaluation
