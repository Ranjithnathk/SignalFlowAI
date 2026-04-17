# SignalFlowAI

**Operational Decision Intelligence — powered by Snowflake Cortex Search + LangGraph Agents**

SignalFlowAI transforms 43 million Amazon customer reviews into structured, evidence-backed operational intelligence. It detects recurring product failures, delivery breakdowns, and quality issues at scale — and surfaces actionable decision briefs through a multi-agent reasoning pipeline.

> Live app: [signalflowai.streamlit.app](https://signalflowai.streamlit.app)

---

## What it does

Most systems tell you *how customers feel*. SignalFlowAI tells you *what is operationally failing and why*.

Given a natural language query like *"What defects are recurring in electronics products?"*, the system:

1. Interprets the business intent and builds structured retrieval filters
2. Retrieves the most semantically relevant complaint evidence from 828K indexed documents
3. Synthesizes a 5-section decision brief: Issue Summary, Recurring Pattern, Root Cause, Business Impact, Recommended Actions
4. Verifies the output against retrieved evidence and assigns a confidence level

Every output is grounded in real customer complaints — no hallucination, full traceability.

---

## Architecture

```
UCSD Amazon Dataset (43M reviews)
        │
        ▼
Python Scripts → AWS S3 (Parquet)
        │
        ▼  COPY INTO
┌─────────────────────────────────────────────────┐
│              SNOWFLAKE                          │
│                                                 │
│  RAW layer (43M reviews + 2M metadata)          │
│      ↓  dbt CLEAN                               │
│  Standardized text, complaint flags             │
│      ↓  dbt CURATED                             │
│  Signal scoring → 43M filtered to 828K          │
│      ↓  dbt RAG                                 │
│  RAG.REVIEW_DOCUMENTS (retrieval_text field)    │
│      ↓                                          │
│  Cortex Search (arctic-embed-l-v2.0)            │
│  828K documents indexed, sub-second retrieval   │
└───────────────────────────┬─────────────────────┘
                            │ top-K complaints
                            ▼
        ┌───────────────────────────────────────┐
        │       LangGraph StateGraph            │
        │                                       │
        │  Query Agent      (GPT-4o-mini)       │
        │       ↓                               │
        │  Retrieval Agent  (GPT-4o-mini)       │
        │       ↓                               │
        │  Reasoning Agent  (GPT-4o)            │
        │       ↓                               │
        │  Verifier Agent   (GPT-4o-mini)       │
        └───────────────────┬───────────────────┘
                            │
                            ▼
        Streamlit Decision Interface
        (Streamlit Community Cloud)
```

![Architecture Diagram](images/SignalFlow_Architecture_Diagram.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | UCSD Amazon Reviews Dataset (2023) |
| Data Lake | AWS S3 (Parquet, Hive-partitioned) |
| Data Warehouse | Snowflake |
| Transformation | dbt (3-layer: CLEAN → CURATED → RAG) |
| Semantic Search | Snowflake Cortex Search (`snowflake-arctic-embed-l-v2.0`) |
| Agent Orchestration | LangGraph StateGraph |
| Reasoning | OpenAI GPT-4o |
| Query / Verify | OpenAI GPT-4o-mini |
| Evaluation Judge | Groq Llama-3.3-70b |
| Pipeline Orchestration | Apache Airflow (3 DAGs) |
| Frontend | Streamlit (Streamlit Community Cloud) |
| Auth | Snowflake RSA key-pair authentication |

---

## Key Results

### Dataset
- **43 million** raw reviews ingested (Electronics + Home & Kitchen)
- **2 million** product metadata records
- **828,000** high-confidence operational complaints after signal scoring

### Signal Scoring System
Reviews earn a score from 0–4 based on four signals. Only reviews scoring ≥ 3 pass through:

| Signal | Condition | Points |
|---|---|---|
| Severity | Overall rating ≤ 2 stars | +1 |
| Verified | Verified purchase | +1 |
| Community | Helpfulness votes ≥ 1 | +1 |
| Detail | Review length ≥ category median | +1 |

### Evaluation (RAGalyst Framework, 10 questions)

| Metric | Score |
|---|---|
| Retrieval Relevance | 0.94 |
| Answerability | 1.00 |
| Answer Correctness | 0.863 |
| Faithfulness | 0.82 |

Judge model: Groq Llama-3.3-70b (architecturally independent from the pipeline it evaluates)

---

## Project Structure

```
SignalFlowAI/
├── src/
│   ├── agents/
│   │   ├── graph.py              # LangGraph StateGraph definition
│   │   ├── query_agent.py        # Interprets query, builds Cortex filters
│   │   ├── retrieval_agent.py    # Calls Cortex Search, assesses evidence
│   │   ├── reasoning_agent.py    # GPT-4o: 5-section decision intelligence
│   │   ├── verifier_agent.py     # Cross-checks output against evidence
│   │   └── state.py              # Shared state schema
│   ├── app/
│   │   └── app.py                # Streamlit UI (Decision Intelligence + Analytics)
│   ├── retrieval/
│   │   └── snowflake_retriever.py  # Cortex Search client
│   ├── reasoning/
│   │   └── llm_reasoner.py       # LLM reasoning layer
│   ├── evaluation/
│   │   ├── qa_generator.py       # GPT-4o-mini benchmark Q&A generation
│   │   ├── run_eval.py           # Evaluation runner
│   │   ├── evaluator.py          # RAGalyst metrics implementation
│   │   ├── view_results.py       # Results viewer
│   │   └── data/                 # eval_details.csv, eval_results.csv
│   └── pipeline/
│       ├── decision_pipeline.py  # Pipeline entry point
│       └── query_interpreter.py  # Query parsing utilities
│
├── signalflowai_dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── clean/                # clean_reviews.sql, clean_meta.sql
│       ├── curated/              # review_enriched_complaints.sql, product_health_daily.sql
│       └── rag/                  # review_documents.sql
│
├── airflow/
│   └── dags/
│       ├── signalflowai_ingest.py   # DAG 1: UCSD → S3 (manual)
│       ├── signalflowai_etl.py      # DAG 2: S3 → Snowflake → dbt → Cortex (daily 3AM)
│       └── signalflowai_eval.py     # DAG 3: Evaluation benchmark (weekly Mon 4AM)
│
├── scripts/
│   ├── fetch_ucsd_to_s3.py       # Stream UCSD data to S3
│   ├── transform_to_parquet.py   # JSON.gz → Parquet
│   └── generate_snowflake_key.py # RSA key pair generator
│
├── sql/
│   ├── 01_setup_database.sql     # Snowflake DB, warehouse, roles
│   ├── 02_storage_integration.sql
│   ├── 03_stage_and_copy.sql
│   ├── 04_raw_tables_and_copy.sql
│   ├── 05_prod_env_setup.sql
│   ├── 06_prod_raw_load.sql
│   ├── 07_verify_data.sql
│   ├── 12_cortex_search_setup.sql
│   └── 13_retrieval_validation.sql
│
├── configs/
│   ├── ucsd_sources.yml          # Dataset source URLs
│   ├── env.dev.yml
│   └── env.prod.yml
│
├── images/
│   └── SignalFlow_Architecture_Diagram.png
│
├── requirements.txt
└── .gitignore
```

---

## Setup

### Prerequisites
- Python 3.10+
- Snowflake account with a warehouse and database
- OpenAI API key
- Groq API key
- AWS S3 bucket (for data ingestion)

### 1. Clone the repository

```bash
git clone https://github.com/Ranjithnathk/SignalFlowAI.git
cd SignalFlowAI
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up environment variables

Create a `.env` file in the project root:

```env
# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=RAG
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_PRIVATE_KEY_PATH=snowflake_rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

# OpenAI
OPENAI_API_KEY=sk-...

# Groq (evaluation only)
GROQ_API_KEY=gsk_...
```

### 5. Generate Snowflake RSA key pair

```bash
python scripts/generate_snowflake_key.py
```

Register the public key in Snowflake:
```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='<contents of snowflake_rsa_key.pub>';
```

### 6. Set up Snowflake (run SQL files in order)

```bash
# In Snowflake worksheet, run:
sql/01_setup_database.sql
sql/02_storage_integration.sql
sql/03_stage_and_copy.sql
sql/04_raw_tables_and_copy.sql
sql/05_prod_env_setup.sql
sql/06_prod_raw_load.sql
sql/12_cortex_search_setup.sql
```

### 7. Run dbt transformations

```bash
cd signalflowai_dbt
dbt run --profiles-dir . --target prod
dbt test --profiles-dir . --target prod
```

---

## Running the App Locally

```bash
streamlit run src/app/app.py
```

Opens at `http://localhost:8501`

---

## Deployment (Streamlit Community Cloud)

1. Push the repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io) → New App
3. Set repository: `Ranjithnathk/SignalFlowAI`, branch: `main`, main file: `src/app/app.py`
4. Under **Advanced Settings → Secrets**, add:

```toml
SNOWFLAKE_USER = "your_user"
SNOWFLAKE_ACCOUNT = "your_account.region"
SNOWFLAKE_WAREHOUSE = "your_warehouse"
SNOWFLAKE_DATABASE = "your_database"
SNOWFLAKE_SCHEMA = "RAG"
SNOWFLAKE_ROLE = "your_role"
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = "your_passphrase"
OPENAI_API_KEY = "sk-..."
GROQ_API_KEY = "gsk_..."
SNOWFLAKE_PRIVATE_KEY_CONTENT = """
-----BEGIN ENCRYPTED PRIVATE KEY-----
(paste full contents of snowflake_rsa_key.p8)
-----END ENCRYPTED PRIVATE KEY-----
"""
```

5. Click **Deploy**

---

## Airflow Orchestration

Three focused DAGs manage the pipeline lifecycle:

| DAG | Schedule | Purpose |
|---|---|---|
| `signalflowai_ingest` | Manual | UCSD → S3 JSON.gz → Parquet |
| `signalflowai_etl` | Daily @ 3 AM | S3 → Snowflake RAW → dbt → Cortex Search refresh |
| `signalflowai_eval` | Weekly, Mondays @ 4 AM | Benchmark Q&A generation + RAGalyst scoring |

The ETL DAG uses a `ShortCircuitOperator` at Task 0 — it checks for new S3 data before running anything. No new data means the entire DAG is skipped, keeping compute costs at zero on quiet days.

---

## Running the Evaluation

```bash
# Generate benchmark questions
python src/evaluation/qa_generator.py

# Run evaluation (calls LangGraph pipeline + Groq judge)
python src/evaluation/run_eval.py

# View results
python src/evaluation/view_results.py
```

Results are written to `src/evaluation/data/eval_details.csv`.

---

## Sample Queries

| Query | Category | Complaint Type |
|---|---|---|
| What defects are recurring in electronics products? | Electronics | Damage / Defect |
| Which home kitchen brands have the most delivery failures? | Home & Kitchen | Delivery Issue |
| Are there systemic missing parts complaints in electronics? | Electronics | Missing Parts |

---

## Dataset

**UCSD Amazon Reviews 2023** — [McAuley Lab](https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_v2/)

- Categories used: Electronics, Home & Kitchen
- Raw reviews: ~43 million
- Product metadata: ~2 million records
- After signal filtering: 828,000 high-confidence operational complaints

---

## License

This project was developed as part of a graduate data engineering course. Dataset usage follows UCSD Amazon Reviews 2023 terms.
