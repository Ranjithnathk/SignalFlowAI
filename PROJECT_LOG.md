# SignalFlow AI
### E-commerce Operational Intelligence from Customer Signals

### Business Problem
- E-commerce platforms receive millions of customer complaints and low-rated reviews.
- Teams fix individual issues reactively, but recurring systemic failures go unnoticed.
- This leads to repeated refunds, damaged brand reputation, and increased churn.
- Not a reactive ticket handling, it is a proactive operational decision intelligence platform.

## Week 1 (01/31/2026)
**Problem Statement:** Today, supply chains face frequent disruptions due to weather, port congestion, supplier delays, and geopolitical issues. Even though this information exists across different systems and news sources, it is scattered and analyzed too late, which causes delivery delays, operational downtime, and revenue loss.

**Proposed Solution:** We planned to bring this data together into a single system and uses AI to detect early warning signals of potential disruptions. It explains why a risk is increasing and recommends actions, helping businesses plan ahead and reduce losses instead of reacting after problems occur.

**Feedback from Professor:** The problem statement is good and the solution is very narrow. Chances of having relevant data would be less.

--

## Week 2 (02/07/2026)
We realized that the core problem we want to solve is not predicting risks, but helping organizations learn from real operational signals and make better preventive decisions. So we reworked on our business problem and project focus.

Updated idea: **SignalFlow AI** is a decision-intelligence system that learns from customer-reported issues such as support tickets and complaints-to identify recurring operational problems and help teams fix root causes before issues repeat.
This project solves a real business problem faced by large platforms, repeated customer issues that are handled reactively but uses publicly available, text-rich datasets. It allows us to demonstrate data engineering, agentic reasoning, and RAG without relying on hard-to-access data sources.

--

## Week 3 (02/14/2026)
We discussed and organized the project flow,
- Data Engineering phase
- AI Layer phase
- Agentic Intelligence phase
- MCP Orchestration phase
- UI phase

**Dataset:** Initially we had an idea of taking real world data, which is Consumer Financial Protection Bureau (CFPB) Dataset from www.catalog.data.gov. But that data is particularly focusing on complaints raised on consumer financial products and services. Our focus was to resolve serious business problem in a top organization like Amazon, so we arrived at choosing Amazon Reviews 2018 dataset.
This dataset was widely used, but our goal was to shift from usual reactive ticket handling systems to build proactive operational decision intelligence.
Out of many categories, we limited our focus into **Electronic** and **Home & Kitchen** categories where there are 43Million reviews and 2Million products (metadata).

--

## Week 4 (02/21/2026)
We focused on Data Engineering phase.

#### Repository & Git Setup
- Created GitHub repository
- Added basic project skeleton structure
- Enabled collaborators for everyone in the team

#### UCSD to S3 Ingestion
- AWS S3 bucket setup, IAM (role, policy) setup
- Implemented data ingestion from UCSD dataset (web) to AWS S3 using python scripts
- Ensure Reviews and Metadata are stored in proper folder structure and dated format on AWS S3
- Transform to Parquet and stored into landing/parquet/

#### Snowflake setup
- Created Warehouse, Database, Schema setup 
- Created Storage Integration from S3
- Created RAW tables for Reviews and Metadata, COPY INTO, and Validation Queries
- Verified the counts of records
- Total 4 SQL files on Snowflake


--

## Week 5 (02/28/2026)
Completed the dbt transformation pipeline and set up the full Snowflake schema structure.

#### dbt Transformation — Completed
- Implemented **CLEAN layer**: `clean_reviews.sql` — standardized review text (HTML stripping, control character removal), parsed dates from two formats (string and unix timestamp), created 5 binary complaint type indicators (damage_defect, missing_parts, delivery_issue, quality_issue, wrong_item), added noise exclusion flag for seller/fake/shipping-cost reviews
- Implemented **CLEAN layer**: `clean_meta.sql` — parsed category JSON array into structured fields (root_category, sub_category, leaf_category, category_path, category_depth)
- Implemented **CURATED layer**: `review_enriched.sql` — joined clean reviews with clean product metadata on asin + category
- Implemented **CURATED layer**: `review_enriched_complaints.sql` — applied 4-signal scoring (severity, verified, community, detail), filtered to signal_score >= 3, added complaint_subtype classification (hard_defect, compatibility_fit_issue, not_received, late_delivery, accessory_missing, etc.)
- Implemented **CURATED layer**: `product_health_daily.sql` — daily aggregation of complaint counts by product for analytics dashboard
- dbt schema tests added: not_null, unique, accepted_values on complaint_type
- Result: **828k high-confidence complaints** from 43 million raw reviews (1.9%)

#### Snowflake Environment
- Production environment fully configured (sql/05_prod_env_setup.sql)
- Production raw load completed (sql/06_prod_raw_load.sql)
- Data verified with counts and sample queries (sql/07_verify_data.sql)

--

## Week 6 (03/07/2026)
Built the RAG document layer and set up Snowflake Cortex Search for semantic retrieval.

#### dbt RAG Layer
- Implemented **RAG layer**: `review_documents.sql` — final 828K complaint documents with all retrieval fields (doc_id, asin, title, brand, category, complaint_type, complaint_subtype, summary, signal_score, review_date)
- This is the table that Cortex Search indexes — one document per high-confidence complaint

#### Cortex Search Setup
- Created `REVIEW_DOCUMENTS_SEARCH` Cortex Search service using `snowflake-arctic-embed-l-v2.0` embedding model (sql/12_cortex_search_setup.sql)
- Configured `TARGET_LAG = '1 hour'` for automatic index refresh
- ATTRIBUTES set on complaint_type, category, complaint_subtype, brand for structured filtering
- 828,000 documents indexed — sub-second semantic retrieval confirmed
- Validated retrieval quality: tested semantic queries directly via `snowflake.cortex.search_preview()` (sql/13_retrieval_validation.sql)

#### Retrieval Layer
- Implemented `src/retrieval/snowflake_retriever.py` — Snowflake connector using RSA key-pair authentication
- `retrieve()` method: accepts query, top_k, and optional Cortex Search filter JSON (`@eq`/`@and` operators)
- Tested semantic retrieval vs keyword retrieval — semantic consistently returned more relevant complaints for ambiguous queries

--

## Week 7 (03/14/2026)
Built the multi-agent LangGraph pipeline replacing the original single-pass reasoning approach.

#### Legacy Pipeline (Baseline)
- Original `src/pipeline/decision_pipeline.py` — sequential: QueryInterpreter (keyword-based filters) → SnowflakeRetriever → LLMReasoner (GPT-4o-mini)
- Identified limitations: monolithic failure mode, no evidence quality assessment, no verification of output

#### LangGraph Multi-Agent Pipeline
- Defined shared state schema in `src/agents/state.py` (AgentState TypedDict with 9 fields)
- Implemented **Query Agent** (`src/agents/query_agent.py`) — GPT-4o-mini, temperature 0; parses user query into Cortex Search filter JSON; validates filter syntax; respects sidebar pre-set filters over LLM-generated ones
- Implemented **Retrieval Agent** (`src/agents/retrieval_agent.py`) — calls Cortex Search, fetches top-K complaints, asks GPT-4o-mini to assess evidence quality and produce retrieval notes
- Implemented **Reasoning Agent** (`src/agents/reasoning_agent.py`) — GPT-4o, temperature 0.2; synthesizes all evidence into 5-section Decision Brief (Issue Summary, Recurring Pattern, Root Cause, Business Impact, Recommended Actions)
- Implemented **Verifier Agent** (`src/agents/verifier_agent.py`) — GPT-4o-mini, temperature 0; cross-checks output claims against evidence; assigns High/Medium/Low confidence
- Assembled LangGraph StateGraph in `src/agents/graph.py`: START → query → retrieval → reasoning → verifier → END
- Tested pipeline end-to-end with multiple queries across both categories and all 5 complaint types

--

## Week 8 (03/21/2026)
Built and iterated on the Streamlit decision interface and analytics dashboard.

#### Streamlit App — Initial Build
- Created `src/app/app.py` with two tabs: Decision Intelligence and Product Health
- Decision Intelligence tab: query input, sidebar filters (category, complaint type, top-k slider), Run Analysis button calling LangGraph pipeline, results display with interpreted intent, evidence count, decision brief, verification badge
- Product Health tab: SQL-driven analytics against 828K Snowflake dataset — complaint type distribution chart, top brands by complaint volume, monthly complaint trend, top complained products table
- RSA key-pair loading for Streamlit Cloud: `_load_snowflake_private_key()` reads from file path (local) or environment variable (Streamlit Cloud)

#### Iterative UI Improvements
- Added active filter indicator in sidebar
- Added recent queries history in collapsible expander
- Set default top-K retrieval to 50
- Added brand/product search with combined AND filter logic and CSV download
- Fixed sidebar collapse button disappearing (caused by CSS hiding Streamlit header — removed that CSS)
- Moved app branding to sidebar as in-flow HTML banner
- Added colorful outlined tab styling (blue for Decision Intelligence, orange for Product Health)

--

## Week 9 (03/28/2026)
Implemented the RAGalyst evaluation framework and ran the full benchmark.

#### Evaluation Framework
- Implemented `src/evaluation/qa_generator.py` — generates 10 benchmark Q&A pairs using GPT-4o-mini; covers all 5 complaint types across both categories
- Implemented `src/evaluation/evaluator.py` — 4 metrics via Groq Llama-3.3-70b as independent judge:
  - **Retrieval Relevance** (0.0–1.0): complaint_type + category alignment of retrieved documents
  - **Answerability** (binary 0/1): evidence sufficiency to answer the question
  - **Answer Correctness** (0.0–1.0): pipeline output vs ground truth (Issue Summary + Recommended Actions comparison)
  - **Faithfulness** (0.0–1.0): claim grounding in retrieved evidence — hallucination detection
- Added exponential backoff retry for Groq rate limits (`_with_retries()`), 5-second sleep between metric calls
- Implemented `src/evaluation/run_eval.py` — runs all 10 questions through the pipeline, scores with evaluator, saves to CSV
- Implemented `src/evaluation/view_results.py` — prints aggregate summary from saved CSV

#### Evaluation Results (10/10 questions)
- Retrieval Relevance: **0.94**
- Answerability: **1.00**
- Answer Correctness: **0.863**
- Faithfulness: **0.82**
- Zero failed questions; all 10 had sufficient evidence

--

## Week 10 (04/04/2026)
Built Airflow orchestration, completed deployment, and fixed production issues.

#### Airflow Orchestration — 3 DAGs
- Original monolithic `signalflowai_pipeline.py` split into 3 focused DAGs:
  - **DAG 1** `signalflowai_ingest.py` — manual trigger; UCSD → S3 (fetch_ucsd_to_s3.py) → Parquet (transform_to_parquet.py)
  - **DAG 2** `signalflowai_etl.py` — daily at 3 AM; `ShortCircuitOperator` checks for new S3 data before running COPY INTO → dbt run → dbt test → Cortex Search refresh
  - **DAG 3** `signalflowai_eval.py` — weekly Monday 4 AM; runs benchmark evaluation and logs scores
- ShortCircuit pattern: ETL DAG checks today's S3 partition before spending any Snowflake compute — zero cost on days with no new data

#### Streamlit Cloud Deployment
- Pushed repository to GitHub (public, branch: main)
- Configured Streamlit Community Cloud: repository = Ranjithnathk/SignalFlowAI, main file = src/app/app.py
- Added all secrets in Streamlit Cloud Advanced Settings (SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_PRIVATE_KEY_CONTENT, OPENAI_API_KEY, GROQ_API_KEY, etc.)
- App live at: https://signalflowai.streamlit.app

#### Production Bug: Snowflake JWT Token Expiry (error 390114)
- Identified: Streamlit Cloud process stays alive for hours; Snowflake JWT tokens expire after ~60 minutes; stale connections cause 390114 error on next query
- Fix applied (3 layers):
  1. Proactive time-based refresh in `retrieval_agent.py` — recreates connection every 45 minutes before JWT expires
  2. `client_session_keep_alive=True` on all Snowflake connections — sends heartbeats to prevent idle session expiry
  3. `@st.cache_resource(ttl=1800)` on analytics connection in `app.py` — forces refresh every 30 minutes

--

## Week 11 (04/11/2026)
Final testing, presentation preparation, and documentation.

#### Testing & Bug Fixes
- Tested full pipeline end-to-end on Streamlit Cloud with multiple query types
- Verified analytics dashboard SQL queries: complaint trends, brand rankings, product search
- Confirmed evaluation scores reproducible on fresh re-run
- Fixed minor UI issues: column widths, table display formatting, download CSV functionality

#### Documentation
- Updated `README.md` with complete setup instructions, architecture diagram reference, evaluation results, and sample queries
- Updated `PROJECT_LOG.md`

#### Presentation Preparation
- Finalized PPT: Problem, Architecture, Data Ingestion, dbt Transformation, Complaint Indicators, Signal Scoring, Cortex Search, Multi-Agent System, Streamlit Interface, Data Flow, Evaluation (metrics + scores), Challenges, Lessons & Future Enhancements
- Verified Live app demo 

--

## Week 12 — Final Presentation (04/18/2026)

#### Presentation Overview
- **Demo**: Live app at https://signalflowai.streamlit.app

#### Project Final State
- Live app deployed and accessible: https://signalflowai.streamlit.app
- GitHub repository public: https://github.com/Ranjithnathk/SignalFlowAI
- All 4 evaluation metrics exceed 0.80 threshold
- Full pipeline operational: 43M raw reviews → 828K signal-scored complaints → Cortex Search → 4-agent LangGraph → Streamlit UI

--