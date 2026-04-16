use role TRAINING_ROLE;
use warehouse SIGNALFLOWAI_ETL_WH;
use database SIGNALFLOWAI_PROD_DB;
use schema RAG;

-- optional, only if RAG schema was not already created
create schema if not exists SIGNALFLOWAI_PROD_DB.RAG;

-- if create fails on Cortex privileges, run these with a role that can grant them:
-- grant database role SNOWFLAKE.CORTEX_EMBED_USER to role TRAINING_ROLE;
-- or
-- grant database role SNOWFLAKE.CORTEX_USER to role TRAINING_ROLE;

create or replace cortex search service REVIEW_DOCUMENTS_SEARCH
  on retrieval_text
  attributes
    doc_id,
    asin,
    category,
    brand,
    complaint_type,
    complaint_subtype,
    review_date,
    signal_score,
    title,
    summary
  warehouse = SIGNALFLOWAI_ETL_WH
  target_lag = '1 hour'
  embedding_model = 'snowflake-arctic-embed-l-v2.0'
as (
  select
    retrieval_text,
    doc_id,
    asin,
    category,
    brand,
    complaint_type,
    complaint_subtype,
    review_date,
    signal_score,
    title,
    summary
  from SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS
);