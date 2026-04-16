use role TRAINING_ROLE;
use warehouse SIGNALFLOWAI_ETL_WH;
use database SIGNALFLOWAI_PROD_DB;
use schema RAG;

-- 1) plain semantic search
select snowflake.cortex.search_preview(
  'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
  '{
    "query": "memory card not recognized camera compatibility issue",
    "columns": ["doc_id", "asin", "category", "brand", "complaint_type", "complaint_subtype", "title", "summary"],
    "limit": 5
  }'
) as result;

-- 2) filtered search by category
select snowflake.cortex.search_preview(
  'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
  '{
    "query": "item arrived broken stopped working after few days",
    "columns": ["doc_id", "asin", "category", "brand", "complaint_type", "complaint_subtype", "title", "summary"],
    "filter": { "@eq": { "category": "electronics" } },
    "limit": 5
  }'
) as result;

-- 3) filtered search by complaint subtype
select snowflake.cortex.search_preview(
  'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
  '{
    "query": "package late never arrived delivery delay",
    "columns": ["doc_id", "asin", "category", "brand", "complaint_type", "complaint_subtype", "title", "summary"],
    "filter": { "@eq": { "complaint_subtype": "late_delivery" } },
    "limit": 5
  }'
) as result;

-- 4) hybrid-style validation: category + subtype + score threshold
select snowflake.cortex.search_preview(
  'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
  '{
    "query": "missing charger cable adapter not included",
    "columns": ["doc_id", "asin", "category", "brand", "complaint_type", "complaint_subtype", "title", "summary", "signal_score"],
    "filter": {
      "@and": [
        { "@eq": { "category": "electronics" } },
        { "@eq": { "complaint_subtype": "accessory_missing" } },
        { "@gte": { "signal_score": 3 } }
      ]
    },
    "limit": 5
  }'
) as result;

-- 5) date-window validation
select snowflake.cortex.search_preview(
  'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
  '{
    "query": "wrong item received different product shipped",
    "columns": ["doc_id", "asin", "category", "brand", "complaint_type", "complaint_subtype", "review_date", "title", "summary"],
    "filter": {
      "@and": [
        { "@eq": { "complaint_type": "wrong_item" } },
        { "@gte": { "review_date": "2017-01-01" } },
        { "@lte": { "review_date": "2018-12-31" } }
      ]
    },
    "limit": 5
  }'
) as result;