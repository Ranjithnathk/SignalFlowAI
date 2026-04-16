select count(*) from SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS;

select *
from SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS
limit 5;

select
    count_if(retrieval_text is null) as null_retrieval_text,
    count_if(trim(retrieval_text) = '') as blank_retrieval_text
from SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS;