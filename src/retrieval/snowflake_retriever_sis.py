"""
SnowflakeRetriever — Snowflake SiS version.

Uses snowflake.snowpark.context.get_active_session() instead of
snowflake.connector.connect() — inside SiS no credentials are needed,
the session is already authenticated by Snowflake.

This file is uploaded to the SiS stage as:
  src/retrieval/snowflake_retriever.py
(overriding the local connector-based version for the SiS deployment only)
"""

import json
import os


class SnowflakeRetriever:
    def __init__(self):
        from snowflake.snowpark.context import get_active_session
        self.session = get_active_session()

    def retrieve(self, query: str, top_k: int = 5, filters: dict | None = None):
        payload = {
            "query": query,
            "columns": [
                "doc_id",
                "asin",
                "category",
                "brand",
                "complaint_type",
                "complaint_subtype",
                "title",
                "summary",
                "signal_score",
                "review_date",
            ],
            "limit": top_k,
        }

        if filters:
            payload["filter"] = filters

        # Escape single quotes for inline SQL embedding
        payload_json = json.dumps(payload).replace("'", "''")

        sql = f"""
        SELECT snowflake.cortex.search_preview(
            'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
            '{payload_json}'
        ) AS result
        """

        row = self.session.sql(sql).collect()
        result_str = row[0][0] if row else "{}"
        return json.loads(result_str).get("results", [])

    def close(self):
        # Snowpark session is managed by Snowflake — nothing to close
        pass
