import os
import json
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def _load_snowflake_private_key() -> bytes:
    """
    Load the RSA private key as DER bytes.
    - Local dev: reads from file at SNOWFLAKE_PRIVATE_KEY_PATH
    - Streamlit Cloud: reads PEM content from SNOWFLAKE_PRIVATE_KEY_CONTENT secret
    """
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
    )
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    passphrase_bytes = passphrase.encode() if passphrase else None

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    if key_path and os.path.exists(key_path):
        with open(key_path, "rb") as f:
            pem_data = f.read()
    else:
        raw = os.getenv("SNOWFLAKE_PRIVATE_KEY_CONTENT", "")
        raw = raw.replace("\\n", "\n").strip()
        pem_data = raw.encode()

    pk = load_pem_private_key(pem_data, password=passphrase_bytes)
    return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


class SnowflakeRetriever:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            private_key=_load_snowflake_private_key(),
        )

    def _reconnect(self):
        """Re-establish the Snowflake connection (called when JWT token expires)."""
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            private_key=_load_snowflake_private_key(),
        )

    @staticmethod
    def _is_auth_error(e: Exception) -> bool:
        msg = str(e)
        return "390114" in msg or "token has expired" in msg.lower() or "must authenticate again" in msg.lower()

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

        payload_json = json.dumps(payload).replace("'", "''")

        sql = f"""
        select snowflake.cortex.search_preview(
            'SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS_SEARCH',
            '{payload_json}'
        ) as result;
        """

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            result = cur.fetchone()[0]
            return json.loads(result)["results"]
        except Exception as e:
            if self._is_auth_error(e):
                cur.close()
                self._reconnect()
                cur2 = self.conn.cursor()
                try:
                    cur2.execute(sql)
                    result = cur2.fetchone()[0]
                    return json.loads(result)["results"]
                finally:
                    cur2.close()
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def close(self):
        if self.conn:
            self.conn.close()