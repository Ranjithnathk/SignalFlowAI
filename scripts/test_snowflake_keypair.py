import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

required = [
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
]

for key in required:
    print(f"{key} =", os.getenv(key))

conn = None
cur = None

try:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )

    cur = conn.cursor()
    cur.execute("select current_user(), current_role(), current_warehouse(), current_database(), current_schema()")
    print(cur.fetchone())
    print("✅ key-pair connection successful")

except Exception as e:
    print("❌ key-pair connection failed")
    print(type(e).__name__, str(e))

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()