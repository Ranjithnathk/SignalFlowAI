import argparse
import gzip
import io
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import yaml
from dotenv import load_dotenv


# ----------------------------
# Helpers
# ----------------------------

def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_env(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise RuntimeError(f"Missing required env var: {var}")
    return val


def resolve_ingest_dt(ingest_dt: Optional[str]) -> str:
    if ingest_dt is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(ingest_dt, "%Y-%m-%d")
        return ingest_dt
    except ValueError:
        raise RuntimeError("--ingest-dt must be in YYYY-MM-DD format")


def source_prefix(dataset: str, category: str, ingest_dt: str) -> str:
    # Matches your ingestion script output
    return f"source/ucsd/{dataset}/category={category}/ingest_dt={ingest_dt}/"


def landing_prefix(dataset: str, category: str, ingest_dt: str) -> str:
    return f"landing/parquet/{dataset}/category={category}/ingest_dt={ingest_dt}/"


def list_s3_keys(s3_client, bucket: str, prefix: str) -> List[str]:
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if not k.endswith("/"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def stream_ucsd_json_gz_lines(s3_client, bucket: str, key: str) -> Iterable[dict]:
    """
    UCSD Amazon v2 files are line-delimited JSON inside a .json.gz.
    We stream from S3 -> gzip -> line iteration.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]  # botocore.response.StreamingBody
    gz = gzip.GzipFile(fileobj=body)

    for raw_line in gz:
        line = raw_line.decode("utf-8", errors="ignore").strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def arrow_table_from_rows(rows: List[Dict]) -> pa.Table:
    # Let Arrow infer schema from dicts; good enough for this pipeline stage
    df = pd.DataFrame(rows)
    return pa.Table.from_pandas(df, preserve_index=False)


def upload_parquet_to_s3(
    *,
    s3_client,
    bucket: str,
    key: str,
    table: pa.Table,
    compression: str = "snappy",
) -> None:
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression=compression)
    data = buf.getvalue().to_pybytes()

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
    )


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("$", "").replace(",", "")
    if s in {"", "None", "nan"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ----------------------------
# Reviews transform
# ----------------------------

def normalize_review(rec: dict, category: str, ingest_dt: str) -> Optional[Dict]:
    asin = rec.get("asin")
    review_text = rec.get("reviewText")

    if not asin or not review_text:
        return None

    review_text = str(review_text)
    review_len = len(review_text)

    # Filter: reviewText length > 30
    if review_len <= 30:
        return None

    overall = rec.get("overall")
    try:
        overall_num = float(overall) if overall is not None else None
    except Exception:
        overall_num = None

    out = {
        "asin": asin,
        "reviewerID": rec.get("reviewerID"),
        "reviewerName": rec.get("reviewerName"),
        "overall": overall_num,
        "summary": rec.get("summary"),
        "reviewText": review_text,
        "unixReviewTime": rec.get("unixReviewTime"),
        "reviewTime": rec.get("reviewTime"),
        "verified": rec.get("verified"),
        "vote": rec.get("vote"),
        # lineage
        "category": category,
        "ingest_dt": ingest_dt,
        # derived
        "review_length": review_len,
        "positive_flag": 1 if (overall_num is not None and overall_num >= 4.0) else 0,
        "neutral_flag": 1 if (overall_num is not None and overall_num == 3.0) else 0,
        "negative_flag": 1 if (overall_num is not None and overall_num <= 2.0) else 0,
    }
    return out


def transform_reviews(
    *,
    s3_client,
    bucket: str,
    category: str,
    ingest_dt: str,
    chunk_rows: int,
) -> Tuple[int, int]:
    """
    Returns (rows_written, unique_asins_count)
    """
    prefix = source_prefix("reviews", category, ingest_dt)
    keys = list_s3_keys(s3_client, bucket, prefix)
    if not keys:
        raise RuntimeError(f"No source review files found under s3://{bucket}/{prefix}")

    rows_written = 0
    asins: Set[str] = set()
    part_no = 0
    buffer_rows: List[Dict] = []

    out_prefix = landing_prefix("reviews", category, ingest_dt)

    for key in keys:
        print(f"[INFO] Reading reviews: s3://{bucket}/{key}")
        for rec in stream_ucsd_json_gz_lines(s3_client, bucket, key):
            norm = normalize_review(rec, category, ingest_dt)
            if norm is None:
                continue

            buffer_rows.append(norm)
            asins.add(norm["asin"])

            if len(buffer_rows) >= chunk_rows:
                table = arrow_table_from_rows(buffer_rows)
                out_key = f"{out_prefix}part-{part_no:05d}.parquet"
                upload_parquet_to_s3(s3_client=s3_client, bucket=bucket, key=out_key, table=table)
                rows_written += len(buffer_rows)
                print(f"[INFO] Wrote {len(buffer_rows)} rows -> s3://{bucket}/{out_key}")
                buffer_rows = []
                part_no += 1

    # flush remainder
    if buffer_rows:
        table = arrow_table_from_rows(buffer_rows)
        out_key = f"{out_prefix}part-{part_no:05d}.parquet"
        upload_parquet_to_s3(s3_client=s3_client, bucket=bucket, key=out_key, table=table)
        rows_written += len(buffer_rows)
        print(f"[INFO] Wrote {len(buffer_rows)} rows -> s3://{bucket}/{out_key}")

    return rows_written, len(asins)


def load_asins_from_reviews_parquet(bucket: str, category: str, ingest_dt: str) -> Set[str]:
    """
    Reads only the 'asin' column from already-created landing parquet for reviews.
    Uses s3fs so it works without requiring pyarrow S3 support.
    """
    fs = s3fs.S3FileSystem(anon=False)
    prefix = f"{bucket}/{landing_prefix('reviews', category, ingest_dt)}"
    # s3fs expects paths like "bucket/key"
    matches = fs.glob(prefix + "*.parquet")
    if not matches:
        raise RuntimeError(
            f"Meta transform requires reviews parquet first. None found under s3://{bucket}/{landing_prefix('reviews', category, ingest_dt)}"
        )

    asins: Set[str] = set()
    for path in matches:
        with fs.open(path, "rb") as f:
            tbl = pq.read_table(f, columns=["asin"])
            col = tbl.column("asin").to_pylist()
            for a in col:
                if a:
                    asins.add(str(a))

    return asins


# ----------------------------
# Meta transform
# ----------------------------

def normalize_meta(rec: dict, category: str, ingest_dt: str, allowed_asins: Set[str]) -> Optional[Dict]:
    asin = rec.get("asin")
    if not asin:
        return None
    asin = str(asin)
    if asin not in allowed_asins:
        return None

    out = {
        "asin": asin,
        "title": rec.get("title"),
        "brand": rec.get("brand"),
        "price": safe_float(rec.get("price")),
        "main_cat": rec.get("main_cat"),
        # category list can be large; keep it but as string for simplicity in Snowflake later
        "category_list": json.dumps(rec.get("category")) if isinstance(rec.get("category"), list) else rec.get("category"),
        # lineage
        "category": category,
        "ingest_dt": ingest_dt,
    }
    return out


def transform_meta(
    *,
    s3_client,
    bucket: str,
    category: str,
    ingest_dt: str,
    chunk_rows: int,
    allowed_asins: Set[str],
) -> int:
    prefix = source_prefix("meta", category, ingest_dt)
    keys = list_s3_keys(s3_client, bucket, prefix)
    if not keys:
        raise RuntimeError(f"No source meta files found under s3://{bucket}/{prefix}")

    rows_written = 0
    part_no = 0
    buffer_rows: List[Dict] = []

    out_prefix = landing_prefix("meta", category, ingest_dt)

    for key in keys:
        print(f"[INFO] Reading meta: s3://{bucket}/{key}")
        for rec in stream_ucsd_json_gz_lines(s3_client, bucket, key):
            norm = normalize_meta(rec, category, ingest_dt, allowed_asins)
            if norm is None:
                continue

            buffer_rows.append(norm)

            if len(buffer_rows) >= chunk_rows:
                table = arrow_table_from_rows(buffer_rows)
                out_key = f"{out_prefix}part-{part_no:05d}.parquet"
                upload_parquet_to_s3(s3_client=s3_client, bucket=bucket, key=out_key, table=table)
                rows_written += len(buffer_rows)
                print(f"[INFO] Wrote {len(buffer_rows)} rows -> s3://{bucket}/{out_key}")
                buffer_rows = []
                part_no += 1

    if buffer_rows:
        table = arrow_table_from_rows(buffer_rows)
        out_key = f"{out_prefix}part-{part_no:05d}.parquet"
        upload_parquet_to_s3(s3_client=s3_client, bucket=bucket, key=out_key, table=table)
        rows_written += len(buffer_rows)
        print(f"[INFO] Wrote {len(buffer_rows)} rows -> s3://{bucket}/{out_key}")

    return rows_written


# ----------------------------
# Main
# ----------------------------

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Transform UCSD JSON.gz (S3 source) -> Parquet (S3 landing).")
    parser.add_argument("--config", default="configs/ucsd_sources.yml", help="Path to UCSD sources YAML")
    parser.add_argument("--category", required=True, help="Category key from YAML (electronics/home_kitchen/dev/...)")
    parser.add_argument(
        "--dataset",
        choices=["reviews", "meta", "both"],
        default="both",
        help="Transform which dataset (default: both). Meta requires reviews parquet for ASIN filter.",
    )
    parser.add_argument("--ingest-dt", default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--chunk-rows", type=int, default=200_000, help="Rows per parquet part file")
    args = parser.parse_args()

    ingest_dt = resolve_ingest_dt(args.ingest_dt)
    bucket = ensure_env("S3_BUCKET")
    region = os.getenv("AWS_REGION", "us-east-2")

    cfg = load_yaml(args.config)
    categories = cfg.get("categories", {})
    if args.category not in categories:
        raise RuntimeError(f"Category '{args.category}' not found in {args.config}. Available: {list(categories.keys())}")

    s3_client = boto3.client("s3", region_name=region)

    print(f"[INFO] Bucket: {bucket} | Region: {region}")
    print(f"[INFO] Category: {args.category} | Ingest DT: {ingest_dt} | Dataset: {args.dataset}")
    print(f"[INFO] Chunk rows: {args.chunk_rows}")

    if args.dataset in {"reviews", "both"}:
        rows_written, uniq_asins = transform_reviews(
            s3_client=s3_client,
            bucket=bucket,
            category=args.category,
            ingest_dt=ingest_dt,
            chunk_rows=args.chunk_rows,
        )
        print(f"[DONE] Reviews parquet written: {rows_written} rows | unique_asins={uniq_asins}")

    if args.dataset in {"meta", "both"}:
        # Meta filter requires ASINs from filtered reviews parquet
        allowed_asins = load_asins_from_reviews_parquet(bucket=bucket, category=args.category, ingest_dt=ingest_dt)
        print(f"[INFO] Loaded allowed ASINs from reviews parquet: {len(allowed_asins)}")

        rows_written = transform_meta(
            s3_client=s3_client,
            bucket=bucket,
            category=args.category,
            ingest_dt=ingest_dt,
            chunk_rows=args.chunk_rows,
            allowed_asins=allowed_asins,
        )
        print(f"[DONE] Meta parquet written: {rows_written} rows")

    print("[SUCCESS] Transform complete.")


if __name__ == "__main__":
    main()