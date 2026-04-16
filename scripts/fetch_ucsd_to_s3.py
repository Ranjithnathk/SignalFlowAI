import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import requests
import yaml
from botocore.exceptions import ClientError
from dotenv import load_dotenv


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_env(var: str) -> str:
    val = os.getenv(var)
    if not val:
        raise RuntimeError(f"Missing required env var: {var}")
    return val


def url_filename(url: str) -> str:
    # e.g. .../Electronics.json.gz -> Electronics.json.gz
    p = urlparse(url).path
    return os.path.basename(p)


def build_s3_key(dataset: str, category: str, ingest_dt: str, filename: str) -> str:
    # Immutable raw replay layer (Hive-style partitions)
    return f"source/ucsd/{dataset}/category={category}/ingest_dt={ingest_dt}/{filename}"


def put_checkpoint(s3_client, bucket: str, key: str, payload: dict) -> None:
    body = json.dumps(payload, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def stream_download_and_upload(
    *,
    url: str,
    s3_client,
    bucket: str,
    key: str,
    max_retries: int,
    timeout: int,
    user_agent: str,
) -> dict:
    """
    Streams URL -> S3 upload_fileobj (no full file on disk).
    Computes MD5 while streaming.
    """
    session = requests.Session()
    headers = {"User-Agent": user_agent}

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with session.get(url, stream=True, timeout=timeout, headers=headers) as r:
                r.raise_for_status()

                content_length = r.headers.get("Content-Length")
                content_type = r.headers.get("Content-Type", "application/octet-stream")

                md5 = hashlib.md5()
                bytes_uploaded = 0

                class _Reader:
                    def __init__(self, raw):
                        self.raw = raw

                    def read(self, amt=1024 * 1024):
                        nonlocal bytes_uploaded
                        chunk = self.raw.read(amt)
                        if chunk:
                            md5.update(chunk)
                            bytes_uploaded += len(chunk)
                        return chunk

                wrapped = _Reader(r.raw)

                s3_client.upload_fileobj(
                    Fileobj=wrapped,
                    Bucket=bucket,
                    Key=key,
                    ExtraArgs={"ContentType": content_type},
                )

                return {
                    "content_length_header": int(content_length) if content_length else None,
                    "bytes_uploaded": bytes_uploaded,
                    "md5_hex": md5.hexdigest(),
                }

        except Exception as e:
            last_err = e
            sleep_s = min(2 ** attempt, 30)
            print(
                f"[WARN] Attempt {attempt}/{max_retries} failed for {url}: {e}\n"
                f"       Retrying in {sleep_s}s...",
                file=sys.stderr,
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed after {max_retries} attempts. Last error: {last_err}")


def resolve_ingest_dt(ingest_dt: str | None) -> str:
    if ingest_dt is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        datetime.strptime(ingest_dt, "%Y-%m-%d")
        return ingest_dt
    except ValueError:
        raise RuntimeError("--ingest-dt must be in YYYY-MM-DD format")


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Fetch UCSD Amazon v2 json.gz and store to S3 source layer.")
    parser.add_argument("--config", default="configs/ucsd_sources.yml", help="Path to UCSD sources YAML")
    parser.add_argument("--category", required=True, help="Category key from YAML (electronics/home_kitchen/dev/...)")

    # Updated:
    # - default dataset = both (so you can run just with --category dev)
    # - ingest-dt defaults to today's UTC date
    parser.add_argument(
        "--dataset",
        choices=["reviews", "meta", "both"],
        default="both",
        help="Which dataset to ingest (default: both)",
    )
    parser.add_argument(
        "--ingest-dt",
        default=None,
        help="Ingest date YYYY-MM-DD (default: today in UTC)",
    )

    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--user-agent", default="SignalFlowAI-DataEng/1.0")
    args = parser.parse_args()

    ingest_dt = resolve_ingest_dt(args.ingest_dt)

    bucket = ensure_env("S3_BUCKET")
    region = os.getenv("AWS_REGION", "us-east-2")

    cfg = load_yaml(args.config)
    categories = cfg.get("categories", {})
    if args.category not in categories:
        raise RuntimeError(f"Category '{args.category}' not found in {args.config}. Available: {list(categories.keys())}")

    s3_client = boto3.client("s3", region_name=region)

    datasets = ["reviews", "meta"] if args.dataset == "both" else [args.dataset]
    run_results = []

    print(f"[INFO] Region: {region}")
    print(f"[INFO] Bucket: {bucket}")
    print(f"[INFO] Category: {args.category}")
    print(f"[INFO] Ingest DT: {ingest_dt}")
    print(f"[INFO] Datasets: {datasets}")

    for ds in datasets:
        url_field = "reviews_url" if ds == "reviews" else "meta_url"
        url = categories[args.category].get(url_field)
        if not url:
            raise RuntimeError(f"Missing '{url_field}' for category '{args.category}' in {args.config}")

        filename = url_filename(url)
        s3_key = build_s3_key(ds, args.category, ingest_dt, filename)

        print("\n" + "-" * 80)
        print(f"[INFO] Dataset: {ds}")
        print(f"[INFO] Source URL: {url}")
        print(f"[INFO] Uploading to s3://{bucket}/{s3_key}")

        stats = stream_download_and_upload(
            url=url,
            s3_client=s3_client,
            bucket=bucket,
            key=s3_key,
            max_retries=args.max_retries,
            timeout=args.timeout,
            user_agent=args.user_agent,
        )

        print("[SUCCESS] Dataset ingestion complete.")
        print(f"          bytes_uploaded={stats['bytes_uploaded']} md5={stats['md5_hex']}")

        run_results.append(
            {
                "dataset": ds,
                "category": args.category,
                "ingest_dt": ingest_dt,
                "source_url": url,
                "s3_key": s3_key,
                "content_length_header": stats["content_length_header"],
                "bytes_uploaded": stats["bytes_uploaded"],
                "md5_hex": stats["md5_hex"],
            }
        )

    # One combined checkpoint for the whole run
    checkpoint_key = "checkpoints/last_successful_ingest.json"
    checkpoint_payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "s3_bucket": bucket,
        "region": region,
        "category": args.category,
        "ingest_dt": ingest_dt,
        "datasets": run_results,
    }

    try:
        put_checkpoint(s3_client, bucket, checkpoint_key, checkpoint_payload)
        print(f"\n[INFO] Wrote checkpoint: s3://{bucket}/{checkpoint_key}")
    except ClientError as e:
        print(f"\n[WARN] Uploaded files but failed to write checkpoint: {e}", file=sys.stderr)

    print("\n[DONE] All requested ingestions completed.")


if __name__ == "__main__":
    main()