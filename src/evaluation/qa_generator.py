"""
QA Benchmark Generator for SignalFlowAI Phase 9 Evaluation.

Connects to Snowflake, samples complaints from each (category, complaint_type)
combination, and uses GPT-4o-mini to generate synthetic Q&A pairs.

Output: src/evaluation/data/qa_benchmark.csv

Run:
    python src/evaluation/qa_generator.py
"""

from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
QA_PER_COMBINATION = 1   # Q&A pairs generated per (category, complaint_type) - 10 total across all combinations
SAMPLE_SIZE = 15          # complaints sampled per combination to feed the LLM

CATEGORIES = ["electronics", "home_kitchen"]
COMPLAINT_TYPES = [
    "damage_defect",
    "missing_parts",
    "delivery_issue",
    "wrong_item",
    "quality_issue",
]

OUTPUT_DIR = ROOT / "src" / "evaluation" / "data"
OUTPUT_PATH = OUTPUT_DIR / "qa_benchmark.csv"


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------
def _get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )


def fetch_sample_complaints(
    conn, category: str, complaint_type: str, n: int = SAMPLE_SIZE
) -> list[dict]:
    sql = f"""
        SELECT
            doc_id, asin, brand, category,
            complaint_type, complaint_subtype,
            title, summary, signal_score
        FROM SIGNALFLOWAI_PROD_DB.RAG.REVIEW_DOCUMENTS
        WHERE category = '{category}'
          AND complaint_type = '{complaint_type}'
          AND summary IS NOT NULL
          AND title IS NOT NULL
        ORDER BY signal_score DESC
        LIMIT {n}
    """
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# QA generation via GPT-4o-mini
# ---------------------------------------------------------------------------
QA_GEN_PROMPT = """
You are building a benchmark evaluation dataset for an e-commerce complaint intelligence system.

You are given a set of real product complaints from the category: {category}, complaint type: {complaint_type}.

Your task: generate exactly {n} question-answer pairs that a business analyst would ask when investigating operational patterns across this category.

CRITICAL RULES for questions:
- Questions MUST be category-level or complaint-type-level, NOT about a single specific product
- Good: "What are the most common defect patterns in electronics products?"
- Good: "What delivery failures are recurring in home kitchen orders?"
- Bad:  "What is wrong with the Dropcam Wi-Fi camera?" (too product-specific)
- Questions should ask about patterns, trends, or recurring issues across multiple products/brands

CRITICAL RULES for answers:
- Answers must be grounded strictly in the complaints provided
- Each answer MUST follow exactly this format:
    Main issue: <concise description of the recurring pattern across products>
    Affected scope: <product group, brand cluster, or complaint subtype - NOT one specific product>
    Recommended action: <one of: investigate product quality and supplier manufacturing issues | audit logistics and delivery operations | review fulfillment and packaging processes | monitor issue trend and investigate further>
- Include doc_ids of 2-3 most representative complaints as evidence

Return valid JSON object with key "pairs" containing an array:
{{
  "pairs": [
    {{
      "question_id": "q_1",
      "question": "...",
      "ground_truth_answer": "Main issue: ...\\nAffected scope: ...\\nRecommended action: ...",
      "ground_truth_doc_ids": ["doc_id_1", "doc_id_2"],
      "category": "{category}",
      "complaint_type": "{complaint_type}"
    }}
  ]
}}
"""


def generate_qa_pairs(
    complaints: list[dict],
    category: str,
    complaint_type: str,
    n: int,
    id_offset: int,
    client: OpenAI,
) -> list[dict]:
    context = "\n\n".join(
        f"doc_id: {c['doc_id']}\n"
        f"title: {c.get('title','')}\n"
        f"brand: {c.get('brand','')}\n"
        f"complaint_type: {c.get('complaint_type','')}\n"
        f"subtype: {c.get('complaint_subtype','')}\n"
        f"summary: {c.get('summary','')}"
        for c in complaints
    )

    system_prompt = QA_GEN_PROMPT.format(
        category=category, complaint_type=complaint_type, n=n
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Complaints:\n\n{context}\n\nGenerate {n} Q&A pairs.",
            },
        ],
    )

    raw = response.choices[0].message.content.strip()
    data = json.loads(raw)

    # Prefer "pairs" key, fall back to first list value in the dict
    if isinstance(data, dict):
        pairs = data.get("pairs") or next(
            (v for v in data.values() if isinstance(v, list)), []
        )
    else:
        pairs = data

    # Re-index question IDs sequentially
    for i, pair in enumerate(pairs):
        pair["question_id"] = f"q_{id_offset + i + 1}"

    return pairs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    conn = _get_conn()

    all_pairs: list[dict] = []
    id_offset = 0

    combinations = [
        (cat, ct) for cat in CATEGORIES for ct in COMPLAINT_TYPES
    ]

    for category, complaint_type in combinations:
        print(f"  Generating QA for: {category} / {complaint_type} ...", end=" ", flush=True)
        complaints = fetch_sample_complaints(conn, category, complaint_type)

        if not complaints:
            print("no data, skipping.")
            continue

        try:
            pairs = generate_qa_pairs(
                complaints=complaints,
                category=category,
                complaint_type=complaint_type,
                n=QA_PER_COMBINATION,
                id_offset=id_offset,
                client=client,
            )
            all_pairs.extend(pairs)
            id_offset += len(pairs)
            print(f"{len(pairs)} pairs generated.")
        except Exception as e:
            print(f"FAILED: {e}")

    conn.close()

    if not all_pairs:
        print("No QA pairs generated. Exiting.")
        return

    fieldnames = [
        "question_id", "question", "ground_truth_answer",
        "ground_truth_doc_ids", "category", "complaint_type",
    ]

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for pair in all_pairs:
            if isinstance(pair.get("ground_truth_doc_ids"), list):
                pair["ground_truth_doc_ids"] = "|".join(pair["ground_truth_doc_ids"])
            writer.writerow(pair)

    print(f"\nSaved {len(all_pairs)} Q&A pairs to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
