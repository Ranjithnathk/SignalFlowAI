"""
SignalFlowAI Phase 9 — Evaluation Runner.

Loads the QA benchmark, runs every question through the full 4-agent pipeline,
scores each result using Groq/Llama-3.3-70b as judge, and saves results.

Run:
    python src/evaluation/run_eval.py

Output:
    src/evaluation/data/eval_results.csv  — per-question scores only
    src/evaluation/data/eval_details.csv  — ground truth vs pipeline answer (human-readable)
    src/evaluation/data/eval_summary.csv  — aggregate metrics
"""

from __future__ import annotations

import ast
import csv
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.agents.graph import build_agent_graph
from src.evaluation.evaluator import evaluate_result

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = ROOT / "src" / "evaluation" / "data"
QA_PATH = DATA_DIR / "qa_benchmark.csv"
RESULTS_PATH = DATA_DIR / "eval_results.csv"
DETAILS_PATH = DATA_DIR / "eval_details.csv"
SUMMARY_PATH = DATA_DIR / "eval_summary.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_qa_benchmark() -> list[dict]:
    if not QA_PATH.exists():
        raise FileNotFoundError(
            f"QA benchmark not found at {QA_PATH}.\n"
            "Run: python src/evaluation/qa_generator.py first."
        )
    with open(QA_PATH, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_filters(category: str, complaint_type: str) -> dict | None:
    clauses = []
    if category and category.lower() not in ("any", ""):
        clauses.append({"@eq": {"category": category.lower()}})
    if complaint_type and complaint_type.lower() not in ("any", ""):
        clauses.append({"@eq": {"complaint_type": complaint_type.lower()}})
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"@and": clauses}


def run_pipeline(graph, question: str, category: str, complaint_type: str) -> dict:
    filters = build_filters(category, complaint_type)
    state: dict = {"user_query": question, "top_k": 10}
    if filters:
        state["filters"] = filters
    return graph.invoke(state)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading QA benchmark...")
    qa_pairs = load_qa_benchmark()
    print(f"  {len(qa_pairs)} questions loaded.\n")

    print("Building agent graph...")
    graph = build_agent_graph()
    print("  Graph ready.\n")

    results = []

    for i, row in enumerate(qa_pairs, 1):
        question_id = row.get("question_id", f"q_{i}")
        question = row["question"]
        ground_truth = row["ground_truth_answer"]
        category = row.get("category", "Any")
        complaint_type = row.get("complaint_type", "Any")

        print(f"[{i}/{len(qa_pairs)}] {question_id}: {question[:70]}...")

        try:
            pipeline_result = run_pipeline(graph, question, category, complaint_type)
        except Exception as e:
            print(f"  Pipeline failed: {e}")
            results.append({
                "question_id": question_id,
                "question": question,
                "category": category,
                "complaint_type": complaint_type,
                "retrieval_relevance": None,
                "answerability": None,
                "answer_correctness": None,
                "faithfulness": None,
                "evidence_count": 0,
                "confidence_raw": "",
                "error": str(e),
            })
            continue

        try:
            scores = evaluate_result(
                query=question,
                ground_truth=ground_truth,
                pipeline_result=pipeline_result,
            )
        except Exception as e:
            print(f"  Evaluation failed: {e}")
            scores = {
                "retrieval_relevance": None,
                "answerability": None,
                "answer_correctness": None,
                "faithfulness": None,
                "evidence_count": pipeline_result.get("evidence_count", 0),
                "confidence_raw": "",
            }

        final_answer = pipeline_result.get("final_answer", "")
        verification = pipeline_result.get("verification", "")
        interpreted_intent = pipeline_result.get("interpreted_intent", "")
        retrieval_notes = pipeline_result.get("retrieval_notes", "")

        result_row = {
            "question_id": question_id,
            "question": question,
            "category": category,
            "complaint_type": complaint_type,
            **scores,
            "error": "",
            # store answers for details file
            "_ground_truth": ground_truth,
            "_pipeline_answer": final_answer,
            "_verification": verification,
            "_interpreted_intent": interpreted_intent,
            "_retrieval_notes": retrieval_notes,
        }
        results.append(result_row)

        print(
            f"  Relevance={scores.get('retrieval_relevance')} | "
            f"Answerability={scores.get('answerability')} | "
            f"Correctness={scores.get('answer_correctness')} | "
            f"Faithfulness={scores.get('faithfulness')} | "
            f"Confidence={scores.get('confidence_raw')}"
        )

        time.sleep(10.0)  # Proactive buffer between questions — avoids Groq free-tier rate limit

    # -----------------------------------------------------------------------
    # Save per-question scores (clean, no answer text)
    # -----------------------------------------------------------------------
    score_fieldnames = [
        "question_id", "question", "category", "complaint_type",
        "retrieval_relevance", "answerability", "answer_correctness",
        "faithfulness", "evidence_count", "confidence_raw", "error",
    ]
    with open(RESULTS_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=score_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(f"\nScores saved to {RESULTS_PATH}")

    # -----------------------------------------------------------------------
    # Save human-readable details: ground truth vs pipeline answer
    # -----------------------------------------------------------------------
    detail_fieldnames = [
        "question_id", "question", "category", "complaint_type",
        "retrieval_relevance", "answerability", "answer_correctness", "faithfulness",
        "confidence_raw",
        "ground_truth_answer",
        "pipeline_interpreted_intent",
        "pipeline_retrieval_notes",
        "pipeline_answer",
        "pipeline_verification",
    ]
    detail_rows = []
    for r in results:
        detail_rows.append({
            "question_id": r["question_id"],
            "question": r["question"],
            "category": r["category"],
            "complaint_type": r["complaint_type"],
            "retrieval_relevance": r.get("retrieval_relevance", ""),
            "answerability": r.get("answerability", ""),
            "answer_correctness": r.get("answer_correctness", ""),
            "faithfulness": r.get("faithfulness", ""),
            "confidence_raw": r.get("confidence_raw", ""),
            "ground_truth_answer": r.get("_ground_truth", ""),
            "pipeline_interpreted_intent": r.get("_interpreted_intent", ""),
            "pipeline_retrieval_notes": r.get("_retrieval_notes", ""),
            "pipeline_answer": r.get("_pipeline_answer", ""),
            "pipeline_verification": r.get("_verification", ""),
        })

    with open(DETAILS_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=detail_fieldnames)
        writer.writeheader()
        writer.writerows(detail_rows)

    print(f"Details saved to {DETAILS_PATH}  ← open this to compare ground truth vs pipeline answers")

    # -----------------------------------------------------------------------
    # Compute and save aggregate summary
    # -----------------------------------------------------------------------
    valid = [r for r in results if r.get("retrieval_relevance") is not None]

    def avg(key):
        vals = [float(r[key]) for r in valid if r.get(key) is not None]
        return round(sum(vals) / len(vals), 3) if vals else None

    summary = {
        "total_questions": len(qa_pairs),
        "evaluated": len(valid),
        "failed": len(qa_pairs) - len(valid),
        "avg_retrieval_relevance": avg("retrieval_relevance"),
        "avg_answerability": avg("answerability"),
        "avg_answer_correctness": avg("answer_correctness"),
        "avg_faithfulness": avg("faithfulness"),
        "judge_model": "llama-3.3-70b-versatile (Groq)",
        "pipeline_model": "GPT-4o (reasoning) + GPT-4o-mini (query/retrieval/verifier)",
        "retrieval_backend": "Snowflake Cortex Search (snowflake-arctic-embed-l-v2.0)",
    }

    with open(SUMMARY_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)

    print(f"Summary saved to {SUMMARY_PATH}")
    print("\n=== EVALUATION SUMMARY ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
