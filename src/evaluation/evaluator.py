"""
SignalFlowAI Phase 9 - Evaluation Engine.

Judges are deliberately run on Groq (Llama-3.3-70b) to show differentiation
from the OpenAI-based reasoning pipeline.

Four metrics (per RAGalyst paper definitions):
  1. Retrieval Relevance  - continuous 0–1: are retrieved complaints relevant to the query?
  2. Answerability        - binary 0 or 1: does the evidence support answering the question?
  3. Answer Correctness   - continuous 0–1: does the pipeline output match ground truth?
  4. Faithfulness         - continuous 0–1: are the answer's claims grounded in retrieved evidence? (Groq judge)
"""

from __future__ import annotations

import json
import os
import re
import time

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
def _groq_client() -> OpenAI:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise ValueError("GROQ_API_KEY not set in .env")
    return OpenAI(api_key=api_key, base_url="https://api.groq.com/openai/v1")


GROQ_JUDGE_MODEL = "llama-3.3-70b-versatile"


def _with_retries(fn, retries: int = 5, delay: float = 15.0):
    """Retry with exponential backoff - handles Groq free-tier rate limits (429)."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            err = str(e).lower()
            is_rate_limit = "rate limit" in err or "429" in err or "too many" in err
            if attempt < retries - 1:
                wait = min(delay * (2 ** attempt), 120.0) if is_rate_limit else 2.0
                time.sleep(wait)
            else:
                raise e


def _extract_score(text: str) -> float:
    """Parse a JSON score from the judge's response."""
    text = text.strip()
    try:
        return float(json.loads(text).get("score", 0.0))
    except Exception:
        pass
    match = re.search(r'"score"\s*:\s*([0-9.]+)', text)
    if match:
        return float(match.group(1))
    return 0.0


# ---------------------------------------------------------------------------
# Metric 1: Retrieval Relevance
# Judge: Groq / Llama-3.3-70b
# ---------------------------------------------------------------------------
def judge_retrieval_relevance(
    query: str,
    evidence: list[dict],
) -> float:
    """
    Score 0-1: How relevant are the retrieved complaints to the query?
    1.0 = majority of complaints match the query's complaint type and category
    0.5 = partial match - complaint type correct but scope mixed
    0.0 = off-topic complaints retrieved
    """
    evidence_text = "\n".join(
        (
            f"  Complaint {i+1}:"
            f" category={r.get('category','')},"
            f" complaint_type={r.get('complaint_type','')},"
            f" subtype={r.get('complaint_subtype','')},"
            f" brand={r.get('brand','')},"
            f" signal_score={r.get('signal_score','')},"
            f" summary={r.get('summary','')}"
        )
        for i, r in enumerate(evidence[:10])
    )

    prompt = f"""You are evaluating retrieval quality for an operational complaint intelligence system.

The system classifies complaints into types (damage_defect, missing_parts, delivery_issue, wrong_item, quality_issue)
and categories (electronics, home_kitchen). The 'complaint_type' field is the system's classification - trust it.

Query: {query}

Retrieved complaints:
{evidence_text}

Score how well the retrieved complaints match the query intent on a continuous scale from 0.0 to 1.0.
Use the full range - do not limit yourself to 0.0, 0.5, or 1.0 only.

Examples:
- 0.9–1.0 = nearly all complaints match the complaint_type and category in the query
- 0.7–0.8 = most match, a couple are edge cases or different subtypes
- 0.4–0.6 = about half match, mixed complaint types
- 0.1–0.3 = few complaints match, mostly off-topic
- 0.0 = completely off-topic

Focus on complaint_type and category alignment - not on the summary wording.

Return only valid JSON: {{"score": 0.0}}"""

    client = _groq_client()
    response = _with_retries(
        lambda: client.chat.completions.create(
            model=GROQ_JUDGE_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a retrieval quality judge for a complaint intelligence system. Return only JSON."},
                {"role": "user", "content": prompt},
            ],
        )
    )
    return max(0.0, min(1.0, _extract_score(response.choices[0].message.content)))


# ---------------------------------------------------------------------------
# Metric 2: Answerability
# Judge: Groq / Llama-3.3-70b
# ---------------------------------------------------------------------------
def judge_answerability(
    query: str,
    evidence: list[dict],
    final_answer: str = "",
) -> float:
    """
    Binary 0 or 1: Can the retrieved evidence support an answer to the question?
    Per RAGalyst paper: Answerability is a binary judgment - 1 if the evidence is
    sufficient to answer, 0 if it is not. Measures evidence quality, not answer quality.
    """
    all_types = [r.get("complaint_type", "") for r in evidence]
    dominant_type = max(set(all_types), key=all_types.count) if all_types else ""
    match_count = sum(1 for t in all_types if t == dominant_type)

    # Compact evidence summary - complaint_type + subtype only, to keep prompt short
    evidence_summary = "; ".join(
        f"{r.get('complaint_type','')}({r.get('complaint_subtype','')})"
        for r in evidence[:10]
    )

    prompt = f"""You are a RAG evaluation judge. Answer with a binary score only.

Question: {query}

Retrieved evidence: {len(evidence)} complaints.
Dominant complaint_type: {dominant_type} ({match_count}/{len(evidence)} complaints match).
Complaint types present: {evidence_summary}

Can the retrieved evidence support a meaningful answer to the question above?
Answer 1 if YES - the evidence contains relevant complaints that match the question's topic.
Answer 0 if NO - the evidence is off-topic or too sparse to answer.

Return only valid JSON: {{"score": 1}} or {{"score": 0}}"""

    client = _groq_client()
    response = _with_retries(
        lambda: client.chat.completions.create(
            model=GROQ_JUDGE_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a binary answerability judge. Return only JSON with score 0 or 1."},
                {"role": "user", "content": prompt},
            ],
        )
    )
    raw_score = _extract_score(response.choices[0].message.content)
    return 1.0 if raw_score >= 0.5 else 0.0


# ---------------------------------------------------------------------------
# Metric 3: Answer Correctness
# Judge: Groq / Llama-3.3-70b
# ---------------------------------------------------------------------------
def _extract_pipeline_actions(final_answer: str) -> str:
    """
    Pull just the 'Recommended Actions' section from the pipeline's 5-section output.
    Falls back to the full answer if the section is not found.
    """
    match = re.search(
        r"Recommended Actions[:\s\-]*\n(.*?)(?=\n[A-Z][a-zA-Z ]+:|$)",
        final_answer,
        re.DOTALL | re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    # Also try to get Issue Summary as fallback for main issue comparison
    return final_answer.strip()


def _extract_pipeline_issue(final_answer: str) -> str:
    """Pull just the 'Issue Summary' section for main-issue comparison."""
    match = re.search(
        r"Issue Summary[:\s\-]*\n(.*?)(?=\n[A-Z][a-zA-Z ]+:|$)",
        final_answer,
        re.DOTALL | re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    return final_answer.strip()


def judge_answer_correctness(
    query: str,
    ground_truth: str,
    predicted_answer: str,
) -> float:
    """
    Score 0-1: Does the predicted decision intelligence match the ground truth?
    Compares Issue Summary (main issue) and Recommended Actions separately
    so the format mismatch between ground truth and pipeline output doesn't penalise the score.
    1.0 = same main issue AND semantically equivalent recommended action
    0.5 = same main issue but different action direction
    0.0 = different main issue
    """
    pipeline_issue = _extract_pipeline_issue(predicted_answer)
    pipeline_actions = _extract_pipeline_actions(predicted_answer)

    prompt = f"""You are evaluating answer correctness for a complaint intelligence system.

Question: {query}

Ground truth answer:
{ground_truth}

Predicted main issue (from pipeline Issue Summary):
{pipeline_issue}

Predicted recommended actions (from pipeline Recommended Actions):
{pipeline_actions}

Step 1 - Extract the action direction from the ground truth. It will be one of:
  A) investigate product quality and supplier manufacturing issues
  B) audit logistics and delivery operations
  C) review fulfillment and packaging processes
  D) monitor issue trend and investigate further

Step 2 - Check if the predicted Recommended Actions section points toward the same direction (A/B/C/D).

Step 3 - Check if the predicted main issue matches the ground truth main issue in meaning.

Step 4 - Score on a continuous 0.0–1.0 scale:
- 0.85–1.0: main issue clearly matches AND action direction is the same (A=A, B=B, etc.)
- 0.65–0.84: main issue matches but action direction is adjacent (e.g. quality vs fulfillment)
- 0.40–0.64: main issue partially matches (same complaint domain but different focus)
- 0.15–0.39: main issue only broadly overlaps
- 0.0–0.14: different main issue entirely

Do NOT anchor at 0.8. Reason through each step explicitly and arrive at a specific score.
Use decimals - scores like 0.72, 0.85, 0.55 are expected and correct.

Return only valid JSON: {{"score": 0.0}}"""

    client = _groq_client()
    response = _with_retries(
        lambda: client.chat.completions.create(
            model=GROQ_JUDGE_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a strict but fair answer correctness judge. Return only JSON."},
                {"role": "user", "content": prompt},
            ],
        )
    )
    return max(0.0, min(1.0, _extract_score(response.choices[0].message.content)))


# ---------------------------------------------------------------------------
# Metric 4: Faithfulness
# Judge: Groq / Llama-3.3-70b
# ---------------------------------------------------------------------------
def judge_faithfulness(
    pipeline_answer: str,
    evidence: list[dict],
) -> float:
    """
    Score 0.0–1.0: Is the pipeline answer grounded in the retrieved evidence?
    Per RAGalyst paper: measures hallucination - are claims in the answer supported
    by the evidence, or does the answer introduce facts not present in the context?
    1.0 = all claims supported | 0.0 = answer contradicts or ignores the evidence.
    """
    evidence_summaries = "\n".join(
        f"  [{i+1}] complaint_type={r.get('complaint_type','')} | "
        f"subtype={r.get('complaint_subtype','')} | "
        f"summary={str(r.get('summary',''))[:100]}"
        for i, r in enumerate(evidence[:10])
    )

    # Extract only the Issue Summary + Recommended Actions to keep prompt focused
    answer_excerpt = pipeline_answer.strip()[:800]

    prompt = f"""You are evaluating faithfulness for a RAG-based complaint intelligence system.

Definition: Faithfulness = are the claims in the generated answer supported by the retrieved evidence?
Score 1.0 if all key claims (issue type, pattern, root cause, recommended action) are grounded in the evidence.
Score lower if the answer introduces claims that are not mentioned or contradicted by the evidence.

Retrieved evidence ({len(evidence)} complaints):
{evidence_summaries}

Generated answer (excerpt):
{answer_excerpt}

Score on a continuous 0.0–1.0 scale:
- 0.9–1.0: every claim in the answer is directly supported by the retrieved evidence
- 0.6–0.8: most claims are supported, minor unsupported generalisations present
- 0.3–0.5: several claims go beyond or contradict the evidence
- 0.0–0.2: answer mostly fabricated or contradicts the retrieved evidence

Return only valid JSON: {{"score": 0.0}}"""

    client = _groq_client()
    response = _with_retries(
        lambda: client.chat.completions.create(
            model=GROQ_JUDGE_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a faithfulness judge for a RAG system. Return only JSON."},
                {"role": "user", "content": prompt},
            ],
        )
    )
    return max(0.0, min(1.0, _extract_score(response.choices[0].message.content)))


# ---------------------------------------------------------------------------
# Run all metrics for one pipeline result
# ---------------------------------------------------------------------------
def evaluate_result(
    query: str,
    ground_truth: str,
    pipeline_result: dict,
) -> dict:
    """
    Run all 4 metrics against one pipeline result dict (as returned by graph.invoke).
    Returns a flat dict of scores.
    """
    evidence = pipeline_result.get("evidence", [])
    final_answer = pipeline_result.get("final_answer", "")
    verification = pipeline_result.get("verification", "")

    retrieval_relevance = judge_retrieval_relevance(query, evidence)
    time.sleep(5.0)  # Groq rate limit buffer between metric calls

    answerability = judge_answerability(query, evidence)
    time.sleep(5.0)

    answer_correctness = judge_answer_correctness(query, ground_truth, final_answer)
    time.sleep(5.0)

    faithfulness = judge_faithfulness(final_answer, evidence)
    time.sleep(5.0)

    return {
        "retrieval_relevance": round(retrieval_relevance, 3),
        "answerability": round(answerability, 3),
        "answer_correctness": round(answer_correctness, 3),
        "faithfulness": round(faithfulness, 3),
        "evidence_count": len(evidence),
        "confidence_raw": _extract_confidence_label(verification),
    }


def _extract_confidence_label(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    if "high" in t:
        return "High"
    if "medium" in t:
        return "Medium"
    if "low" in t:
        return "Low"
    return ""
