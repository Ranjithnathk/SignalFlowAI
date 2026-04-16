import os
import json
from dotenv import load_dotenv
from openai import OpenAI
from src.retrieval.snowflake_retriever import SnowflakeRetriever

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

_retriever = None

def _get_retriever():
    global _retriever
    if _retriever is None:
        _retriever = SnowflakeRetriever()
    return _retriever

RETRIEVAL_AGENT_PROMPT = """
You are Retrieval Agent for SignalFlowAI.

Your job:
1. Review retrieved complaint evidence.
2. Assess whether the evidence is relevant and coherent.
3. Produce a short retrieval note for downstream reasoning.

Rules:
- Focus on evidence quality, consistency, and issue concentration.
- Do not generate final recommendations.
- Keep it concise.
- Return JSON only.

Return this JSON object:
{
  "retrieval_notes": "..."
}
"""

def _normalize_filters(filters):
    if filters in [None, "null", "None", "", [], {}]:
        return None
    if isinstance(filters, dict):
        return filters
    return None

def retrieval_agent_node(state):
    query = state["user_query"]
    filters = _normalize_filters(state.get("filters"))
    top_k = state.get("top_k", 10)

    results = _get_retriever().retrieve(query=query, top_k=top_k, filters=filters)

    evidence_preview = []
    for r in results:
        evidence_preview.append({
            "title": r.get("title", ""),
            "category": r.get("category", ""),
            "complaint_type": r.get("complaint_type", ""),
            "complaint_subtype": r.get("complaint_subtype", ""),
            "summary": r.get("summary", ""),
        })

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": RETRIEVAL_AGENT_PROMPT},
            {
                "role": "user",
                "content": f"User query: {query}\n\nRetrieved evidence:\n{json.dumps(evidence_preview, indent=2)}"
            },
        ],
    )

    parsed = json.loads(response.choices[0].message.content)

    return {
        "evidence": results,
        "evidence_count": len(results),
        "retrieval_notes": parsed.get("retrieval_notes", ""),
    }