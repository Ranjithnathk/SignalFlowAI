import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

REASONING_AGENT_PROMPT = """
You are Reasoning Agent for SignalFlowAI.

Your job:
Turn retrieved complaint evidence into decision intelligence.

Rules:
- Use only the provided evidence.
- Be concise and business-focused.
- Mention how many complaints were analyzed.
- If evidence is weak or mixed, say so.
- Do not invent exact counts beyond the evidence window.

Return exactly in this format:

Issue Summary:
- ...

Likely Recurring Pattern:
- ...

Root Cause Hypothesis:
- ...

Business Impact:
- ...

Recommended Actions:
- ...
"""


def _build_context(evidence):
    parts = []
    for idx, item in enumerate(evidence, start=1):
        parts.append(
            f"""Complaint {idx}:
Product: {item.get('title', '')}
Brand: {item.get('brand', '')}
Category: {item.get('category', '')}
Complaint Type: {item.get('complaint_type', '')}
Complaint Subtype: {item.get('complaint_subtype', '')}
Summary: {item.get('summary', '')}
"""
        )
    return "\n".join(parts)


def reasoning_agent_node(state):
    query = state["user_query"]
    intent = state.get("interpreted_intent", "")
    retrieval_notes = state.get("retrieval_notes", "")
    evidence = state.get("evidence", [])
    evidence_count = state.get("evidence_count", 0)

    context = _build_context(evidence)

    user_prompt = f"""
User Query:
{query}

Interpreted Intent:
{intent}

Retrieval Notes:
{retrieval_notes}

Complaints Analyzed:
{evidence_count}

Evidence:
{context}
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.2,
        messages=[
            {"role": "system", "content": REASONING_AGENT_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    return {
        "final_answer": response.choices[0].message.content
    }