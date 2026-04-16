import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

VERIFIER_AGENT_PROMPT = """
You are Verifier Agent for SignalFlowAI.

Your job:
1. Check whether the final answer is supported by the retrieved complaint evidence.
2. Assess whether the evidence is sufficient and relevant.
3. Give a confidence level.

Rules:
- Use only the provided query, evidence, and final answer.
- Do not invent facts.
- Keep the response concise.
- Return exactly in this format:

Verification:
- ...

Confidence:
- High / Medium / Low
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

def verifier_agent_node(state):
    query = state["user_query"]
    evidence = state.get("evidence", [])
    final_answer = state.get("final_answer", "")

    context = _build_context(evidence)

    user_prompt = f"""
User Query:
{query}

Retrieved Evidence:
{context}

Final Answer:
{final_answer}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[
            {"role": "system", "content": VERIFIER_AGENT_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    return {
        "verification": response.choices[0].message.content
    }