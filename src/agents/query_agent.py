import os
import json
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

QUERY_AGENT_PROMPT = """
You are Query Agent for SignalFlowAI.

Your job:
1. Understand the user query.
2. Infer the main operational issue type.
3. Build structured retrieval filters only when clearly needed.

Allowed complaint types:
- damage_defect
- missing_parts
- delivery_issue
- wrong_item
- quality_issue

Allowed categories:
- electronics
- home_kitchen

IMPORTANT FILTER FORMAT RULES:
- Filters must use Cortex Search operators only.
- Valid operators:
  - {"@eq": {"field": "value"}}
  - {"@and": [ ... ]}
- Never return filters like {"category": "electronics"}.
- If no filter is clearly needed, return null.

Examples:
1.
{
  "interpreted_intent": "delivery issues in electronics",
  "filters": {
    "@and": [
      {"@eq": {"category": "electronics"}},
      {"@eq": {"complaint_type": "delivery_issue"}}
    ]
  }
}

2.
{
  "interpreted_intent": "memory card failures in cameras",
  "filters": null
}

Return JSON only in this format:
{
  "interpreted_intent": "...",
  "filters": {... or null}
}
"""

def _is_valid_filter(obj):
    if obj is None:
        return True
    if not isinstance(obj, dict):
        return False

    valid_ops = {"@eq", "@and", "@gte", "@lte"}

    for key, value in obj.items():
        if key not in valid_ops:
            return False

        if key == "@eq":
            return isinstance(value, dict) and len(value) == 1

        if key in {"@gte", "@lte"}:
            return isinstance(value, dict) and len(value) == 1

        if key == "@and":
            return isinstance(value, list) and all(_is_valid_filter(v) for v in value)

    return False


def _normalize_filters(filters):
    if filters in [None, "null", "None", "", [], {}]:
        return None
    if _is_valid_filter(filters):
        return filters
    return None

def query_agent_node(state):
    query = state["user_query"]

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": QUERY_AGENT_PROMPT},
            {"role": "user", "content": f"User query: {query}"}
        ],
    )

    parsed = json.loads(response.choices[0].message.content)

    # If the caller pre-set filters (e.g. from sidebar), honour them.
    # Only use LLM-generated filters when none were provided.
    existing_filters = state.get("filters")
    resolved_filters = (
        existing_filters if existing_filters
        else _normalize_filters(parsed.get("filters"))
    )

    return {
        "interpreted_intent": parsed.get("interpreted_intent", ""),
        "filters": resolved_filters,
    }