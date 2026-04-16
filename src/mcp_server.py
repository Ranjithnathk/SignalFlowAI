"""
SignalFlowAI MCP Server

Exposes three tools via Model Context Protocol (MCP):
  - retrieve_complaints   : Semantic search against Snowflake Cortex Search
  - generate_decision     : Full 4-agent LangGraph pipeline
  - get_product_health    : Trend data from CURATED.PRODUCT_HEALTH_DAILY

Run:
    python src/mcp_server.py

Configure in Claude Desktop (~/.config/claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "signalflowai": {
          "command": "python",
          "args": ["<absolute-path>/src/mcp_server.py"]
        }
      }
    }
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Path setup — allow running from project root
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

# ---------------------------------------------------------------------------
# Lazy-loaded singletons (avoid heavy imports at module level)
# ---------------------------------------------------------------------------
_retriever = None
_graph = None


def _get_retriever():
    global _retriever
    if _retriever is None:
        from src.retrieval.snowflake_retriever import SnowflakeRetriever
        _retriever = SnowflakeRetriever()
    return _retriever


def _get_graph():
    global _graph
    if _graph is None:
        from src.agents.graph import build_agent_graph
        _graph = build_agent_graph()
    return _graph


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _build_filters(category: str, complaint_type: str) -> dict | None:
    """Build a valid Cortex Search filter dict from plain-text selections."""
    clauses = []
    if category and category.lower() not in ("any", ""):
        clauses.append({"@eq": {"category": category.lower()}})
    if complaint_type and complaint_type.lower() not in ("any", ""):
        clauses.append({"@eq": {"complaint_type": complaint_type.lower()}})
    if not clauses:
        return None
    return clauses[0] if len(clauses) == 1 else {"@and": clauses}


def _format_complaints(results: list[dict]) -> str:
    if not results:
        return "No complaints found matching the query and filters."
    lines = [f"Retrieved {len(results)} complaint(s):\n"]
    for i, r in enumerate(results, 1):
        lines += [
            f"Complaint {i}:",
            f"  Product  : {r.get('title') or 'Unknown'}",
            f"  Brand    : {r.get('brand', '')}",
            f"  Category : {r.get('category', '')}",
            f"  Type     : {r.get('complaint_type', '')}",
            f"  Subtype  : {r.get('complaint_subtype', '')}",
            f"  Signal   : {r.get('signal_score', '')}",
            f"  Date     : {r.get('review_date', '')}",
            f"  Summary  : {r.get('summary', '')}",
            "",
        ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
mcp = FastMCP("SignalFlowAI")


@mcp.tool()
def retrieve_complaints(
    query: str,
    category: str = "Any",
    complaint_type: str = "Any",
    top_k: int = 10,
) -> str:
    """
    Retrieve the top-K most relevant complaint records from SignalFlowAI's
    Snowflake Cortex Search index (828K high-signal operational complaints).

    Args:
        query:          Natural language question or issue description.
        category:       'electronics', 'home_kitchen', or 'Any'.
        complaint_type: 'damage_defect', 'missing_parts', 'delivery_issue',
                        'wrong_item', 'quality_issue', or 'Any'.
        top_k:          Number of complaints to return (1–20).
    """
    filters = _build_filters(category, complaint_type)
    top_k = max(1, min(top_k, 20))
    results = _get_retriever().retrieve(query=query, top_k=top_k, filters=filters)
    return _format_complaints(results)


@mcp.tool()
def generate_decision(
    query: str,
    category: str = "Any",
    complaint_type: str = "Any",
    top_k: int = 10,
) -> str:
    """
    Run the full SignalFlowAI 4-agent LangGraph pipeline and return structured
    decision intelligence grounded in real complaint evidence.

    Pipeline:
        Query Agent (gpt-4o-mini)
          → Retrieval Agent (Snowflake Cortex Search + gpt-4o-mini)
          → Reasoning Agent (gpt-4o)
          → Verifier Agent (gpt-4o-mini)

    Output sections:
        Issue Summary · Likely Recurring Pattern · Root Cause Hypothesis ·
        Business Impact · Recommended Actions · Confidence level

    Args:
        query:          Operational intelligence question to answer.
        category:       'electronics', 'home_kitchen', or 'Any'.
        complaint_type: 'damage_defect', 'missing_parts', 'delivery_issue',
                        'wrong_item', 'quality_issue', or 'Any'.
        top_k:          Complaints to retrieve for reasoning (1–20).
    """
    filters = _build_filters(category, complaint_type)
    top_k = max(1, min(top_k, 20))

    initial_state: dict = {"user_query": query, "top_k": top_k}
    if filters:
        initial_state["filters"] = filters

    result = _get_graph().invoke(initial_state)

    lines = [
        f"QUERY            : {query}",
        f"INTERPRETED INTENT: {result.get('interpreted_intent', '')}",
        f"EVIDENCE COUNT   : {result.get('evidence_count', 0)} complaints",
        f"RETRIEVAL NOTES  : {result.get('retrieval_notes', '')}",
        "",
        "=" * 60,
        "DECISION INTELLIGENCE",
        "=" * 60,
        result.get("final_answer", "No answer generated."),
        "",
        "=" * 60,
        "VERIFICATION",
        "=" * 60,
        result.get("verification", "No verification output."),
    ]
    return "\n".join(lines)


@mcp.tool()
def get_product_health(
    asin: str = "",
    brand: str = "",
    category: str = "",
    limit: int = 30,
) -> str:
    """
    Query product health trend data from SignalFlowAI's
    CURATED.PRODUCT_HEALTH_DAILY table.

    Returns daily complaint counts, negative/positive review rates, and
    average ratings — useful for spotting rising failure trends over time.

    Args:
        asin:     Product ASIN (e.g. 'B00G6QBCWC'). Optional.
        brand:    Brand name to filter by. Optional.
        category: 'electronics' or 'home_kitchen'. Optional.
        limit:    Max rows to return (default 30, max 100).
    """
    import snowflake.connector

    limit = max(1, min(limit, 100))

    conditions = []
    if asin:
        conditions.append(f"asin = '{asin.upper()}'")
    if brand:
        conditions.append(f"UPPER(brand) = UPPER('{brand}')")
    if category:
        conditions.append(f"category = '{category.lower()}'")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    sql = f"""
        SELECT
            review_date,
            asin,
            brand,
            category,
            complaint_type_candidate,
            review_count,
            negative_reviews,
            positive_reviews,
            ROUND(negative_rate, 4)  AS negative_rate,
            ROUND(positive_rate, 4)  AS positive_rate,
            ROUND(avg_rating, 2)     AS avg_rating
        FROM SIGNALFLOWAI_PROD_DB.CURATED.PRODUCT_HEALTH_DAILY
        {where_clause}
        ORDER BY review_date DESC
        LIMIT {limit}
    """

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="CURATED",
        role=os.getenv("SNOWFLAKE_ROLE"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        private_key_file_pwd=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )

    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0].lower() for d in cur.description]
        cur.close()
    finally:
        conn.close()

    if not rows:
        return "No product health data found for the given filters."

    lines = [f"Product Health Data — {len(rows)} record(s):\n"]
    header = f"{'Date':<12} {'ASIN':<12} {'Brand':<20} {'Type':<20} {'Reviews':>7} {'NegRate':>8} {'AvgRating':>10}"
    lines.append(header)
    lines.append("-" * len(header))

    for row in rows:
        r = dict(zip(cols, row))
        lines.append(
            f"{str(r.get('review_date','')):<12} "
            f"{str(r.get('asin','')):<12} "
            f"{str(r.get('brand',''))[:18]:<20} "
            f"{str(r.get('complaint_type_candidate',''))[:18]:<20} "
            f"{str(r.get('review_count',''))!s:>7} "
            f"{str(r.get('negative_rate',''))!s:>8} "
            f"{str(r.get('avg_rating',''))!s:>10}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
