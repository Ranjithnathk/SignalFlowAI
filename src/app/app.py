from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import html as html_lib
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Path setup - allow running from project root: streamlit run src/app/app.py
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from src.agents.graph import build_agent_graph  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COMPLAINT_TYPES = [
    "Any",
    "damage_defect",
    "missing_parts",
    "delivery_issue",
    "wrong_item",
    "quality_issue",
]

CATEGORIES = ["Any", "electronics", "home_kitchen"]

_LABEL_MAP = {
    "Any": "Any",
    "electronics": "Electronics",
    "home_kitchen": "Home & Kitchen",
    "damage_defect": "Damage Defect",
    "missing_parts": "Missing Parts",
    "delivery_issue": "Delivery Issue",
    "wrong_item": "Wrong Item",
    "quality_issue": "Quality Issue",
}

def _fmt(val: str) -> str:
    return _LABEL_MAP.get(val, val.replace("_", " ").title())

# Sample queries keyed by (category, complaint_type).
# Falls back to ("Any", "Any") when no specific entry exists.
_SAMPLE_QUERIES_MAP: dict[tuple[str, str], list[str]] = {
    ("Any", "Any"): [
        "What are the most common product failures across all categories?",
        "Which brands have the highest complaint rates?",
        "What recurring operational issues should we address first?",
    ],
    ("Any", "damage_defect"): [
        "Which products arrive damaged most frequently?",
        "What defect patterns are recurring across brands?",
        "Are there specific defects that indicate a manufacturing problem?",
    ],
    ("Any", "missing_parts"): [
        "Which products are most often missing parts on arrival?",
        "What missing parts complaints are recurring across brands?",
        "Are missing parts linked to specific suppliers or warehouses?",
    ],
    ("Any", "delivery_issue"): [
        "What delivery failures are happening across all products?",
        "Which carriers or routes have the most delivery complaints?",
        "Are delivery issues concentrated in specific time periods?",
    ],
    ("Any", "wrong_item"): [
        "How often are customers receiving wrong items?",
        "What wrong item patterns suggest a fulfillment mapping error?",
        "Which product lines have the most wrong item complaints?",
    ],
    ("Any", "quality_issue"): [
        "What quality problems are customers reporting most often?",
        "Which brands have systemic quality complaints?",
        "Are quality issues linked to specific product categories?",
    ],
    ("electronics", "Any"): [
        "What are the most common complaints about electronics products?",
        "Which electronics brands have the most recurring failures?",
        "What operational issues are affecting electronics sales?",
    ],
    ("electronics", "damage_defect"): [
        "What electronics products are arriving defective?",
        "Which electronics brands have recurring hard defect complaints?",
        "Are there specific electronic components failing repeatedly?",
    ],
    ("electronics", "missing_parts"): [
        "Which electronics products are missing accessories on arrival?",
        "What missing parts complaints exist for electronics?",
        "Are cables or adapters commonly missing in electronics orders?",
    ],
    ("electronics", "delivery_issue"): [
        "What delivery issues are happening with electronics products?",
        "Which electronics orders are not being received by customers?",
        "Are electronics delivery failures linked to specific carriers?",
    ],
    ("electronics", "wrong_item"): [
        "How often are customers receiving wrong electronics items?",
        "What wrong item patterns exist in electronics fulfillment?",
        "Which electronics SKUs have the most wrong item complaints?",
    ],
    ("electronics", "quality_issue"): [
        "What quality problems are customers reporting with electronics?",
        "Which electronics brands have poor build quality complaints?",
        "Are there compatibility issues recurring in electronics reviews?",
    ],
    ("home_kitchen", "Any"): [
        "What are the most common complaints in home and kitchen products?",
        "Which home kitchen brands have the most recurring failures?",
        "What operational issues are impacting home kitchen sales?",
    ],
    ("home_kitchen", "damage_defect"): [
        "Which home kitchen products arrive damaged most often?",
        "What defects are recurring in home kitchen appliances?",
        "Are there specific home kitchen items with hard defect patterns?",
    ],
    ("home_kitchen", "missing_parts"): [
        "Which home kitchen products are missing parts on arrival?",
        "What missing parts complaints exist for kitchen appliances?",
        "Are accessories or installation parts missing from home kitchen orders?",
    ],
    ("home_kitchen", "delivery_issue"): [
        "What delivery problems are affecting home kitchen orders?",
        "Which home kitchen products have the most delivery failures?",
        "Are home kitchen delivery issues linked to item size or weight?",
    ],
    ("home_kitchen", "wrong_item"): [
        "How often are customers receiving wrong home kitchen items?",
        "What wrong item patterns exist in home kitchen fulfillment?",
        "Which home kitchen SKUs are most often incorrectly shipped?",
    ],
    ("home_kitchen", "quality_issue"): [
        "What quality issues are customers reporting for home kitchen products?",
        "Which home kitchen brands have recurring build quality complaints?",
        "Are there material or durability complaints in home kitchen reviews?",
    ],
}


def get_sample_queries(category: str, complaint_type: str) -> list[str]:
    key = (category, complaint_type)
    if key in _SAMPLE_QUERIES_MAP:
        return _SAMPLE_QUERIES_MAP[key]
    # Partial matches
    if (category, "Any") in _SAMPLE_QUERIES_MAP:
        return _SAMPLE_QUERIES_MAP[(category, "Any")]
    if ("Any", complaint_type) in _SAMPLE_QUERIES_MAP:
        return _SAMPLE_QUERIES_MAP[("Any", complaint_type)]
    return _SAMPLE_QUERIES_MAP[("Any", "Any")]

SECTION_LABELS = [
    "Issue Summary",
    "Likely Recurring Pattern",
    "Root Cause Hypothesis",
    "Business Impact",
    "Recommended Actions",
]

SECTION_ICONS = {
    "Issue Summary": "📋",
    "Likely Recurring Pattern": "🔁",
    "Root Cause Hypothesis": "🔍",
    "Business Impact": "💼",
    "Recommended Actions": "✅",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_graph():
    return build_agent_graph()


def _parse_sections(text: str) -> dict[str, str]:
    """Split the reasoning agent's flat text output into named sections."""
    sections: dict[str, str] = {}
    pattern = "(" + "|".join(re.escape(s) for s in SECTION_LABELS) + "):"
    parts = re.split(pattern, text)

    i = 1
    while i < len(parts) - 1:
        label = parts[i].strip()
        body = parts[i + 1].strip() if i + 1 < len(parts) else ""
        sections[label] = body
        i += 2

    return sections


def _parse_verification(text: str) -> tuple[str, str]:
    """Return (verification_body, confidence_level) from verifier output."""
    if not text:
        return "", ""

    conf_match = re.search(
        r"Confidence[:\s\-]*\n?[•\-\*]?\s*(High|Medium|Low)",
        text,
        re.IGNORECASE,
    )
    confidence = conf_match.group(1).capitalize() if conf_match else ""

    ver_match = re.search(
        r"Verification[:\s\-]*\n(.*?)(?=Confidence[:\s\-]|$)",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    verification_body = ver_match.group(1).strip() if ver_match else text.strip()

    return verification_body, confidence


def _confidence_badge(level: str) -> str:
    colors = {"High": "#1e8e3e", "Medium": "#f9a825", "Low": "#c62828"}
    color = colors.get(level, "#555")
    return (
        f'<span style="background:{color};color:white;padding:3px 12px;'
        f'border-radius:12px;font-weight:600;font-size:0.85rem;">{level}</span>'
    )


def _build_filters(category: str, complaint_type: str) -> dict | None:
    """Construct a valid Cortex Search filter dict from sidebar selections."""
    clauses = []
    if category != "Any":
        clauses.append({"@eq": {"category": category}})
    if complaint_type != "Any":
        clauses.append({"@eq": {"complaint_type": complaint_type}})

    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"@and": clauses}




# ---------------------------------------------------------------------------
# Product Health Dashboard - Snowflake direct queries
# ---------------------------------------------------------------------------

def _load_snowflake_private_key() -> bytes:
    """
    Load the RSA private key as DER bytes.
    - Local dev: reads from the file at SNOWFLAKE_PRIVATE_KEY_PATH
    - Streamlit Cloud: reads PEM content from SNOWFLAKE_PRIVATE_KEY_CONTENT secret
    Both paths return DER bytes accepted by snowflake.connector's private_key param.
    """
    from cryptography.hazmat.primitives.serialization import (
        load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
    )
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
    passphrase_bytes = passphrase.encode() if passphrase else None

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    if key_path and os.path.exists(key_path):
        with open(key_path, "rb") as f:
            pem_data = f.read()
    else:
        pem_data = os.getenv("SNOWFLAKE_PRIVATE_KEY_CONTENT", "").encode()

    pk = load_pem_private_key(pem_data, password=passphrase_bytes)
    return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


@st.cache_resource
def _get_snowflake_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        private_key=_load_snowflake_private_key(),
    )


# ---------------------------------------------------------------------------
# Analytics Dashboard - constants and query helpers
# ---------------------------------------------------------------------------

# Only the 5 high-impact complaint types used in this project
_VALID_TYPES = ("damage_defect", "delivery_issue", "missing_parts", "quality_issue", "wrong_item")
_TYPES_IN = "'" + "', '".join(_VALID_TYPES) + "'"   # safe constant - no user input


def _run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    """Execute a SQL query against the cached Snowflake connection."""
    cur = _get_snowflake_conn().cursor()
    try:
        cur.execute(sql, params or [])
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


@st.cache_data(ttl=600)
def _query_overview(category: str) -> dict:
    """High-level summary from the 828K signal-scored RAG dataset."""
    params: list = []
    extra = ""
    if category and category != "Any":
        extra = "AND LOWER(category) = %s"
        params.append(category.lower())

    df = _run_query(f"""
        SELECT
            COUNT(*)               AS total_complaints,
            COUNT(DISTINCT brand)  AS total_brands,
            COUNT(DISTINCT asin)   AS total_products,
            COUNT(DISTINCT LOWER(category)) AS total_categories
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
        {extra}
    """, params)
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=600)
def _query_complaint_type_dist(category: str) -> pd.DataFrame:
    """Complaint count per type - only the 5 defined types, no 'other'."""
    params: list = []
    extra = ""
    if category and category != "Any":
        extra = "AND LOWER(category) = %s"
        params.append(category.lower())

    return _run_query(f"""
        SELECT complaint_type, COUNT(*) AS complaint_count
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
        {extra}
        GROUP BY complaint_type
        ORDER BY complaint_count DESC
    """, params)


@st.cache_data(ttl=600)
def _query_category_dist() -> pd.DataFrame:
    """Complaint count split by category."""
    return _run_query(f"""
        SELECT category, COUNT(*) AS complaint_count
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
        GROUP BY category
        ORDER BY complaint_count DESC
    """)


@st.cache_data(ttl=600)
def _query_top_brands(category: str, limit: int = 15) -> pd.DataFrame:
    """Top brands by complaint count, from the 828K dataset."""
    params: list = []
    extra = ""
    if category and category != "Any":
        extra = "AND LOWER(category) = %s"
        params.append(category.lower())

    return _run_query(f"""
        SELECT
            brand,
            category,
            COUNT(*)               AS complaint_count,
            COUNT(DISTINCT asin)   AS products_affected,
            ROUND(AVG(signal_score), 2) AS avg_signal_score
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
        {extra}
        GROUP BY brand, category
        ORDER BY complaint_count DESC
        LIMIT {int(limit)}
    """, params)


@st.cache_data(ttl=600)
def _query_subcategory_dist(category: str, complaint_type: str) -> pd.DataFrame:
    """Top complaint sub-types - excludes blank/other entries."""
    params: list = []
    extra_parts = []
    if category and category != "Any":
        extra_parts.append("AND LOWER(category) = %s")
        params.append(category.lower())
    if complaint_type and complaint_type != "Any":
        extra_parts.append("AND LOWER(complaint_type) = %s")
        params.append(complaint_type.lower())

    extra = " ".join(extra_parts)
    return _run_query(f"""
        SELECT complaint_subtype, COUNT(*) AS complaint_count
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
          AND complaint_subtype IS NOT NULL
          AND complaint_subtype NOT IN ('', 'other', 'unknown')
        {extra}
        GROUP BY complaint_subtype
        ORDER BY complaint_count DESC
        LIMIT 15
    """, params)


@st.cache_data(ttl=600)
def _query_top_products(category: str, complaint_type: str, limit: int = 10) -> pd.DataFrame:
    """Top products by complaint count (aggregated across complaint types)."""
    params: list = []
    extra_parts = []
    if category and category != "Any":
        extra_parts.append("AND LOWER(category) = %s")
        params.append(category.lower())
    if complaint_type and complaint_type != "Any":
        extra_parts.append("AND LOWER(complaint_type) = %s")
        params.append(complaint_type.lower())

    extra = " ".join(extra_parts)
    return _run_query(f"""
        SELECT
            COALESCE(NULLIF(TRIM(title), ''), 'Untitled Product') AS product,
            asin,
            brand,
            category,
            COUNT(*)                            AS complaint_count,
            COUNT(DISTINCT complaint_type)      AS complaint_types_count
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
          AND title IS NOT NULL
        {extra}
        GROUP BY title, asin, brand, category
        ORDER BY complaint_count DESC
        LIMIT {int(limit)}
    """, params)


@st.cache_data(ttl=600)
def _query_brand_products(category: str, brand_filter: str) -> pd.DataFrame:
    """Products and complaint breakdown for a specific brand."""
    params: list = [f"%{brand_filter.strip().lower()}%"]
    extra = ""
    if category and category != "Any":
        extra = "AND LOWER(category) = %s"
        params.append(category.lower())

    return _run_query(f"""
        SELECT
            COALESCE(NULLIF(TRIM(title), ''), 'Untitled Product') AS product,
            asin,
            complaint_type,
            COUNT(*) AS complaint_count
        FROM RAG.REVIEW_DOCUMENTS
        WHERE complaint_type IN ({_TYPES_IN})
          AND LOWER(brand) LIKE %s
        {extra}
        GROUP BY title, asin, complaint_type
        ORDER BY complaint_count DESC
        LIMIT 50
    """, params)


# ---------------------------------------------------------------------------
# Analytics Dashboard - render
# ---------------------------------------------------------------------------

def _render_product_health_tab(category: str, complaint_type: str) -> None:
    st.markdown("### Review Analytics Dashboard")
    st.caption(
        "Based on **828K signal-scored complaints** (signal score ≥ 3) across Electronics and "
        "Home & Kitchen - filtered from 43M raw reviews via the ETL pipeline. "
        "Covers the 5 high-impact complaint types only."
    )

    # -----------------------------------------------------------------------
    # Section 1: Overview metrics
    # -----------------------------------------------------------------------
    with st.spinner("Loading overview…"):
        try:
            ov = _query_overview(category)
        except Exception as e:
            st.error(f"Could not load overview: {e}")
            return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("High-Signal Complaints", f"{int(ov.get('total_complaints', 0)):,}")
    c2.metric("Brands Analyzed", f"{int(ov.get('total_brands', 0)):,}")
    c3.metric("Products Covered", f"{int(ov.get('total_products', 0)):,}")
    c4.metric("Categories", f"{int(ov.get('total_categories', 0))}")

    st.divider()

    # -----------------------------------------------------------------------
    # Section 2: Complaint type distribution + Category or Sub-category
    # -----------------------------------------------------------------------
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### Complaints by Type")
        try:
            ct_df = _query_complaint_type_dist(category)
            if not ct_df.empty:
                ct_df["complaint_type"] = ct_df["complaint_type"].map(_fmt)
                fig = px.pie(
                    ct_df,
                    names="complaint_type",
                    values="complaint_count",
                    color_discrete_sequence=px.colors.qualitative.Bold,
                    hole=0.35,
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Complaint volume across the 5 defined complaint types.")
        except Exception as e:
            st.error(str(e))

    with col_right:
        if category == "Any":
            st.markdown("#### Electronics vs Home Kitchen")
            try:
                cat_df = _query_category_dist()
                if not cat_df.empty:
                    cat_df["category"] = cat_df["category"].map(_fmt)
                    st.bar_chart(cat_df.set_index("category")[["complaint_count"]], use_container_width=True)
                    st.caption("Complaint volume split by category.")
            except Exception as e:
                st.error(str(e))
        else:
            lbl = _fmt(complaint_type) if complaint_type != "Any" else "all types"
            st.markdown(f"#### Sub-categories - {lbl}")
            try:
                sub_df = _query_subcategory_dist(category, complaint_type)
                if not sub_df.empty:
                    st.bar_chart(sub_df.set_index("complaint_subtype")[["complaint_count"]], use_container_width=True)
                    st.caption(f"Top complaint sub-types in {_fmt(category)}.")
                else:
                    st.info("No sub-category data for this selection.")
            except Exception as e:
                st.error(str(e))

    st.divider()

    # -----------------------------------------------------------------------
    # Section 3: Top brands table
    # -----------------------------------------------------------------------
    st.markdown("#### Top Brands by Complaint Volume")
    try:
        top_df = _query_top_brands(category, limit=15)
        if not top_df.empty:
            display = top_df.copy()
            display.columns = ["Brand", "Category", "Complaints", "Products Affected", "Avg Signal Score"]
            display["Category"] = display["Category"].map(_fmt)
            st.dataframe(display, use_container_width=True, hide_index=True)
        else:
            st.info("No brand data for this selection.")
    except Exception as e:
        st.error(str(e))

    st.divider()

    # -----------------------------------------------------------------------
    # Section 4: Top 10 most complained products
    # -----------------------------------------------------------------------
    lbl = ""
    if category != "Any":
        lbl += f" in {_fmt(category)}"
    if complaint_type != "Any":
        lbl += f" - {_fmt(complaint_type)}"

    st.markdown(f"#### Top 20 Most Complained Products{lbl}")
    try:
        prod_df = _query_top_products(category, complaint_type, limit=20)
        if not prod_df.empty:
            display_p = prod_df.copy()
            display_p.columns = ["Product", "Product ID", "Brand", "Category", "Complaints", "Complaint Types"]
            display_p["Product"] = display_p["Product"].apply(html_lib.unescape)
            display_p["Category"] = display_p["Category"].map(_fmt)
            st.dataframe(display_p, use_container_width=True, hide_index=True)
        else:
            st.info("No product data for this selection.")
    except Exception as e:
        st.error(str(e))

    st.divider()

    # -----------------------------------------------------------------------
    # Section 5: Brand deep-dive - products list
    # -----------------------------------------------------------------------
    st.markdown("#### Brand Deep-Dive - Products & Complaints")
    brand_filter = st.text_input(
        "Search brand (partial match)",
        placeholder="e.g. Sony, Samsung, Instant Pot",
        key="brand_filter",
    )

    if not brand_filter.strip():
        st.info("Enter a brand name to see its products and complaint breakdown.")
        return

    with st.spinner(f"Loading products for '{brand_filter}'…"):
        try:
            bp_df = _query_brand_products(category, brand_filter)
        except Exception as e:
            st.error(str(e))
            return

    if bp_df.empty:
        st.warning(f"No products found for brand matching '{brand_filter}'.")
        return

    # Complaint type summary bar chart for this brand
    brand_ct = (
        bp_df.groupby("complaint_type")["complaint_count"]
        .sum()
        .reset_index()
        .sort_values("complaint_count", ascending=False)
    )
    st.markdown(f"**Complaint type breakdown for '{brand_filter}'**")
    brand_ct["complaint_type"] = brand_ct["complaint_type"].map(_fmt)
    st.bar_chart(brand_ct.set_index("complaint_type")[["complaint_count"]], use_container_width=True)

    # Products table
    st.markdown(f"**Products under '{brand_filter}'** - top {len(bp_df)} by complaint count")
    display = bp_df.copy()
    display.columns = ["Product", "Product ID", "Complaint Type", "Complaint Count"]
    display["Product"] = display["Product"].apply(html_lib.unescape)
    display["Complaint Type"] = display["Complaint Type"].map(_fmt)
    st.dataframe(display, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Decision Intelligence tab rendering
# ---------------------------------------------------------------------------

def _render_decision_tab(selected_category: str, selected_complaint: str, top_k: int) -> None:
    # Sample query buttons - dynamic based on sidebar filters
    sample_queries = get_sample_queries(selected_category, selected_complaint)
    st.markdown("**Quick start - try a sample query:**")
    cols = st.columns(len(sample_queries))
    for col, sample in zip(cols, sample_queries):
        if col.button(sample, use_container_width=True):
            st.session_state["query_box"] = sample
            st.rerun()

    st.markdown("")

    # Query input
    query = st.text_input(
        "Enter your operational intelligence query",
        key="query_box",
        placeholder="e.g. What delivery issues are happening with electronics products?",
    )

    run_clicked = st.button("Run Analysis", type="primary", use_container_width=True)

    # Active filter indicator - always visible so user knows what's applied
    _filter_parts = []
    if selected_category != "Any":
        _filter_parts.append(f"Category: **{_fmt(selected_category)}**")
    if selected_complaint != "Any":
        _filter_parts.append(f"Type: **{_fmt(selected_complaint)}**")
    _filter_parts.append(f"Top **{top_k}** complaints")
    st.caption("Filters active - " + " | ".join(_filter_parts) if len(_filter_parts) > 1
               else "No category/type filters - searching across all data | Top **{}** complaints".format(top_k))

    if not run_clicked:
        return

    query = (query or "").strip()
    if not query:
        st.warning("Please enter a query.")
        return

    # Build any sidebar filters
    sidebar_filters = _build_filters(selected_category, selected_complaint)

    # Run the LangGraph pipeline
    graph = get_graph()

    initial_state = {"user_query": query, "top_k": top_k}
    if sidebar_filters:
        initial_state["filters"] = sidebar_filters

    with st.spinner("Running agent pipeline…"):
        try:
            result = graph.invoke(initial_state)
        except Exception as exc:
            st.error(f"Pipeline error: {exc}")
            return

    # -----------------------------------------------------------------------
    # Decision Intelligence Output
    # -----------------------------------------------------------------------
    final_answer = result.get("final_answer", "")
    sections = _parse_sections(final_answer)

    evidence_count = result.get("evidence_count", 0)
    st.caption(f"Analysis grounded in **{evidence_count} complaints** retrieved from Snowflake Cortex Search")
    st.markdown("## Decision Intelligence")

    if not sections:
        st.markdown(final_answer)
    else:
        # Top row: Issue Summary (full width)
        if "Issue Summary" in sections:
            with st.container(border=True):
                st.markdown(f"#### {SECTION_ICONS['Issue Summary']} Issue Summary")
                st.markdown(sections["Issue Summary"])

        st.markdown("")

        # Middle row: Pattern + Root Cause side by side
        col_a, col_b = st.columns(2)
        with col_a:
            if "Likely Recurring Pattern" in sections:
                with st.container(border=True):
                    st.markdown(f"#### {SECTION_ICONS['Likely Recurring Pattern']} Likely Recurring Pattern")
                    st.markdown(sections["Likely Recurring Pattern"])
        with col_b:
            if "Root Cause Hypothesis" in sections:
                with st.container(border=True):
                    st.markdown(f"#### {SECTION_ICONS['Root Cause Hypothesis']} Root Cause Hypothesis")
                    st.markdown(sections["Root Cause Hypothesis"])

        st.markdown("")

        # Bottom row: Business Impact + Recommended Actions side by side
        col_c, col_d = st.columns(2)
        with col_c:
            if "Business Impact" in sections:
                with st.container(border=True):
                    st.markdown(f"#### {SECTION_ICONS['Business Impact']} Business Impact")
                    st.markdown(sections["Business Impact"])
        with col_d:
            if "Recommended Actions" in sections:
                with st.container(border=True):
                    st.markdown(f"#### {SECTION_ICONS['Recommended Actions']} Recommended Actions")
                    st.markdown(sections["Recommended Actions"])

    # -----------------------------------------------------------------------
    # Verification
    # -----------------------------------------------------------------------
    verification_text = result.get("verification", "")
    ver_body, confidence = _parse_verification(verification_text)

    st.divider()
    st.markdown("### Verification")
    ver_col, conf_col = st.columns([4, 1])
    with ver_col:
        if ver_body:
            st.markdown(ver_body)
        else:
            st.caption("No verification output.")
    with conf_col:
        if confidence:
            st.markdown("**Confidence**")
            st.markdown(_confidence_badge(confidence), unsafe_allow_html=True)



# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="SignalFlowAI - Decision Intelligence",
        page_icon="⚡",
        layout="wide",
    )

    st.markdown("""
    <style>
    /* Metric cards — border accent only, works in light + dark */
    div[data-testid="metric-container"] {
        border: 1px solid #29B5E8;
        border-radius: 12px;
        padding: 14px 18px;
    }
    div[data-testid="metric-container"] label {
        color: #29B5E8 !important;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.07em;
        text-transform: uppercase;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.7rem;
        font-weight: 800;
    }

    /* Sidebar — accent border only */
    section[data-testid="stSidebar"] {
        border-right: 2px solid #29B5E8;
    }
    section[data-testid="stSidebar"] h2 {
        color: #29B5E8 !important;
        font-size: 0.85rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }

    /* Primary button */
    .stButton > button[kind="primary"] {
        background: linear-gradient(90deg, #0060A9 0%, #29B5E8 100%);
        border: none;
        border-radius: 8px;
        font-weight: 700;
        font-size: 0.95rem;
        letter-spacing: 0.04em;
        padding: 10px 0;
        color: white !important;
        transition: opacity 0.18s ease;
    }
    .stButton > button[kind="primary"]:hover { opacity: 0.85; }

    /* Sample query buttons */
    .stButton > button[kind="secondary"] {
        border: 1px solid #29B5E8;
        border-radius: 8px;
        color: #29B5E8 !important;
        background: transparent;
        font-size: 0.82rem;
        transition: background 0.15s ease;
    }
    .stButton > button[kind="secondary"]:hover {
        background: rgba(41,181,232,0.12);
    }

    /* Bordered containers — accent border, no forced background */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border-color: #29B5E8 !important;
        border-radius: 14px !important;
    }

    /* Tab bar */
    div[data-testid="stTabs"] button[data-baseweb="tab"] {
        font-weight: 600;
        font-size: 0.95rem;
    }
    div[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
        color: #29B5E8 !important;
        border-bottom-color: #29B5E8 !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.markdown(
        """
        <h1 style='margin-bottom:0'>⚡ SignalFlowAI</h1>
        <p style='color:#888;margin-top:4px;font-size:1.05rem;'>
        Operational Decision Intelligence - powered by Snowflake Cortex Search + LangGraph Agents
        </p>
        <hr style='margin-top:8px;margin-bottom:24px;border-color:#333'>
        """,
        unsafe_allow_html=True,
    )

    # Sidebar - shared across both tabs
    with st.sidebar:
        st.header("Filters (optional)")
        st.caption("Narrow retrieval to a specific category or complaint type.")
        selected_category = st.selectbox("Category", CATEGORIES, format_func=_fmt)
        selected_complaint = st.selectbox("Complaint Type", COMPLAINT_TYPES, format_func=_fmt)

        st.divider()
        st.header("Retrieval")
        top_k = st.slider(
            "Top complaints to retrieve",
            min_value=5,
            max_value=20,
            value=10,
            step=5,
            help="Cortex Search returns these many complaints ranked by semantic similarity to your query.",
        )

    # Tabs
    tab_decision, tab_health = st.tabs(["Decision Intelligence", "Product Health"])

    with tab_decision:
        _render_decision_tab(selected_category, selected_complaint, top_k)

    with tab_health:
        _render_product_health_tab(selected_category, selected_complaint)


if __name__ == "__main__":
    main()
