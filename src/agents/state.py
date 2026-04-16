from typing import TypedDict, Optional, List, Dict, Any


class AgentState(TypedDict, total=False):
    user_query: str
    interpreted_intent: str
    filters: Optional[dict]
    top_k: int
    retrieval_notes: str
    evidence: List[Dict[str, Any]]
    evidence_count: int
    final_answer: str
    verification: str