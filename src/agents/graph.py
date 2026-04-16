from langgraph.graph import StateGraph, START, END
from src.agents.state import AgentState
from src.agents.query_agent import query_agent_node
from src.agents.retrieval_agent import retrieval_agent_node
from src.agents.reasoning_agent import reasoning_agent_node
from src.agents.verifier_agent import verifier_agent_node

def build_agent_graph():
    graph = StateGraph(AgentState)

    graph.add_node("query_agent", query_agent_node)
    graph.add_node("retrieval_agent", retrieval_agent_node)
    graph.add_node("reasoning_agent", reasoning_agent_node)
    graph.add_node("verifier_agent", verifier_agent_node)

    graph.add_edge(START, "query_agent")
    graph.add_edge("query_agent", "retrieval_agent")
    graph.add_edge("retrieval_agent", "reasoning_agent")
    graph.add_edge("reasoning_agent", "verifier_agent")
    graph.add_edge("verifier_agent", END)

    return graph.compile()