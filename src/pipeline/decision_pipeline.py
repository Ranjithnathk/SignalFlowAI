from src.pipeline.query_interpreter import QueryInterpreter
from src.retrieval.snowflake_retriever import SnowflakeRetriever
from src.reasoning.llm_reasoner import LLMReasoner


class DecisionPipeline:
    def __init__(self):
        self.interpreter = QueryInterpreter()
        self.retriever = SnowflakeRetriever()
        self.reasoner = LLMReasoner()

    def run(self, query: str, top_k: int = 10):
        filters = self.interpreter.build_filters(query)
        results = self.retriever.retrieve(query=query, top_k=top_k, filters=filters)
        answer = self.reasoner.generate(query, results)

        return {
            "query": query,
            "filters": filters,
            "results": results,
            "answer": answer
        }

    def close(self):
        self.retriever.close()