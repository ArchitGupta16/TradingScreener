from langgraph.graph import StateGraph
from typing import TypedDict

class GraphState(TypedDict):
    user_query: str
    style: str
    quant_contract: dict
    screened_df: object

graph = StateGraph(GraphState)
