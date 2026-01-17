from pydantic import BaseModel
from typing import List

class StockInsight(BaseModel):
    symbol: str
    score: float
    reasoning: List[str]
    cautions: List[str]

class ScreenerResult(BaseModel):
    style: str
    results: List[StockInsight]
