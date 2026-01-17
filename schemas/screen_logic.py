from pydantic import BaseModel
from typing import List, Dict

class QuantFilter(BaseModel):
    symbol: str
    reason: str

class QuantContract(BaseModel):
    stocks: List[QuantFilter]
