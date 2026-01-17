from pydantic import BaseModel, Field
from datetime import datetime

class ScreenIntent(BaseModel):
    style: str
    market: str
    cap: str
    count: int = Field(default=10)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
