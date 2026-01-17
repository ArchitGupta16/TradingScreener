from utils.llm import get_llm

def reasoning_agent(symbol: str, summary: str) -> dict:
    llm = get_llm()

    prompt = f"""
You are an intraday trading assistant.

Stock: {symbol}
Summary:
{summary}

Evaluate suitability.
Return JSON with:
score (0–10),
reasoning (list),
cautions (list)
"""

    response = llm(prompt)
    return response
