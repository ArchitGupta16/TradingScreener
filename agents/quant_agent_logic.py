import re
from schemas.screen_logic import QuantContract
from utils.llm import get_llm

def extract_json(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```json", "", text)
    text = re.sub(r"^```", "", text)
    text = re.sub(r"```$", "", text)
    return text.strip()

def screen_logic_agent(criteria, stock_details):
    prompt = """
    You are a quantitative trading strategist.

    You are given a dataframe containing stock symbols and the following precomputed indicators
    (calculated using the last 6 months of historical data):
    - RSI
    - ATR
    - Volume Ratio

    Based on the user's intent and criteria, select the best matching stocks from this dataframe.

    Selection rules:
    - ONLY use the provided indicators (RSI, ATR, volume ratio).
    - ONLY select symbols that exist in the dataframe.
    - Prefer stronger momentum (RSI), confirmation via volume ratio, and reasonable volatility via ATR.
    - If multiple stocks match, prioritize stronger momentum and volume confirmation.

    For each selected stock:
    - Provide exactly ONE concise sentence explaining why it was selected.
    - The reason must mention at least one of: RSI, volume, or ATR.

    Output rules:
    - Return ONLY valid JSON.
    - No markdown, no explanations, no extra text.
    - Do NOT include fields other than those defined below.
    - All fields are mandatory.

    Schema (must be followed exactly):
    {
      "stocks": [
        {
          "symbol": "string",
          "reason": "string"
        }
      ]
    }

    Criteria:
    {criteria}

    Dataframe:
    {stock_details}
    """

    raw = get_llm().invoke(
        prompt.format(criteria=criteria, stock_details=stock_details)
    )

    cleaned = extract_json(raw)
    return QuantContract.parse_raw(cleaned)
