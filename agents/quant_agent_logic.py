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
        You are given a data frame with their symbol, rsi, atr, volume ratio already calculated.
        This calculation was done using historical data of last 6 months.
        Based on the users input, intent and criteria, select the top matching stocks from this dataframe.
        
        For each stock selected just give a one liner reason why it was selected based on momentum, volume, volatility.
        Return ONLY valid JSON. No markdown. No explanation.

        The answer MUST strictly follow this schema, which is a list of stocks with reasons for each:
          "stocks": [
            {{
              "symbol": "string",
              "reason": "string"
            }}
          ]

        Rules:
        - stocks MUST be a string (not a list)
        - reason MUST be a one liner
        - ALL fields are required

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
