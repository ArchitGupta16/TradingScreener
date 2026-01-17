from schemas.intent import ScreenIntent
from utils.llm import get_llm
import re

def extract_json(text: str) -> str:
    text = text.strip()

    # Remove markdown fences
    text = re.sub(r"^```json", "", text)
    text = re.sub(r"^```", "", text)
    text = re.sub(r"```$", "", text)

    # Keep only the JSON object
    start = text.find("{")
    end = text.rfind("}")

    if start != -1 and end != -1:
        text = text[start:end + 1]

    return text.strip()


def style_agent(user_query: str) -> ScreenIntent:
    prompt = """
        You are a trading intent classifier. You need to clasify the user's input to which
        style of trading they want to do: INTRADAY, SWING, or LONG_TERM. Default to LONG_TERM if not specified.
        
        Also determine which market they want to trade in: NSE or BSE.

        Also specify the market cap preference: LARGE, MID OR SMALL. Default to None

        Also find if they want certain number of stocks in the output. Default to 10

        Read the user query carefully to determine the values.

        Constraints:
            Only provide one market either NSE or BSE
            Return ONLY valid JSON. No markdown. No explanation. If something is defaulted, dont indicate that in response.

        Schema:
        {{
        "style": "INTRADAY | SWING | LONG_TERM",
        "market": "BSE | NSE",
        "cap": "LARGE | MID | SMALL | NONE"
        "count": number_of_stocks (int)
        }}

        User query: {query}
        """

    raw = get_llm().invoke(prompt.format(query=user_query))
    cleaned = extract_json(raw)

    return ScreenIntent.parse_raw(cleaned)
