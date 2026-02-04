from openai import OpenAI
import os
import dotenv
from google import genai
from google.genai import types

dotenv.load_dotenv()

def openai_caller(prompt: str) -> str:

    client = OpenAI(
        api_key=os.environ.get('OPENAI_API_KEY'),
        base_url="https://openrouter.ai/api/v1"
    )

    response = client.responses.create(
        model=os.environ.get('MODEL_NAME'),
        tools=[{"type": "web_search"}],
        input=prompt
    )

    # client = genai.Client(
    #     api_key=os.environ.get('OPENAI_API_KEY')
    # )

    # grounding_tool = types.Tool(
    #     google_search=types.GoogleSearch()
    # )

    # config = types.GenerateContentConfig(
    #     tools=[grounding_tool]
    # )

    # response = client.models.generate_content(
    #     model="gemini-3-flash-preview",
    #     contents="Who won the euro 2024?",
    #     config=config,
    # )

    # print(response.text)
    return response.output_text