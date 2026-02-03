from openai import OpenAI
import os
import dotenv

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

    return response.output_text
