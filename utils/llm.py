from langchain_community.llms import Ollama

def get_llm(model: str = "deepseek-r1:latest"):
    return Ollama(
        model=model,
        temperature=0.2
    )

