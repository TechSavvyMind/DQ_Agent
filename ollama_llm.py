# from langchain_ollama import ChatOllama


# ollama_llm = ChatOllama(
#     model="gpt-oss:20b-cloud",
#     validate_model_on_init=True,
#     temperature=0.6
# )
# # print(ollama_llm.invoke("What is the capital of France?"))

from langchain_nvidia_ai_endpoints import ChatNVIDIA  # type: ignore[import]


client = ChatNVIDIA(
  model="deepseek-ai/deepseek-v4-flash",
  api_key="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",
  temperature=1,
  top_p=0.95,
  max_tokens=16384,
  extra_body={"chat_template_kwargs":{"thinking":True,"reasoning_effort":"high"}},
)



print(client.invoke("What is the capital of France?"))