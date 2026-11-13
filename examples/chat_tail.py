"""走 Tail 的 openai 用法。

和 ``chat.py`` 的差异只有两行(用 ``diff chat.py chat_tail.py`` 可验证):

    + from tail.transport.client import TailTransport
    + http_client=httpx2.Client(transport=TailTransport()),

请求与响应的写法一行没动——仍然是 ``client.chat.completions.create(...,
stream=True)``,仍然是 ``event.choices[0].delta.content``。
"""

import httpx2
from openai import OpenAI

from tail.transport.client import TailTransport

# 同一个 client 必须复用:它按会话保存「网关已有哪些前缀」的凭据,
# 第二轮起请求体只剩新增的那一段。换成 AsyncOpenAI 就用 TailAsyncTransport。
client = OpenAI(
    base_url="http://127.0.0.1:8765/v1",
    api_key="sk-xxx",
    http_client=httpx2.Client(transport=TailTransport()),
)

messages = [{"role": "system", "content": "你是一个简洁的助手。"}]

while True:
    question = input("你: ")
    if question in ("quit", "exit", ""):
        break
    messages.append({"role": "user", "content": question})

    stream = client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
        stream=True,
    )

    print("AI: ", end="", flush=True)
    answer = []
    for event in stream:
        delta = event.choices[0].delta.content
        if delta:
            answer.append(delta)
            print(delta, end="", flush=True)
    print()

    messages.append({"role": "assistant", "content": "".join(answer)})
