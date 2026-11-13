"""原生 openai 用法(不走 Tail)。

这是基准文件:和 ``chat_tail.py`` 逐行 diff,就能看出接入 Tail 到底要改什么。
"""

from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8765/v1",
    api_key="sk-xxx",
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
