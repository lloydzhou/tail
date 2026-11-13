"""Tail —— 在 LLM SDK 与上游推理服务之间插入的一层「文本去重传输层」。

问题:多轮对话里,每一轮都要把**完整的上下文**重新发给上游。上下文越长、
轮次越多,上行带宽浪费越大。

做法:在公网这一跳上不传整份 JSON,只传「服务端还没有的那一段文本」::

    客户端                                     网关
    ─────────────────────────────────────────────────────────────
    messages JSON                              (无)
      ↓ 渲染器 (render)                         │
    一维文本流                                  │
      ↓ 前缀自检,只留尾部 bytes                │
    尾部字节 + X-Tail-Have 头 ══ 公网 ══→        ↓ 切块 + 父链哈希(唯一权威)
                                                ↓ 按 (L, H) 从存储走链取前缀
                                              一维文本流
                                                ↓ 反渲染器 (render)
                                              messages JSON
                                                ↓
                                              上游推理 API

职责:

  - **渲染器 / 反渲染器**(``render``):懂 LLM 语义。JSON ⇄ 文本。
  - **前缀客户端**(``codec.PrefixClient``):只认字节。文本 + 凭据 → 尾部字节流。
  - **前缀读取器**(``codec.PrefixReader``):只认字节。前缀 + 尾部 → 完整文本流,
    并且是**块哈希的唯一产生地**。
  - **链式存储**(``storage``):`blk:{hash} → parent‖content`,按链尾还原前缀。
  - **网关**(``gateway``):把上面几层串成一条请求路径。

**换协议只需要换渲染器。** 想支持 Anthropic Messages 或 Responses API,
写一个新的 render 模块即可,分块、存储、网关一行都不用改。

用法(服务端)::

    python -m tail.serve --backend https://api.deepseek.com \
        --redis redis://127.0.0.1:6379/0

用法(客户端)::

    from tail.transport.gateway import TailClient
    client = TailClient()
    prepared = client.prepare(payload)        # 凭据 + 要发的尾部字节流
"""

from __future__ import annotations

__all__ = ["transport"]

__version__ = "3.0.0"
