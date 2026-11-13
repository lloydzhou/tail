"""Tail —— 在 LLM SDK 与上游推理服务之间插入的一层「文本去重传输层」。

问题:多轮对话里,每一轮都要把**完整的上下文**重新发给上游。上下文越长、
轮次越多,上行带宽浪费越大。

做法:在公网这一跳上,不传 JSON、也不传完整文本,而是传「引用 + 新内容」
组成的帧流。两边各加一个转换器::

    客户端                                   网关
    ─────────────────────────────────────────────────────────
    messages JSON                            帧流
      ↓ 渲染器 (render)                        ↓ 解码器 (codec.Decoder)
    一维文本流                                一维文本流
      ↓ CDC 分块 + 引用/新块决策                 ↓ 反渲染器 (render)
    帧流 ────────── 公网 ──────────→           messages JSON
                                                 ↓
                                              上游推理 API

四层职责:

  - **渲染器 / 反渲染器**(``render``):懂 LLM 语义。JSON ⇄ 文本。
  - **编码器 / 解码器**(``codec``):只认字节。文本 ⇄ 帧流。
  - **块存储**(``storage``):扁平块集合,内容寻址。
  - **网关**(``gateway``):把上面几层串成一条请求路径。

**换协议只需要换渲染器。** 想支持 Anthropic Messages 或 Responses API,
写一个新的 render 模块即可,编解码、分块、存储、网关一行都不用改。

用法(服务端)::

    python -m tail.serve --backend https://api.deepseek.com --dbm ./blocks.db

用法(客户端)::

    from tail.transport.gateway import TailClient
    client = TailClient()
    body = client.encode_request(payload)     # 发给网关的 body
"""

from __future__ import annotations

__all__ = ["transport"]

__version__ = "3.0.0"
