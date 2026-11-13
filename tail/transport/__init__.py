"""transport 层:文本去重传输。

模块划分(依赖方向自上而下,下面三层互相不知道上面存在):

    render.py    渲染器 / 反渲染器   懂 LLM 语义   messages JSON ⇄ 一维文本
    codec.py     前缀凭据 + 尾部    只认字节      (L, tip) + 尾部字节 ⇄ 一维文本
    chunking.py  定长块 + 块哈希     只认字节      字节 → 块列表
    storage.py   链式块存储          只认块        blk:{hash} → (parent, content)
    gateway.py   线缆头、协议层、串起来的请求路径
"""

from __future__ import annotations

from . import chunking, codec, gateway, render, storage

__all__ = ["chunking", "codec", "gateway", "render", "storage"]
