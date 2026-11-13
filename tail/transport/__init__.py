"""transport 层:文本去重传输。

模块划分(依赖方向自上而下,下面三层互相不知道上面存在):

    render.py    渲染器 / 反渲染器   懂 LLM 语义   messages JSON ⇄ 一维文本
    codec.py     编码器 / 解码器     只认字节      一维文本 ⇄ 帧流
    chunking.py  定长块 + 块哈希      只认字节      字节 → 块列表
    frames.py    帧格式              只认字节      帧 ⇄ 字节流
    storage.py   块存储              只认块        扁平块集合
    gateway.py   串起来的请求路径
"""

from __future__ import annotations

from . import chunking, codec, frames, gateway, render, storage

__all__ = ["chunking", "codec", "frames", "gateway", "render", "storage"]
