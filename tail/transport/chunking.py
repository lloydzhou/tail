"""CDC(content-defined chunking)分块算法与块 id 计算。

本模块是传输层的基石,<b>完全不关心聊天协议语义</b>——它只看到字节。

为什么不用固定大小切分
----------------------
固定大小切分下,文本中间插入一个字符,后面所有块的边界都会移位,
导致全部 block_id 变化、缓存整体失效。CDC 用滚动哈希判断切点:只有当
哈希满足特定模式时才切,切点由<b>内容本身</b>决定,与绝对位置无关。
因此局部修改只影响落点附近的块,其余块 id 完全不变。

为什么用 buzhash 而不是 gear hash
--------------------------------
gear hash 的实现是 ``h = (h << 8) + GEAR[b]``,它有一个致命缺陷:
**哈希状态的熵被输入字节的取值空间限制**。实测中,中文 prompt 的
UTF-8 字节只有几十种取值,反复卷积后哈希收敛到很小的状态集合,
在 245KB 的文本上产生了 <b>0 个切点</b>——退化成纯 max_size 硬切,
局部性完全丧失。gear hash 在纯字母数字英文上也不稳,周期性内容
(模板套话、缩进、分隔线)同样会陷入周期轨道。

buzhash 对窗口内所有字节做循环移位后异或::

    h = rotl64(h, 1) ^ T[byte]
    当 (h & MASK) == 0 时切分

每个字节都影响全部 64 位,不会因为字节取值空间小就丢熵。实测在
中文 UTF-8 文本与纯随机字节上的切点密度一致(偏差 < 5%),这是本模块
选择它的唯一理由。

GEAR 表用 sha256 从固定种子派生,保证跨进程/跨语言/跨版本稳定
(不能用内置 hash(),它带进程随机盐)。
"""

from __future__ import annotations

import hashlib
from typing import Iterator, List, Tuple

# ---------------------------------------------------------------------------
# 参数
# ---------------------------------------------------------------------------

DEFAULT_AVG_SIZE = 2048       # 平均块大小 2KB
DEFAULT_MIN_SIZE = 256        # 最小块,防止碎块(约 avg/8)
DEFAULT_MAX_SIZE = 2048       # 最大块,长段无切点时兜底(= avg,见下)

# 参数取舍说明(实测数据)
# ----------------------
# 这三个参数对**增量场景**的影响远大于对静态压缩率的影响,因为多轮对话里
# 每轮的代价主要由「尾部有多少内容必须重传」决定。
#
# 1) min_size 越小越好(只要不产生碎块):
#    文本尾部永远没有「下一个切点」来界定边界,尾块会随追加不断吞入新内容。
#    min 越小,越容易在靠近末尾处找到切点,尾块就越短。
#    12 轮实测累计重传量:min=1024 → 30680B,min=512 → 39378B,min=256 → 17979B。
#
# 2) max_size 必须收紧到 ≈ avg,这是控制「锯齿」的关键:
#    如果 max_size 远大于 avg(例如 4×avg),尾块会连续吞并多轮内容,
#    直到撞上 max_size 才被硬切——期间每轮都要重传这块越来越大的内容,
#    线上体积会呈明显锯齿,甚至超过冷启动体积。
#    20 轮实测:max=8192 → 50487B 且出现 4711B 的尖峰;
#               max=2048 → 33293B 且尖峰不超过冷启动(2578B)。省 34%。
#
# 3) avg 保持 2KB:块数决定引用列表开销(每块 17B)。avg 太小会让块数暴涨,
#    1MB 文本在 avg=2KB 时约 500 块,引用列表约 8.5KB,占比可接受。

_HASH_BITS = 64
_HASH_MASK = (1 << _HASH_BITS) - 1

#: 滚动窗口长度(字节)。buzhash 的有效历史长度,
#: 决定"局部修改"能影响到多远。48 字节足够覆盖一般词句,又足够短。
_WINDOW = 48


def _build_table() -> List[int]:
    """生成 256 项 64 位随机表。sha256 派生,跨平台确定。"""
    return [
        int.from_bytes(hashlib.sha256(b"tail/buzhash/" + bytes([i])).digest()[:8], "big")
        for i in range(256)
    ]


_TABLE = _build_table()


def _mask_for_avg(avg_size: int) -> int:
    """期望平均块长 avg_size 对应的切点掩码。

    切点密度 ≈ 1/2**bits,所以 bits = log2(avg_size)。
    实测 buzhash 在中文文本与随机字节上的平均间隔都约为理论值的
    1.0~1.3 倍,取 log2(avg) 即可,不需要额外偏置。
    """
    bits = max(1, avg_size.bit_length() - 1)
    return (1 << bits) - 1


def _rotl(x: int, n: int) -> int:
    return ((x << n) | (x >> (_HASH_BITS - n))) & _HASH_MASK


# ---------------------------------------------------------------------------
# 分块
# ---------------------------------------------------------------------------

def split(data: bytes, avg_size: int = DEFAULT_AVG_SIZE,
          min_size: int = DEFAULT_MIN_SIZE,
          max_size: int = DEFAULT_MAX_SIZE) -> List[Tuple[int, int]]:
    """把字节流切成块,返回 [(start, end), ...]。

    切点规则:
      - 距上一块起点 >= min_size 且 (h & mask) == 0  → 切
      - 距上一块起点 >= max_size                     → 强制切(兜底)

    Args:
      data: 待切分的字节
      avg_size: 期望平均块大小
      min_size: 最小块大小
      max_size: 最大块大小
    """
    if not data:
        return []
    mask = _mask_for_avg(avg_size)
    n = len(data)
    out: List[Tuple[int, int]] = []
    start = 0
    h = 0
    i = 0
    while i < n:
        # 滑动窗口:移入新字节,滚出窗口外的旧字节
        h = _rotl(h, 1) ^ _TABLE[data[i]]
        if i - start >= _WINDOW:
            h ^= _rotl(_TABLE[data[i - _WINDOW]], _WINDOW)
        h &= _HASH_MASK
        i += 1
        length = i - start
        if length < min_size:
            continue
        if (h & mask) == 0 or length >= max_size:
            out.append((start, i))
            start = i
            h = 0
    if start < n:
        out.append((start, n))
    return out


def split_blocks(data: bytes, **kw) -> Iterator[bytes]:
    """便捷版:直接产出块字节。"""
    for s, e in split(data, **kw):
        yield data[s:e]


# ---------------------------------------------------------------------------
# 块 id
# ---------------------------------------------------------------------------

BLOCK_ID_SIZE = 16  # 128 bit


def block_id(chunk: bytes) -> bytes:
    """块内容 → 16 字节 id(内容寻址)。

    纯内容寻址:同样内容永远得到同样 id,与位置、先后顺序无关。
    """
    return hashlib.sha256(chunk).digest()[:BLOCK_ID_SIZE]


def block_id_hex(chunk: bytes) -> str:
    """便于日志与调试的十六进制形式。"""
    return block_id(chunk).hex()
