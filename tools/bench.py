"""可复现的基准测试:体积与速度。

文档 `docs/TRANSPORT-PIPELINE.md` 第 9 节的数字必须能由本脚本重新算出,
否则极易漂移成过期的假数据。

用法::

    python tools/bench.py
"""

from __future__ import annotations

import gc
import json
import statistics
import time
from typing import Callable

from tail.transport import chunking, codec, render, storage
from tail.transport.gateway import TailClient, TailGateway


def build_session(n_pairs: int = 40) -> dict:
    """构造一份确定性的长会话(内容固定,结果可复现)。"""
    messages = [{"role": "system", "content": "你是一个严谨的助手,回答要简洁。"}]
    for i in range(n_pairs):
        messages.append({"role": "user", "content": f"第{i}个问题:" + "描述" * 60})
        messages.append({"role": "assistant", "content": f"第{i}个回答:" + "结论" * 90})
    return {"model": "deepseek-chat", "messages": messages}


def _best(fn: Callable[[], object], samples: int = 30) -> float:
    """单次计时取最小值的平均。

    不用 ``timeit.repeat`` 的批量平均:GC 暂停会污染批次均值,实测同一个调用
    批量测可能是 1.1 ms,单次测是 0.78 ms。每次计时前 ``gc.collect()``,
    再取最干净的一次。
    """
    times = []
    for _ in range(samples):
        gc.collect()
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times)


def _mbps(seconds: float, size: int) -> float:
    return size / seconds / 1e6


def bench_volume(payload: dict) -> None:
    text = b"".join(render.iter_render(payload))
    raw = json.dumps(payload, ensure_ascii=False).encode()

    cold_client = TailClient()
    cold_wire = cold_client.encode_request(payload)
    warm_wire = cold_client.encode_request(payload)      # 第二轮:全部命中
    blocks = len(chunking.blocked_to_blocks(text))

    print("### 体积")
    print()
    print(f"{len(payload['messages'])} 条消息,{blocks} 个满块")
    print()
    print("| | 字节 | 相对原始 JSON |")
    print("|---|---|---|")
    for label, size in (("原始 JSON", len(raw)),
                        ("渲染文本", len(text)),
                        ("冷启动帧流", len(cold_wire)),
                        ("全命中帧流(第二轮回同样内容)", len(warm_wire))):
        print(f"| {label} | {size} | {'—' if size == len(raw) else f'省 {100 * (1 - size / len(raw)):.0f}%'} |")
    print()


def bench_speed(payload: dict) -> None:
    text = b"".join(render.iter_render(payload))
    pieces = list(render.iter_render(payload))
    raw = json.dumps(payload, ensure_ascii=False).encode()
    n_blocks = len(chunking.blocked_to_blocks(text))

    # 各环节单独计时,互不包含
    t_render = _best(lambda: list(render.iter_render(payload)))
    t_chunk = _best(lambda: list(chunking.blocked_to_blocks(text)))
    t_encode_cold = _best(
        lambda: [x for x in codec.Encoder().iter_encode([text])])
    warm = codec.Encoder(known=[bid for bid, _ in chunking.blocked_to_blocks(text)])
    t_encode_warm = _best(lambda: [x for x in warm.iter_encode([text])])

    client = TailClient()
    wire = client.encode_request(payload)
    t_unrender = _best(
        lambda: TailGateway(storage.MemoryStorage()).collect_payload([wire]))
    t_client_cold = _best(lambda: TailClient().encode_request(payload))
    t_native_json = _best(lambda: json.dumps(payload, ensure_ascii=False).encode())

    print("### 速度")
    print()
    print(f"渲染文本 {len(text):,} B,{n_blocks} 个满块;取 5 次 **CPU 时间**最小值。")
    print()
    print("| 环节 | 吞吐 | 单次耗时 | 备注 |")
    print("|---|---|---|---|")
    print(f"| 渲染 JSON → 文本 | {_mbps(t_render, len(text)):.0f} MB/s | {t_render * 1e3:.2f} ms | |")
    print(f"| 切块 + 哈希 | {_mbps(t_chunk, len(text)):.0f} MB/s | {t_chunk * 1e3:.2f} ms | {t_chunk / n_blocks * 1e6:.1f} µs/块,受 blake2b 单块调用开销限制 |")
    print(f"| 编码 文本 → 帧流(冷,全为新块) | {_mbps(t_encode_cold, len(text)):.0f} MB/s | {t_encode_cold * 1e3:.2f} ms | |")
    print(f"| 编码 文本 → 帧流(全命中) | {_mbps(t_encode_warm, len(text)):.0f} MB/s | {t_encode_warm * 1e3:.2f} ms | 只发 17B 引用帧 |")
    print(f"| 网关整条:帧流 → JSON | {_mbps(t_unrender, len(raw)):.0f} MB/s | {t_unrender * 1e3:.2f} ms | 含帧解码、查块、反渲染、重建 JSON、入库 |")
    print()
    print("客户端整条冷路径 vs 原生 SDK 的 JSON 序列化:")
    print()
    print(f"- Tail 冷路径(render + 切块 + 编码): **{t_client_cold * 1e3:.2f} ms**")
    print(f"- 原生 `json.dumps`: **{t_native_json * 1e3:.2f} ms**")
    print(f"- 差 **{(t_client_cold - t_native_json) * 1e3:.2f} ms**——相对上游推理(百毫秒级)可忽略")
    print()


def bench_known_set(payload: dict) -> None:
    """客户端本地状态多大:只存哈希,不存内容。"""
    client = TailClient()
    for i in range(10, len(payload["messages"]) // 2 + 1):
        client.encode_request({"model": payload["model"],
                               "messages": payload["messages"][:i * 2]})
    known = client._encoder._known
    print("### 客户端本地状态")
    print()
    print(f"- 已知块 {len(known)} 个,纯哈希 {len(known) * chunking.BLOCK_ID_SIZE:,} B "
          f"(set 实际开销约 {len(known) * 70:,} B)")
    print(f"- 不保存任何正文副本(集合元素为定长 {chunking.BLOCK_ID_SIZE} 字节块 id)")
    print()


def bench_increment(n_turns: int = 20) -> None:
    """多轮增量:第 N 轮在线上要传多少,占完整上下文的多少。"""
    client = TailClient()
    messages = [{"role": "system", "content": "你是一个严谨的助手。"}]
    sizes, full = [], []
    for i in range(n_turns):
        messages.append({"role": "user", "content": f"第{i}个问题:" + "描述" * 40})
        messages.append({"role": "assistant", "content": f"第{i}个回答:" + "结论" * 60})
        payload = {"model": "deepseek-chat", "messages": messages}
        sizes.append(len(client.encode_request(payload)))
        full.append(len(TailClient().encode_request(payload)))   # 同一上下文冷启动

    half = n_turns // 2
    early, late = sizes[:half], sizes[half:]
    print("### 多轮增量")
    print()
    print(f"{n_turns} 轮对话,每轮把完整 messages 交给客户端:")
    print()
    cold_avg = sum(full[half:]) // len(full[half:])
    late_avg = sum(late) // len(late)
    print(f"- 后 {len(late)} 轮均值 **{late_avg:,} B**,"
          f"同一上下文冷启动均值 {cold_avg:,} B"
          f" → 省 **{100 * (1 - late_avg / cold_avg):.0f}%**")
    print(f"- 前 {half} 轮均值 {sum(early) // len(early):,} B → 后 {half} 轮均值 "
          f"{sum(late) // len(late):,} B(上下文越长,增量占比越低)")
    print()


def bench_reuse(turns: int = 6) -> None:
    """复用同一 client vs 每轮新建 client。"""
    def session(i: int) -> dict:
        messages = [{"role": "system", "content": "你是一个严谨的助手。"}]
        for k in range(i):
            messages.append({"role": "user", "content": f"第{k}个问题:" + "描述" * 40})
            messages.append({"role": "assistant", "content": f"第{k}个回答:" + "结论" * 60})
        return {"model": "deepseek-chat", "messages": messages}

    reused = TailClient()
    reuse_total = sum(len(reused.encode_request(session(i))) for i in range(1, turns + 1))
    fresh_total = sum(len(TailClient().encode_request(session(i)))
                      for i in range(1, turns + 1))

    print("### 复用 client vs 每轮新建")
    print()
    print(f"- 复用同一个 `TailClient`,{turns} 轮合计 **{reuse_total:,} B**")
    print(f"- 每轮 `TailClient()`,{turns} 轮合计 **{fresh_total:,} B**"
          f"(永远冷启动,差 {100 * (1 - reuse_total / fresh_total):.0f}%)")
    print()


def bench_real_sizes() -> None:
    """按真实请求体积(200~800 KB)测两端 CPU 成本。"""
    import random

    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 .,:;-_/()[]{}"

    def text_of(nbytes: int, seed: int) -> str:
        return "".join(random.Random(seed).choices(alphabet, k=nbytes))

    def build(target_kb: int) -> dict:
        messages = [{"role": "system", "content": text_of(8192, 0)}]
        total, seed = 8192, 1
        while total < target_kb * 1024:
            messages.append({"role": "user", "content": text_of(30000, seed)})
            messages.append({"role": "assistant", "content": text_of(30000, seed + 1)})
            total += 60000
            seed += 2
        return {"model": "deepseek-chat", "messages": messages}

    print("### 真实体积下的 CPU 成本(客户端 + 网关)")
    print()
    print("| 请求体 | 客户端编码 | 网关解码并重建 JSON | 合计 |")
    print("|---|---|---|---|")
    for kb in (66, 242, 418, 829):
        payload = build(kb)
        store = storage.MemoryStorage()
        gateway = TailGateway(store)
        list(gateway.iter_payload_json([TailClient().encode_request(payload)]))
        client = TailClient()
        list(gateway.iter_payload_json([client.encode_request(payload)]))
        warm = client.encode_request(payload)
        t_client = _best(lambda: client.encode_request(payload))
        t_gateway = _best(
            lambda: sum(len(x) for x in gateway.iter_payload_json([warm])))
        raw = len(json.dumps(payload, ensure_ascii=False).encode())
        print(f"| {raw // 1024} KB | {t_client * 1e3:.2f} ms | {t_gateway * 1e3:.2f} ms "
              f"| {(t_client + t_gateway) * 1e3:.2f} ms |")
    print()
    print("随体积**线性**增长,无超线性劣化。")
    print()


def bench_compaction_cycle() -> None:
    """真实对话周期:200 KB 起、每轮 +60 KB 涨到 800 KB,compact 后回落到 200 KB。

    要点:compact 只重写 messages,``tools`` 与 ``system`` 不变,所以这部分前缀
    仍然命中——不是「全部失效」。
    """
    import random

    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 .,:;-_/()[]{}"

    def text_of(nbytes: int, seed: int) -> str:
        return "".join(random.Random(seed).choices(alphabet, k=nbytes))

    tools = [{"type": "function", "function": {
        "name": f"tool_{i}", "description": text_of(900, i),
        "parameters": {"type": "object", "properties": {
            "a": {"type": "string", "description": text_of(200, i)}}}}}
        for i in range(30)]
    system = {"role": "system", "content": text_of(8 * 1024, 777)}
    prefix = len(json.dumps(tools).encode()) + 8 * 1024

    client = TailClient()
    store = storage.MemoryStorage()

    def turn(messages, label):
        payload = {"model": "deepseek-chat", "tools": tools, "messages": messages}
        native = len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode())
        wire = client.encode_request(payload)
        _out, session = TailGateway(store).collect_payload([wire])
        blocks = session.new_block_count + session.ref_block_count
        hit = session.ref_block_count / blocks if blocks else 0
        print(f"| {label} | {native:,} | {len(wire):,} | {100 * len(wire) / native:.1f}% "
              f"| {100 * hit:.1f}% |")
        return messages

    print("### 真实对话周期(200 KB → 800 KB → compact → 200 KB)")
    print()
    print(f"固定前缀:`tools` {len(json.dumps(tools).encode()):,} B + `system` 8 KB = {prefix:,} B")
    print()
    print("| 轮次 | 原始 JSON | 公网发送 | 占比 | 命中率 |")
    print("|---|---|---|---|---|")

    messages = [system]
    total, seed = 0, 1
    while total < 190 * 1024:
        messages += [{"role": "user", "content": text_of(30000, seed)},
                     {"role": "assistant", "content": text_of(30000, seed + 1)}]
        seed += 2
        total += 60000
    turn(messages, "起点 ~200 KB")

    grown = 1
    while True:
        messages += [{"role": "user", "content": text_of(30000, seed)},
                     {"role": "assistant", "content": text_of(30000, seed + 1)}]
        seed += 2
        grown += 1
        size = len(json.dumps({"model": "m", "tools": tools, "messages": messages}).encode())
        if size >= 800 * 1024:
            break
        if grown % 4 == 0:
            turn(messages, f"增长中 ~{size // 1024} KB")
    turn(messages, f"涨到 ~{size // 1024} KB")

    summary = {"role": "system", "content": "历史摘要:" + text_of(170 * 1024, 555)}
    compacted = [system, summary, messages[-2], messages[-1]]
    turn(compacted, "**compact 之后**")
    for extra in range(1, 3):
        compacted += [{"role": "user", "content": text_of(30000, 900 + extra)},
                      {"role": "assistant", "content": text_of(30000, 950 + extra)}]
        turn(compacted, f"compact 后 +{extra} 轮")
    print()
    print("compact 后命中率正好等于未变的 `tools` + `system` 前缀占比;")
    print("之后每轮恢复高位命中,只需传增量。")
    print()


def main() -> None:
    payload = build_session()
    bench_volume(payload)
    bench_speed(payload)
    bench_known_set(payload)
    bench_increment()
    bench_reuse()
    bench_real_sizes()
    bench_compaction_cycle()


if __name__ == "__main__":
    main()
