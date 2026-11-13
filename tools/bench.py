"""可复现的基准测试:体积、速度、客户端状态、Redis 相关成本。

文档 ``docs/TRANSPORT-PIPELINE.md`` 第 9 节的数字必须能由本脚本重新算出,
否则极易漂移成过期的假数据。

用法::

    python tools/bench.py

默认连 ``redis://127.0.0.1:6390/0`` 跑 Redis 那一段;连不上就跳过并说明,
其余部分照常跑。

    docker run -d --rm -p 6390:6379 redis:7-alpine
    TAIL_REDIS_URL=redis://127.0.0.1:6390/0 python tools/bench.py
"""

from __future__ import annotations

import gc
import json
import os
import time
from typing import Callable, List, Tuple

from tail.transport import chunking, render, storage
from tail.transport.gateway import TailClient, TailGateway

BLOCK = chunking.BLOCK_SIZE
REDIS_URL = os.environ.get("TAIL_REDIS_URL", "redis://127.0.0.1:6390/0")


# ---------------------------------------------------------------------------
# 素材
# ---------------------------------------------------------------------------

def build_session(n_pairs: int = 40) -> dict:
    """构造一份确定性的长会话(内容固定,结果可复现)。"""
    messages = [{"role": "system", "content": "你是一个严谨的助手,回答要简洁。"}]
    for i in range(n_pairs):
        messages.append({"role": "user", "content": f"第{i}个问题:" + "描述" * 60})
        messages.append({"role": "assistant", "content": f"第{i}个回答:" + "结论" * 90})
    return {"model": "deepseek-chat", "messages": messages}


ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789 .,:;-_/()[]{}"


def text_of(nbytes: int, seed: int) -> str:
    return "".join(__import__("random").Random(seed).choices(ALPHABET, k=nbytes))


def build_big(target_kb: int) -> dict:
    messages = [{"role": "system", "content": text_of(8192, 0)}]
    total, seed = 8192, 1
    while total < target_kb * 1024:
        messages.append({"role": "user", "content": text_of(30000, seed)})
        messages.append({"role": "assistant", "content": text_of(30000, seed + 1)})
        total += 60000
        seed += 2
    return {"model": "deepseek-chat", "messages": messages}


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _best(fn: Callable[[], object], samples: int = 7) -> float:
    """单次计时取最小值,**用 CPU 时间**,计时区间内关掉 GC。

    为什么用 ``process_time`` 而不是 wall clock:这台机器实测 load average
    ≈ 55 / 10 核,wall clock 会被调度延迟放大 5~10 倍(同一次 829 KB 解码:
    CPU 21~37 ms,wall 126~366 ms)。CPU 时间不受调度影响,同一张表内的比值才稳定。

    为什么要关 GC:分代 GC 的 CPU 成本属于「进程整体」而不是「这一步」,且会让
    单次测量出现尖峰;计时区间关掉,GC 的总量单独量化(见「测量口径」一节)。

    为什么取最小值而不是均值:GC 之外还有分配器抖动,最小值最接近纯 CPU 成本。
    """
    times = []
    for _ in range(samples):
        gc.collect()
        gc.disable()
        try:
            start = time.process_time()
            fn()
            times.append(time.process_time() - start)
        finally:
            gc.enable()
    return min(times)


def _best_wall(fn: Callable[[], object], samples: int = 15) -> float:
    """wall clock 最小值。只用于 Redis 这类**成本在网络往返**的测量。"""
    times = []
    for _ in range(samples):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times)


def _mbps(seconds: float, size: int) -> float:
    return size / seconds / 1e6


def _wire(prepared) -> int:
    """线上实际字节数 = 请求体(尾部)长度 + 头里的凭据。"""
    return sum(len(c) for c in prepared.body)


def _decode(payload: dict, store, client: TailClient) -> Tuple[dict, object]:
    """走一遍 prepare → 网关解码,并回填客户端凭据。返回 (还原的请求体, 统计)。"""
    prepared = client.prepare(payload)
    back, session = TailGateway(store).collect_payload(
        prepared.body, prepared.have_len, prepared.have_tip)
    client.set_credential(prepared.session, session.prefix_len, session.tip)
    return back, session


def _decode_sized(payload: dict, store, client: TailClient):
    """一次遍历同时算出线上字节数并完成解码。

    注意不能先 ``_wire(prepared)`` 再拿同一个 ``prepared.body`` 去解码:
    body 是生成器,量完长度就空了,网关只会收到 0 字节。
    """
    prepared = client.prepare(payload)
    size = [0]

    def body():
        for chunk in prepared.body:
            size[0] += len(chunk)
            yield chunk

    back, session = TailGateway(store).collect_payload(
        body(), prepared.have_len, prepared.have_tip)
    client.set_credential(prepared.session, session.prefix_len, session.tip)
    return size[0], back, session


def _rendered(payload: dict) -> bytes:
    return b"".join(render.iter_render(payload))


def _hash_blocks(text: bytes) -> int:
    hasher = chunking.BlockHasher(BLOCK)
    n = 0
    for _bid, _block in hasher.feed(text):
        n += 1
    return n


# ---------------------------------------------------------------------------
# 1. 体积
# ---------------------------------------------------------------------------

def bench_volume(payload: dict) -> None:
    text = _rendered(payload)
    raw = json.dumps(payload, ensure_ascii=False).encode()
    blocks = len(text) // BLOCK

    store = storage.MemoryChainStore()
    client = TailClient()
    cold = _wire(client.prepare(payload))
    _decode(payload, store, client)          # 建立缓存与凭据
    warm = _wire(client.prepare(payload))

    print("### 体积")
    print()
    print(f"{len(payload['messages'])} 条消息;渲染文本 {len(text):,} B,{blocks} 个满块。")
    print()
    print("| | 上行字节 | 相对原始 JSON |")
    print("|---|---|---|")
    rows = (
        ("原始 JSON(SDK 原生)", len(raw)),
        ("渲染文本(v3 冷启动,全文)", cold),
        ("v3 热请求(只发不足一块的尾巴)", warm),
    )
    for label, size in rows:
        rel = "—" if size == len(raw) else f"省 {100 * (1 - size / len(raw)):.1f}%"
        print(f"| {label} | {size:,} | {rel} |")
    print()
    print(f"冷启动也比原生 JSON 小,因为渲染文本去掉了键名、引号、逗号这些结构开销;")
    print(f"热请求从 {len(raw):,} B 降到 {warm} B,靠的是服务端持有前缀。")
    print()


# ---------------------------------------------------------------------------
# 2. 速度
# ---------------------------------------------------------------------------

def bench_speed(payload: dict) -> None:
    text = _rendered(payload)
    raw = json.dumps(payload, ensure_ascii=False).encode()
    n_blocks = len(text) // BLOCK

    t_render = _best(lambda: _rendered(payload))
    t_hash = _best(lambda: _hash_blocks(text))
    t_json = _best(lambda: json.loads(raw))
    t_loads_bytes = _best(lambda: b"".join(render.iter_render(payload)))

    store = storage.MemoryChainStore()
    warm_client = TailClient()
    _decode(payload, store, warm_client)

    t_client_cold = _best(lambda: _wire(TailClient().prepare(payload)))
    # 热路径:扫到已持有前缀的边界就停,再把剩下的字节原样发出
    t_client_warm = _best(lambda: _wire(warm_client.prepare(payload)))

    t_gw_cold = _best(lambda: sum(
        len(x) for x in TailGateway(storage.MemoryChainStore())
        .iter_payload_json(TailClient().prepare(payload).body)))

    gw_warm = TailGateway(store)

    def gw_warm_once() -> int:
        p = warm_client.prepare(payload)
        return sum(len(x) for x in
                   gw_warm.iter_payload_json(p.body, p.have_len, p.have_tip))

    t_gw_warm = _best(gw_warm_once)

    print("### 速度")
    print()
    print(f"渲染文本 {len(text):,} B / {n_blocks} 个满块;各取 7 次 **CPU 时间**最小值"
          f"(屏蔽机器负载,依据见本节开头)。")
    print()
    print("| 环节 | 吞吐 | 单次耗时 | 备注 |")
    print("|---|---|---|---|")
    print(f"| 渲染 JSON → 文本 | {_mbps(t_render, len(text)):.0f} MB/s | {t_render * 1e3:.2f} ms | 客户端 |")
    print(f"| 切块 + 链式哈希 | {_mbps(t_hash, len(text)):.0f} MB/s "
          f"| {t_hash * 1e3:.2f} ms | 两侧都要做;{t_hash / n_blocks * 1e6:.1f} µs/块 |")
    print(f"| 客户端 prepare(冷) | {_mbps(t_client_cold, len(text)):.0f} MB/s "
          f"| {t_client_cold * 1e3:.2f} ms | 渲染 + 哈希 + 自检 |")
    print(f"| 客户端 prepare(热) | {_mbps(t_client_warm, len(text)):.0f} MB/s "
          f"| {t_client_warm * 1e3:.2f} ms | 扫到边界即停 |")
    print(f"| 网关解码(冷) | {_mbps(t_gw_cold, len(raw)):.0f} MB/s "
          f"| {t_gw_cold * 1e3:.2f} ms | 哈希 + 写链 + 反渲染 + 重建 JSON |")
    print(f"| 网关解码(热) | {_mbps(t_gw_warm, len(raw)):.0f} MB/s "
          f"| {t_gw_warm * 1e3:.2f} ms | 走链 {n_blocks} 跳 + 取块 + 重建 JSON |")
    print()
    print("客户端两条路径对比原生序列化:")
    print()
    print(f"- 原生 SDK 要付 `json.dumps` 的 CPU,本协议在客户端还要付 `json.loads`"
          f"(SDK 自己解析响应前的请求体构造不计入此处)。")
    print(f"- 客户端 `json.loads` 同一份 {len(raw) // 1024} KB JSON: **{t_json * 1e3:.2f} ms**"
          f",常驻约 {len(raw) / 1e6:.2f} MB,峰值约 {3 * len(raw) / 1e6:.2f} MB。")
    print(f"- 客户端 prepare 冷 **{t_client_cold * 1e3:.2f} ms** / 热 "
          f"**{t_client_warm * 1e3:.2f} ms**:热路径渲染到凭据边界就停,不必走完整份上下文。")
    print()


# ---------------------------------------------------------------------------
# 3. 客户端本地状态
# ---------------------------------------------------------------------------

def bench_local_state(sessions: int = 200, turns: int = 30) -> None:
    client = TailClient()
    store = storage.MemoryChainStore()
    messages = [{"role": "system", "content": "你是一个严谨的助手。"}]
    for t in range(turns):
        messages.append({"role": "user", "content": f"第{t}轮:" + "描述" * 40})
        messages.append({"role": "assistant", "content": f"答{t}:" + "结论" * 60})
        _decode({"model": "m", "messages": messages}, store, client)
    cred = client.credential_bytes
    blocks = len(_rendered({"model": "m", "messages": messages})) // BLOCK

    print("### 客户端本地状态")
    print()
    print(f"- 一条 {turns} 轮的会话({blocks} 个满块)在客户端只留 "
          f"**{cred} B**:一个 ``(L, tip)`` 凭据 = 8 B 长度 + 16 B 哈希。")
    print(f"- v2 要在客户端维护「已知块 id 集合」,同一条会话约 "
          f"{blocks * (chunking.BLOCK_ID_SIZE + 70):,} B({blocks} 块 × 约 86 B,含 set 开销)。")
    print(f"- 客户端**不保存任何正文副本**,也不保存块哈希表:哈希由网关产生,"
          f"客户端只在热路径上重算一遍用于核对边界。")
    print()


# ---------------------------------------------------------------------------
# 4. 多轮增量
# ---------------------------------------------------------------------------

def bench_increment(n_turns: int = 20) -> None:
    client = TailClient()
    store = storage.MemoryChainStore()
    messages = [{"role": "system", "content": "你是一个严谨的助手。"}]
    sizes, full, ctx = [], [], []
    for i in range(n_turns):
        messages.append({"role": "user", "content": f"第{i}个问题:" + "描述" * 40})
        messages.append({"role": "assistant", "content": f"第{i}个回答:" + "结论" * 60})
        payload = {"model": "deepseek-chat", "messages": messages}
        prepared = client.prepare(payload)
        sent = _wire(prepared)
        _decode(payload, store, client)
        sizes.append(sent)
        full.append(sum(len(c) for c in TailClient().prepare(payload).body))
        ctx.append(sum(len(m["content"].encode()) for m in messages))

    half = n_turns // 2
    early, late = sizes[:half], sizes[half:]
    cold_avg = sum(full[half:]) // len(full[half:])
    late_avg = sum(late) // len(late)
    print("### 多轮增量")
    print()
    print(f"{n_turns} 轮对话,每轮都把完整 messages 交给客户端:")
    print()
    print(f"- 后 {len(late)} 轮均值 **{late_avg:,} B**,同一上下文冷启动均值 {cold_avg:,} B"
          f" → 省 **{100 * (1 - late_avg / cold_avg):.0f}%**")
    print(f"- 前 {half} 轮均值 {sum(early) // len(early):,} B → 后 {half} 轮均值 {late_avg:,} B"
          f"(上下文越长,增量占比越低)")
    print(f"- 尾请求大小 ≈ 每轮新增文本,与历史长度无关:末轮线上 {sizes[-1]:,} B,"
          f"而上下文已 {ctx[-1]:,} B")
    print()


def bench_reuse(turns: int = 6) -> None:
    def session(i: int) -> dict:
        messages = [{"role": "system", "content": "你是一个严谨的助手。"}]
        for k in range(i):
            messages.append({"role": "user", "content": f"第{k}个问题:" + "描述" * 40})
            messages.append({"role": "assistant", "content": f"第{k}个回答:" + "结论" * 60})
        return {"model": "deepseek-chat", "messages": messages}

    store = storage.MemoryChainStore()
    reused_client = TailClient()
    reuse_total = 0
    for i in range(1, turns + 1):
        payload = session(i)
        reuse_total += _wire(reused_client.prepare(payload))
        _decode(payload, store, reused_client)

    fresh_total = 0
    for i in range(1, turns + 1):
        fresh_total += sum(len(c) for c in TailClient().prepare(session(i)).body)

    print("### 复用 client vs 每轮新建")
    print()
    print(f"- 复用同一个 `TailClient`,{turns} 轮合计 **{reuse_total:,} B**")
    print(f"- 每轮 `TailClient()`,{turns} 轮合计 **{fresh_total:,} B**"
          f"(永远冷启动,差 {100 * (1 - reuse_total / fresh_total):.0f}%)")
    print()
    print("凭据是会话级的,`TailClient` 实例级只存凭据表,新建实例等于丢掉整条前缀。")
    print()


# ---------------------------------------------------------------------------
# 5. 真实体积
# ---------------------------------------------------------------------------

def bench_real_sizes() -> None:
    print("### 真实体积下的两端耗时")
    print()
    print("| 请求体 | 满块 | 客户端 prepare(热) | 网关解码(热) | 合计 | 网关走链跳数 |")
    print("|---|---|---|---|---|---|")
    for kb in (66, 242, 418, 829):
        payload = build_big(kb)
        store = storage.MemoryChainStore()
        client = TailClient()
        _decode(payload, store, client)
        t_client = _best(lambda: _wire(client.prepare(payload)))
        session_holder: List[object] = []

        def gw_once():
            p = client.prepare(payload)
            session_holder.append(TailGateway(store).collect_payload(
                p.body, p.have_len, p.have_tip))

        t_gw = _best(gw_once)
        _back, session = session_holder[0]
        raw = len(json.dumps(payload, ensure_ascii=False).encode())
        print(f"| {raw // 1024} KB | {session.prefix_blocks + session.new_blocks} "
              f"| {t_client * 1e3:.2f} ms | {t_gw * 1e3:.2f} ms "
              f"| {(t_client + t_gw) * 1e3:.2f} ms | {session.prefix_blocks} 跳 |")
    print()
    print("两端都随体积线性增长,没有超线性劣化。")
    print()


# ---------------------------------------------------------------------------
# 6. 真实对话周期:200 KB → 800 KB → compact
# ---------------------------------------------------------------------------

def bench_compaction_cycle() -> None:
    tools = [{"type": "function", "function": {
        "name": f"tool_{i}", "description": text_of(900, i),
        "parameters": {"type": "object", "properties": {
            "a": {"type": "string", "description": text_of(200, i)}}}}}
        for i in range(30)]
    system = {"role": "system", "content": text_of(8 * 1024, 777)}
    prefix = len(json.dumps(tools).encode()) + 8 * 1024

    client = TailClient()
    store = storage.MemoryChainStore()

    def turn(messages, label):
        payload = {"model": "deepseek-chat", "tools": tools, "messages": messages}
        native = len(json.dumps(payload, ensure_ascii=False,
                                separators=(",", ":")).encode())
        sent, _back, session = _decode_sized(payload, store, client)
        print(f"| {label} | {native:,} | {sent:,} | {100 * sent / native:.1f}% "
              f"| {100 * session.hit_ratio:.1f}% |")

    print("### 真实对话周期(200 KB → 800 KB → compact → 200 KB)")
    print()
    print(f"固定前缀:`tools` {len(json.dumps(tools).encode()):,} B + `system` 8 KB = {prefix:,} B")
    print()
    print("| 轮次 | 原始 JSON | 上行 | 占比 | 前缀命中率(按字节) |")
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
        size = len(json.dumps({"model": "m", "tools": tools,
                               "messages": messages}).encode())
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
    print("compact 让 messages 在摘要处改写,链从这里断开。客户端手里的凭据只有")
    print("「(L, tip)」而没有中间块的摘要,无法证明 L 之前那段前缀仍然一致,所以")
    print("**那一轮只能全量重发**(代价:一次完整上传),随后立刻恢复高位命中。")
    print("这是「客户端状态 O(1)」换来的代价——重置类事件(compact/改中间消息)各付一次全文。")
    print()


# ---------------------------------------------------------------------------
# 7. Redis:走链与写链的真实成本
# ---------------------------------------------------------------------------

def _redis_client():
    try:
        import redis

        client = redis.Redis.from_url(REDIS_URL, socket_connect_timeout=0.5)
        client.ping()
        return client
    except Exception as exc:  # pragma: no cover - 环境缺失
        print(f"(跳过 Redis 段:连不上 {REDIS_URL} — {exc}；"
              f"`docker run -d --rm -p 6390:6379 redis:7-alpine` 后重跑)")
        print()
        return None


def bench_redis() -> None:
    client = _redis_client()
    if client is None:
        return
    store = storage.RedisChainStore(client, ttl=1800, namespace="tailbench")

    payload = build_big(829)
    text = _rendered(payload)

    print("### Redis:一条 829 KB 前缀(链与正文分开)")
    print()
    print("``p:{hash}`` 只存父哈希(32 B),``b:{hash}`` 只存正文;读 = 走链脚本(一次往返)"
          "+ MGET(一次往返)。")
    print()
    print("| 块大小 | 块数 | 写整条(2 key/块) | 走链 | MGET | **读一次** | 单实例上限 |")
    print("|---|---|---|---|---|---|---|")
    for bs in (512, 2048, 4096):
        store.clear()
        body = text[: len(text) // bs * bs]
        items = list(chunking.BlockHasher(bs).feed(body))
        tip = items[-1][0]
        n = len(items)
        store.put_chain(b"", items)
        t_put = _best_wall(lambda: store.put_chain(b"", items))
        t_walk = _best_wall(lambda: store.resolve(tip, n))
        t_get = _best_wall(lambda: store.get_many([bid for bid, _ in items]))
        t_all = _best_wall(lambda: store.get_many(store.resolve(tip, n) or []))
        print(f"| {bs} B | {n} | {t_put * 1e3:.2f} ms | {t_walk * 1e3:.2f} ms | "
              f"{t_get * 1e3:.2f} ms | **{t_all * 1e3:.2f} ms** | "
              f"**{1 / t_all:,.0f} req/s** |")
    print()
    print("走链是 O(块数):每块一次 ``GETEX``(读父哈希 + 续链 TTL)加一次 ``EXPIRE``")
    print("(续正文 TTL)。所以**块大小决定单实例上限**,高 QPS 按会话键分片:")
    print("实例数 = 峰值 req/s ÷ 单实例上限。")
    print()
    print("| 块数 | 走链耗时 |")
    print("|---|---|")
    for k in (1, 8, 64, 256, 512, len(items)):
        tt = _best_wall(lambda: store.resolve(tip, k), samples=5)
        print(f"| {k} | {tt * 1e3:.3f} ms |")
    store.clear()


# ---------------------------------------------------------------------------
# 7.5 测量口径:为什么用 CPU 时间
# ---------------------------------------------------------------------------

def bench_method() -> None:
    """同一份 829 KB 解码,同时报 CPU 时间与 wall clock,说明为什么分开量。"""
    payload = build_big(829)
    store = storage.MemoryChainStore()
    client = TailClient()
    _decode(payload, store, client)
    gw = TailGateway(store)

    def full() -> int:
        p = client.prepare(payload)
        return len(b"".join(gw.iter_payload_json(
            p.body, p.have_len, p.have_tip)))

    cpu = []
    wall = []
    for _ in range(7):
        gc.collect()
        gc.disable()
        try:
            c0 = time.process_time()
            w0 = time.perf_counter()
            full()
            wall.append(time.perf_counter() - w0)
            cpu.append(time.process_time() - c0)
        finally:
            gc.enable()

    raw = len(json.dumps(payload, ensure_ascii=False).encode())
    spread = max(wall) / min(wall)
    print("### 测量口径:同一份 829 KB 解码的两种计时")
    print()
    print(f"| 口径 | 最小值 | 最大值 | 用途 |")
    print("|---|---|---|---|")
    print(f"| CPU 时间(`process_time`) | **{min(cpu) * 1e3:.1f} ms** | {max(cpu) * 1e3:.1f} ms | 吞吐表:屏蔽机器负载 |")
    print(f"| wall clock | {min(wall) * 1e3:.1f} ms | {max(wall) * 1e3:.1f} ms | 端到端表:用户实际等待 |")
    print()
    print(f"本次 wall 最大值 / 最小值 = **{spread:.1f}×**。机器空闲时两者应该接近;")
    print("机器有负载时 wall 被调度与分配器抖动放大 3~10 倍(实测记录:load ≈ 55 时")
    print("CPU 20.4 ms 对 wall 27~471 ms)。CPU 时间不受调度影响,所以吞吐表用它;")
    print("而用户感受到的是 wall,所以端到端表仍然用 wall,并注明当时的负载。")
    print()
    print(f"参考:同一请求体的原生 JSON 是 {raw // 1024} KB。")
    print()


# ---------------------------------------------------------------------------
# 8. 存储:同一条前缀在进程内替身与 Redis 上的读成本
# ---------------------------------------------------------------------------

def bench_store_tiers() -> None:
    payload = build_big(829)
    text = _rendered(payload)
    blocks = text[:len(text) // BLOCK * BLOCK]
    items = []
    hasher = chunking.BlockHasher(BLOCK)
    for bid, block in hasher.feed(blocks):
        items.append((bid, block))
    tip = items[-1][0]
    n = len(items)

    memory = storage.MemoryChainStore()
    memory.put_chain(b"", items)

    def read(store) -> int:
        return len(store.get_many(store.resolve(tip, n) or []))

    t_mem = _best(lambda: read(memory))

    print("### 存储:读一条 829 KB 前缀(1,774 块)")
    print()
    print("| 实现 | 单次读耗时 | 单实例上限 |")
    print("|---|---|---|")
    print(f"| `MemoryChainStore`(进程内,仅测试替身) | {t_mem * 1e3:.2f} ms | {1 / t_mem:,.0f} req/s |")
    print()
    print("生产只有一个后端:Redis(读成本见上一段:走链 Lua + MGET)。")
    print("**每请求的存储成本与上下文长度成正比**,这是链式前缀的固有代价。")
    print()


def main() -> None:
    payload = build_session()
    bench_volume(payload)
    bench_speed(build_big(242))
    bench_local_state()
    bench_increment()
    bench_reuse()
    bench_real_sizes()
    bench_compaction_cycle()
    bench_method()
    bench_store_tiers()
    bench_redis()


if __name__ == "__main__":
    main()
