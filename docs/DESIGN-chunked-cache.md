# Tail 分层 Segment-Merkle 前缀缓存设计文档

| 版本 | 日期       | 作者   | 变更说明                                                 |
|------|------------|--------|----------------------------------------------------------|
| v2.0 | 2026-06-23 | 架构组 | 分层 Chunking + Merkle 增量(初稿)                      |
| v2.1 | 2026-06-23 | 架构组 | 按决策修订:放弃用户空间/quota;system/tools 不强行抽取;引入 segment |

> 本文档是 Tail 网关缓存层的升级设计。前置:
> - 《传输层 KV Cache优化系统设计文档 v1.0》(协议与架构)
> - README.md(当前实现状态)
>
> v1.0 问题:前缀整体存一份,N→N+1 全量重写,跨对话无法复用稳定段。
> v2.1 解决:(1) system/tools/messages 三段独立缓存,**缺失即空,不强行抽取**;
> (2) messages 段按 **segment(一来一回)** 切分 + Merkle 链增量,加一段只增 O(1);
> (3) 内容寻址跨对话复用。

---

## 1. 目标与约束

### 1.1 目标
1. **跨对话复用稳定段**:同一 system / tools 在多个对话只存一份。
2. **增量存储**:对话增长一个回合,只新增一个 segment,**不重写已有前缀**。
3. **跨对话复用回合**:同 agent 同问题,segment 内容寻址命中。
4. **严格保持 messages 数组结构**:还原后字节级一致。
5. **SDK 零改动**:分段逻辑只在网关侧,SDK 仍按 messages 总条数切增量。

### 1.2 硬约束
- **C1**:`messages` 数组**不可拆解或重排**。还原必须字节级一致。
- **C2**:system / tools **缺失即空,绝不强行抽取**(messages 里的 system message 不被移出)。
- **C3**:磁盘 + 分布式 + 低延迟(沿用 v1.0,Kvrocks)。
- **C4**:无用户空间隔离,无 per-api_key 配额限制(按 v2.1 决策放弃)。

### 1.3 非目标
- 不替代服务端 KV Cache。
- 不改变对外 HTTP 协议(`X-Cache-Hash` / `X-Cache-Prefix-Length` 语义不变)。
- 不做用户级隔离 / 配额(全局共享,内容寻址去重)。

---

## 2. 数据模型

### 2.1 请求的三段视图

```
request = {
  system:   string | null     # OpenAI:顶层无此字段 → null
                              # Claude:顶层 system → 取用
                              # **绝不从 messages 里抽取 system**
  tools:    [...] | null      # request.tools;缺失 → null
  messages: [m0, ..., mN-1]   # 完整有序数组,原样保留
}
```

**关键(满足 C1+C2)**:
- system / tools 是**顶层字段**的镜像。OpenAI Chat API 顶层没有 system 字段 → **system 段直接为 null**,即使 messages[0] 是 system message,**也不抽取**。
- messages 数组永远完整、有序、不被修改。

### 2.2 三段独立哈希

| 段        | 哈希函数                              | 缺失时 |
|-----------|---------------------------------------|--------|
| system    | `H(system)`(顶层字段)                | `"0"`  |
| tools     | `H(tools)`                            | `"0"`  |
| messages  | `H(segments[:k])`(Segment-Merkle,§3) | `"0"`  |

**组合 key**:
```
cache_key = sys_hash :: tools_hash :: msg_hash
例:  "a1b2..::c3d4..::e5f6.."
缺失: "0::c3d4..::e5f6.."   (无 system,符合 C2)
       "0::0::e5f6.."        (无 system 无 tools)
```

### 2.3 KV 存储布局

```
sys:    {sys_hash}       → system 全文            # sys_hash != "0" 才存
tools:  {tools_hash}     → tools 全文             # tools_hash != "0" 才存
seg:    {seg_hash}       → segment 全文(JSON)     # 一个回合,Merkle 叶子
pfx:    {pfx_hash}       → Merkle 节点            # 前缀 = segment 序列
meta:   {cache_key}      → {sys_hash, tools_hash, pfx_hash, len, expire_at}
```

`meta:{cache_key}` 是对外入口(客户端拿到的 `X-Cache-Hash`)。

---

## 3. Segment 定义与 Merkle 链

### 3.1 Segment 定义(核心)

一个 segment = **一个 LLM 回合**,形式化:

```
segment = (1 × assistant_message)?
        + (m × tool_result)
        + (n × user_message)
        约束:
          - assistant_message 至多 1 条(回合的回复;首个 segment 可没有)
          - m + n ≥ 1  且  m · n = 0
            (至少有一条非 assistant 消息;且 tool_result 与 user_message 不能同时出现)
```

> **关于 `m · n = 0`**:一个回合要么是工具结果回合(m>0, n=0),
> 要么是用户消息回合(m=0, n≥1),二者互斥。这排除了"同一回合里既有 tool_result
> 又有 user_message"的混合形态——这种混合在标准 LLM 交互流中不会出现:
> tool_result 总是紧贴在 assistant 的 tool_call 之后(同回合),而新的 user_message
> 标志着新的人类输入(下一回合的开始)。

**合法形态**:

| 形态 | 结构 | (m, n) | 场景 |
|------|------|--------|------|
| 首 segment(无 assistant) | `[system?, user]` 或 `[user]` | (0, 1) | 对话开头 |
| 标准 QA 回合 | `[assistant, user]` | (0, 1) | 一问一答 |
| 单工具回合 | `[assistant, tool_result]` | (1, 0) | 单工具调用 |
| 并行工具回合 | `[assistant, tool_result, tool_result]` | (2, 0) | 并行工具 |
| 续写停止 | `[assistant, user, user]` | (0, 2) | 用户连发 / 中断后续写 |

**非法形态(被 m·n=0 排除)**:
- `[assistant, tool_result, user]`(m=1, n=1)→ 这应是**两个** segment:
  `[assistant, tool_result]` + 下一回合的 `[user]`(user 是新回合的开始)

**切分算法(网关侧)**:从 messages[0] 开始顺序扫描:
1. 遇到 `assistant` → 它属于"当前回合的回复",继续吸收
2. 遇到 `tool` / `user` → 当前回合的非 assistant 部分
3. 一旦当前回合已有非 assistant 消息,**下一条 assistant** 触发**新回合开始**
4. messages 扫完 → 最后一个(可能未闭合)回合收尾

```
切分伪码:
segs = []
cur = []
for msg in messages:
    if msg.role == "assistant" and cur_has_non_assistant(cur):
        segs.append(cur); cur = []      # 新回合
    cur.append(msg)
if cur: segs.append(cur)
```

### 3.2 为什么 segment 粒度最优

| 维度 | 逐条 Merkle | Segment Merkle(本设计) |
|------|-------------|--------------------------|
| 还原往返 | O(N) | O(N/seg_size) ≈ O(N/2~3),**省 2-3x** |
| 跨对话复用粒度 | 单条(太细,难命中) | **一个回合**(语义边界,易命中) |
| 增量边界 | 每条 | 每回合(天然对齐 LLM 交互) |
| SDK 感知 | 需要知道每条边界 | **不需要**(SDK 仍按总条数切,§4.4) |

### 3.3 Segment 哈希

```
seg_hash = sha256_hex16( stable_encode(segment) )
```

`stable_encode` 对 segment 内的 messages 按顺序用 v1.0 的定长编码拼接(防边界碰撞)。

### 3.4 Merkle 前缀链

前缀 hash 由 segment 链累积定义:

```
H(prefix_0) = "0"                                          # 空(0 个 segment)
H(prefix_k) = sha256_hex16( H(prefix_{k-1}) || seg_hash_k )   # k >= 1
```

**节点存储**:
```
pfx:{H(prefix_k)} → {
    prev:    H(prefix_{k-1})       # "0" 表根
    seg_ref: seg_hash_k            # 本节点对应的 segment
    seg_count: k                   # 前缀含的 segment 数(冗余校验)
}
```

### 3.5 写入:加一个回合 = O(1)

新回合到来(messages 增长):

1. 切出新 segment → 算 `seg_hash`
2. 若 `seg:{seg_hash}` 不存在 → 写 `seg:{seg_hash}` = segment 全文(内容寻址去重)
3. 算 `new_pfx = sha256_hex16(old_pfx || seg_hash)`
4. 写 `pfx:{new_pfx}` = { prev: old_pfx, seg_ref: seg_hash, seg_count: k+1 }

**永不重写已有节点**。一个回合 = 1 个 segment(可能已存在)+ 1 个几十字节节点。

### 3.6 读:还原前缀 = O(seg_count) 次读 + 批量 mget

```
reconstruct(pfx_hash):
    seg_hashes = []
    cur = pfx_hash
    while cur != "0":
        node = kv.get("pfx:" + cur)
        seg_hashes.push(node.seg_ref)
        cur = node.prev
    seg_hashes.reverse()
    segments = kv.mget(["seg:" + s for s in seg_hashes])   # 批量
    messages = flatten(segments)                            # 保持原顺序
    return messages
```

**批量 MGET** 把 N 次往返压成 1~2 次。100 回合对话还原 < 5ms。

---

## 4. 协议(对外 HTTP)

### 4.1 不变
- 路径、body、`X-Cache-Hash` / `X-Cache-Prefix-Length` 语义全不变
- `X-Cache-Hash` 现在是组合 key(三段拼接),客户端不感知内部结构

### 4.2 请求头(SDK 发)
```
X-Cache-Hash: a1b2..::c3d4..::e5f6..   # 上次的组合 key
X-Cache-Prefix-Length: 12                # messages 前缀总条数(不是 segment 数!)
```

### 4.3 SDK 切分逻辑(详见 §5)

**SDK 完全不感知 segment**。分段、Merkle、内容寻址全部在网关侧。
SDK 侧唯一要做的是:按 messages 总条数切增量,带 `X-Cache-Prefix-Length`。

但"按条数切"本身有正确性陷阱(compact / 多 session 交叉 / 编辑旧消息),
这些风险与修复在 **§5 SDK 前缀一致性保证** 中专门讨论。

### 4.4 命中判定(网关 access 阶段)

```
1. cache_key = req.header["X-Cache-Hash"]
2. meta = kv.get("meta:" + cache_key);  nil → fast_fail 422
3. 逐段验证 + 还原:
   - sys:    meta.sys_hash != "0" ? kv.get("sys:"+sys_hash) : null
   - tools:  meta.tools_hash != "0" ? kv.get("tools:"+tools_hash) : null
   - msgs:   reconstruct(meta.pfx_hash)         # §3.6
4. 任一段缺失/还原失败 → 整体 miss(fast_fail 422)
5. 全命中 → body.system = sys(若有); body.tools = tools(若有);
            body.messages = reconstructed + 增量
6. set_body_data, clear 内部 header, proxy_pass
```

---

## 5. SDK 前缀一致性保证

> 本章独立讨论 SDK(monkey patch)层的正确性问题。这些问题与网关/分段设计正交,
> 但若不处理,会导致**静默数据错误**(比报错更危险)。

### 5.1 问题:仅靠长度匹配前缀是不够的

v1.0 / v2.0 的 SDK 本地缓存(`_CacheState`)存的是**整个旧 messages 数组**,
切增量时仅用 `len()` 判断:

```
if len(messages) >= len(state.prefix_messages):
    increment = messages[len(state.prefix_messages):]   # 假设前 n 条没变
```

这**盲目假设** `messages[:n]` 与缓存的前缀完全一致。一旦假设被破坏,SDK 仍发
旧 hash + 错误的增量 → 网关按旧 hash 还原前缀 + 拼增量 = **错误的完整 messages 转发后端**。

### 5.2 四种破坏场景

| 场景 | 描述 | 当前行为 | 严重度 |
|------|------|---------|--------|
| 正常追加 | 单 session 末尾加消息 | ✅ 正确 | — |
| **compact** | SDK / 应用层压缩历史(删中间、合并、摘要替换) | ❌ 静默错误转发 | **高** |
| **编辑旧消息** | 用户改了已发的某条 message | ❌ 静默错误转发 | **高** |
| **重排** | messages 顺序被调换 | ❌ 静默错误转发 | **高** |
| **多 session 交叉** | 同一 client 实例串行/并发跑多个对话 | ❌ 增量切错 | **高** |

四种"高"指向同一根因:**长度相等 ≠ 内容一致**,必须校验内容。

### 5.3 修复 1:切分前校验前缀指纹(必须做)

切增量前,校验 `messages[:n]` 的指纹等于缓存的前缀指纹,而非盲目信任长度:

```python
class _CacheState:
    prefix_count:  int     # n,前缀长度
    prefix_digest: str     # H(messages[:n]) 的指纹,几十字节(替代存整个数组)
    cache_hash:    str
    expire_time:   float

def try_slice(state, messages):
    if not state.is_valid():
        return None
    n = state.prefix_count
    if len(messages) < n:
        return None
    # 关键:指纹校验,而非仅长度
    if H(messages[:n]) != state.prefix_digest:
        # 前缀变了(compact/编辑/重排)→ 放弃增量,降级为发全量
        return None
    return {
        "increment": messages[n:],
        "prefix_length": n,
        "cache_hash": state.cache_hash,
    }
```

**收益**:
- compact / 编辑 / 重排全部降级为"发全量"(正确,只是没省带宽),**不再静默错误**
- `prefix_digest` 只存几十字节,**替代存整个 prefix_messages 数组**,内存也省了
- 指纹算法与网关侧 system/tools hash 一致:`sha256_hex16(stable_encode(messages[:n]))`

> 注:指纹校验在 SDK 本地做(O(n) 哈希计算,微秒级),不增加网关负担。
> 网关侧无需改动 —— 它本来就按 hash 还原,SDK 发全量时 hash 重新计算即可。

### 5.4 修复 2:多 session 隔离(建议做)

同一 openai client 实例可能服务多个对话(典型:web 后端一个 client 池,每次请求不同用户)。
当前 key = `(model, base_url)` 是全局单一 `_CacheState`,多 session 交叉会互相覆盖。

**session_id 的来源**(openai SDK 无 session 概念,三个选项):

| 方案 | 怎么拿 session_id | 优缺点 |
|------|-------------------|--------|
| **A. 显式 API** | `openai_patch.set_session("sess1")` 调用方手动设 | 简单可靠,但要求改代码(破坏零改动) |
| **B. messages 指纹自动** | 不隔离,靠修复 1 的指纹校验兜底 | 零改动,但同线程串行切 session 会频繁降级全量 |
| **C. 并发上下文局部** | `threading.local` / `contextvars`,每线程/协程独立缓存 | 零改动,覆盖"多 session = 多并发"的主流场景 |

**推荐 C + B 组合**:
- **C** 处理"多线程/多协程各跑各的 session"(最常见的服务端形态)
- **B**(修复 1 的指纹校验)作为最终安全网:即使 C 没覆盖(比如同线程串行跑多 session),
  也只会降级为发全量,**不会出错**

```python
import threading, contextvars

# 每个并发上下文一份独立缓存表
_tls = threading.local()
_ctx_cache = contextvars.ContextVar("kvcache_sessions", default=None)

def _get_table():
    # 优先 contextvars(协程友好),回退 threading.local
    tbl = _ctx_cache.get()
    if tbl is None:
        tbl = getattr(_tls, "table", None)
        if tbl is None:
            tbl = {}
            _tls.table = tbl
    return tbl
```

这样保持 SDK **对外零改动**,所有错误场景都变成"不省带宽"而非"数据错误"。

### 5.5 修复后的不变量

修复后,无论 SDK 上层怎么折腾 messages,系统保证:

| 上层行为 | SDK 行为 | 网关收到 | 后端收到 | 正确性 |
|---------|---------|---------|---------|--------|
| 正常追加 | 发增量 | hash + 增量 | 完整正确 | ✅ |
| compact | 发全量 | 全量(无 hash) | 完整正确 | ✅ |
| 编辑旧消息 | 发全量 | 全量 | 完整正确 | ✅ |
| 重排 | 发全量 | 全量 | 完整正确 | ✅ |
| 多 session 并发 | 各上下文独立增量 | 各自 hash + 增量 | 各自完整正确 | ✅ |
| 多 session 串行(同上下文) | 指纹不匹配 → 发全量 | 全量 | 完整正确 | ✅ |

**核心保证:最坏情况是"不省带宽",永远不会"数据错误"。**

### 5.6 与分段设计的关系

修复 1、2 都在 SDK 侧,与 §3 的 segment/Merkle 设计**正交**:
- SDK 仍按 messages 总条数切(不感知 segment)
- 网关拿到完整/增量 messages 后,自己切 segment、算 Merkle
- SDK 的指纹校验保证"发出去的增量确实是对应前缀的延伸",网关才能正确还原

## 6. 请求处理流程

### 6.1 写入(log_by_lua,异步)

请求 2xx 后:
```
1. 镜像顶层字段:sys = body.system (有则取); tools = body.tools (有则取)
2. 算三段 hash:
     sys_hash    = sys   ? sha256_hex16(sys)   : "0"
     tools_hash  = tools? sha256_hex16(tools) : "0"
     segments    = split_into_segments(messages)         # §3.1
     pfx_hash    = merkle_chain(segments)                # §3.4
3. 写(若不存在):
     sys:{sys_hash}    = sys        (sys_hash != "0")
     tools:{tools_hash}= tools      (tools_hash != "0")
     for seg in segments:
         seg:{H(seg)} = seg         (内容寻址去重)
     for node in chain: pfx:{H(node)} = node
4. cache_key = sys_hash :: tools_hash :: pfx_hash
5. meta:{cache_key} = {sys_hash, tools_hash, pfx_hash, len=#messages, expire_at}
6. X-Cache-Hash = cache_key
```

### 6.2 还原(access_by_lua,同步)

见 §4.4。

### 6.3 降级与容错
- 任一 KV 失败 → miss → fast_fail 422
- Merkle 链中间节点过期 → 该 pfx 无法还原 → miss
- cjson 解析失败 → miss

---

## 7. 淘汰与容量管理

### 7.1 TTL 分级
| key | TTL | 理由 |
|-----|-----|------|
| `meta:` | 短(1-2h) | 入口,热度高 |
| `sys:` / `tools:` | 长(24h) | 稳定,复用价值高 |
| `seg:` | 长(24h) | 跨对话去重核心 |
| `pfx:` | 中(6h) | 每对话独有 |

### 7.2 容量 LRU(全局,无用户配额)
按 v2.1 决策:**放弃 per-api_key 配额**,改用全局容量 LRU:
- Kvrocks 设 `maxmemory` + `maxmemory-policy=allkeys-lru`
- 各 key 类型用 §6.1 的 TTL 自然过期;超容量时 LRU 驱逐

### 7.3 seg 池 GC
`seg:` 被多个 pfx 引用,采用**容量 LRU**(简单,Kvrocks 原生支持),
不做引用计数(避免写放大)。

---

## 8. 存储节省量化

### 8.1 单对话增长(v1.0 vs v2.1)

对话从 0 增长到 N 条 message(平均 segment 长度 k≈2.5):

| | v1.0(整体存) | v2.1(Segment-Merkle) |
|--|--------------|----------------------|
| 写入量 | Σ(i=1..N) i·msg_size = O(N²·msg_size) | N·msg_size + (N/k)·50B = O(N·msg_size) |
| N=100,msg=500B | ~2.5MB | ~50KB |
| **节省** | — | **~50x** |

### 8.2 跨对话复用(同 agent 服务多用户)

1000 个对话共享 8KB system + 2KB tools:

| | v1.0 | v2.1 |
|--|------|------|
| system 存储 | 8KB × 1000 = 8MB | 8KB × 1 = **8KB** |
| tools 存储 | 2KB × 1000 = 2MB | 2KB × 1 = **2KB** |
| 同问题 segment | 每对话各存 | **内容寻址命中,1 份** |

### 8.3 segment 粒度的隐藏收益

两个用户问同 agent "你好":
- v1.0 / 逐条 Merkle:无复用
- v2.1 segment:`seg:{H([user:"你好"])}` 内容相同 → **命中**,省一个回合的存储 + 后续前缀 hash 相同

---

## 9. 安全性
| 风险 | 缓解 |
|------|------|
| 哈希碰撞 | SHA-256 截 64bit,生日碰撞需 ~2³² 条;工程忽略 |
| 前缀劫持 | seg/pfx hash 由 server 算,客户端无法伪造 |
| 跨用户信息泄漏 | 按决策 v2.1 **不做隔离**,全局共享内容寻址去重(若需隔离,meta 加 api_key 前缀即可,§10) |

---

## 10. 与现有实现的关系

### 10.1 兼容共存
- v2.1 用新 key 前缀(`sys:`/`tools:`/`seg:`/`pfx:`/`meta:`)
- v1.0 用 `prefix_cache:` 前缀
- 按 `X-Cache-Hash` 是否含 `::` 区分版本

### 10.2 SDK 零改动
SDK 切分逻辑不变(按 messages 总条数),`X-Cache-Hash` 存的是组合 key 字符串,
SDK 不解析内部结构。

### 10.3 落地范围
- `openresty/lua/kvcache/segment.lua` —— segment 切分(§3.1)
- `openresty/lua/kvcache/merkle.lua` —— Merkle 链(§3.4)
- `openresty/lua/kvcache/chunked_store.lua` —— 分段存储(§5)
- (Python 参考版 segment/merkle/chunked_store 已从仓库移除,算法见本设计文档 §3)
- 网关 `gateway.lua`:access/log 分支(检测 `::` 走 v2.1)
- 测试重点:(a) segment 切分覆盖所有合法形态;(b) 加一段只增 O(1);(c) 还原字节级一致;(d) 跨对话 segment 复用

---

## 11. 开放问题

| 问题 | 选项 | 建议 |
|------|------|------|
| segment 还原:回溯 vs 跳表 | 回溯 O(N/k) / 跳表 | 默认回溯,长对话开跳表 |
| seg 池 GC | 容量 LRU / 引用计数 | 容量 LRU(已定) |
| cache_key 是否暴露分段 | 明文三段 / 整体 hash | 明文三段(便于调试) |
| 跨用户是否隔离 seg 池 | 共享(已定)/ 隔离 | 共享;若需安全,meta 加 api_key 前缀 |

---

## 12. 总结

v2.1 在不破坏 messages 数组、不改变对外协议、SDK 零改动的前提下,通过:
1. **三段独立 hash**(system/tools/messages),system/tools **缺失即空,不强行抽取**;
2. **Segment-Merkle 链**,按 LLM 一来一回的语义边界切分,加一段只增 O(1);
3. **内容寻址**,跨对话复用 system/tools/segment;
4. **全局容量 LRU**,放弃 per-api_key 配额(按决策)。

相对 v1.0:存储节省 ~50x(长对话),跨对话复用率显著提升,且 SDK 零改动、可平滑共存。
下一步:按 §9.3 落地。
