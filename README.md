# Tail —— 给 LLM 请求做 tail -f

> *Send the tail, the head is cached.*

日志没人每次都 `cat` 全文，只盯新增的那几行。**Tail 对 LLM 请求做同一件事**：
前缀在网关里已经有了，客户端只发增量。

实现上它是一层「文本去重传输层」，插在 SDK 和上游网关之间。客户端把每轮重复的
`messages` 前缀换成**请求头里的一个凭据**，请求体只剩新增的那一小段。
**热请求上行省 99%，5 Mbps 链路上 TTFT 快 55~60 倍**，而上游收到的 prompt 与原来逐字节相同。

那既然缓存命中率已经 99%+，为什么我每一轮还要把重复的东西全部传一遍？

## 为什么值得做

缓存省掉的是**上游的算力**，省不掉我的**上行带宽**和**等待时间**：SDK 每一轮
仍然把完整 `messages` 序列化、打包、上传一次。重复的东西，公网上再走一遍。

所以网关这一跳值得优化。注意收益落在**两拨人**身上，不要混为一谈：
**用户省时间，服务商省容量（或钱）。** 节省 TTFT 就是在珍惜生命。

### 对用户：省的是时间，不是钱

要说清楚：**那点上行流量对用户根本不是成本**。800 KB × 20 轮 = 16 MB，按公网价
也就一分多钱，没人会为这个换方案。

用户真正有感的只有一件事：**每次请求要等多久**。而请求「发完」是 TTFT 的前置段——
链路上行越窄，这段等待越明显。限速模拟公网实测：

| 上行带宽 | 原生 | Tail | 每轮省下 |
|---|---|---|---|
| 10 Mbps | 740 ms | **25 ms** | 0.72 s |
| 5 Mbps | 1,468 ms | **24 ms** | 1.44 s |
| 2 Mbps | 3,650 ms | **23 ms** | 3.63 s |

一个每天 200 轮的开发者：10 Mbps 下一天少等 **2.3 分钟**，一年 **约 14 小时**；
2 Mbps（移动网络、跨境链路）下一年 **约 72 小时**。

**上行少两个数量级，等待就少一个数量级。** 这是 Tail 对用户的全部价值。

### 对服务提供商：同样的带宽，能多撑两个数量级的人

带宽成本落在**服务提供方**这一侧——入口流量、跨区链路、带宽峰值容量，
都是按 GB 和带宽峰值计费的。

**先明确「一轮」是什么**：上行（把这轮的 messages 传上去）→ 推理（到首 token）→
流式回传 → 可能的工具调用 → 下一轮。所以同一个会话相邻两次请求的间隔 **T**
不是「思考 10 秒」，是**整个轮周期**，通常 20~60 秒，带工具调用会更长。
上行只占其中一小段，但它**卡在关键路径最前面**：prefill 必须等 body 收完才能开始。

于是每个会话的请求速率 = 1/T，入口的平均上行 = **同时在线会话数 × 单轮体积 / T**。
关键差别在于「单轮体积」：

- 原生 SDK：每轮把**整个上下文**（800 KB）推上去。
- Tail：每轮只推**这一轮新增的内容**——纯对话约 2 KB，带工具结果约 10 KB。

| 同时在线会话 | 轮周期 T | 请求速率 | 原生平均上行 | Tail（2 KB/轮） | Tail（10 KB/轮） |
|---|---|---|---|---|---|
| 1 万 | 30 s | 333 req/s | 2.1 Gbps | **5 Mbps** | 27 Mbps |
| 1 万 | 60 s | 167 req/s | 1.1 Gbps | 2.5 Mbps | 13 Mbps |
| 10 万 | 30 s | 3,333 req/s | **21 Gbps** | **53 Mbps** | 267 Mbps |
| 10 万 | 60 s | 1,667 req/s | 11 Gbps | 27 Mbps | 133 Mbps |

**比值约 400 倍（纯对话）到 80 倍（带工具结果）**，而且上下文越长这个倍数越大——
原生上行随上下文线性涨，Tail 的上行只跟「每轮新增」有关。10 万在线、30 秒一轮的
原生要 **21 Gbps**：那是几十个 100G 口、跨区专线和运营商带宽包的账，而且得按峰值签；
Tail 只要 **53 Mbps**，一个 1G 口用掉 5%。

折算成账单：这一档原生上行 **230 TB/天、约 6.9 PB/月**，一个月省下约 **6.9 PB**——
按大客户价 ¥0.10/GB 约 **¥69 万/月**，按按量价 ¥0.80/GB 则是 ¥550 万/月。
两档都列是有意的：PB 级流量不可能按按量价结算，真实采购是包年保底或阶梯价；
量越大单价越低，但省下的绝对值都落在**百万/月**这一档。

**存储不是对价**：块只存每轮真正的新内容（纯对话约 2 KB/轮，带工具结果约 10 KB/轮），
这一档每天约 0.6~2.9 TB，配 3 天 TTL 后占用 **1.7~8.6 TB**，
ESSD 约 **¥860 ~ ¥4,300/月**——相对月省 ¥69 万是零头。存储会随 TTL 自动回收，
上行流量不会。

**一个诚实的推论**：瓶颈会**从带宽转到网关 CPU**。上行省 400 倍之后，网关要多做
「切块、父链哈希、走链取块、反渲染、重建 JSON」——按实测 888 KB 上下文约 **20 ms CPU**
（`tools/bench.py`），3,333 req/s 就是 **约 70 核**；上下文均值按 400 KB 算是 35 核左右。
这比推理集群小，但已经不是零：它意味着网关要按「CPU 密集服务」来扩容，而不是
「转发代理」。L1 块缓存（进程内）与按会话分片是标配，不是优化项。

**所以这是个双边收益的东西**：用户拿走时间，服务商拿走容量（或成本）。
任何一方单独看，都不足以支撑这个复杂度；两边加起来才成立。

### 代价（诚实版）

| 项 | 量 |
|---|---|
| 客户端额外 CPU | 0.8 ms / 242 KB 请求（热路径要重算整条前缀的父链哈希做自检） |
| 网关解码 CPU | 20 ms / 888 KB 上下文（线性，约 45 MB/s/核） |
| 固定引用开销 | **≈ 0**：线上没有逐块帧头，热请求只剩不满一块的尾巴（≤511 B） |
| 前缀变更（正常行为，不算代价） | compact / 改中间消息 / 改参数那一轮全文重发一次（实测 280 KB）；agent 本来就常做这些，传完当轮立即恢复增量 |
| 回环上反而更慢 | 25 ms vs 3.7 ms —— 回环没有带宽瓶颈，收益只在真实链路上出现 |
| 部署复杂度 | 网关从「转发代理」变成「有状态缓存层」：要 Redis、要 TTL、要 L1 容量规划 |

## 它解决什么问题

一个 100 轮、每轮 1K token 的对话，到第 100 轮时请求体约 400KB，其中 99% 是前 99 轮
已经传过的内容。Tail 把这部分换成了请求头里的一个凭据：`X-Tail-Have: 359424;<16 字节哈希>`。

实测（20 轮中文对话，每轮追加一轮问答，`tools/bench.py`）：

| 口径 | 数 |
|---|---|
| 后 10 轮平均上行 | **880 B** |
| 同一上下文冷启动 | 10,198 B |
| 末轮上行 / 上下文长度 | 858 B / 12,610 B（6.8%） |

关键性质：**上下文线性增长，线上体积基本持平。** 38 KB 的上下文热请求只剩 **374 B**、
908 KB 的只剩 **315 B**——都是「不满一个 512 B 块」的那截尾巴。

## 怎么做的

```
客户端                                          网关
──────────────────────────────────────────────────────────────
messages JSON
  │ ① 渲染器（懂 LLM 语义，确定性）
  ▼
一维文本流
  │ ② 前缀自检：边渲染边算父链哈希，走到已知 (L, H) 就停
  ▼
X-Tail-Have: L;H  +  body = 文本第 L 字节之后的部分
────────────────── 公网（只剩这一段）──────────────────→
                                            │ ③ 切块 + 父链哈希（**唯一权威**）
                                            │ ④ 按 (L,H) 从 Redis 走链取前缀，
                                            │    新块连同父链一次原子写回
                                            ▼
                                       一维文本流
                                            │ ⑤ 反渲染器（懂 LLM 语义）
                                            ▼
                                       messages JSON → 上游推理 API
```

职责单一，可独立替换：

| 层 | 模块 | 输入 → 输出 | 懂 LLM 语义？ |
|---|---|---|---|
| 渲染器 | `render.py` | messages JSON → 一维文本 | ✅ |
| 前缀客户端 | `codec.PrefixClient` | 文本 + 凭据 → 尾部字节 | ❌ |
| 前缀读取器 | `codec.PrefixReader` | 前缀 + 尾部字节 → 完整文本（**在这里切块、算哈希**） | ❌ |
| 反渲染器 | `render.py` | 一维文本 → messages JSON | ✅ |
| 分块与哈希 | `chunking.py` | 字节流 → (块哈希, 块) | ❌ |
| 链式存储 | `storage.py` | 链尾 + 块数 → 块列表 / 块内容 | ❌ |

**中间几层完全不知道聊天协议。** 要支持 Anthropic Messages 或 Responses API，
只需写一个新的 `render` 模块，其余一行都不用改。

## 三条不变式

1. **渲染必须确定性** —— 同一份 messages 必须渲染成逐字节相同的文本，否则
   块内容变化、`block_hash` 变化、缓存整体失效。实现上：字段顺序固定、
   `json.dumps(sort_keys=True)`。格式版本走 HTTP 头，不写进正文。
2. **哈希只在网关算** —— 客户端一个哈希都不发，只发「我认为你已经有哪些」（`L;H`）。
   网关自己切块、自己算父链、自己写存储。所以「客户端投毒污染别人的块」在结构上
   不可能发生：客户端发错最多落在一棵无关的链上，等价于 miss。
3. **缓存缺失只退化，不报错** —— 前缀不完整时网关立刻中断上游请求、回 409 +
   `X-Tail-Retransmit: 1`，客户端丢掉凭据重发全文一次，对调用方透明。

## 为什么不用原 JSON 直接传

**完整协议、管线与设计动机见 [`docs/TRANSPORT-PIPELINE.md`](docs/TRANSPORT-PIPELINE.md)。**

要点：

- **JSON 是树，块边界没有自然落点。** 渲染成固定格式文本后是一维字节流，
  块边界按固定 `block_size` 对齐即可（vLLM 的做法）。
- **前缀缓存消不掉新增消息里的结构字节。** 每条新消息都带 `{"role":"`, `","content":"`
  这些零信息开销。不过要诚实：正文很长的对话里这只占个位数百分比（实测长对话
  40,141 B → 38,262 B，只省 4.7%）。**渲染的真正价值是把树摊平成一维字节流，
  让块能按固定间隔切、跨轮复用**，不是省那点标点。
- **尾部增量只能处理追加。** compact、编辑旧消息、重排、中间插入都会导致整条前缀
  失效；分块去重下只重传受影响的块。

## 快速开始

```bash
pip install -e ".[test]"

# 起网关（不填 --redis 就用进程内存储，重启即丢，只适合开发）
docker run -d --rm -p 6379:6379 redis:7-alpine
python -m tail.serve --backend https://api.deepseek.com --port 8765 \
    --redis redis://127.0.0.1:6379/0 --ttl 1800
```

客户端：**现有的 `openai` 代码不用改请求逻辑**，只多传一个 `http_client`。

```python
import httpx2
from openai import OpenAI
from tail.transport.client import TailTransport

client = OpenAI(
    base_url="http://127.0.0.1:8765/v1",
    api_key="sk-xxx",
    http_client=httpx2.Client(transport=TailTransport()),   # ← 唯一新增
)

# 下面和原生用法一模一样，流式也一样
for event in client.chat.completions.create(
        model="deepseek-chat", messages=messages, stream=True):
    print(event.choices[0].delta.content or "", end="")
```

异步用 `AsyncOpenAI` + `httpx2.AsyncClient(transport=TailAsyncTransport())`。

### 客户端只记凭据，不缓存内容

本地状态是「会话键 → 若干 `(L, H)` 凭据」，每条 **24 字节**（8 B 长度 + 16 B 哈希），
每会话最多留 4 条（MRU，容忍并发乱序的响应）。**不留正文副本，也不留块哈希表。**

实测（800 KB 上下文，1,774 个满块）：

| | |
| --- | --- |
| 客户端本地状态 | **96 B**（30 轮会话，4 条凭据封顶） |
| 冷启动上行 | 908,603 B（就是渲染文本本身；同体积 JSON 是 909,206 B） |
| 全命中后上行 | **315 B**（只剩不满一块的尾巴：908,603 mod 512） |
| 每轮新增一条提问时上行 | 约 0.4~1 KB（就是那条新提问） |

三点要注意：

1. **必须复用同一个 client 实例**——凭据挂在 transport 上，每轮新建 `OpenAI(...)`
   等于永远冷启动（6 轮合计 14,109 B vs 5,405 B，差 62%）。
2. **复用是前缀形状的**——块哈希沿用 vLLM 的父链 `hash(parent, block)`，纯追加几乎
   免费；但要**客户端能证明**旧前缀没变：它只持有 `(L, H)` 而没有中间块摘要。
   所以前缀本身一变（compact、改中间消息、改顶层参数）这一轮就全文重发一次
   （实测 280 KB）——这是**正常行为**，agent 本来就会频繁做这些，传完当轮立刻恢复增量。
3. **服务端是权威**——`X-Tail-Have` 是「我认为你有」，不是「你必须信」。网关按
   `(L, H)` 走链，走不通就回 409 让客户端重发。客户端不会因为猜错而拼出错误前缀。

`TailTransport` 只拦截 `/chat/completions` 的 JSON 请求，其余请求（`/models`、
embeddings、文件上传……）原样透传；网关回 `X-Tail-Retransmit` 时它自动丢掉该会话
凭据并重发一次全量，对上层透明。

完整可运行示例见 [examples/chat.py](examples/chat.py)（原生）与
[examples/chat_tail.py](examples/chat_tail.py)（接入 Tail），两者 diff 只有 3 行。

网关也接受普通 `application/json` 请求并原样转发，便于灰度对比与排障。

### 依赖说明：为什么是 `httpx2`

`httpx` 稳定版停在 0.28.1（后续只有 `1.0.dev*` 预发布），继任包是
[`pydantic/httpx2`](https://github.com/pydantic/httpx2)（同一作者 Tom Christie）。
`openai` 3.x 已依赖 `httpx2`，Starlette 1.10 的 `TestClient` 也优先用 `httpx2`
并对 `httpx` 报弃用警告。所以本项目统一用 `httpx2`，客户端与网关只有一套 HTTP 栈。

`TailTransport` 本身与库无关：它按请求对象所属的库自动选择内层 transport，
因此仍在用 `httpx` 的 `openai` 1.x 也能直接接。

### 直接用底层 API（不用 SDK）

```python
from tail.transport.gateway import TailClient

client = TailClient()
prepared = client.prepare({"model": "deepseek-chat", "messages": messages})
resp = httpx2.post(
    "http://127.0.0.1:8765/v1/chat/completions",
    content=prepared.body,                      # 生成器，边渲染边发
    headers={"content-type": "application/x-tail-v3",
             "x-tail-session": prepared.session.hex(),
             "x-tail-have": (f"{prepared.have_len};{prepared.have_tip.hex()}"
                             if prepared.have_len else ""),
             "authorization": f"Bearer {api_key}"},
)
client.update_from_headers(prepared.session, resp.headers)   # 拿回下一轮凭据
if resp.headers.get("x-tail-retransmit"):
    client.invalidate()                        # 前缀失效，下一轮重发全文
```

## 线上格式

`Content-Type: application/x-tail-v3`。**body 就是渲染文本的一段原始字节**——
没有帧、没有逐块元数据。

```
POST /v1/chat/completions
content-type: application/x-tail-v3
x-tail-session: 3f2ab0c9…                会话键 = 渲染文本首块的块哈希（16 B）
x-tail-have: 46208;9c1e77d3…             可选：我认为你已有前 46,208 B，链尾是 9c1e…

<body>                                   渲染文本第 L 字节之后的全部字节

200 OK
x-tail-version: 3
x-tail-have: 51456;71b0c4ae…             下一轮凭据（服务端是权威）
x-tail-status: hit                       hit / miss / degraded
x-tail-hit: 0.998                        本轮前缀命中率（按字节）
```

前缀失效时回 `409` + `x-tail-retransmit: 1`，客户端丢掉凭据重发全文一次。

为什么没有帧：v2 里每块 17 B 引用帧存在的唯一理由是「切块与哈希在客户端，
网关必须被告知块边界」。把切块与哈希搬到网关之后，这个理由消失了——
全命中的线上体积从 1,774 × 17 B ≈ **30 KB** 降到 **315 B**，
固定开销从「上下文 3.3%」降到 **≈ 0**。

## 分块与块哈希

**完全照搬 vLLM Automatic Prefix Caching**
（[设计文档](https://docs.vllm.ai/en/v0.8.3/_sources/design/v1/prefix_caching.md)），
没有自创算法：

```
block_hash = hash(父块哈希, 本块字节)
```

- **定长块**，块边界只由位置决定，与内容无关。
- **哈希链**：父哈希让相同内容挂在不同前缀下得到不同哈希，这是"前缀"语义的来源。
- **只缓存满块**（vLLM: "We only cache full blocks"），尾块不入存储。

DeepSeek 侧对应的是"按固定 token 间隔落盘"
（[上下文硬盘缓存](https://api-docs.deepseek.com/zh-cn/guides/kv_cache/)），
同样是固定间隔。

参数：`BLOCK_SIZE = 512` 字节 / `BLOCK_ID_SIZE = 16` 字节 / blake2b。

**`BLOCK_SIZE` 为什么不是 vLLM 的 16 token（≈48 B）**：块越小，走链跳数越多，
而每次读前缀都要从链尾一跳一跳走到链头。829 KB 上下文按 512 B 切是 **1,774 跳**，
按 48 B 切就是 **17,706 跳**——存储读成本差 10 倍，换来的只是「尾巴少重发 400 字节」。

| block_size | 48 B | 128 B | 256 B | **512 B** | 1024 B | 4096 B |
|---|---|---|---|---|---|---|
| 829 KB 的块数（走链跳数） | 17,706 | 6,640 | 3,320 | **1,774** | 887 | 221 |
| 每轮重发的尾块上限 | 47 B | 127 B | 255 B | **511 B** | 1,023 B | 4,095 B |

512 是在「尾块浪费 ≤ 0.06%」与「走链跳数可控」之间的折中。

哈希用 blake2b 而非 `hash()`：Borg 明确要求块 id 必须是密码学强哈希，
且内置 `hash()` 带进程随机盐，不能跨进程复用缓存。

## 存储：链和内容在同一个 value 里

因为 `block_hash = hash(父哈希, 本块)`，**链尾摘要等价于整条前缀的入口**：

```
blk:{block_hash} → parent(32 B hex) ‖ content(≤ 512 B)

H → block[1773] → parent → block[1772] → ... → block[0] → 链根
```

给定链尾 `H` 和前缀块数，从后往前走就能还原整条前缀，不需要单独的索引 key。
这是 vLLM 的 `block_hash = hash(parent_hash, block)` 原样搬到字节层——
差别只在链放在哪：vLLM 放进程内存的 block table，这里放存储，好让多个网关实例共享。

**读写都是 Lua，保证原子性**：

- **写**：一个脚本写完整串块（连同 TTL）。否则网关只写了一半就把新 tip 告诉客户端，
  对端拿到悬挂的链，表现为「刚发过的前缀立刻 miss」。
- **读**：一个脚本完成「走链 + 整条续期」。per-block TTL 下读和续期必须是同一件事，
  否则出现「读到一半某块过期」的中间态；只在写入时设 TTL 则长会话的头部块先死，
  而任何前缀都必须从头部开始，整条作废。

三个后端（`make_store(url)` 自动拼）：

- `MemoryChainStore` —— 进程内，重启即丢（默认，测试用）
- `RedisChainStore` —— 生产：链式 value + 两个 Lua 脚本（`pip install redis`）
- `CachedChainStore` —— 进程内 LRU 包在任意后端外（**L1**）

**L1 不是优化项，是前提**：一条 829 KB 前缀读一次要 1,774 跳。Redis 单实例的吞吐要按
**块**算——实测约 **15~21 万块/秒/实例**，所以「每条请求都读完整前缀」的只读上限随上下文
线性变差：

| 上下文 | 66 KB | 242 KB | 418 KB | 829 KB |
| --- | --- | --- | --- | --- |
| 单 Redis 实例（全量前缀） | 1,106 req/s | 408 req/s | 208 req/s | **90~140 req/s** |
| L1 命中（进程内） | — | — | — | **2,900~8,900 req/s** |

那是**全量前缀、每轮必读**的假设，不是系统容量。规划按未命中率算：

```
所需 Redis 实例数 ≈ 目标 req/s × 每请求块数(上下文/512) × L1 未命中率 ÷ 200,000
```

10 万在线、30 秒一轮（3,333 req/s）、829 KB 前缀：未命中 **1% → 0.3 个实例**，
10% → 3 个，**完全不用 L1 → 30 个**。热会话命中 L1 后根本不碰 Redis，Redis 退化成
持久层与跨实例共享层——这就是「差 25~40 倍」的真实含义。

## 测试

```bash
python -m pytest tests/ -v
```

92 个测试（含 11 个 Redis 真机用例，连不上 Redis 时自动 skip），覆盖分块与前缀复用、
渲染确定性、前缀自检、链式存储、Lua 写链/走链/续期、客户端 → ASGI 网关 → mock 上游
的端到端链路（含 409 降级与恢复），以及用真实 `openai` SDK 经 `TailTransport`
接入的回归测试。
