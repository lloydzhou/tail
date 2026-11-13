# Tail —— 在 SDK 与上游之间插入一层「文本去重传输层」

> 多轮对话里，每一轮都要把**完整上下文**重新发一遍给上游。上下文越长、轮次越多，
> 上行带宽浪费越大。Tail 在公网这一跳上只传「引用 + 新增内容」。

## 它解决什么问题

一个 100 轮、每轮 1K token 的对话，到第 100 轮时请求体约 400KB，其中 99% 是前 99 轮
已经传过的内容。Tail 把这些重复内容换成了 16 字节的引用。

实测（20 轮中文对话，每轮追加一轮问答）：

| 轮次 | 上下文文本 | 线上体积 | 占比 |
|---|---|---|---|
| 1（冷启动） | 2.5 KB | 2.5 KB | 100% |
| 5 | 5.5 KB | 1.7 KB | 30% |
| 10 | 9.2 KB | 1.8 KB | 20% |
| 20 | 16.7 KB | 2.1 KB | 13% |

关键性质：**上下文线性增长，线上体积基本持平。**

## 怎么做的

```
客户端                                        网关
─────────────────────────────────────────────────────────────
messages JSON                                 帧流
  │ ① 渲染器（懂 LLM 语义）                      │ ② 解码器（只认字节）
  ▼                                             ▼
一维文本流                                     一维文本流
  │ ② 编码器（只认字节）                          │ ① 反渲染器（懂 LLM 语义）
  ▼                                             ▼
帧流 ─────────── 公网（体积最小的一段）────────→ messages JSON
                                                   │
                                                   ▼
                                                上游推理 API
```

四层，职责单一，可独立替换：

| 层 | 模块 | 输入 → 输出 | 懂 LLM 语义？ |
|---|---|---|---|
| 渲染器 | `render.py` | messages JSON → 一维文本 | ✅ |
| 编码器 | `codec.py` | 一维文本 → 帧流 | ❌ 只认字节 |
| 解码器 | `codec.py` | 帧流 → 一维文本 | ❌ 只认字节 |
| 反渲染器 | `render.py` | 一维文本 → messages JSON | ✅ |
| 分块 | `chunking.py` | 字节 → 块（定长） | ❌ |
| 存储 | `storage.py` | 块集合 | ❌ |

**中间三层完全不知道聊天协议。** 要支持 Anthropic Messages 或 Responses API，
只需写一个新的 `render` 模块，其余一行都不用改。

## 三条不变式

1. **渲染必须确定性** —— 同一份 messages 必须渲染成逐字节相同的文本，否则
   块内容变化、`block_hash` 变化、缓存整体失效。实现上：字段顺序固定、
   `json.dumps(sort_keys=True)`。格式版本走 HTTP 头，不写进正文。
2. **中间层无语义** —— 编码器/解码器不知道哪个字节是 role。
3. **缓存缺失只退化，不报错** —— 取不到块时响应带 `X-Tail-Retransmit: 1`（状态码不变），
   客户端清空本地已知块集合后重发全文。存储全空、服务重启时系统仍能正常工作。

## 为什么不用原 JSON 直接传

**完整协议、管线与设计动机见 [`docs/TRANSPORT-PIPELINE.md`](docs/TRANSPORT-PIPELINE.md)。**

要点：

- **JSON 是树，块边界没有自然落点。** 渲染成固定格式文本后是一维字节流，
  块边界按固定 `block_size` 对齐即可（vLLM 的做法）。
- **前缀缓存消不掉新增消息里的结构字节。** 每条新消息都带 `{"role":"`, `","content":"`
  这些零信息开销。不过要诚实：正文很长的对话里这只占 3~4%，渲染成文本本身几乎不省
  （长对话实测只省 3.5%，而 1000 条短消息能省 55%）。**渲染的真正价值是把树摊平成
  一维字节流，让块能按固定间隔切、跨轮复用。**
- **尾部增量只能处理追加。** compact、编辑旧消息、重排、中间插入都会导致整条前缀
  失效；分块去重下只重传受影响的块。

## 快速开始

```bash
pip install -e ".[test]"

# 起网关（内存存储；生产用 --dbm 或 --redis）
python -m tail.serve --backend https://api.deepseek.com --port 8765 --dbm ./blocks.db
```

客户端：

```python
from tail.transport.gateway import TailClient

client = TailClient()

# 每轮把完整 messages 交给它，返回的就是要发的 body
body = client.encode_request({
    "model": "deepseek-chat",
    "messages": messages,
})

# 以帧流形式发给网关
resp = httpx.post(
    "http://127.0.0.1:8765/v1/chat/completions",
    content=body,
    headers={"content-type": "application/x-tail-v2",
             "authorization": f"Bearer {api_key}"},
)
if resp.headers.get("x-tail-retransmit"):
    client.invalidate()          # 缓存失效，下一轮重发全文
```

网关也接受普通 `application/json` 请求并原样转发，便于灰度对比与排障。

## 线上格式

`Content-Type: application/x-tail-v2`，body 为二进制帧流：

```
0x01 <block_hash>                      引用：这段内容网关已有
0x02 <block_hash> <4 字节长度> <块字节>   新块：直传内容
0x03                                   结束
```

引用帧固定 17 字节。新块帧带 `block_hash`：哈希是 vLLM 式的
`hash(父哈希 + 本块)`，解码端不知道父哈希，算不出正确值，必须由发送方随帧传来。

用二进制而非 JSON 包一层的理由：无二次转义、可流式解析、引用帧极其紧凑。

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

**`BLOCK_SIZE` 是实测调出来的，不能照抄 vLLM 的 16 token。** 本层每块要付
`1+16` 字节帧头，48 字节（16 token 的中文换算）的块光帧头就占 35%，实测反而更差：

| block_size | 48B | 64B | 128B | 256B | **512B** | 1024B | 4096B |
|---|---|---|---|---|---|---|---|
| 20 轮对话省 | -2% | 19% | 52% | 66% | **69%** | 68% | 26% |

哈希用 blake2b 而非 `hash()`：Borg 明确要求块 id 必须是密码学强哈希，
且内置 `hash()` 带进程随机盐，不能跨进程复用缓存。

## 存储

扁平块集合，**没有链、没有 prev 指针、没有引用图**。顺序由请求里的帧序列直接表达，
所以不需要引用计数、不需要访问驱动续期、不需要缺块补发协议、不存在「部分命中」概念。

```
blk:{block_hash} → 块内容
```

- `MemoryStorage` —— 进程内，重启即丢（默认）
- `DbmStorage` —— 零依赖，单机部署
- `RedisStorage` —— MGET 批量取块，生产用（`pip install redis`）

## 测试

```bash
python -m pytest tests/ -v
```

67 个测试，覆盖分块与前缀复用、帧格式往返、渲染确定性、编解码、存储后端、
以及「客户端 → ASGI 网关 → mock 上游」的端到端链路（含存储失效降级与恢复）。

## 目录

```
tail/
├── __init__.py
├── serve.py              # ASGI 网关服务 + CLI（python -m tail.serve）
└── transport/
    ├── render.py         # 渲染器 / 反渲染器（唯一懂 LLM 语义的地方）
    ├── codec.py          # 编码器 / 解码器（只认字节）
    ├── chunking.py       # 定长分块 + block_hash
    ├── frames.py         # 帧格式
    ├── storage.py        # 块存储后端
    └── gateway.py        # 串成请求路径
tests/
├── test_transport.py     # 单元 + 集成
├── test_e2e.py           # 端到端（真 ASGI + mock 上游）
└── mock_backend.py       # 模拟上游推理服务
docs/
└── TRANSPORT-PIPELINE.md           # 协议、数据管线与设计动机
```

## 未做的部分

- ❌ Anthropic Messages / OpenAI Responses API 转换（留给新的 renderer）
- ❌ 直接对接推理引擎的 token / KV cache 接口
- ❌ 跨模型族的中间格式（harmony 等）
- ❌ 客户端 `known` 集合的持久化（目前进程内，重启后退化为冷启动）
