# Tail 传输层:协议与数据管线

> 本文描述**当前实现**的完整协议与管线,所有常量、字节布局、行为均与代码逐项核对。
> 代码位置:`tail/transport/`。
>
> 审核要点集中在每节的「设计依据」——每一个决策都应能指向一个行业参考实现
> (vLLM / DeepSeek / OpenAI / Borg)或一组实测数据,没有自创机制。

---

## 1. 它解决什么问题

客户端 SDK 与推理服务之间的请求体是 OpenAI Chat Completions 的 JSON。多轮对话里,
每一轮都要把**整份历史**重新上传一次,其中绝大部分与上一轮完全相同。

目标:

1. **减少公网传输字节**——重复的前缀不再传。
2. **不改变上游协议**——推理服务收到的是**标准** OpenAI JSON,它完全不需要知道 Tail 存在。

Tail 插在中间:

```
客户端 SDK
   │  ① 渲染:JSON → 一维文本(固定顺序,确定性)
   │  ② 编码:文本 → 定长块 → 帧流(已知块只发 17 字节引用)
   ▼
 ══════ 公网 ══════   ③ 只传「新增的块」
   ▼
 网关
   │  ④ 解码:帧流 → 文本(边收边查存储补全引用)
   │  ⑤ 反渲染:文本 → 标准 OpenAI JSON
   ▼
推理服务(DeepSeek / vLLM / ...)   ⑥ 收到的就是普通 JSON
```

**为什么先把 JSON 变成文本**:JSON 是树,块边界没有自然落点;摊平成一维字节流后
才能按固定间隔切块并跨轮复用。这一层还顺手省掉每条消息的 `{"role":"`,
`","content":"` 这类零信息结构字节。

---

## 2. 分层与不变式

| 层 | 模块 | 输入 → 输出 | 懂 LLM 语义 |
|---|---|---|---|
| 渲染器 | `render.py` | messages JSON → 一维文本 | ✅ |
| 编码器 | `codec.py` | 文本 → 帧流 | ❌ 只认字节 |
| 解码器 | `codec.py` | 帧流 → 文本 | ❌ 只认字节 |
| 反渲染器 | `render.py` | 一维文本 → messages JSON | ✅ |
| 分块/哈希 | `chunking.py` | 字节 → 块 + 块哈希 | ❌ 只认字节 |
| 存储 | `storage.py` | `block_hash → 块字节` | ❌ 只认字节 |

三条不变式:

1. **渲染必须确定性**——同一份 messages 必须渲染成**逐字节相同**的文本,否则块内容
   变化、`block_hash` 变化、缓存整体失效。实现手段:`sort_keys=True` 的 JSON 序列化、
   固定的顶层参数顺序。
2. **中间层无语义**——编码器/解码器不知道哪个字节是 role。要支持 Anthropic Messages
   或 Responses API,只需写新的 `render` 模块,中间两层一行不改。
3. **缓存缺失只退化,不报错**——取不到块时返回 `X-Tail-Retransmit: 1`,客户端清空
   本地已知块集合后重发全文。存储全空、服务重启时系统仍能正常工作。

---

## 3. 渲染格式

### 3.1 标记集合

包装语法 `<|name|>` 取自 **OpenAI Harmony** 的 formatting token
(`<|start|>` / `<|message|>` / `<|end|>`)。名字本身**没有一个是我发明的**:

- **role 标记**:`<|system|>` `<|user|>` `<|assistant|>` `<|tool|>` `<|developer|>`
  `<|function|>` —— 就是 OpenAI 的 `role` 取值原文。
- **字段标记**:`<|tool_calls|>` `<|tool_call_id|>` `<|name|>` `<|extra|>` ——
  `tool_calls` / `tool_call_id` / `name` 是 OpenAI 字段名原文;`extra` 见 §3.5。
- **顶层参数标记**:`<|model|>` `<|temperature|>` 等 21 个,全部是 OpenAI
  请求体字段名原文(顺序固定,见 `render.PARAM_ORDER`)。

### 3.2 布局

```
<|model|>"deepseek-chat"<|temperature|>0.7<|system|>"你是助手。"<|user|>"你好"
```

规则:

1. 顶层参数在前,顺序由 `PARAM_ORDER` 固定(确定性要求)。
2. 参数值用 `json.dumps(sort_keys=True)` 序列化。
3. 每个 message 以 role 标记开头,正文是 **JSON 字符串**(不是裸文本)。
4. message 内部字段以 `<|字段名|>` 引出,值同样是 JSON 字符串。
5. **格式版本不进正文**,走 HTTP 头 `X-Tail-Version`。写进正文会让版本号落在第一个块上,
   升版即全量失效。

### 3.3 正文为什么必须是 JSON 字符串

这是为了消除歧义,不是风格选择。若正文裸输出,用户在 prompt 里写一句
"请把 `<|user|>` 当作分隔符"就会**把整条流解析错位**(实测直接抛
`JSONDecodeError`)。

改用 JSON 字符串承载后,正文里的标记由 JSON 的字符串/转义语义保护,解析器只需
跟踪"当前是否在 JSON 字符串内部",在字符串内部出现的 `<|` 不算结构。
**没有引入任何新的转义符号**——用的就是 JSON 自己的引号与反斜杠。

代价实测:纯中文约 **0.6%**,含大量引号/换行的代码约 **13~19%**。

### 3.4 tool_calls

按 OpenAI 原结构做确定性 JSON,**不发明** `name(args)` 之类的语法:

```
<|assistant|><|tool_calls|>[{"function":{"arguments":"{\"city\": \"北京\"}","name":"get_weather"},"id":"c1","type":"function"}]
```

`arguments` 按**原字符串**保留(不重新序列化)——重新序列化会改变空白
(`{"k": 1}` → `{"k":1}`),字节就不一致了。

> 曾经用 `name(args)` 这种"函数调用"写法,有两个真实缺陷:`id`/`type` 字段无处安放
> 直接丢失;assistant 正文里出现 `f({"a":1})` 时会被误判成 tool_call。

### 3.5 `content` 的三种形态

OpenAI 里 `content: null`、`content` 缺失、`content: ""` 是三种不同语义,但它们
都不产生正文,纯看字节流无法区分。用 `<|extra|>` 携带存在性:

```
<|assistant|><|tool_calls|>[...]<|extra|>{"__content__":null}
<|user|><|extra|>{"__content__":""}
<|user|><|extra|>{"__content__":{"__missing__":true}}     ← 原文没有 content 键
```

`extra` 同时承载无法映射到上述字段的原始键,做到**不静默丢数据**。

---

## 4. 分块与块哈希

**完全照搬 vLLM Automatic Prefix Caching**,没有自创算法。

参考:<https://docs.vllm.ai/en/v0.8.3/_sources/design/v1/prefix_caching.md>

### 4.1 vLLM 做什么

文档原文的图示与规则:

```
       Block 1: |<--- block tokens ---->|
       Block 2: |<------- prefix ------>| |<--- block tokens --->|
       Block 3: |<------------------ prefix -------------------->| |<--- block tokens ---->|
```

1. **定长块**,块边界只由位置决定,与内容无关。
2. **块哈希 = hash(父块哈希 + 本块 tokens + 额外哈希)**:

   ```
   block_hash = hash(parent_hash, block_tokens, extra_hashes)
   ```

   父哈希让相同内容挂在不同前缀下得到不同哈希——这就是"前缀"语义。

   本层目前**不用** `extra_hashes`:vLLM 用它区分 LoRA ID 与多模态输入,
   而这层工作在字节上、不感知 LoRA/多模态。形式上保留位置但不引入空参数,
   等真有第二种 renderer 需要隔离块空间时再加。
3. **只缓存满块**("We only cache full blocks")。

DeepSeek 侧对应的是**按固定 token 间隔落盘**(见
[上下文硬盘缓存](https://api-docs.deepseek.com/zh-cn/guides/kv_cache/)),
同样是固定间隔。

### 4.2 本层的实现

```python
BLOCK_SIZE    = 512                    # 字节
BLOCK_ID_SIZE = 16                     # 128 bit
_HASH_NAME    = "blake2b"

block_hash(block, parent_hash=b"") = blake2b(parent_hash + block)
```

本层工作在**字节**上,因为网关拿不到 tokenizer。

哈希用 blake2b 而非内置 `hash()`:Borg 官方文档明确要求块 id 必须是**密码学强哈希**,
且内置 `hash()` 带进程随机盐,不能跨进程复用缓存。

### 4.3 `BLOCK_SIZE` 为什么是 512 而不是照抄 16 token

vLLM 的 16 token 换算成中文约 48 字节。但本层每块要付 `1+16+4 = 21` 字节帧头,
48 字节的块光帧头就占 35%,**实测反而比不发引用帧更差**:

| block_size | 48B | 64B | 128B | 256B | **512B** | 1024B | 4096B |
|---|---|---|---|---|---|---|---|
| 20 轮对话省 | **-2%** | 19% | 52% | 66% | **69%** | 68% | 26% |

512 取在平台区:再小帧头占比上升,再大复用粒度变粗。

### 4.4 已否决的方案:CDC(内容定义分块)

早期实现过 buzhash 的 CDC(滚动哈希找切点),**已整体移除**。否决理由:

- DeepSeek 与 vLLM 都用固定间隔,不用内容定义切点。
- CDC 必须逐字节计算滚动哈希才能找切点,实测只有 **0.66 MB/s**(2.5MB 请求要 4 秒);
  定长切块是 **486 MB/s**。
- 对话场景天然是**尾部追加**,前缀不动——恰是定长块最有利的形态。
- 那版还错把 `max_size` 设成等于 `avg_size`,35% 的块退化成硬切,
  等于付了 CDC 的全部代价却只拿到部分收益。

---

## 5. 线缆格式

`Content-Type: application/x-tail-v2`,body 为二进制帧流。

```
类型 1 字节 + 定长头部 + 可变内容
```

### 5.1 三种帧

| 类型 | 布局 | 说明 |
|---|---|---|
| `0x01` 引用帧 | `01` + 16B `block_hash` | 固定 **17B**。这个块网关已有 |
| `0x02` 新块帧 | `02` + 16B `block_hash` + 4B 长度(大端) + 块字节 | 固定头 **21B** |
| `0x03` 结束帧 | `03` | 固定 **1B** |

实际字节(块 `b"x"*512`):

```
引用帧:  017e44ea981aeaab1e7e9b7fe4b8b3a65a                       (17B)
新块帧:  027e44ea981aeaab1e7e9b7fe4b8b3a65a 00000003 616263        (24B,内容 "abc")
结束帧:  03                                                       (1B)
```

### 5.2 新块帧为什么也带 `block_hash`

块哈希是**哈希链** `hash(父哈希 + 本块)`。解码端收到新块时并不知道它的父哈希
(前一个块可能是引用帧,也可能被跳过),**算不出正确值**。所以哈希由发送方算好
随帧传来——这与 vLLM 里 `block_hash` 由调度侧计算、随 `KVCacheBlock` 流转是同一个道理。

### 5.3 为什么用二进制而不是 JSON

- **无二次转义**:JSON 里块内容要再转义引号和反斜杠,体积与 CPU 都翻倍。
- **可流式解析**:读 1 字节类型 + 定长头部,不必等整个 body。
- **引用帧极紧凑**:17 字节,1000 块仅 17KB。

### 5.4 HTTP 头

| 头 | 方向 | 含义 |
|---|---|---|
| `X-Tail-Version` | 响应 | 协议版本,当前 `"2"` |
| `X-Tail-Status` | 响应 | `hit` / `miss` / `degraded`,仅供日志监控 |
| `X-Tail-Hit` | 响应 | 命中率,三位小数,如 `0.973` |
| `X-Tail-Retransmit` | 响应 | 存在缺块时置 `1`,客户端应重发全文 |

---

## 6. 传输管线(逐步骤)

**整条链路全程流式**。没有任何一步会先把整个请求体收进内存。

```
① 客户端:iter_encode_request(payload)                      [流式生成]
     render.iter_render(payload)        → 文本片段流
     chunking.BlockHasher.feed(piece)   → 满块 (block_hash, block)
     codec.Encoder._encode_block        → 引用帧 或 新块帧
                                         (已知块 → 17B 引用帧,否则 → 新块帧)

② 公网:HTTP chunked transfer                                [边生成边发]

③ 网关:request.stream() → _AsyncToSyncBody                 [流式接收 + 背压]
     后台协程持续读 request.stream(),片段放进**有界队列**(maxsize=8)。
     队列满 → 后台 pump 阻塞 → 不把整个 body 读进内存。
     同步解码器在另一线程从队列取,通过 executor 桥接。

④ 网关:codec.Decoder.feed(chunk)                           [流式解码]
     frames.FrameDecoder.feed(chunk)    → 逐帧
     引用帧 → 查 storage.get_many() 取块内容
     新块帧 → 内容直传,同时 put_many() 入库
                                         → 文本片段流

⑤ 网关:render.RenderParser.feed(piece)                     [流式反渲染]
     按 <|标记|> 切分,正文/字段值用 json.loads 还原
                                         → (PARAM | MESSAGE_START | MESSAGE_END)

⑥ 网关:iter_payload_json → _AsyncJsonStream                [流式重建 + 转发]
     每条 message 完成即刻序列化成 JSON 片段,边发上游

⑦ 上游:收到标准 OpenAI JSON —— 它不知道 Tail 存在
```

### 6.1 启动 HTTP chunked 到上游

网关用 `httpx` 的 `AsyncByteStream` 适配:每次 `next()` 得到一个 JSON 片段就
立刻 `yield`,不等全部生成完。解码是 CPU 密集的同步代码,放进**单线程**
`ThreadPoolExecutor`(惰性迭代器非线程安全,用默认线程池会丢消息)。

### 6.2 普通 JSON 透传路径

`Content-Type` 不是 `application/x-tail-v2` 时,网关**原样转发**,同样直接透传
`request.stream()`——不先收齐。用于灰度对比与排障。

### 6.3 流式性的实测证据

body 34610B / 34 片,每片间隔 20ms:

```
客户端全部发完耗时:         674ms
上游收到首片时,客户端已发出:  2/34 片      ← 才发出 6% 就开始转发
上游 首片 → 收完:            673ms
```

若先 `await request.body()` 收齐再转发,上游首片会在 **674ms** 之后才到。
这条已固化为测试 `test_upstream_receives_before_client_finishes`,
并验证过:把实现改回收齐再转发,该测试立即失败。

---

## 7. 存储

扁平块集合,**没有链、没有 prev 指针、没有引用图**。顺序由请求里的帧序列直接表达,
因此不需要引用计数、不需要访问驱动续期、不需要缺块补发协议、不存在"部分命中"概念。

```
blk:{block_hash}  →  块字节
```

| 后端 | 说明 |
|---|---|
| `MemoryStorage` | 进程内字典 + 可选 LRU,重启即丢(默认) |
| `DbmStorage` | 零依赖,单机部署 |
| `RedisStorage` | MGET 批量取块,生产用 |

**只存满块**(vLLM: "only cache full blocks")。不满的尾块内容还会被后续追加改变,
缓存它没有意义。

---

## 8. 降级与恢复

任何一块取不到(存储被清、服务重启、LRU 淘汰),解码器记录到 `missing`:

1. 网关照常把能解的块拼出来转发,同时在响应头带 `X-Tail-Retransmit: 1`。
2. 客户端调用 `invalidate()` 清空本地已知块集合。
3. 下一轮所有块都当新块重发,网关重新填充存储。

**不需要任何补发协议**——新块帧本身就带完整内容,这是扁平块集合设计的好处。

---

## 9. 实测数据

### 体积(81 条消息的典型长会话)

| | 字节 | 相对原始 JSON |
|---|---|---|
| 原始 JSON | 17853 | — |
| 渲染文本 | 16300 | 省 9% |
| 冷启动帧流 | 16973 | 省 5% |
| **全命中帧流** | **545** | **省 97%** |

冷启动只省 5%,因为帧头占 4.1%(每 512B 块付 21B)。之后每轮只传新增块。

### 多轮增量(20 轮对话,上下文约 3.8KB)

```
前 10 轮均值 935B  →  后 10 轮均值 1197B
后 10 轮只传上下文的 31%  →  省 70%
```

### 速度

| 环节 | 吞吐 | 备注 |
|---|---|---|
| 渲染 JSON → 文本 | 76 MB/s | |
| 反渲染 文本 → JSON | 10 ~ 100 MB/s | **强依赖消息条数**,见下 |
| 切块 + 哈希 | 496 MB/s | |
| 编码 文本 → 帧流 | 437 MB/s | |

反渲染的每条消息有固定开销(一次 `json.loads` + 一次 message 组装),所以吞吐与
「消息条数」而非「字节数」相关:

| 形态 | 大小 | 消息数 | 吞吐 |
|---|---|---|---|
| 1 条大正文 | 30KB | 1 | 99.7 MB/s |
| 20 条中等 | 36KB | 20 | 94.3 MB/s |
| 100 条短消息 | 31KB | 100 | 9.7 MB/s |

真实对话里 100 条短消息是长会话的常见形态,此时约 10 MB/s——即 30KB 请求耗时
约 3ms,相对上游推理(百毫秒级)仍可忽略。**这是保持流式的代价**:逐条解析才能
做到"一条 message 完成即转发"

注:`json.loads` 在 81 条消息的请求里共调用 83 次、合计 **0.047ms**,占典型请求
耗时(网络 + 上游推理 100ms+)的 **0.05%**。Python 标准库没有流式 JSON 解析器,
但这里也不需要——每次只解析约 52 字节的单个字段值,换流式解析器只会更慢。

---

## 10. 显式不采用的做法

为便于 review,列出被明确否决、且代码中**已无残留**的方案:

| 方案 | 否决理由 |
|---|---|
| CDC / buzhash 分块 | 参考实现都用固定间隔;逐字节哈希仅 0.66 MB/s;已整体移除 |
| `\x1e`/`\x1f` 控制字符分隔 message | 正文里完全可能出现这两个字节;已删 |
| 正文里的 `<|v2|>` 版本头 | 版本号落在第一个块上,升版即全量失效;且参考实现无此物;已删 |
| `name(args)` 的 tool_calls 语法 | `id`/`type` 丢失;正文里的括号会被误判;已删 |
| 分段(segment)后段内独立切块 | 依赖 `\x1f`;定长块下无收益;`segment.py` 已删 |
| 先 `await request.body()` 再解码 | 整包落内存,违背流式目的;已改为 `request.stream()` |
| 链式存储(`pfx:{hash} → [seg hashes]`) | 读取要顺链走,且顺序信息与帧序列重复;已改扁平集合 |

---

## 11. 审核清单

建议按这个顺序核对:

1. **渲染确定性**:同一 payload 连续渲染 1000 次是否字节完全相同?
   (`TestRender::test_deterministic` / `test_deterministic_ignores_key_order`)
2. **往返保真**:随机 payload 渲染→反渲染是否完全相等?
   (`test_roundtrip_with_hostile_content` 覆盖正文含 `<|user|>`、引号、反斜杠、换行)
3. **流式切块一致性**:任意字节边界喂入,结果是否与一次性切分完全一致?
   (`test_matches_one_shot` / `test_hashes_match_blocked_hashes`)
4. **哈希链**:相同内容在不同前缀下 `block_hash` 是否不同?(`test_block_hash_depends_on_parent`)
5. **前缀复用**:尾部追加时,前面满块的 id 是否全部不变?(`test_append_only_grows_tail`)
6. **全程流式**:上游开始收包时客户端是否远未发完?(`test_upstream_receives_before_client_finishes`)
7. **降级恢复**:存储清空后是否正常降级并要求重发,恢复后是否继续命中?
8. **体积**:冷启动 / 全命中 / 多轮增量三项是否符合 §9。
