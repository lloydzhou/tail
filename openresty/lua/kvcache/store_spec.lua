-- Lua 单元测试:store v2.1(三段 Segment-Merkle 存储操作)。
-- 连真 Kvrocks(端口 6666);不可达则跳过。
local store = require "kvcache.store"

local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

local cfg = {
    hash_ns = "test_spec", kvrocks_host = "127.0.0.1", kvrocks_port = 6666,
    ttl = 100, renew_ttl = 1800, ttl_stable = 86400,
}

-- ping 检查 Kvrocks 是否可达
if not store.ping(cfg) then
    print("Kvrocks 不可达,跳过 store_spec")
    os.exit(0)
end

-- 每个测试前清空命名空间
local function clean() store.clear(cfg) end

-- 1. ping
check("ping Kvrocks", store.ping(cfg))

-- 2. put_request + get_meta roundtrip
clean()
local cache_key = store.put_request(cfg, {
    system = "SYS", tools = {{"tool1"}},
    messages = {{role="user",content="q1"},{role="assistant",content="a1"},{role="user",content="q2"}},
}, os.time() + 1000)
check("put_request 返回 cache_key", cache_key ~= nil and cache_key:find("::") ~= nil)
check("cache_key 三段", select(2, cache_key:gsub("::", "")) == 2)  -- 2 个 :: = 3 段

local meta = store.get_meta(cfg, cache_key)
check("get_meta 返回 meta", meta ~= nil)
check("meta.len = 3", meta and meta.len == 3)
check("meta 含 pfx_hash", meta and meta.pfx_hash ~= nil and meta.pfx_hash ~= "0")
check("meta 含 sys_hash != 0", meta and meta.sys_hash ~= "0")

-- 3. 还原 messages
clean()
store.put_request(cfg, {
    system = nil, tools = nil,
    messages = {{role="user",content="q1"},{role="assistant",content="a1"},{role="user",content="q2"},{role="assistant",content="a2"},{role="user",content="q3"}},
}, os.time() + 1000)
-- 重新算 pfx_hash(从同 messages)
local segment = require "kvcache.segment"
local merkle = require "kvcache.merkle"
local segs = segment.split({{role="user",content="q1"},{role="assistant",content="a1"},{role="user",content="q2"},{role="assistant",content="a2"},{role="user",content="q3"}})
local seg_hashes = {}
for i, s in ipairs(segs) do seg_hashes[i] = merkle.segment_hash(s) end
local pfx_hash = merkle.chain_hash(seg_hashes)
local reconstructed = store.reconstruct(cfg, pfx_hash)
check("reconstruct 还原成功", reconstructed ~= nil)
check("reconstruct 5 条", reconstructed and #reconstructed == 5)
check("reconstruct 顺序正确", reconstructed and reconstructed[1].content == "q1" and reconstructed[5].content == "q3")

-- 4. 缺失段(无 system 无 tools)
clean()
local ck2 = store.put_request(cfg, {
    system = nil, tools = nil, messages = {{role="user",content="hi"}},
}, os.time() + 1000)
check("缺失段 cache_key 以 0::0 开头", ck2:sub(1,4) == "0::0")

-- 5. get_segment_field:sys/tools
clean()
store.put_request(cfg, {
    system = "MYSYS", tools = nil, messages = {{role="user",content="q"}},
}, os.time() + 1000)
-- 算 sys_hash
local hashing = require "kvcache.hashing"
local sys_hash = hashing.sha256_hex16("MYSYS")
local sys_val = store.get_segment_field(cfg, "sys", sys_hash)
check("get sys 全文", sys_val == "MYSYS")
check("NULL_HASH sys 返回 nil", store.get_segment_field(cfg, "sys", "0") == nil)

-- 6. 链断裂:删中间 pfx 节点 → reconstruct 返回 nil
clean()
store.put_request(cfg, {
    system = nil, tools = nil,
    messages = {{role="user",content="q1"},{role="assistant",content="a1"},{role="user",content="q2"},{role="assistant","a2"},{role="user",content="q3"}},
}, os.time() + 1000)
-- 删根节点(第一个 pfx)
local resty_redis = require "resty.redis"
local red = resty_redis:new()
red:connect("127.0.0.1", 6666)
-- 找到并删一个 pfx 节点(根)
-- 重新算所有节点 hash
local segs2 = segment.split({{role="user",content="q1"},{role="assistant",content="a1"},{role="user",content="q2"},{role="assistant","a2"},{role="user",content="q3"}})
local sh2 = {}
for i, s in ipairs(segs2) do sh2[i] = merkle.segment_hash(s) end
local nodes = merkle.build_nodes(sh2)
local root_pfx = nodes[1].pfx_hash
red:del("test_spec:pfx:" .. root_pfx)
red:set_keepalive(10000, 100)
local broken = store.reconstruct(cfg, nodes[#nodes].pfx_hash)
check("链断裂 reconstruct 返回 nil", broken == nil)

-- 7. 软过期:expire_at 已过 → get_meta 返回 nil
clean()
local ck_expired = store.put_request(cfg, {
    system = nil, tools = nil, messages = {{role="user",content="q"}},
}, os.time() - 100)  -- 已过期
check("过期 meta 返回 nil", store.get_meta(cfg, ck_expired) == nil)

clean()
print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
