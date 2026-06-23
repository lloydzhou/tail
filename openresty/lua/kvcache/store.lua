-- v2.1 分层 Segment-Merkle 缓存存储(直连 Kvrocks)。
-- 对应设计文档 §2.3、§3、§6、§7.4。
--
-- 五种 key(命名空间 ns 默认 "prefix_cache"):
--   {ns}:sys:{sys_hash}    → system 全文
--   {ns}:tools:{tools_hash}→ tools 全文
--   {ns}:seg:{seg_hash}    → segment 全文(JSON)
--   {ns}:pfx:{pfx_hash}    → Merkle 节点 {prev, seg_ref, seg_count}
--   {ns}:meta:{cache_key}  → {sys_hash, tools_hash, pfx_hash, len, expire_at}
--
-- cache_key = sys_hash :: tools_hash :: pfx_hash(缺失段为 "0")
--
-- 约束:cosocket 只在 access 阶段;log 阶段用 ngx.timer.at 异步。
local resty_redis = require "resty.redis"
local cjson = require "cjson.safe"
local segment = require "kvcache.segment"
local merkle = require "kvcache.merkle"
local hashing = require "kvcache.hashing"

local M = {}

M.NULL_HASH = "0"  -- 缺失段(system/tools)的固定占位

-- 构造配置好超时 + 连接池的 redis client(已连接)。失败返回 nil + err。
local function connect(cfg)
    local red = resty_redis:new()
    red:set_timeouts(100, 300, 300)
    local ok, err = red:connect(cfg.kvrocks_host, cfg.kvrocks_port)
    if not ok then return nil, err end
    return red
end

local function k(cfg, kind, h) return cfg.hash_ns .. ":" .. kind .. ":" .. h end

-- ===========================================================================
-- 读取(access 阶段,允许 cosocket)
-- ===========================================================================

-- 读 meta。返回 meta table 或 nil。
function M.get_meta(cfg, cache_key)
    local red, err = connect(cfg)
    if not red then return nil end
    local blob = red:get(k(cfg, "meta", cache_key))
    red:set_keepalive(10000, 100)
    if blob == nil or blob == ngx.null then return nil end
    local meta = cjson.decode(blob)
    if not meta then return nil end
    if meta.expire_at and meta.expire_at <= (ngx and ngx.time() or os.time()) then
        return nil  -- 软过期
    end
    return meta
end

-- 读 system/tools 全文。NULL_HASH → 返回 nil(缺失);否则返回字符串或 nil。
function M.get_segment_field(cfg, kind, hash_value)
    if hash_value == M.NULL_HASH then return nil end
    local red, err = connect(cfg)
    if not red then return nil end
    local blob = red:get(k(cfg, kind, hash_value))
    red:set_keepalive(10000, 100)
    if blob == nil or blob == ngx.null then return nil end
    -- sys/tools 存的是原文(可能 string 或 JSON),直接返回让调用方处理
    return blob
end

-- 还原 messages 前缀:沿 prev 回溯 + mget seg。
-- 回溯时对每个 pfx 节点 EXPIRE 续期(§7.4)。
-- 链断/损坏返回 nil。
-- @return messages table 或 nil
function M.reconstruct(cfg, pfx_hash)
    if pfx_hash == merkle.EMPTY_HASH then return {} end
    local red, err = connect(cfg)
    if not red then return nil end

    -- 1. 沿 prev 回溯,收集 seg_ref,同时对每个 pfx 续期
    local seg_refs = {}
    local cur = pfx_hash
    local seen = {}
    while cur ~= merkle.EMPTY_HASH do
        if seen[cur] then red:set_keepalive(10000, 100); return nil end  -- 防环
        seen[cur] = true
        local blob = red:get(k(cfg, "pfx", cur))
        if blob == nil or blob == ngx.null then
            red:set_keepalive(10000, 100); return nil  -- 链断
        end
        -- 续期(失败不影响,pcall 保护)
        pcall(function() red:expire(k(cfg, "pfx", cur), cfg.renew_ttl or 1800) end)
        local node = cjson.decode(blob)
        if not node or not node.seg_ref then
            red:set_keepalive(10000, 100); return nil
        end
        seg_refs[#seg_refs + 1] = node.seg_ref
        cur = node.prev
    end
    red:set_keepalive(10000, 100)

    -- seg_refs 现在是逆序(seg_k, seg_{k-1}, ..., seg_1),reverse
    local n = #seg_refs
    for i = 1, math.floor(n / 2) do
        seg_refs[i], seg_refs[n - i + 1] = seg_refs[n - i + 1], seg_refs[i]
    end

    -- 2. 批量 mget 所有 seg,flatten 还原 messages
    if n == 0 then return {} end
    local red2, err2 = connect(cfg)
    if not red2 then return nil end
    local keys = {}
    for i, sh in ipairs(seg_refs) do keys[i] = k(cfg, "seg", sh) end
    local blobs = red2:mget(unpack(keys))
    red2:set_keepalive(10000, 100)
    if not blobs then return nil end

    local messages = {}
    for _, blob in ipairs(blobs) do
        if blob == ngx.null or blob == nil then return nil end  -- 某 segment 缺失
        local seg_msgs = cjson.decode(blob)
        if not seg_msgs then return nil end
        for _, m in ipairs(seg_msgs) do messages[#messages + 1] = m end
    end
    return messages
end

-- ===========================================================================
-- 写入(供 log 阶段在 ngx.timer.at 回调里调用)
-- ===========================================================================

-- 写一个完整请求,返回 cache_key。
-- @param request table: { system=string|nil, tools=table|nil, messages=table, model=string }
-- @param expire_at number: Unix 秒
-- @return cache_key string
function M.put_request(cfg, request, expire_at)
    local red, err = connect(cfg)
    if not red then return nil, err end

    -- 三段 hash
    local sys_hash = M.NULL_HASH
    local tools_hash = M.NULL_HASH
    local system_val = request.system
    local tools_val = request.tools

    if system_val ~= nil then
        sys_hash = hashing.sha256_hex16(tostring(system_val))
        red:setnx(k(cfg, "sys", sys_hash), tostring(system_val))
        red:expire(k(cfg, "sys", sys_hash), cfg.ttl_stable or 86400)
    end
    if tools_val ~= nil then
        local tools_blob = cjson.encode(tools_val)
        tools_hash = hashing.sha256_hex16(tools_blob)
        red:setnx(k(cfg, "tools", tools_hash), tools_blob)
        red:expire(k(cfg, "tools", tools_hash), cfg.ttl_stable or 86400)
    end

    -- messages → segment → merkle 链
    local messages = request.messages or {}
    local segs = segment.split(messages)
    local seg_hashes = {}
    for i, s in ipairs(segs) do
        local sh = merkle.segment_hash(s)
        seg_hashes[i] = sh
        red:setnx(k(cfg, "seg", sh), cjson.encode(s))
        red:expire(k(cfg, "seg", sh), cfg.ttl_stable or 86400)
    end
    local nodes = merkle.build_nodes(seg_hashes)
    for _, n in ipairs(nodes) do
        red:setnx(k(cfg, "pfx", n.pfx_hash), cjson.encode(n.node))
        red:expire(k(cfg, "pfx", n.pfx_hash), cfg.renew_ttl or 1800)
    end
    local pfx_hash = (#nodes > 0) and nodes[#nodes].pfx_hash or merkle.EMPTY_HASH

    local cache_key = sys_hash .. "::" .. tools_hash .. "::" .. pfx_hash
    local meta = {
        sys_hash = sys_hash, tools_hash = tools_hash, pfx_hash = pfx_hash,
        len = #messages, expire_at = expire_at,
    }
    red:set(k(cfg, "meta", cache_key), cjson.encode(meta), "EX", cfg.ttl or 21600)
    red:set_keepalive(10000, 100)
    return cache_key
end

-- ===========================================================================
-- 运维/测试辅助
-- ===========================================================================

function M.ping(cfg)
    local red, err = connect(cfg)
    if not red then return false end
    local ok = red:ping()
    red:set_keepalive(10000, 100)
    return ok == "PONG"
end

-- 清空本命名空间(测试用)。
function M.clear(cfg)
    local red, err = connect(cfg)
    if not red then return end
    local cursor = "0"
    repeat
        local res = red:scan(cursor, "MATCH", cfg.hash_ns .. ":*", "COUNT", 200)
        if not res then break end
        cursor = res[1]
        for i = 2, #res do red:del(res[i]) end
    until cursor == "0"
    red:set_keepalive(10000, 100)
end

return M
