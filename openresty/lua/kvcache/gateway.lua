-- 网关协商核心(v2.1 Segment-Merkle,对应设计文档第 5 章)。
--
-- 提供三个入口(access_by_lua / header_filter_by_lua / log_by_lua):
--   - access_phase:        读 Kvrocks 同步命中判定 + 请求体改写(允许 cosocket)
--   - header_filter_phase: 注入 X-Cache-* 响应头(禁止 cosocket,只读 ctx)
--   - log_phase:           异步写 Kvrocks(用 ngx.timer.at,此阶段禁止 cosocket)
--
-- cache_key 格式(v2.1):sys_hash :: tools_hash :: pfx_hash(缺失段为 "0")。
--
-- 关键:三段 hash 都是纯算法(对内容做 SHA),不需要 KV。
-- 所以 header_filter(禁止 cosocket)能纯算出 cache_key;
-- log_phase 的 timer 负责把内容写进 KV(那才需要 cosocket)。
local protocol = require "kvcache.protocol"
local store = require "kvcache.store"
local hashing = require "kvcache.hashing"
local segment = require "kvcache.segment"
local merkle = require "kvcache.merkle"
local cjson = require "cjson.safe"

local M = {}

-- 计算一个完整请求的 cache_key(纯算法,无需 KV)。
-- @param request table: { system=string|nil, tools=table|nil, messages=table }
-- @return cache_key string
local function compute_cache_key(request)
    local sys_hash = store.NULL_HASH
    local tools_hash = store.NULL_HASH
    if request.system ~= nil then
        sys_hash = hashing.sha256_hex16(tostring(request.system))
    end
    if request.tools ~= nil then
        tools_hash = hashing.sha256_hex16(cjson.encode(request.tools))
    end
    local segs = segment.split(request.messages or {})
    local seg_hashes = {}
    for i, s in ipairs(segs) do seg_hashes[i] = merkle.segment_hash(s) end
    local pfx_hash = merkle.chain_hash(seg_hashes)
    return sys_hash .. "::" .. tools_hash .. "::" .. pfx_hash
end

-- 读取并解析请求体。返回 body table。
local function read_request_body()
    ngx.req.read_body()
    local raw = ngx.req.get_body_data()
    if not raw then
        local file = ngx.req.get_body_file()
        if file then
            local f = io.open(file, "rb")
            if f then raw = f:read("*a"); f:close() end
        end
    end
    if not raw then return nil end
    return cjson.decode(raw)
end

-- fast_fail 响应:不转发后端,直接 412。
local function respond_fast_fail(cfg)
    local expire = protocol.compute_expire(cfg.ttl, cfg.jitter)
    ngx.header[protocol.HEADER_RESP_CACHE_HASH] = ""
    ngx.header[protocol.HEADER_RESP_CACHE_EXPIRE] = tostring(expire)
    ngx.header[protocol.HEADER_RESP_CACHE_HIT] = "false"
    ngx.header["Content-Type"] = "application/json"
    ngx.status = 412
    ngx.say(cjson.encode({
        error = {
            message = "prefix cache miss; retry with full messages",
            type = "prefix_cache_miss",
            code = "cache_miss",
        },
    }))
    ngx.exit(412)
end

-- access 阶段:同步读 Kvrocks,还原三段 + 请求体改写。
function M.access_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        -- 在读 body 前先捕获所有 header(read_body 后某些场景 header var 可能受影响)
        local cache_key = ngx.var.http_x_cache_hash
        local prefix_len_hdr = ngx.var.http_x_cache_prefix_length
        local declared_len = tonumber(prefix_len_hdr)

        local body = read_request_body()
        local messages = (body and body.messages) or {}
        ngx.ctx.kvcache_model = (body and body.model) or "deepseek-chat"
        ngx.ctx.kvcache_final_messages = messages
        -- 捕获顶层 system/tools(给 log 阶段写;绝不从 messages 抽取,§2.4 C2)
        ngx.ctx.kvcache_system = body and body.system
        ngx.ctx.kvcache_tools = body and body.tools

        -- 网关内部 Header 一律不转发给后端(第 5.3 节)。
        ngx.req.clear_header(protocol.HEADER_CACHE_HASH)
        ngx.req.clear_header(protocol.HEADER_CACHE_PREFIX_LENGTH)

        if cache_key and #cache_key > 0 then
            local meta = store.get_meta(cfg, cache_key)
            local consistent = meta ~= nil
                and declared_len ~= nil
                and meta.len == declared_len

            if consistent then
                -- 命中:还原三段
                local sys_val = store.get_segment_field(cfg, "sys", meta.sys_hash)
                local tools_val = store.get_segment_field(cfg, "tools", meta.tools_hash)
                local prefix_msgs = store.reconstruct(cfg, meta.pfx_hash)
                if prefix_msgs == nil then
                    -- 链断 → miss
                    ngx.ctx.kvcache_hit = false
                    if cfg.miss_mode ~= protocol.MISS_PASSTHROUGH then
                        respond_fast_fail(cfg)
                        return
                    end
                else
                    -- 拼装完整 messages = prefix + 增量
                    local full = {}
                    for _, m in ipairs(prefix_msgs) do full[#full + 1] = m end
                    for _, m in ipairs(messages) do full[#full + 1] = m end
                    ngx.ctx.kvcache_final_messages = full
                    ngx.ctx.kvcache_hit = true
                    if body then
                        body.messages = full
                        if sys_val ~= nil then body.system = sys_val end
                        if tools_val ~= nil then
                            -- tools 存的是 JSON 字符串,还原成 table
                            local t = cjson.decode(tools_val)
                            if t then body.tools = t end
                        end
                        ngx.req.set_body_data(cjson.encode(body))
                    end
                end
            else
                ngx.ctx.kvcache_hit = false
                if cfg.miss_mode ~= protocol.MISS_PASSTHROUGH then
                    respond_fast_fail(cfg)
                    return
                end
            end
        else
            ngx.ctx.kvcache_hit = false
        end
    end)
    if not ok then
        ngx.log(ngx.ERR, "kvcache access_phase error: ", err)
        ngx.ctx.kvcache_hit = false
        ngx.ctx.kvcache_skip_cache_set = true
    end
end

-- header_filter 阶段:纯算 cache_key(无需 KV,因为三段 hash 都是纯算法)+ 注入响应头。
function M.header_filter_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        if ngx.ctx.kvcache_skip_cache_set then return end
        local messages = ngx.ctx.kvcache_final_messages
        if not messages then return end
        -- 纯算 cache_key(header_filter 禁止 cosocket,但 hash 计算不需要网络)
        local request = {
            system = ngx.ctx.kvcache_system,
            tools = ngx.ctx.kvcache_tools,
            messages = messages,
        }
        local cache_key = compute_cache_key(request)
        ngx.ctx.kvcache_new_cache_key = cache_key  -- 供 log_phase 复用
        local expire = ngx.ctx.kvcache_expire or protocol.compute_expire(cfg.ttl, cfg.jitter)
        ngx.header[protocol.HEADER_RESP_CACHE_HASH] = cache_key
        ngx.header[protocol.HEADER_RESP_CACHE_EXPIRE] = tostring(expire)
        ngx.header[protocol.HEADER_RESP_CACHE_HIT] = ngx.ctx.kvcache_hit and "true" or "false"
    end)
    if not ok then
        ngx.log(ngx.ERR, "kvcache header_filter_phase error: ", err)
    end
end

-- log 阶段:请求结束后异步写 Kvrocks(三段)。此阶段禁止 cosocket,故用 timer。
function M.log_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        if ngx.ctx.kvcache_skip_cache_set then return end
        local messages = ngx.ctx.kvcache_final_messages
        if not messages then return end
        local status = ngx.status or 200
        if status < 200 or status >= 300 then return end

        -- header_filter 已算好 cache_key;log 只负责把内容写进 KV
        local cache_key = ngx.ctx.kvcache_new_cache_key
        if not cache_key then return end
        local expire = ngx.ctx.kvcache_expire or protocol.compute_expire(cfg.ttl, cfg.jitter)

        -- 捕获完整请求快照(timer 回调里不能访问 ngx.ctx)
        local request = {
            system = ngx.ctx.kvcache_system,
            tools = ngx.ctx.kvcache_tools,
            messages = messages,
        }

        local function write_kvrocks(premature)
            if premature then return end
            local ok2, err2 = pcall(store.put_request, cfg, request, expire)
            if not ok2 then
                ngx.log(ngx.WARN, "kvcache v2.1 write failed: ", err2)
            end
        end
        local ok_timer, err_timer = ngx.timer.at(0, write_kvrocks)
        if not ok_timer then
            ngx.log(ngx.WARN, "kvcache failed to create write timer: ", err_timer)
        end
    end)
    if not ok then
        ngx.log(ngx.ERR, "kvcache log_phase error: ", err)
    end
end

return M
