-- 网关协商核心(对应设计文档第 5 章)。
--
-- 提供三个入口(access_by_lua / header_filter_by_lua / log_by_lua):
--   - access_phase:        读 Kvrocks 同步命中判定 + 请求体改写(允许 cosocket)
--   - header_filter_phase: 注入 X-Cache-* 响应头(禁止 cosocket,只读 ctx)
--   - log_phase:           异步写 Kvrocks(用 ngx.timer.at,此阶段禁止 cosocket)
--
-- 缓存后端:只用 Kvrocks(硬盘),不再有 L1 共享内存层。
--
-- 关键约定:「前缀」= 本次完整 messages 中、可作为下次复用的稳定头部。
--   每次请求结束后把【本次完整 messages】作为新前缀缓存,返回新哈希。
local hashing = require "kvcache.hashing"
local protocol = require "kvcache.protocol"
local store = require "kvcache.store"
local cjson = require "cjson.safe"

local M = {}

-- 读取并解析请求体。返回 body table 与原始 bytes。
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
    if not raw then return nil, "" end
    return cjson.decode(raw), raw
end

-- fast_fail 响应:不转发后端,直接 422。
local function respond_fast_fail(cfg)
    local expire = protocol.compute_expire(cfg.ttl, cfg.jitter)
    ngx.header[protocol.HEADER_RESP_CACHE_HASH] = ""
    ngx.header[protocol.HEADER_RESP_CACHE_EXPIRE] = tostring(expire)
    ngx.header[protocol.HEADER_RESP_CACHE_HIT] = "false"
    ngx.header["Content-Type"] = "application/json"
    ngx.status = 422
    ngx.say(cjson.encode({
        error = {
            message = "prefix cache miss; retry with full messages",
            type = "prefix_cache_miss",
            code = "cache_miss",
        },
    }))
    ngx.exit(422)
end

-- access 阶段:同步读 Kvrocks 判定命中 + 请求体改写。
function M.access_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        local cache_hash = ngx.var.http_x_cache_hash
        local prefix_len_hdr = ngx.var.http_x_cache_prefix_length

        local body, raw = read_request_body()
        local messages = (body and body.messages) or {}
        ngx.ctx.kvcache_model = (body and body.model) or "deepseek-chat"
        ngx.ctx.kvcache_final_messages = messages

        -- 网关内部 Header 一律不转发给后端(第 5.3 节)。
        ngx.req.clear_header(protocol.HEADER_CACHE_HASH)
        ngx.req.clear_header(protocol.HEADER_CACHE_PREFIX_LENGTH)

        if cache_hash and #cache_hash > 0 then
            local declared_len = tonumber(prefix_len_hdr)
            local entry, level = store.get(cfg, cache_hash)
            local consistent = entry
                and entry.model == ngx.ctx.kvcache_model
                and declared_len ~= nil
                and entry.prefix_length == declared_len
            if consistent then
                -- 命中:prefix + 增量 → 完整 messages
                local full = {}
                for _, m in ipairs(entry.messages) do full[#full + 1] = m end
                for _, m in ipairs(messages) do full[#full + 1] = m end
                ngx.ctx.kvcache_final_messages = full
                ngx.ctx.kvcache_hit = true
                if body then
                    body.messages = full
                    ngx.req.set_body_data(cjson.encode(body))
                end
            else
                ngx.ctx.kvcache_hit = false
                if cfg.miss_mode == protocol.MISS_PASSTHROUGH then
                    -- 把当前 messages 当完整请求转发
                else
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

-- header_filter 阶段:只注入缓存协商 Header(禁止 cosocket,只读 ctx)。
function M.header_filter_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        local messages = ngx.ctx.kvcache_final_messages
        if not messages or ngx.ctx.kvcache_skip_cache_set then
            return
        end
        local new_hash = ngx.ctx.kvcache_new_hash
        if not new_hash then
            new_hash = hashing.prefix_hash(messages)
            ngx.ctx.kvcache_new_hash = new_hash
        end
        local expire = ngx.ctx.kvcache_expire or protocol.compute_expire(cfg.ttl, cfg.jitter)
        ngx.header[protocol.HEADER_RESP_CACHE_HASH] = new_hash
        ngx.header[protocol.HEADER_RESP_CACHE_EXPIRE] = tostring(expire)
        ngx.header[protocol.HEADER_RESP_CACHE_HIT] = ngx.ctx.kvcache_hit and "true" or "false"
    end)
    if not ok then
        ngx.log(ngx.ERR, "kvcache header_filter_phase error: ", err)
    end
end

-- log 阶段:请求结束后异步写 Kvrocks。此阶段禁止 cosocket,故用 timer。
function M.log_phase()
    local ok, err = pcall(function()
        local cfg = protocol.get_config()
        local messages = ngx.ctx.kvcache_final_messages
        if not messages or ngx.ctx.kvcache_skip_cache_set then
            return
        end
        local status = ngx.status or 200
        if status < 200 or status >= 300 then
            return
        end
        local new_hash = ngx.ctx.kvcache_new_hash
        if not new_hash then return end
        local expire = ngx.ctx.kvcache_expire or protocol.compute_expire(cfg.ttl, cfg.jitter)

        -- 捕获数据(timer 回调里不能访问 ngx.ctx)
        local blob = cjson.encode({
            messages = messages,
            model = ngx.ctx.kvcache_model,
            prefix_length = #messages,
            expire_at = expire,
        })

        -- 异步写 Kvrocks
        local function write_kvrocks(premature)
            if premature then return end
            local ok2, err2 = pcall(store.set_async, cfg, new_hash, blob)
            if not ok2 then
                ngx.log(ngx.WARN, "kvcache Kvrocks async write failed: ", err2)
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
