-- 前缀缓存存储:直接用 Kvrocks(Redis 协议,数据落硬盘)。
-- 按需求去掉了 L1 共享内存层,统一以 Kvrocks 作为唯一缓存后端。
-- 对应设计文档第 5.1 节「Redis 集群(二级缓存)」——这里用 Kvrocks 替代 Redis,
-- 并把它作为主缓存(硬盘存储,可存远超内存的量)。
--
-- 约束:cosocket 只能在 access/rewrite/content/rewardset 阶段使用,
-- log_by_lua/header_filter_by_lua 阶段禁止。因此:
--   - 读(access 阶段):同步 cosocket 直连 Kvrocks。
--   - 写(log 阶段):用 ngx.timer.at 异步执行。
--
-- Value 用 cjson 序列化:{ messages=..., model=..., prefix_length=..., expire_at=... }
local resty_redis = require "resty.redis"
local cjson = require "cjson.safe"

local M = {}

-- 构造一个配置好超时 + 连接池的 redis client(已连接)。
-- 失败返回 nil + err。
local function connect(cfg)
    local red = resty_redis:new()
    red:set_timeouts(100, 300, 300)
    local ok, err = red:connect(cfg.kvrocks_host, cfg.kvrocks_port)
    if not ok then
        return nil, err
    end
    return red
end

-- 同步读取缓存(access 阶段调用)。
-- @param cfg protocol.get_config()
-- @param hash string
-- @return entry table or nil, level or err
function M.get(cfg, hash)
    local now = ngx and ngx.time() or os.time()
    local red, err = connect(cfg)
    if not red then return nil, "connect_fail:" .. tostring(err) end
    local key = cfg.hash_ns .. ":" .. hash
    local blob = red:get(key)
    -- 访问驱动续期(§7.4):命中则在归还连接前刷新 TTL,活跃链不过期。
    -- 续期失败不影响本次读取(降级为不续期)。
    if blob ~= nil and blob ~= ngx.null then
        pcall(function()
            red:expire(key, cfg.renew_ttl or 1800)
        end)
    end
    red:set_keepalive(10000, 100)
    if blob == nil or blob == ngx.null then
        return nil, "miss"
    end
    local entry = cjson.decode(blob)
    if not entry then return nil, "corrupt" end
    if entry.expire_at and entry.expire_at <= now then
        return nil, "expired"
    end
    return entry, "kvrocks"
end

-- 同步写入(只在允许 cosocket 的阶段可用,如 access)。
function M.set_sync(cfg, hash, entry)
    local blob = cjson.encode(entry)
    if not blob or #blob > cfg.max_prefix_bytes then
        return false, "too_large"
    end
    local red, err = connect(cfg)
    if not red then return false, err end
    local key = cfg.hash_ns .. ":" .. hash
    local ttl = math.max(1, entry.expire_at - (ngx and ngx.time() or os.time()))
    local res = red:set(key, blob, "EX", ttl)
    red:set_keepalive(10000, 100)
    return res == "OK", nil
end

-- 异步写入(供 log_by_lua 阶段在 ngx.timer.at 回调里调用)。
-- @param cfg table, hash string, blob string(已序列化)
function M.set_async(cfg, hash, blob)
    if not blob or #blob > cfg.max_prefix_bytes then
        return false, "too_large"
    end
    local red, err = connect(cfg)
    if not red then return false, err end
    local key = cfg.hash_ns .. ":" .. hash
    -- TTL 从 blob 里反解 expire_at 比较麻烦,这里用 cfg.ttl(够用)
    local res = red:set(key, blob, "EX", cfg.ttl or 21600)
    red:set_keepalive(10000, 100)
    return res == "OK", nil
end

-- 删除一条缓存(测试/运维用)。
function M.del(cfg, hash)
    local red, err = connect(cfg)
    if not red then return false, err end
    local key = cfg.hash_ns .. ":" .. hash
    local res = red:del(key)
    red:set_keepalive(10000, 100)
    return res, nil
end

return M
