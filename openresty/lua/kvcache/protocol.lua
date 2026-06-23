-- 协议常量与配置(对应设计文档第 4 章、第 9 章)。
local M = {}

-- 请求方向 Header(Client -> Gateway)
M.HEADER_CACHE_HASH = "X-Cache-Hash"
M.HEADER_CACHE_PREFIX_LENGTH = "X-Cache-Prefix-Length"

-- 响应方向 Header(Gateway -> Client)
M.HEADER_RESP_CACHE_HASH = "X-Cache-Hash"
M.HEADER_RESP_CACHE_EXPIRE = "X-Cache-Expire"
M.HEADER_RESP_CACHE_HIT = "X-Cache-Hit"

-- 默认策略
M.DEFAULT_CACHE_TTL = 6 * 3600           -- 秒,与服务端 KV Cache 对齐(数小时)
M.DEFAULT_TTL_JITTER = 600               -- ±秒,防雪崩
M.DEFAULT_MAX_PREFIX_BYTES = 8 * 1024 * 1024  -- 单条前缀上限 8MB,防滥用
M.DEFAULT_HASH_NS = "prefix_cache"        -- Kvrocks key 前缀
M.DEFAULT_RENEW_TTL = 30 * 60             -- 访问驱动续期 TTL(秒),见 §7.4
M.DEFAULT_STABLE_TTL = 24 * 3600          -- sys/tools/seg 稳定内容 TTL(秒)

-- 缓存未命中处理模式
M.MISS_FAST_FAIL = "fast_fail"            -- 默认:不转发,返回 422 由 SDK 重试
M.MISS_PASSTHROUGH = "passthrough"        -- 文档字面:把当前 messages 当完整转发

-- 读取运行时配置(从 nginx.conf 的 set $var 注入),带默认值兜底。
-- 在非请求上下文(如 resty CLI 单测)下安全返回默认值。
function M.get_config()
    local var = {}
    pcall(function()
        if ngx and ngx.var then
            var.kvcache_backend_url      = ngx.var.kvcache_backend_url
            var.kvcache_ttl              = ngx.var.kvcache_ttl
            var.kvcache_jitter           = ngx.var.kvcache_jitter
            var.kvcache_max_prefix_bytes = ngx.var.kvcache_max_prefix_bytes
            var.kvcache_hash_ns          = ngx.var.kvcache_hash_ns
            var.kvcache_miss_mode        = ngx.var.kvcache_miss_mode
            var.kvcache_kvrocks_host     = ngx.var.kvcache_kvrocks_host
            var.kvcache_kvrocks_port     = ngx.var.kvcache_kvrocks_port
            var.kvcache_renew_ttl        = ngx.var.kvcache_renew_ttl
        end
    end)
    return {
        backend_url       = var.kvcache_backend_url or "http://127.0.0.1:8080",
        ttl               = tonumber(var.kvcache_ttl) or M.DEFAULT_CACHE_TTL,
        jitter            = tonumber(var.kvcache_jitter) or M.DEFAULT_TTL_JITTER,
        max_prefix_bytes  = tonumber(var.kvcache_max_prefix_bytes) or M.DEFAULT_MAX_PREFIX_BYTES,
        hash_ns           = var.kvcache_hash_ns or M.DEFAULT_HASH_NS,
        miss_mode         = var.kvcache_miss_mode or M.MISS_FAST_FAIL,
        kvrocks_host      = var.kvcache_kvrocks_host or "127.0.0.1",
        kvrocks_port      = tonumber(var.kvcache_kvrocks_port) or 6666,
        renew_ttl         = tonumber(var.kvcache_renew_ttl) or M.DEFAULT_RENEW_TTL,
        ttl_stable        = M.DEFAULT_STABLE_TTL,  -- sys/tools/seg 长 TTL
    }
end

-- 带抖动的过期时间(秒级 Unix 时间戳)。
function M.compute_expire(ttl, jitter)
    ttl = ttl or M.DEFAULT_CACHE_TTL
    jitter = jitter or M.DEFAULT_TTL_JITTER
    local now = ngx and ngx.time() or os.time()
    local delta = 0
    if jitter > 0 then
        delta = math.random(-jitter, jitter)
    end
    return now + ttl + delta
end

return M
