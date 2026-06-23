-- Lua 单元测试:protocol 模块。
local protocol = require "kvcache.protocol"

local pass, fail = 0, 0
local function check(name, cond)
    if cond then pass = pass + 1; print(("  PASS  %s"):format(name))
    else fail = fail + 1; print(("  FAIL  %s"):format(name)); end
end

-- 1. 协议头常量(对应设计文档第 4 章)
check("HEADER_CACHE_HASH", protocol.HEADER_CACHE_HASH == "X-Cache-Hash")
check("HEADER_CACHE_PREFIX_LENGTH", protocol.HEADER_CACHE_PREFIX_LENGTH == "X-Cache-Prefix-Length")
check("HEADER_RESP_CACHE_HASH", protocol.HEADER_RESP_CACHE_HASH == "X-Cache-Hash")
check("HEADER_RESP_CACHE_EXPIRE", protocol.HEADER_RESP_CACHE_EXPIRE == "X-Cache-Expire")
check("HEADER_RESP_CACHE_HIT", protocol.HEADER_RESP_CACHE_HIT == "X-Cache-Hit")

-- 2. 默认策略常量
check("DEFAULT_CACHE_TTL=21600", protocol.DEFAULT_CACHE_TTL == 21600)
check("DEFAULT_TTL_JITTER=600", protocol.DEFAULT_TTL_JITTER == 600)
check("MISS_FAST_FAIL", protocol.MISS_FAST_FAIL == "fast_fail")
check("MISS_PASSTHROUGH", protocol.MISS_PASSTHROUGH == "passthrough")

-- 3. compute_expire 带抖动在合理范围
local ttl, jitter = 100, 10
for _ = 1, 100 do
    local e = protocol.compute_expire(ttl, jitter)
    local now = ngx and ngx.time() or os.time()
    assert(e >= now + ttl - jitter and e <= now + ttl + jitter, "expire 越界: " .. tostring(e))
end
check("compute_expire 抖动范围正确", true)

-- 4. jitter=0 时精确
local e0 = protocol.compute_expire(50, 0)
local now = ngx and ngx.time() or os.time()
check("compute_expire jitter=0 精确", e0 == now + 50)

-- 5. get_config 返回带默认值的表
local cfg = protocol.get_config()
check("get_config.backend_url 存在", type(cfg.backend_url) == "string")
check("get_config.ttl 数字", type(cfg.ttl) == "number")
check("get_config.miss_mode 默认 fast_fail", cfg.miss_mode == "fast_fail")

print(("\n总计: %d 通过, %d 失败"):format(pass, fail))
if fail > 0 then os.exit(1) end
