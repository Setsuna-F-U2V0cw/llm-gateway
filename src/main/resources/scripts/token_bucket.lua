-- =====================================================================
-- 令牌桶限流 Lua 脚本（原子执行，防止并发超卖）
-- =====================================================================
-- KEYS[1]: 令牌桶 Redis Key，格式 "tpm:{userId}"
-- ARGV[1]: 本次请求预扣的 Token 数（预估值）
-- ARGV[2]: 令牌桶容量上限（max_tokens）
-- ARGV[3]: 每秒补充的 Token 数（refill_rate = TPM / 60）
-- ARGV[4]: 当前时间戳（秒）
-- 返回值:  1 = 允许通过；0 = 触发限流
-- =====================================================================

local key        = KEYS[1]
local cost       = tonumber(ARGV[1])
local capacity   = tonumber(ARGV[2])
local refillRate = tonumber(ARGV[3])
local now        = tonumber(ARGV[4])

-- 读取桶当前状态：{tokens, last_refill_time}
local bucket = redis.call("HMGET", key, "tokens", "last_refill_time")
local tokens        = tonumber(bucket[1])
local lastRefillTime = tonumber(bucket[2])

if tokens == nil then
    -- 首次访问，初始化为满桶
    tokens = capacity
    lastRefillTime = now
end

-- 计算距上次补充经过的秒数，按速率补充令牌（不超过容量上限）
local elapsed = math.max(0, now - lastRefillTime)
local refilled = math.floor(elapsed * refillRate)
tokens = math.min(capacity, tokens + refilled)

-- 判断是否有足够令牌
if tokens < cost then
    -- 令牌不足，拒绝请求，但仍更新时间戳（让补充继续计算）
    redis.call("HMSET", key, "tokens", tokens, "last_refill_time", now)
    redis.call("EXPIRE", key, 120)
    return 0
end

-- 预扣令牌
tokens = tokens - cost
redis.call("HMSET", key, "tokens", tokens, "last_refill_time", now)
redis.call("EXPIRE", key, 120)
return 1
