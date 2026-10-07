-- =====================================================================
-- 令牌桶结算 Lua 脚本（多退少补，原子执行）
-- =====================================================================
-- KEYS[1]: 令牌桶 Redis Key，格式 "tpm:{userId}"
-- ARGV[1]: delta = estimated - actual
--          >0 预扣过多，归还；<0 实际更多，补扣
-- ARGV[2]: 令牌桶容量上限（clamp 上界）
-- ARGV[3]: key TTL（秒），与 acquire 使用同一套解析值
-- 返回值:  结算后的 tokens
--
-- 时钟用 Redis TIME。key 已 EXPIRE 时重建完整 hash（tokens + last_refill_time），
-- 禁止裸 HINCRBY 造出没有 last_refill_time 的残缺桶。
-- =====================================================================

local key      = KEYS[1]
local delta    = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local ttl      = tonumber(ARGV[3])
local now      = tonumber(redis.call("TIME")[1])

local function clamp(tokens)
    if tokens < 0 then tokens = 0 end
    if tokens > capacity then tokens = capacity end
    return tokens
end

-- 长 SSE 期间 TTL 仍可能到期：按满桶 + delta 重建，而不是 no-op 漏补扣。
if redis.call("EXISTS", key) == 0 then
    local tokens = clamp(capacity + delta)
    redis.call("HMSET", key, "tokens", tokens, "last_refill_time", now)
    redis.call("EXPIRE", key, ttl)
    return tokens
end

local tokens = tonumber(redis.call("HGET", key, "tokens"))
if tokens == nil then
    -- 残缺 hash（缺 tokens 字段）无法安全结算，避免写半截状态
    return 0
end
tokens = clamp(tokens + delta)
redis.call("HMSET", key, "tokens", tokens, "last_refill_time", now)
redis.call("EXPIRE", key, ttl)
return tokens
