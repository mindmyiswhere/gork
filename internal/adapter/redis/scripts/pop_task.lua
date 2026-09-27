-- KEYS[1] = tasks:queue (ZSET)
-- KEYS[2] = leases (ZSET)
-- ARGV[1] = worker_id
-- ARGV[2] = now (unix nano)
-- ARGV[3] = lease_deadline (unix nano) — когда считать задачу зависшей
-- ARGV[4] = supported types, comma-separated (пусто = все)

local queue_key  = KEYS[1]
local leases_key = KEYS[2]
local worker_id  = ARGV[1]
local now        = ARGV[2]
local deadline   = ARGV[3]
local types_csv  = ARGV[4]

local supported = {}
if types_csv ~= "" then
    for t in string.gmatch(types_csv, "([^,]+)") do
        supported[t] = true
    end
end

local candidates = redis.call('ZRANGE', queue_key, 0, 99)

for _, task_id in ipairs(candidates) do
    local task_type = redis.call('HGET', 'task:' .. task_id, 'type')
    if next(supported) == nil or supported[task_type] then
        redis.call('ZREM', queue_key, task_id)
        redis.call('HSET', 'task:' .. task_id,
            'status', 'RUNNING',
            'worker_id', worker_id,
            'updated_at', now,
            'lease_deadline', deadline)
        redis.call('SADD', 'tasks:running', task_id)
        redis.call('SREM', 'tasks:pending', task_id)
        redis.call('ZADD', leases_key, deadline, task_id)
        return task_id
    end
end

return nil