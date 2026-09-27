-- KEYS[1] = tasks:queue (ZSET)
-- ARGV[1] = worker_id
-- ARGV[2] = now (unix nano)
-- ARGV[3] = comma-separated supported types (пусто = все)

local queue_key = KEYS[1]
local worker_id = ARGV[1]
local now       = ARGV[2]
local types_csv = ARGV[3]

-- Разбираем список типов в таблицу для быстрой проверки
local supported = {}
if types_csv ~= "" then
    for t in string.gmatch(types_csv, "([^,]+)") do
        supported[t] = true
    end
end

-- Смотрим первые 100 задач в очереди — этого достаточно для фильтрации
local candidates = redis.call('ZRANGE', queue_key, 0, 99)

for _, task_id in ipairs(candidates) do
    local task_type = redis.call('HGET', 'task:' .. task_id, 'type')

    -- если у воркера пустой список — он умеет всё
    if next(supported) == nil or supported[task_type] then
        -- атомарно: убираем из очереди и помечаем RUNNING
        redis.call('ZREM', queue_key, task_id)
        redis.call('HSET', 'task:' .. task_id,
            'status', 'RUNNING',
            'worker_id', worker_id,
            'updated_at', now)
        redis.call('SADD', 'tasks:running', task_id)
        redis.call('SREM', 'tasks:pending', task_id)
        return task_id
    end
end

return nil