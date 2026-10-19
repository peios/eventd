-- eventd TRM Appendix A — Configuration Keys: every live tuning key under
-- `Machine\System\eventd`, its default, its range, and what eventd does
-- with a value it cannot use.
--
-- One file-scope VM, the image's own eventd, driven live through the
-- registry. eventd records every change it actually applies as a
-- `eventd.config.changed` event naming the key in `config.name`, its new
-- integer in `config.value` (TRM §3.2), and records nothing for
-- a change it ignores (config.rs `apply_reload`), so the event is the
-- witness for every claim here:
--
--   * the default is D exactly when setting the key to D, from absent, is
--     no change at all — eventd compares the value it would apply with the
--     one in use, and an absent key is in use at its default;
--   * a value is in range when setting it is recorded, and out of range
--     (or of the wrong type) when it is not;
--   * after any rejected value, deleting the key is recorded as a change
--     from the value that was still in use, which is the "retained" half.
--
-- Absence is asserted only behind a barrier: a later change to
-- `CrossTypeMaxLookbackSeconds`, which `applied_changes` lists last
-- (config.rs:819), so by the time its record is queryable every record an
-- earlier change would have produced in the same reload is written too.
-- When the key under test is that barrier key itself, a second key is
-- changed twice instead, which spans two whole reloads.
--
-- The restart-only keys (store and socket paths, StorageShards) and the
-- startup behaviour are in config-restart; the retention byte limits'
-- effect is in config-retention.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-config" })

local CC = eventd.T.config_change
local BARRIER = "CrossTypeMaxLookbackSeconds"
local BARRIER_2 = "CrossTypeWindowMs"

local function q(s) return '"' .. s .. '"' end

--- Every config_change record naming `key`, newest first.
local function changes(key)
    return eventd.rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) .. " SINCE 1h ago")
end

local barrier_seq = 0
--- Make sure every reload that could see what was set before this call
--- has finished writing its records.
local function barrier(key)
    local bkey, rounds = BARRIER, 1
    if key == BARRIER then bkey, rounds = BARRIER_2, 2 end
    for _ = 1, rounds do
        barrier_seq = barrier_seq + 1
        local v = (bkey == BARRIER) and (100000 + barrier_seq) or (100000 + barrier_seq)
        eventd.set(vm, bkey, "dword:" .. v):assert_ok()
        eventd.wait_rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(bkey) ..
            " AND config.value == " .. v .. " SINCE 1h ago",
            function(rs) return #rs >= 1 end, { desc = "barrier " .. bkey .. "=" .. v })
    end
end

--- Set `key` to `value` ("dword:5") and assert eventd applied nothing.
local function ignored(t, key, value, why)
    local before = #changes(key)
    eventd.set(vm, key, value):assert_ok()
    barrier(key)
    local after = changes(key)
    t:assert_eq(#after, before, why .. " — " .. key .. "=" .. value ..
        " recorded no change; newest: " .. json.encode(after[1]))
end

--- Set `key` to `value` and wait for the change eventd records for it.
local function applied(t, key, value, rendered, why)
    eventd.set(vm, key, value):assert_ok()
    -- `rendered` is the integer eventd records, compared as a number.
    local rows, ok = eventd.wait_rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) ..
        " AND config.value == " .. rendered .. " SINCE 1h ago",
        function(rs) return #rs >= 1 end, { timeout = 10, desc = key .. "=" .. rendered })
    t:assert(ok and #rows >= 1, why .. " — " .. key .. "=" .. value ..
        " was recorded as applied")
    return rows and rows[1]
end

--- Delete `key` and wait until eventd is back at its default.
local function reset(key)
    eventd.unset(vm, key)
    barrier(key)
end

--- The whole default/range/invalid sweep for one REG_DWORD key.
---
--- k: key, default, min, max, and optional pre/post hooks run around it
--- (to hold a companion key where two keys constrain each other).
local function sweep_dword(t, k)
    if k.pre then k.pre() end
    reset(k.key)
    local D = k.default
    ignored(t, k.key, "dword:" .. D, "the documented default " .. D .. " is the value in use")
    local first = (k.min ~= D) and k.min or k.max
    applied(t, k.key, "dword:" .. first, tostring(first), "an in-range value is applied")
    if k.min > 0 then
        ignored(t, k.key, "dword:" .. (k.min - 1), "one below the minimum " .. k.min .. " is ignored")
    end
    local in_use = first
    local second = (first == k.min) and k.max or k.min
    if second ~= D then
        applied(t, k.key, "dword:" .. second, tostring(second), "the range's other end is applied")
        in_use = second
    end
    if k.max < 0xffffffff then
        ignored(t, k.key, "dword:" .. (k.max + 1), "one above the maximum " .. k.max .. " is ignored")
    end
    ignored(t, k.key, "sz:" .. tostring(first), "a REG_SZ is the wrong type and is ignored")
    if in_use ~= D then
        applied(t, k.key, "dword:" .. D, tostring(D), "setting the default back is a change")
    end
    ignored(t, k.key, "dword:" .. D, "and the default again is none")
    local before = #changes(k.key)
    eventd.unset(vm, k.key)
    barrier(k.key)
    t:assert_eq(#changes(k.key), before,
        "deleting the key, which puts it back at its default " .. D .. ", changes nothing")
    if k.post then k.post() end
    if k.extra then k.extra(t) end
end

-- ---------------------------------------------------------------------------
-- A caller other than SYSTEM
--
-- TEST_USER, minted in Administrators so that the default descriptors
-- (`Security\*\*` and `Security\Admin`, which grant SYSTEM and
-- Administrators) give it what an administrator has, and a test can take
-- that away with a more specific descriptor. SeChangeNotifyPrivilege,
-- because a minted token holds no privileges and the walk to /run/eventd
-- needs traverse.
-- ---------------------------------------------------------------------------

local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
--- A fresh spec each time: minting records the logon session in it.
local function reader()
    return {
        user_sid = token.SID.TEST_USER,
        privs_present = NOTIFY, privs_enabled = NOTIFY,
        groups = {
            { sid = token.SID.EVERYONE, attributes = ENABLED },
            { sid = token.SID.AUTHENTICATED_USERS, attributes = ENABLED },
            { sid = token.SID.ADMINISTRATORS, attributes = ENABLED },
        },
    }
end

--- evctl as the worker `w`, decoded like eventd.query.
local function user_query(w, text)
    local r = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", text } })
    local out = { exit_code = r.exit_code, ok = r.exit_code == 0, stderr = r.stderr, rows = {} }
    if out.ok then
        for line in r.stdout:gmatch("[^\n]+") do out.rows[#out.rows + 1] = json.decode(line) end
    end
    return out
end

-- ---------------------------------------------------------------------------
-- Where the keys live, and what eventd does with keys and values it cannot use
-- ---------------------------------------------------------------------------

test("eventd reads its configuration from Machine\\System\\eventd and nowhere near it", {
    spec = "eventd *config.every-key-lives-under-machine-system-eventd",
}, function(t)
    -- The same name one level down (the Security subkey) and on the service
    -- definition is not eventd's configuration.
    local sub = eventd.SECURITY
    local svc = [[Machine\System\Services\eventd]]
    local before = #changes("MaxBatchSize")
    eventd.set(vm, "MaxBatchSize", "dword:20000", { key = sub }):assert_ok()
    eventd.set(vm, "MaxBatchSize", "dword:20000", { key = svc }):assert_ok()
    barrier("MaxBatchSize")
    t:assert_eq(#changes("MaxBatchSize"), before,
        "a MaxBatchSize under Security or the service key is not read")
    eventd.unset(vm, "MaxBatchSize", { key = sub })
    eventd.unset(vm, "MaxBatchSize", { key = svc })
    local row = applied(t, "MaxBatchSize", "dword:20000", "20000",
        "the same value directly under Machine\\System\\eventd")
    t:assert(row and row["config.type-previous"] == nil and row["config.value-previous"] == nil,
        "and it changed from no stored value, so neither previous field is there: " .. json.encode(row))
    reset("MaxBatchSize")
end)

test("a value eventd does not know is ignored, live and at boot", {
    spec = "eventd *config.unknown-keys-in-the-subtree-are-ignored",
}, function(t)
    local name = "PtUnknown" .. eventd.marker()
    local before = #eventd.rows(vm, "EVENTS " .. CC .. " SINCE 1h ago")
    eventd.set(vm, name, "dword:7"):assert_ok()
    eventd.set(vm, name .. "Sz", "sz:hello"):assert_ok()
    barrier(name)
    local all = eventd.rows(vm, "EVENTS " .. CC .. " SINCE 1h ago")
    for _, r in ipairs(all) do
        t:assert(not tostring(r["config.name"]):find("PtUnknown", 1, true),
            "no change names the unknown key: " .. json.encode(r))
    end
    -- the barrier is the only new change
    t:assert_eq(#all, before + 1, "only the barrier's change was recorded")
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "eventd is still answering")
    eventd.unset(vm, name)
    eventd.unset(vm, name .. "Sz")
end)

-- Every tuning key in a1, with a valid value other than its default.
-- StorageShards is not one: a1 lists it with the restart-only keys.
local TUNING = {
    WalCheckpointPages = 100, MaxBatchSize = 100, MaxBatchLatencyMs = 10,
    LogMaxBatchSize = 100, LogMaxBatchLatencyMs = 10, MaxLogDatagramBytes = 1048576,
    MetricMaxBatchSize = 100, MetricMaxBatchLatencyMs = 10, MaxMetricDatagramBytes = 1048576,
    MetricSeriesCacheSize = 1000, MetricAuthorizationCacheSize = 256,
    HealthMetricIntervalSeconds = 0,
    AdaptiveIndexWindowHours = 1, AdaptiveIndexPolicyIntervalMinutes = 1440,
    AdaptiveIndexCreateThreshold = 10000, AdaptiveIndexDropThreshold = 1000,
    SheddingWindowSeconds = 10, SheddingBatchPercent = 50, EmergencySheddingBufferPercent = 50,
    EventRetentionDays = 3650, LogRetentionDays = 3650, MetricRetentionDays = 3650,
    RetentionCheckIntervalMinutes = 1440, RetentionDeleteBatchRows = 100,
    QueryTimeoutMs = 300000, MaxConcurrentQueries = 4096, MaxStreamingQueries = 1024,
    MaxQueriesPerUser = 4096, MaxDistinctStreamValues = 1000, MaxQueryRequestBytes = 16777216,
    QueryResponseTargetBytes = 1024, MaxQueryHeldBytes = 16777216,
    AdaptiveRollupMinSamples = 100, AdaptiveRollupBatchRows = 16, AdaptiveRollupMaxRows = 0,
    CrossTypeWindowMs = 1000, CrossTypeMaxLookbackSeconds = 3600,
}

test("every tuning key is applied by the running eventd, without a restart", {
    spec = "eventd *config.every-tuning-parameter-applies-immediately",
}, function(t)
    local pid = eventd.pid(vm)
    for key, v in pairs(TUNING) do eventd.set(vm, key, "dword:" .. v):assert_ok() end
    local missing = {}
    for key, v in pairs(TUNING) do
        local _, ok = eventd.wait_rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) ..
            " AND config.value == " .. v .. " SINCE 1h ago",
            function(rs) return #rs >= 1 end, { timeout = 5, desc = key })
        if not ok then missing[#missing + 1] = key end
    end
    -- And one whose effect a caller sees: a request over the new hard limit
    -- is refused by the same process, straight after.
    eventd.set(vm, "MaxQueryRequestBytes", "dword:1024"):assert_ok()
    applied(t, "MaxQueryRequestBytes", "dword:1024", "1024", "a 1 KiB request limit")
    local refused = not eventd.query(vm, "EVENTS pt.none WHERE pad == \"" .. string.rep("a", 1100) .. "\"").ok
    for key in pairs(TUNING) do eventd.unset(vm, key) end
    barrier("x")
    table.sort(missing)
    t:assert_eq(#missing, 0, "every key's change was applied live; not applied: " .. table.concat(missing, ", "))
    t:assert(refused, "the request limit binds the next query")
    t:assert_eq(eventd.pid(vm), pid, "all by the eventd that was already running")
end)

-- ---------------------------------------------------------------------------
-- What a key's value does, where the appendix says more than its default
-- ---------------------------------------------------------------------------

--- A streaming query left running in the background; kill it when done.
local function stream_in_background(who)
    local text = "LOGS FROM " .. eventd.marker("idle") .. " STREAM"
    if who then return who:run_async("/usr/bin/evctl", { args = { text } }) end
    return vm:run_async("/usr/bin/evctl", { args = { text } })
end

local function stop(proc)
    pcall(function() proc:kill("kill") end)
    pcall(function() proc:wait("5s") end)
end

--- A streaming query that eventd refuses ends at once; one it accepts is
--- still running when the wait gives up and kills it.
local function stream_refused(who)
    local text = "LOGS FROM " .. eventd.marker("refuse") .. " STREAM"
    local p = who and who:run_async("/usr/bin/evctl", { args = { text } })
        or vm:run_async("/usr/bin/evctl", { args = { text } })
    local r = p:wait("4s")
    return r.exit_code == 1, r
end

local function settle_until_query_ok()
    wait_until(function() return eventd.query(vm, "EVENTS TAKE 1").ok end,
        { timeout = 15, interval = 0.25, desc = "eventd to accept queries again" })
end

local extras = {}

extras.MaxConcurrentQueries = function(t)
    applied(t, "MaxConcurrentQueries", "dword:1", "1", "one query at a time")
    local s = stream_in_background()
    local refused = pcall(wait_until, function()
        local r = eventd.query(vm, "EVENTS TAKE 1")
        return not r.ok and tostring(r.stderr):find("too many concurrent queries", 1, true) ~= nil
    end, { timeout = 10, interval = 0.25, desc = "a second query to be refused" })
    stop(s)
    reset("MaxConcurrentQueries")
    settle_until_query_ok()
    t:assert(refused, "with one streaming query open and MaxConcurrentQueries=1, a non-streaming " ..
        "query is refused: the streaming query counts against the global limit")
end

extras.MaxStreamingQueries = function(t)
    applied(t, "MaxStreamingQueries", "dword:1", "1", "one streaming query at a time")
    local s = stream_in_background()
    vm:run("sleep 1.5")
    local refused, r = stream_refused()
    local plain = eventd.query(vm, "EVENTS TAKE 1")
    stop(s)
    reset("MaxStreamingQueries")
    t:assert(refused, "a second streaming query, on another connection, is refused: exit " ..
        tostring(r.exit_code) .. " stderr " .. tostring(r.stderr))
    t:assert(tostring(r.stderr):find("too many concurrent streaming queries", 1, true),
        "as a streaming limit: " .. tostring(r.stderr))
    t:assert(plain.ok, "while a non-streaming query still runs")
end

extras.MaxQueriesPerUser = function(t)
    applied(t, "MaxQueriesPerUser", "dword:1", "1", "one query per user")
    -- SYSTEM is not counted: two of its streaming queries and a third,
    -- non-streaming one all run.
    local a, b = stream_in_background(), stream_in_background()
    vm:run("sleep 1.5")
    local third = eventd.query(vm, "EVENTS TAKE 1")
    local a_running = a:status() == "running"
    local b_running = b:status() == "running"
    -- An ordinary user is: one streaming query, and the next is refused.
    local user_refused, user_msg
    token.as_principal(t, vm, reader(), function(w)
        local u = stream_in_background(w)
        vm:run("sleep 1.5")
        local r = user_query(w, "EVENTS TAKE 1")
        user_refused = not r.ok and tostring(r.stderr):find("from this user", 1, true) ~= nil
        user_msg = tostring(r.exit_code) .. " " .. tostring(r.stderr)
        stop(u)
    end)
    stop(a); stop(b)
    reset("MaxQueriesPerUser")
    t:assert(a_running and b_running, "SYSTEM's two streaming queries both stayed open: " ..
        tostring(a_running) .. " " .. tostring(b_running))
    t:assert(third.ok, "and a third SYSTEM query ran: " .. tostring(third.stderr))
    t:assert(user_refused, "while another user's second query was refused: " .. tostring(user_msg))
end

extras.MaxQueryRequestBytes = function(t)
    local long = "EVENTS pt.none WHERE pad == \"" .. string.rep("a", 1100) .. "\" TAKE 1"
    t:assert(eventd.query(vm, long).ok, "a 1.1 KB query is accepted at the default")
    applied(t, "MaxQueryRequestBytes", "dword:1024", "1024", "a 1 KiB request limit")
    local r = eventd.query(vm, long)
    local short = eventd.query(vm, "EVENTS pt.none TAKE 1")
    reset("MaxQueryRequestBytes")
    t:assert(not r.ok, "the same 1.1 KB query is refused against a 1024-byte hard limit")
    t:assert(short.ok, "while a short one is not")
end

extras.QueryResponseTargetBytes = function(t)
    local origin = eventd.marker("big")
    local message = string.rep("m", 4000)
    eventd.send_log(vm, { origin = origin, is_error = false, message = message })
    applied(t, "QueryResponseTargetBytes", "dword:1024", "1024", "a 1 KiB response target")
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    reset("QueryResponseTargetBytes")
    t:assert_eq(#rows, 1, "the 4 KB record is still returned")
    t:assert_eq(rows[1] and #rows[1].message, 4000,
        "whole: one record exceeds the soft target rather than being cut or refused")
end

extras.HealthMetricIntervalSeconds = function(t)
    local Q = "METRIC eventd.logs.stored SINCE 10m ago"
    local function samples() return eventd.rows(vm, Q) end
    applied(t, "HealthMetricIntervalSeconds", "dword:1", "1", "a 1 s health interval")
    local rows, ok = eventd.wait_rows(vm, Q, function(rs) return #rs >= 3 end, { timeout = 15 })
    t:assert(ok, "at 1 s, eventd.* health samples appear: " .. #rows)
    applied(t, "HealthMetricIntervalSeconds", "dword:0", "0", "0 turns them off")
    vm:run("sleep 2")
    local off = #samples()
    vm:run("sleep 5")
    t:assert_eq(#samples(), off, "with the interval at 0, no further health samples are recorded")
    -- Back at the default, they resume at about one per 15 s.
    reset("HealthMetricIntervalSeconds")
    local again, ok2 = eventd.wait_rows(vm, Q, function(rs) return #rs >= off + 2 end, { timeout = 50 })
    t:assert(ok2, "at the default the samples resume: " .. #again .. " after " .. off)
    if ok2 then
        table.sort(again, function(x, y) return x.timestamp < y.timestamp end)
        local gap = (again[#again].timestamp - again[#again - 1].timestamp) / 1e9
        t:assert(gap >= 13 and gap <= 18, "15 s apart at the default: " .. gap)
    end
end

extras.CrossTypeWindowMs = function(t)
    -- A log 4 s before an event, one 4 s after, and one 10 s after. A
    -- window of 15 s centred on the event reaches 7.5 s each way.
    local etype = "pt.cw" .. eventd.marker()
    local before, after, late = eventd.marker("b"), eventd.marker("a"), eventd.marker("l")
    eventd.send_log(vm, { origin = before, is_error = false, message = "x" })
    vm:run("sleep 4")
    eventd.emit(vm, etype, { n = 1 })
    vm:run("sleep 4")
    eventd.send_log(vm, { origin = after, is_error = false, message = "x" })
    vm:run("sleep 6")
    eventd.send_log(vm, { origin = late, is_error = false, message = "x" })
    eventd.wait_rows(vm, "LOGS FROM " .. late .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local function matches(origin)
        return #eventd.rows(vm, "EVENTS " .. etype .. " WHERE LOG " .. origin ..
            " EXISTS SINCE 10m ago") == 1
    end
    t:assert(matches(before), "a log 4 s before the event is inside the window: it is centred, not trailing")
    t:assert(matches(after), "a log 4 s after is inside")
    t:assert(not matches(late), "a log 10 s after is outside a 15 s centred window")
    applied(t, "CrossTypeWindowMs", "dword:30000", "30000", "a 30 s window")
    local wide = matches(late)
    reset("CrossTypeWindowMs")
    t:assert(wide, "and inside a 30 s one")
end

extras.AdaptiveIndexDropThreshold = function(t)
    -- With the create threshold at its default 100, a drop threshold of
    -- 100 is not below it.
    ignored(t, "AdaptiveIndexDropThreshold", "dword:100", "a drop threshold equal to the create threshold")
    applied(t, "AdaptiveIndexDropThreshold", "dword:99", "99", "one below it is accepted")
    reset("AdaptiveIndexDropThreshold")
end

extras.AdaptiveRollupMaxRows = function(t)
    -- Two series of 300 samples each, one per second over the last five
    -- minutes, in the past so that whole one-minute windows are complete.
    local now = tonumber((vm:run("date +%s").stdout:gsub("%s", ""))) * 1000000000
    local function seed(name)
        for chunk = 0, 9 do
            local batch = {}
            for i = 1, 30 do
                local n = chunk * 30 + i
                batch[#batch + 1] = { name = name, type = "gauge", value = n,
                    timestamp = now - (330 - n) * 1000000000 }
            end
            eventd.send_metric(vm, batch)
        end
        eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
            function(rs) return #rs == 300 end, { desc = "300 samples of " .. name })
    end
    local function rollups(name)
        return eventd.sql(vm, eventd.DB.metrics,
            "SELECT count(*) FROM rollups r JOIN series s ON s.id = r.series_id WHERE s.name = '" ..
            name .. "'")[1][1]
    end
    -- Whether the default cap seeds rollups is §5.6's to prove; this is
    -- only what a cap of 0 does to an eligible query (one series, a
    -- window aggregate, a lower bound, past the raw-input minimum).
    local name = eventd.marker("ru")
    seed(name)
    applied(t, "AdaptiveRollupMinSamples", "dword:100", "100", "eligible from 100 raw inputs")
    local W = " SINCE 10m ago AVG_OVER 1m"
    local first = eventd.rows(vm, "METRIC " .. name .. W)
    applied(t, "AdaptiveRollupMaxRows", "dword:0", "0", "the cache off")
    local again = eventd.rows(vm, "METRIC " .. name .. W)
    eventd.rows(vm, "METRIC " .. name .. W)
    vm:run("sleep 3")
    local written = rollups(name)
    reset("AdaptiveRollupMaxRows")
    eventd.unset(vm, "AdaptiveRollupMinSamples")
    t:assert_eq(written, 0, "with the cap at 0, repeated eligible queries leave no rollup rows")
    -- The last window may have closed between the two queries; every
    -- window before it is fixed.
    t:assert(#first >= 5, "the window query answered: " .. #first)
    table.remove(first); table.remove(again)
    t:assert_eq(json.encode(again), json.encode(first), "and the results are those the cache-on query gave")
end

local DWORDS = {
    { anchor = "eventd *config.wal-checkpoint-pages-defaults-to-1000-on-every-database",
      key = "WalCheckpointPages", default = 1000, min = 100, max = 100000 },
    { anchor = "eventd *config.max-batch-size-defaults-to-10000-events-per-transaction",
      key = "MaxBatchSize", default = 10000, min = 100, max = 100000 },
    { anchor = "eventd *config.max-batch-latency-defaults-to-100-ms",
      key = "MaxBatchLatencyMs", default = 100, min = 10, max = 5000 },
    { anchor = "eventd *config.log-max-batch-size-defaults-to-5000-records-per-transaction",
      key = "LogMaxBatchSize", default = 5000, min = 100, max = 100000 },
    { anchor = "eventd *config.log-max-batch-latency-defaults-to-500-ms",
      key = "LogMaxBatchLatencyMs", default = 500, min = 10, max = 5000 },
    { anchor = "eventd *config.max-log-datagram-bytes-defaults-to-the-262144-floor-and-cannot-go-lower",
      key = "MaxLogDatagramBytes", default = 262144, min = 262144, max = 1048576 },
    { anchor = "eventd *config.metric-max-batch-size-defaults-to-5000-samples-per-transaction",
      key = "MetricMaxBatchSize", default = 5000, min = 100, max = 100000 },
    { anchor = "eventd *config.metric-max-batch-latency-defaults-to-1000-ms",
      key = "MetricMaxBatchLatencyMs", default = 1000, min = 10, max = 5000 },
    { anchor = "eventd *config.max-metric-datagram-bytes-defaults-to-the-262144-floor-and-cannot-go-lower",
      key = "MaxMetricDatagramBytes", default = 262144, min = 262144, max = 1048576 },
    { anchor = "eventd *config.metric-series-cache-size-defaults-to-50000-entries",
      key = "MetricSeriesCacheSize", default = 50000, min = 1000, max = 1000000 },
    { anchor = "eventd *config.metric-authorization-cache-size-defaults-to-16384-verdicts",
      key = "MetricAuthorizationCacheSize", default = 16384, min = 256, max = 1000000 },
    { anchor = "eventd *config.health-metric-interval-defaults-to-15-seconds-and-zero-turns-it-off",
      key = "HealthMetricIntervalSeconds", default = 15, min = 0, max = 3600 },
    { anchor = "eventd *config.adaptive-index-window-defaults-to-24-hours",
      key = "AdaptiveIndexWindowHours", default = 24, min = 1, max = 168 },
    { anchor = "eventd *config.adaptive-index-policy-interval-defaults-to-and-cannot-go-below-60-minutes",
      key = "AdaptiveIndexPolicyIntervalMinutes", default = 60, min = 60, max = 1440 },
    -- The create threshold's minimum (10) equals the drop threshold's
    -- default, and the drop threshold must stay below it; the drop
    -- threshold is held at 1 while the create threshold's range is swept.
    { anchor = "eventd *config.adaptive-index-create-threshold-defaults-to-100-queries",
      key = "AdaptiveIndexCreateThreshold", default = 100, min = 10, max = 10000,
      pre = function() eventd.set(vm, "AdaptiveIndexDropThreshold", "dword:1"):assert_ok(); barrier("x") end,
      post = function() eventd.unset(vm, "AdaptiveIndexDropThreshold"); barrier("x") end },
    -- And the drop threshold's maximum (1000) is above the create
    -- threshold's default, so the create threshold is held at its maximum.
    { anchor = "eventd *config.adaptive-index-drop-threshold-defaults-to-10-queries",
      key = "AdaptiveIndexDropThreshold", default = 10, min = 1, max = 1000,
      pre = function() eventd.set(vm, "AdaptiveIndexCreateThreshold", "dword:10000"):assert_ok(); barrier("x") end,
      post = function() eventd.unset(vm, "AdaptiveIndexCreateThreshold"); barrier("x") end },
    { anchor = "eventd *config.shedding-window-defaults-to-30-seconds",
      key = "SheddingWindowSeconds", default = 30, min = 10, max = 300 },
    { anchor = "eventd *config.graduated-shedding-defaults-to-75-percent-of-batches-over-75-percent-of-max-batch-size",
      key = "SheddingBatchPercent", default = 75, min = 50, max = 100 },
    { anchor = "eventd *config.emergency-shedding-defaults-to-75-percent-ring-buffer-fill",
      key = "EmergencySheddingBufferPercent", default = 75, min = 50, max = 95 },
    { anchor = "eventd *config.event-retention-defaults-to-30-days",
      key = "EventRetentionDays", default = 30, min = 1, max = 3650 },
    { anchor = "eventd *config.log-retention-defaults-to-14-days",
      key = "LogRetentionDays", default = 14, min = 1, max = 3650 },
    { anchor = "eventd *config.metric-retention-defaults-to-90-days",
      key = "MetricRetentionDays", default = 90, min = 1, max = 3650 },
    { anchor = "eventd *config.retention-check-interval-defaults-to-60-minutes",
      key = "RetentionCheckIntervalMinutes", default = 60, min = 1, max = 1440 },
    { anchor = "eventd *config.retention-delete-batch-defaults-to-10000-rows-per-transaction",
      key = "RetentionDeleteBatchRows", default = 10000, min = 100, max = 100000 },
    { anchor = "eventd *config.query-timeout-defaults-to-30000-ms",
      key = "QueryTimeoutMs", default = 30000, min = 1000, max = 300000 },
    { anchor = "eventd *config.max-concurrent-queries-defaults-to-128-globally-including-streaming",
      key = "MaxConcurrentQueries", default = 128, min = 1, max = 4096 },
    { anchor = "eventd *config.max-streaming-queries-defaults-to-64-globally",
      key = "MaxStreamingQueries", default = 64, min = 1, max = 1024 },
    { anchor = "eventd *config.max-queries-per-user-defaults-to-16-and-does-not-count-system",
      key = "MaxQueriesPerUser", default = 16, min = 1, max = 4096 },
    { anchor = "eventd *config.max-distinct-stream-values-defaults-to-100000-per-query",
      key = "MaxDistinctStreamValues", default = 100000, min = 1000, max = 10000000 },
    { anchor = "eventd *config.max-query-request-bytes-is-a-hard-limit-defaulting-to-65536",
      key = "MaxQueryRequestBytes", default = 65536, min = 1024, max = 16777216 },
    { anchor = "eventd *config.query-response-target-bytes-is-a-soft-target-defaulting-to-65536",
      key = "QueryResponseTargetBytes", default = 65536, min = 1024, max = 16777216 },
    { anchor = "eventd *config.max-query-held-bytes-defaults-to-256-mib-across-all-queries",
      key = "MaxQueryHeldBytes", default = 268435456, min = 16777216, max = 4294967295 },
    { anchor = "eventd *config.adaptive-rollup-min-samples-defaults-to-1000-raw-inputs",
      key = "AdaptiveRollupMinSamples", default = 1000, min = 100, max = 1000000 },
    { anchor = "eventd *config.adaptive-rollup-batch-rows-defaults-to-512-windows-per-command",
      key = "AdaptiveRollupBatchRows", default = 512, min = 16, max = 4096 },
    { anchor = "eventd *config.adaptive-rollup-max-rows-defaults-to-100000-and-0-disables-the-cache",
      key = "AdaptiveRollupMaxRows", default = 100000, min = 0, max = 10000000 },
    { anchor = "eventd *config.cross-type-window-defaults-to-a-centred-15000-ms",
      key = "CrossTypeWindowMs", default = 15000, min = 1000, max = 300000 },
    { anchor = "eventd *config.cross-type-max-lookback-defaults-to-604800-seconds",
      key = "CrossTypeMaxLookbackSeconds", default = 604800, min = 3600, max = 2592000 },
}

for _, k in ipairs(DWORDS) do
    k.extra = extras[k.key]
    test(string.format("%s is a REG_DWORD defaulting to %d, range %d-%d, invalid values ignored",
        k.key, k.default, k.min, k.max), {
        spec = k.anchor,
        tags = k.tags,
    }, function(t) sweep_dword(t, k) end)
end

-- ---------------------------------------------------------------------------
-- The security subtree
-- ---------------------------------------------------------------------------

test("read-path descriptors are the default values of keys under Machine\\System\\eventd\\Security", {
    spec = "eventd *config.read-path-descriptors-live-under-the-security-subtree",
}, function(t)
    for _, sub in ipairs({ [[Events\*]], [[Logs\*]], [[Metrics\*]], [[Admin]] }) do
        local sd, raw = eventd.read_descriptor(vm, eventd.SECURITY .. "\\" .. sub)
        t:assert(sd and access.parse_sd(sd), "Security\\" .. sub .. " holds a descriptor: " .. raw)
    end
    -- A pattern key of its own governs reads of the origin it names.
    local origin = eventd.marker("sec")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "guarded" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local key = eventd.SECURITY .. [[\Logs\]] .. origin
    local seen_default, seen_guarded, seen_after
    token.as_principal(t, vm, reader(), function(w)
        seen_default = #user_query(w, "LOGS FROM " .. origin .. " SINCE 10m ago").rows
        eventd.write_descriptor(vm, key, access.simple({
            access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM) })):assert_ok()
        local ok = pcall(wait_until, function()
            return #user_query(w, "LOGS FROM " .. origin .. " SINCE 10m ago").rows == 0
        end, { timeout = 5, interval = 0.25 })
        seen_guarded = ok and 0 or 1
        vm:run("reg del '" .. key .. "'")
        pcall(wait_until, function()
            return #user_query(w, "LOGS FROM " .. origin .. " SINCE 10m ago").rows == 1
        end, { timeout = 5, interval = 0.25 })
        seen_after = #user_query(w, "LOGS FROM " .. origin .. " SINCE 10m ago").rows
    end)
    t:assert_eq(seen_default, 1, "an Administrator reads the origin under the default Logs\\* descriptor")
    t:assert_eq(seen_guarded, 0, "a descriptor at Security\\Logs\\<origin> granting only SYSTEM hides it")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago"), 1, "from them, not from SYSTEM")
    t:assert_eq(seen_after, 1, "and removing it restores the default")
end)

test("a descriptor change is in force for the next query, without a restart", {
    spec = "eventd *config.a-security-descriptor-change-applies-from-the-next-query",
}, function(t)
    local etype = "pt.sd" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local key = eventd.SECURITY .. [[\Events\]] .. etype
    local pid = eventd.pid(vm)
    local before, denied, granted
    token.as_principal(t, vm, reader(), function(w)
        before = #user_query(w, "EVENTS " .. etype .. " SINCE 10m ago").rows
        eventd.write_descriptor(vm, key, access.simple({
            access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM) })):assert_ok()
        -- "the next query": allow only the registry watch's own latency.
        denied = pcall(wait_until, function()
            return #user_query(w, "EVENTS " .. etype .. " SINCE 10m ago").rows == 0
        end, { timeout = 3, interval = 0.1 })
        eventd.write_descriptor(vm, key, access.simple({
            access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM),
            access.ace(access.ACE.ALLOWED, 0x1, token.SID.TEST_USER) })):assert_ok()
        granted = pcall(wait_until, function()
            return #user_query(w, "EVENTS " .. etype .. " SINCE 10m ago").rows == 1
        end, { timeout = 3, interval = 0.1 })
    end)
    vm:run("reg del '" .. key .. "'")
    t:assert_eq(before, 1, "visible under the default descriptor")
    t:assert(denied, "a descriptor denying the caller takes effect for the following queries")
    t:assert(granted, "and so does one granting it again")
    t:assert_eq(eventd.pid(vm), pid, "with no restart in between")
end)

test("Security\\Admin governs INDEX and grants it to SYSTEM and Administrators by default", {
    spec = "eventd *config.security-admin-governs-eventd-administer-and-defaults-to-system-and-administrators",
}, function(t)
    local key = eventd.SECURITY .. [[\Admin]]
    local original = eventd.read_descriptor(vm, key)
    local sd = access.parse_sd(original)
    local grants = {}
    for _, ace in ipairs(sd.dacl.aces) do
        if ace.type == access.ACE.ALLOWED then grants[token.sid_string(ace.sid)] = ace.mask end
    end
    t:assert_eq(grants["S-1-5-18"], 0x4, "SYSTEM is granted EVENTD_ADMINISTER: " .. json.encode(grants))
    t:assert_eq(grants["S-1-5-32-544"], 0x4, "and so are Administrators")
    local n = 0
    for _ in pairs(grants) do n = n + 1 end
    t:assert_eq(n, 2, "and nobody else")
    t:assert(eventd.query(vm, "EVENTS INDEX " .. eventd.marker("f")).ok, "SYSTEM may INDEX")
    local admin_ok, after_ok, after_msg
    token.as_principal(t, vm, reader(), function(w)
        admin_ok = user_query(w, "EVENTS INDEX " .. eventd.marker("f")).ok
        -- The same Administrator, once Admin grants the right to SYSTEM only.
        eventd.write_descriptor(vm, key, access.simple({
            access.ace(access.ACE.ALLOWED, 0x4, token.SID.LOCAL_SYSTEM) })):assert_ok()
        pcall(wait_until, function()
            return not user_query(w, "EVENTS INDEX " .. eventd.marker("f")).ok
        end, { timeout = 3, interval = 0.1 })
        local r = user_query(w, "EVENTS INDEX " .. eventd.marker("f"))
        after_ok, after_msg = r.ok, tostring(r.stderr)
    end)
    eventd.write_descriptor(vm, key, original):assert_ok()
    t:assert(admin_ok, "an Administrator may INDEX under the default")
    t:assert(not after_ok, "and may not once Security\\Admin stops granting it: " .. after_msg)
    t:assert(after_msg:find("EVENTD_ADMINISTER", 1, true), "refused for EVENTD_ADMINISTER: " .. after_msg)
end)

test("the Admin descriptor is registry policy; eventd-meta.db holds no descriptor", {
    spec = "eventd *config.the-admin-descriptor-is-registry-policy-not-metadata-database-data",
}, function(t)
    local admin = eventd.read_descriptor(vm, eventd.SECURITY .. [[\Admin]])
    local tables = eventd.sql(vm, eventd.DB.meta,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    local names = {}
    for _, r in ipairs(tables) do names[#names + 1] = r[1] end
    for _, name in ipairs(names) do
        local cols = eventd.sql(vm, eventd.DB.meta, "SELECT * FROM " .. name)
        for _, row in ipairs(cols) do
            for _, v in ipairs(row) do
                t:assert(not tostring(v):lower():find(eventd.hex(admin), 1, true),
                    "no row of " .. name .. " carries the Admin descriptor")
            end
        end
    end
    t:assert_eq(table.concat(names, ","), "desired_indexes,index_counters,meta,sequence_checkpoints",
        "and its tables are index policy, sequence checkpoints and its own metadata")
end)
