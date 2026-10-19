-- eventd TRM §1.3 — Terminology: the terms the manual introduces, each held
-- to what its definition says the thing is or does.
--
-- One file-scope VM with two vCPUs: drain threads, writer threads and the
-- default shard count are one per CPU (or per attached buffer), and on one
-- CPU "one per CPU" and "one in total" cannot be told apart. Events are
-- placed on a chosen CPU by emitting them from a worker pinned there.
--
-- Thread names come from pipeline.rs (`eventd-drain-{cpu}`,
-- `eventd-writer-{shard:04}`); Linux keeps the first 15 bytes of a thread
-- name, so every writer reads `eventd-writer-0` and they are counted by
-- prefix.
--
-- The tests run in order and the last one changes the shard count, so it
-- comes last. The gap record and logical-live-size terms are proved in
-- constant-stores and config-retention, alongside the constants they share
-- a mechanism with.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local pip = require("helpers.pip")
local sys = require("helpers.sys")
peinit.claim(1, { cpus = 2 })

local vm = eventd.boot({ name = "ev-term", cpus = 2 })

local CC = eventd.T.config_change
local function q(s) return '"' .. s .. '"' end

local function count_prefix(names, prefix)
    local n = 0
    for _, name in ipairs(names) do if name:sub(1, #prefix) == prefix then n = n + 1 end end
    return n
end

--- Emit `etype` from a worker pinned to `cpu`.
local function emit_on(cpu, etype, payload)
    local w = vm:spawn_worker()
    local pid = w:syscall(sys.NR.getpid).ret
    local a = pip.setaffinity(w, pid, 1 << cpu)
    assert(a.ret == 0, "pinned to CPU " .. cpu)
    local r = eventd.emit(w, etype, payload or { cpu = cpu })
    w:kill(); w:join()
    assert(r.ret == 0, "emitted on CPU " .. cpu)
end

--- Which shard files hold an event of `etype`.
local function shards_holding(etype)
    local out = {}
    for _, s in ipairs(eventd.shards(vm)) do
        local n = eventd.sql(vm, s, "SELECT count(*) FROM events WHERE event_type = '" .. etype .. "'")[1][1]
        if n > 0 then out[#out + 1] = s:match("shard%-%d+%.db") end
    end
    return out
end

--- Set `key` and wait for its config change record; `rendered` is the
--- integer the record carries as config.value.
local function applied(key, value, rendered)
    eventd.set(vm, key, value):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) .. " AND config.value == " ..
        rendered .. " SINCE 1h ago", function(rs) return #rs >= 1 end)
end

local function secondary_indexes(shard)
    local out = {}
    for _, r in ipairs(eventd.sql(vm, shard, "SELECT name FROM sqlite_master WHERE type = 'index' " ..
        "AND name LIKE 'idx_events_%' AND name <> 'idx_events_timestamp'")) do
        out[#out + 1] = r[1]
    end
    return out
end

-- ---------------------------------------------------------------------------
-- Threads and shards
-- ---------------------------------------------------------------------------

test("there is exactly one drain thread per CPU", {
    spec = "eventd *term.there-is-exactly-one-drain-thread-per-cpu",
}, function(t)
    local names = eventd.thread_names(vm)
    t:assert_eq(count_prefix(names, "eventd-drain-"), 2, "two drain threads on two CPUs: " .. table.concat(names, ","))
    local set = {}
    for _, n in ipairs(names) do set[n] = true end
    t:assert(set["eventd-drain-0"] and set["eventd-drain-1"], "one for CPU 0 and one for CPU 1")
end)

test("each shard has exactly one writer thread", {
    spec = "eventd *term.each-shard-has-exactly-one-writer-thread-and-no-other-writer",
}, function(t)
    local shards = #eventd.shards(vm)
    t:assert_eq(shards, 2, "two shards")
    t:assert_eq(count_prefix(eventd.thread_names(vm), "eventd-writer-"), shards, "and as many writer threads")
    -- Every write to a shard is a writer-thread commit: an event emitted
    -- on each CPU lands in exactly one shard, and nothing else writes them.
    local etype = "pt.wr" .. eventd.marker()
    emit_on(0, etype); emit_on(1, etype)
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    t:assert_eq(#shards_holding(etype), 2, "the two events went to the two shards' writers")
end)

test("the query path treats every shard as one store", {
    spec = "eventd *term.the-query-path-treats-every-shard-as-one-store",
}, function(t)
    local etype = "pt.one" .. eventd.marker()
    emit_on(0, etype, { n = 1 })
    emit_on(1, etype, { n = 2 })
    emit_on(0, etype, { n = 3 })
    local rows = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 3 end)
    t:assert_eq(#shards_holding(etype), 2, "the events are split across both shard files")
    t:assert_eq(#rows, 3, "and one query returns all three")
    for i = 2, #rows do
        t:assert(rows[i - 1]["event.time"] >= rows[i]["event.time"], "in one order across shards (newest first)")
    end
end)

-- Route closed: the handoff channel is internal to eventd and has no
-- observable capacity; its bounds are compile-time constants
-- (config.rs:19-20, HANDOFF_SLOTS and HANDOFF_BYTES) passed straight to
-- BoundedQueue::new at pipeline.rs:156, never derived from MaxBatchSize.
test("the handoff channel is bounded by slots and by bytes", {
    spec = "eventd *term.the-handoff-channel-is-bounded-by-slots-and-bytes-independently-of-batch-size",
    skip = true,
    covered_by = "cargo:eventd eventd-core queue::tests::enforces_slot_and_byte_bounds_before_publish",
}, function() end)

-- ---------------------------------------------------------------------------
-- Synthetic events
-- ---------------------------------------------------------------------------

test("synthetic events bypass KMES and carry no stamps and no sequence numbers", {
    spec = "eventd *term.synthetic-events-bypass-kmes-and-carry-no-stamps-or-sequence-numbers",
}, function(t)
    local rings = { assert(kmes.attach(vm, 0)), assert(kmes.attach(vm, 1)) }
    applied("LogRetentionDays", "dword:17", "17")
    local seen = {}
    for _, ring in ipairs(rings) do
        for _, e in ipairs(kmes.drain(ring)) do seen[#seen + 1] = e.type end
        kmes.detach(ring)
    end
    eventd.unset(vm, "LogRetentionDays")
    for _, ty in ipairs(seen) do
        t:assert(ty ~= CC, "no config change record passed through KMES: " .. ty)
    end
    local row = eventd.rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "LogRetentionDays" SINCE 10m ago TAKE 1')[1]
    t:assert(row, "the config_change was stored nonetheless")
    for _, f in ipairs({ "event.sequence", "event.cpu", "emitter.class", "emitter.token.guid",
                         "emitter.true-token.guid", "emitter.process.guid" }) do
        t:assert(row[f] == nil, f .. " is null on a synthetic event: " .. json.encode(row))
    end
    t:assert_eq(row["event.type"], "eventd.config.changed", "and its type is one of eventd's five")
    t:assert(row["event.time"] ~= nil, "while it has a time of its own: " .. json.encode(row))
end)

-- ---------------------------------------------------------------------------
-- Adaptive indexing
-- ---------------------------------------------------------------------------

test("the desired index set is one global list in priority order", {
    spec = "eventd *term.the-desired-index-set-is-one-global-priority-ordered-list",
}, function(t)
    local f1, f2 = "pt" .. eventd.marker("a"), "pt" .. eventd.marker("b")
    eventd.rows(vm, "EVENTS INDEX " .. f1)
    eventd.rows(vm, "EVENTS INDEX " .. f2)
    local rows
    pcall(wait_until, function()
        rows = eventd.sql(vm, eventd.DB.meta, "SELECT field_path, priority FROM desired_indexes " ..
            "WHERE field_path IN ('" .. f1 .. "', '" .. f2 .. "') ORDER BY priority")
        return #rows == 2
    end, { timeout = 10, interval = 0.5 })
    t:assert_eq(#rows, 2, "both fields are in the desired set: " .. json.encode(rows))
    t:assert(rows[1][2] ~= rows[2][2], "each with its own priority: " .. json.encode(rows))
    for _, s in ipairs(eventd.shards(vm)) do
        t:assert(eventd.schema(vm, s).desired_indexes == nil, s .. " keeps no desired set of its own")
    end
    t:assert(eventd.schema(vm, eventd.DB.meta).desired_indexes, "the one list is in eventd-meta.db")
end)

test("material indexes converge on the desired set when quiet and are shed under pressure", {
    spec = "eventd *term.material-indexes-converge-when-quiet-and-diverge-under-pressure"
        .. " eventd *term.shedding-drops-secondary-indexes",
}, function(t)
    local field = "ptidx" .. eventd.marker()
    local etype = "pt.ix" .. eventd.marker()
    emit_on(0, etype, { [field] = 1 }); emit_on(1, etype, { [field] = 2 })
    eventd.rows(vm, "EVENTS INDEX " .. field)
    local shards = eventd.shards(vm)
    local converged = pcall(wait_until, function()
        for _, s in ipairs(shards) do if #secondary_indexes(s) == 0 then return false end end
        return true
    end, { timeout = 30, interval = 0.5 })
    t:assert(converged, "while quiet, every shard builds the desired index")
    -- Pressure: full batches of the smallest size, most of the window.
    applied("MaxBatchSize", "dword:100", "100")
    applied("SheddingWindowSeconds", "dword:10", "10")
    applied("SheddingBatchPercent", "dword:50", "50")
    local entries = {}
    for i = 1, 256 do entries[i] = { type = "pt.press", payload = kmes.PAYLOAD } end
    local shed = false
    for _ = 1, 60 do
        for _ = 1, 4 do kmes.emit_batch(vm, entries) end
        if #secondary_indexes(shards[1]) == 0 then shed = true; break end
    end
    local timestamp_kept = eventd.schema(vm, shards[1]).idx_events_timestamp ~= nil
    eventd.unset(vm, "MaxBatchSize"); eventd.unset(vm, "SheddingWindowSeconds")
    eventd.unset(vm, "SheddingBatchPercent")
    t:assert(shed, "under a run of full batches the shard's secondary index is dropped")
    t:assert(timestamp_kept, "while its timestamp index, which is not adaptive, stays")
    -- And quiet again, it converges back.
    eventd.rows(vm, "EVENTS INDEX " .. field)
    local back = pcall(wait_until, function() return #secondary_indexes(shards[1]) > 0 end,
        { timeout = 30, interval = 0.5 })
    t:assert(back, "once quiet, the shard converges on the desired set again")
end)

-- ---------------------------------------------------------------------------
-- Metrics: the series cache and rollups
-- ---------------------------------------------------------------------------

test("the series cache is a bounded in-memory map", {
    spec = "eventd *term.the-series-cache-is-a-bounded-in-memory-map-from-series-identity-to-row",
}, function(t)
    applied("MetricSeriesCacheSize", "dword:1000", "1000")
    applied("HealthMetricIntervalSeconds", "dword:1", "1")
    local name = eventd.marker("sc")
    for d = 1, 30 do
        local batch = {}
        for i = 1, 50 do
            batch[#batch + 1] = { name = name, type = "gauge", value = i, labels = eventd.map{ k = d .. "-" .. i } }
        end
        eventd.send_metric(vm, batch)
    end
    pcall(wait_until, function()
        return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name = '" .. name .. "'")[1][1] == 1500
    end, { timeout = 30, interval = 0.5 })
    vm:run("sleep 3")
    local rows = eventd.rows(vm, "METRIC eventd.metrics.series.cached SINCE 1m ago TAKE 1")
    eventd.unset(vm, "MetricSeriesCacheSize"); eventd.unset(vm, "HealthMetricIntervalSeconds")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name = '" .. name .. "'")[1][1],
        1500, "1500 distinct series were resolved")
    t:assert(rows[1] and rows[1].value <= 1000 and rows[1].value > 0,
        "while the cache holds at most its 1000 entries: " .. json.encode(rows[1]))
end)

-- An eligible window query (one series, a window aggregate, SINCE, 300 raw
-- inputs over AdaptiveRollupMinSamples=100) leaves rows in the rollups
-- table; a late sample then makes its window's row stale. The query's
-- windows before the samples begin are cached too, as empty rows with no
-- value (§5.6), so the window judged is the first one holding samples.
test("a rollup is reused only after its freshness is proved against the raw samples", {
    spec = "eventd *term.a-rollup-is-reused-only-after-its-freshness-is-proved-against-raw-samples",
}, function(t)
    local now = tonumber((vm:run("date +%s").stdout:gsub("%s", ""))) * 1000000000
    local name = eventd.marker("ro")
    for chunk = 0, 9 do
        local batch = {}
        for i = 1, 30 do
            local n = chunk * 30 + i
            batch[#batch + 1] = { name = name, type = "gauge", value = 10, timestamp = now - (330 - n) * 1000000000 }
        end
        eventd.send_metric(vm, batch)
    end
    pcall(wait_until, function()
        return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM samples s JOIN series r ON r.id = s.series_id " ..
            "WHERE r.name = '" .. name .. "'")[1][1] == 300
    end, { timeout = 30, interval = 0.5 })
    applied("AdaptiveRollupMinSamples", "dword:100", "100")
    local Q = "METRIC " .. name .. " SINCE 10m ago AVG_OVER 1m"
    eventd.rows(vm, Q)
    local cached
    pcall(wait_until, function()
        cached = eventd.sql(vm, eventd.DB.metrics, "SELECT r.window_start, r.value FROM rollups r JOIN series s " ..
            "ON s.id = r.series_id WHERE s.name = '" .. name .. "' AND r.source_max_sample_id > 0 " ..
            "ORDER BY r.window_start LIMIT 1")
        return #cached == 1
    end, { timeout = 20, interval = 0.5 })
    t:assert_eq(#cached, 1, "the window query seeded rollups for windows holding samples")
    local window = cached[1][1]
    t:assert_eq(cached[1][2], 10, "the first cached window's average is 10")
    -- A late sample in that window makes the rollup stale.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1000010, timestamp = window + 1000000000 })
    pcall(wait_until, function()
        return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM samples s JOIN series r ON r.id = s.series_id " ..
            "WHERE r.name = '" .. name .. "'")[1][1] == 301
    end, { timeout = 30, interval = 0.5 })
    local value
    for _, r in ipairs(eventd.rows(vm, Q)) do if r.timestamp == window then value = r.value end end
    eventd.unset(vm, "AdaptiveRollupMinSamples")
    t:assert(value and value > 10, "the window is answered from the raw samples, late one included, " ..
        "not from the stale rollup: " .. tostring(value))
end)

-- ---------------------------------------------------------------------------
-- Quarantine
-- ---------------------------------------------------------------------------

test("quarantine renames a corrupt database aside and creates an empty one in its place", {
    spec = "eventd *term.quarantine-renames-a-corrupt-database-aside-and-creates-an-empty-one",
}, function(t)
    local origin = eventd.marker("qu")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "before" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.stop(vm)
    local garbage = string.rep("this is not a sqlite database ", 200)
    vm:write_file(eventd.DB.logs, garbage)
    vm:run("rm -f " .. eventd.DB.logs .. "-wal " .. eventd.DB.logs .. "-shm")
    eventd.start(vm)
    local listing = vm:run("ls -1 " .. eventd.STORE.logs).stdout
    local aside = listing:match("(logs%.db%.corrupt%.%d+)")
    t:assert(aside, "the corrupt file was renamed aside: " .. listing)
    t:assert_eq(aside and vm:read_file(eventd.STORE.logs .. "/" .. aside), garbage, "with its bytes untouched")
    t:assert(listing:find("logs.db\n", 1, true), "and a logs.db is back at the original path")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago"), 0, "empty: the old record is not in it")
    t:assert_eq(eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'")[1][1], 0,
        "and it is a working, empty store")
end)

-- ---------------------------------------------------------------------------
-- Shard reconfiguration (last: it changes the shard count)
-- ---------------------------------------------------------------------------

-- A historical shard takes no new events and is still queried; the one
-- read-write descriptor on it is the retention coordinator's, and every
-- other is read-only.
test("a shard left by an earlier configuration takes no new events, is still queried, and has one writer", {
    spec = "eventd *term.a-historical-shard-takes-no-new-events-is-still-queried-and-is-written-only-by-retention",
}, function(t)
    local etype = "pt.hs" .. eventd.marker()
    emit_on(1, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(json.encode(shards_holding(etype)), '["shard-0001.db"]', "the event is in shard 1")
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    local pid = eventd.restart(vm)
    t:assert_eq(eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1m ago TAKE 1")[1]["store.shard-count"], 1,
        "eventd now runs one shard, so shard-0001.db is historical")
    t:assert_eq(#eventd.rows(vm, "EVENTS " .. etype .. " SINCE 10m ago"), 1, "and its event is still queried")
    local later = "pt.hs" .. eventd.marker()
    emit_on(1, later, { n = 2 })
    eventd.wait_rows(vm, "EVENTS " .. later .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(json.encode(shards_holding(later)), '["shard-0000.db"]',
        "an event from CPU 1 now goes to the one active shard, not the historical one")
    local modes = {}
    local _, _, on = eventd.fds_on(vm, pid, "/var/state/eventd/events/shard-0001.db")
    for _, e in ipairs(on) do modes[#modes + 1] = e.mode end
    t:assert(#modes >= 1, "eventd has shard-0001.db open")
    local rw = 0
    for _, m in ipairs(modes) do
        if m == 2 then rw = rw + 1 else t:assert_eq(m, 0, "every other descriptor on it is O_RDONLY") end
    end
    t:assert_eq(rw, 1, "one descriptor on it is O_RDWR: the retention coordinator's")
end)

test("eventd-meta.db is not a shard and survives shard reconfiguration", {
    spec = "eventd *term.the-metadata-database-is-not-a-shard-and-survives-shard-reconfiguration",
}, function(t)
    -- The previous test reconfigured 2 shards to 1; reconfigure back, and
    -- the metadata written before either change is still there.
    local field = "ptmeta" .. eventd.marker()
    eventd.rows(vm, "EVENTS INDEX " .. field)
    local created = eventd.sql(vm, eventd.DB.meta, "SELECT CAST(value AS TEXT) FROM meta WHERE key = 'created_at'")[1][1]
    eventd.unset(vm, "StorageShards")
    eventd.restart(vm)
    t:assert_eq(eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1m ago TAKE 1")[1]["store.shard-count"], 2,
        "two shards again, eventd-meta.db not counted among them")
    t:assert_eq(eventd.sql(vm, eventd.DB.meta, "SELECT CAST(value AS TEXT) FROM meta WHERE key = 'created_at'")[1][1],
        created, "the same eventd-meta.db: its creation time is unchanged")
    t:assert_eq(#eventd.sql(vm, eventd.DB.meta, "SELECT 1 FROM desired_indexes WHERE field_path = '" .. field .. "'"),
        1, "and the desired index recorded in it survived")
    t:assert(eventd.schema(vm, eventd.DB.meta).events == nil, "it holds no events table: it is not a shard")
end)
