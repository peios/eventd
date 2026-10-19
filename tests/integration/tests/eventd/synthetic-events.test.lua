-- eventd TRM §2.6 — synthetic events: the records eventd writes about
-- itself (startup, shutdown, configuration changes, storage errors, and the
-- gap records of §2.5), written straight into a shard rather than through
-- KMES, and where they go.
--
-- One file-scope VM on one vCPU, booted with `StorageShards = 2` so that
-- "shard 0" is a choice eventd makes rather than the only shard. KMES
-- traffic is watched through the agent's own ring attachment; the rows
-- themselves are read from the shard files with the host-side sqlite copy.
--
-- The last tests make stores full — the metric store, then shard 0's
-- write-ahead log, then shard 1's — with small filled tmpfs mounts (see
-- the section below), for the storage-error and shard-fallback statements.
--
-- Gap records are tested with gap detection (gap-detection.test.lua and,
-- for the shard a gap goes to, kmes-consumption.test.lua).

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(1)

local vm = eventd.boot({
    name = "ev-synth",
    config = { { name = "StorageShards", type = "dword", data = 2 } },
})

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

--- The five types eventd writes itself, as an SQL list.
local OWN_TYPES = {}
for _, k in ipairs({ "startup", "shutdown", "gap", "config_change", "storage_error" }) do
    OWN_TYPES[#OWN_TYPES + 1] = "'" .. eventd.T[k] .. "'"
end
local OWN_TYPES_SQL = "(" .. table.concat(OWN_TYPES, ",") .. ")"

local function is_own_type(ty)
    for _, k in ipairs({ "startup", "shutdown", "gap", "config_change", "storage_error" }) do
        if eventd.T[k] == ty then return true end
    end
    return false
end

--- Synthetic rows from every shard of `on`: {shard, id, type, timestamp,
--- header = {cpu_id, sequence, origin_class, guids...}}.
local function synthetic_rows(on, where)
    local out = {}
    for _, shard in ipairs(eventd.shards(on)) do
        for _, r in ipairs(eventd.sql(on, shard,
            "SELECT id, event_type, timestamp, cpu_id, sequence, origin_class, " ..
            "effective_token_guid, true_token_guid, process_guid FROM events " ..
            "WHERE event_type IN " .. OWN_TYPES_SQL .. (where and (" AND " .. where) or "") ..
            " ORDER BY timestamp")) do
            out[#out + 1] = {
                shard = shard:match("(shard%-%d+)%.db$"), id = r[1], type = r[2], timestamp = r[3],
                header = { r[4], r[5], r[6], r[7], r[8], r[9] },
            }
        end
    end
    table.sort(out, function(a, b) return a.timestamp < b.timestamp end)
    return out
end

local function of_type(rows, event_type)
    local out = {}
    for _, r in ipairs(rows) do
        if r.type == event_type then out[#out + 1] = r end
    end
    return out
end

--- A KMES event's timestamp: the guest's realtime clock, read by emitting.
local function guest_now(on)
    local event_type = "pt.synth." .. eventd.marker("clock")
    assert(eventd.emit(on, event_type, { n = 1 }).ret == 0)
    return eventd.wait_rows(on, "EVENTS " .. event_type .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)[1]
end

local function config_change(on, key, new_value)
    local found
    eventd.wait_rows(on, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago TAKE 1000",
        function(rs)
            for _, r in ipairs(rs) do
                if r["config.name"] == key and r["config.value"] == new_value then found = r; return true end
            end
            return false
        end, { desc = "a config change of " .. key .. " to " .. new_value })
    return found
end

-- ---------------------------------------------------------------------------
-- What they are
-- ---------------------------------------------------------------------------

test("a restart writes shutdown then startup straight into shard 0, and nothing into KMES", {
    spec = "eventd *synthetic.synthetic-events-are-written-straight-to-a-shard-and-never-touch-kmes"
        .. " eventd *synthetic.starting-and-attaching-to-kmes-emits-eventd-daemon-started"
        .. " eventd *synthetic.graceful-shutdown-emits-eventd-daemon-stopped"
        .. " eventd *synthetic.a-runtime-configuration-change-emits-eventd-config-changed"
        .. " eventd *synthetic.a-synthetic-event-has-a-timestamp-and-one-of-five-eventd-types"
        .. " eventd *synthetic.a-daemon-wide-records-timestamp-is-when-eventd-noticed",
}, function(t)
    local ring = assert(kmes.attach(vm, 0))
    local before = guest_now(vm)
    eventd.restart(vm)
    eventd.set(vm, "LogRetentionDays", "dword:12"):assert_ok()
    config_change(vm, "LogRetentionDays", 12)
    eventd.unset(vm, "LogRetentionDays")
    local after = guest_now(vm)
    local through_kmes = kmes.drain(ring)
    kmes.detach(ring)

    for _, e in ipairs(through_kmes) do
        t:assert(not is_own_type(e.type), "nothing eventd writes itself passed through KMES: " .. e.type)
    end
    t:assert(#through_kmes > 0, "while the watch did see KMES traffic (" .. #through_kmes .. " events)")

    before, after = before["event.time"], after["event.time"]
    local rows = synthetic_rows(vm, "timestamp > " .. before .. " AND timestamp < " .. after)
    local shutdown = of_type(rows, eventd.T.shutdown)
    local startup = of_type(rows, eventd.T.startup)
    local change = of_type(rows, eventd.T.config_change)
    t:assert_eq(#shutdown, 1, "the graceful stop wrote one shutdown record")
    t:assert_eq(#startup, 1, "the start wrote one startup record")
    t:assert(#change >= 1, "the live change wrote a config_change record")
    t:assert(shutdown[1].timestamp < startup[1].timestamp, "shutdown came first")
    for _, r in ipairs(rows) do
        t:assert(is_own_type(r.type) and r.type:find("^eventd%.%w"),
            "its type is one of eventd's five: " .. r.type)
        t:assert(r.timestamp > before and r.timestamp < after,
            r.type .. " carries a realtime timestamp from when eventd made it")
    end
    -- The type is the only marker: the table has no record-kind column.
    local columns = {}
    for _, c in ipairs(eventd.sql(vm, eventd.shards(vm)[1], "SELECT name FROM pragma_table_info('events')")) do
        columns[#columns + 1] = c[1]
    end
    t:assert_eq(table.concat(columns, ","),
        "id,boot_id,timestamp,cpu_id,sequence,origin_class,event_type,effective_token_guid," ..
        "true_token_guid,process_guid,payload", "no separate record-type column exists")
end)

test("synthetic records carry no KMES header and no sequence, and sort among events by time", {
    spec = "eventd *synthetic.synthetic-events-carry-no-kmes-header"
        .. " eventd *synthetic.synthetic-events-are-ordered-by-timestamp-and-take-no-sequence-number",
}, function(t)
    for _, r in ipairs(synthetic_rows(vm, "event_type <> '" .. eventd.T.gap .. "'")) do
        for i = 1, 6 do
            t:assert(r.header[i] == nil,
                r.type .. " has no identity stamps, sequence, origin class or CPU (column " .. i .. ")")
        end
    end

    -- A config change made between two events is returned between them.
    local a = "pt.synth." .. eventd.marker("a")
    local b = "pt.synth." .. eventd.marker("b")
    eventd.emit(vm, a, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. a .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.set(vm, "LogRetentionDays", "dword:11"):assert_ok()
    config_change(vm, "LogRetentionDays", 11)
    eventd.emit(vm, b, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. b .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.unset(vm, "LogRetentionDays")

    local order = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS SINCE 2m ago TAKE 5000")) do
        local ty = r["event.type"]
        if ty == a or ty == b
            or (ty == eventd.T.config_change and r["config.value"] == 11) then
            order[#order + 1] = ty == eventd.T.config_change and "change" or
                (ty == a and "a" or "b")
        end
    end
    t:assert_eq(table.concat(order, ","), "b,change,a",
        "newest first, the synthetic record sits between the events by its timestamp")
end)

test("malformed input on the ingestion sockets produces no synthetic record", {
    spec = "eventd *synthetic.malformed-ingestion-input-emits-no-synthetic-event",
}, function(t)
    local before = #synthetic_rows(vm)
    -- Not MessagePack; a map missing every required field; a record with
    -- the wrong types. Then a good record on the same socket, which is
    -- processed after them.
    for _, bad in ipairs({ { raw = "\xc1\xc1\xc1" }, eventd.map({}), { origin = 7, message = false } }) do
        eventd.send_log(vm, bad)
        eventd.send_metric(vm, bad)
    end
    local origin = eventd.marker("synthlog")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "after the bad ones" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local name = eventd.marker("synthm")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    vm:run("sleep 1")
    t:assert_eq(#synthetic_rows(vm), before, "no synthetic record of any kind was written")
end)

test("a storage error about one shard is written to another, writable one", {
    spec = "eventd *synthetic.a-storage-error-skips-the-failing-shard-unless-it-is-writable-again",
}, function(t)
    eventd.stop(vm)
    -- Shard 1 is not a database any more: eventd quarantines it at start.
    vm:write_file(eventd.STORE.events .. "/shard-0001.db", "this is not a SQLite database")
    vm:run("rm -f " .. eventd.STORE.events .. "/shard-0001.db-wal " ..
        eventd.STORE.events .. "/shard-0001.db-shm"):assert_ok()
    eventd.start(vm)

    local rows = eventd.wait_rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 10m ago",
        function(rs) return #rs >= 1 end)
    t:assert_eq(rows[1]["store.shard"], 1, "the quarantine record names shard 1")
    local found = of_type(synthetic_rows(vm), eventd.T.storage_error)
    t:assert(#found >= 1, "it is stored")
    for _, r in ipairs(found) do
        t:assert(r.shard ~= "shard-0001", "and not in the shard it is about: " .. r.shard)
    end
end)

test("access to eventd's own records follows the Events\\eventd and Events\\eventd.daemon descriptors", {
    spec = "eventd *synthetic.access-to-synthetic-events-is-governed-by-the-eventd-events-keys",
}, function(t)
    local function visible(event_type)
        return #eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 1h ago TAKE 10")
    end
    local marker = "pt.synth." .. eventd.marker("acl")
    eventd.emit(vm, marker, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. marker .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert(visible(eventd.T.startup) >= 1, "SYSTEM reads startup records under the wildcard default")
    t:assert(visible(eventd.T.config_change) >= 1, "and config_change records")

    --- With a deny-SYSTEM descriptor at `key`, run `body`, then remove it.
    local function denied_at(key, body)
        local ok, err = pcall(function()
            vm:run("reg new '" .. key .. "'"):assert_ok()
            -- A descriptor whose one ACE grants SYSTEM nothing.
            vm:run("reg set '" .. key .. "' @ hex:" .. peinit.system_descriptor_hex(0)):assert_ok()
            body()
        end)
        vm:run("reg del '" .. key .. "' @")
        vm:run("reg del '" .. key .. "'")
        if not ok then error(err, 0) end
        wait_until(function() return visible(eventd.T.startup) >= 1 end,
            { timeout = 30, interval = 0.5, desc = "eventd's records to be readable again" })
    end

    -- Events\eventd covers all five.
    denied_at(eventd.SECURITY .. [[\Events\eventd]], function()
        wait_until(function() return visible(eventd.T.startup) == 0 end,
            { timeout = 30, interval = 0.5, desc = "eventd's records to be withheld" })
        t:assert_eq(visible(eventd.T.config_change), 0, "every eventd.* type is withheld")
        t:assert_eq(visible(marker), 1, "while ordinary events stay readable")
    end)
    -- Events\eventd.daemon covers only the start and stop records.
    denied_at(eventd.SECURITY .. [[\Events\eventd.daemon]], function()
        wait_until(function() return visible(eventd.T.startup) == 0 end,
            { timeout = 30, interval = 0.5, desc = "start records to be withheld" })
        t:assert_eq(visible(eventd.T.shutdown), 0, "stop records are withheld too")
        t:assert(visible(eventd.T.config_change) >= 1, "but config_change records stay readable")
    end)
end)

-- PEI-1294: the guard is exactly the five names. A KMES event claiming one
-- of them is counted and dropped (and receipted, so no gap appears); a
-- KMES event of any other eventd.* type, or of the retired synthetic.*
-- family, is stored like any other.
test("exactly the five eventd types are reserved, not the eventd root or the old synthetic prefix", {
    spec = "eventd *synthetic.exactly-the-five-types-are-reserved-not-the-eventd-root",
}, function(t)
    local m = eventd.marker("reserved")
    local other = "eventd.test." .. m
    local retired = "synthetic." .. m
    local forged = eventd.T.startup
    local before = #eventd.rows(vm, "EVENTS " .. forged .. " SINCE 1h ago TAKE 1000")
    for _, ty in ipairs({ forged, other, retired }) do
        t:assert_eq(eventd.emit(vm, ty, { marker = m }).ret, 0, "emitted " .. ty)
    end
    eventd.wait_rows(vm, "EVENTS " .. other .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { desc = "another eventd.* type to be stored" })
    eventd.wait_rows(vm, "EVENTS " .. retired .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { desc = "a synthetic.* type to be stored" })
    local forged_rows = eventd.rows(vm, "EVENTS " .. forged .. " SINCE 1h ago TAKE 1000")
    t:assert_eq(#forged_rows, before, "the forged " .. forged .. " was not stored")
    for _, r in ipairs(forged_rows) do
        t:assert(r.marker == nil, "no stored startup record carries the forged payload")
    end
end)

-- All five are essential: eventd consults no emission policy for them,
-- so switching their types off under Machine\Generic\Events changes
-- nothing (PGSS §6.9).
test("eventd's own records are written even with their types switched off", {
    spec = "eventd *synthetic.all-five-types-are-essential-and-no-emission-policy-applies",
}, function(t)
    local key = [[Machine\Generic\Events\eventd]]
    local before = #eventd.rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 1h ago TAKE 1000")
    local ok, err = pcall(function()
        -- The parent may already exist (the platform seed); either is fine.
        vm:run([[reg new 'Machine\Generic\Events']])
        vm:run("reg new '" .. key .. "'"):assert_ok()
        eventd.set(vm, "Enabled", "dword:0", { key = key }):assert_ok()
        eventd.set(vm, "LogRetentionDays", "dword:13"):assert_ok()
        config_change(vm, "LogRetentionDays", 13)
        eventd.restart(vm)
        eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 1h ago TAKE 1000",
            function(rs) return #rs > before end)
    end)
    eventd.unset(vm, "LogRetentionDays")
    eventd.unset(vm, "Enabled", { key = key })
    vm:run("reg del '" .. key .. "'")
    if not ok then error(err, 0) end
    local starts = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 2m ago TAKE 10")
    t:assert(#starts >= 1, "the restart under the policy wrote its start record")
end)

-- ---------------------------------------------------------------------------
-- Full stores (last: they leave stores unwritable)
-- ---------------------------------------------------------------------------

-- A store is made full with a small tmpfs carrying the descriptor eventd's
-- stores need (as storagefail-stores.test.lua does), mounted while eventd
-- is stopped: over the metric store directory, or — so that one event
-- shard fills while the others do not — under one shard's write-ahead log
-- through a bind mount of a single file. The tmpfs is then filled with
-- dd, so the store's next write fails with SQLITE_FULL while the rest of
-- the machine, the log store that carries eventd's stderr included, has
-- room.

local STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

local function small_tmpfs(dir, size)
    vm:run("mkdir -p " .. dir):assert_ok()
    vm:run("mount -t tmpfs -o size=" .. size .. ",policy=synth-ephemeral --synth-sddl '" ..
        STORE_SDDL .. "' none " .. dir):assert_ok()
end

--- Fill a test tmpfs; refuses anything else (the root is RAM-backed).
local function fill(dir)
    assert(vm:read_file("/proc/mounts"):find(" " .. dir .. " tmpfs ", 1, true),
        "refusing to fill " .. dir .. ": not a test tmpfs")
    vm:run("dd if=/dev/zero of=" .. dir .. "/pt-fill bs=1k 2>/dev/null; true")
end

--- With eventd stopped, put shard `i`'s WAL on a full 64 KiB tmpfs.
local function full_wal(i)
    local dir = "/run/pt-wal" .. i
    local wal = string.format("%s/shard-%04d.db-wal", eventd.STORE.events, i)
    small_tmpfs(dir, "64k")
    vm:run(": > " .. dir .. "/wal && touch " .. wal):assert_ok()
    vm:run("mount --bind " .. dir .. "/wal " .. wal):assert_ok()
    fill(dir)
end

local function full_vm() return vm end

--- eventd's own stderr lines (peinit forwards them to the log store).
local function eventd_said(f, text)
    for _, l in ipairs(eventd.rows(f, "LOGS FROM eventd SINCE 10m ago TAKE 500")) do
        if l.message:find(text, 1, true) then return true end
    end
    return false
end

--- Emit `batches` x 64 events of ~1 KB from the agent (one vCPU, CPU 0).
local function fill_events(f, batches)
    local payload = eventd.msgpack({ b = eventd.bin(string.rep("y", 900)) })
    local entries = {}
    for i = 1, 64 do entries[i] = { type = "pt.synth.fill", payload = payload } end
    for _ = 1, batches do
        local r = kmes.emit_batch(f, entries)
        assert(r.ret == 0, "emit_batch: errno " .. tostring(r.errno))
    end
end

local function config_changes_in(f, shard)
    return eventd.sql(f, string.format("%s/shard-%04d.db", eventd.STORE.events, shard),
        "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.config_change .. "'")[1][1]
end

-- The full-store tests run in order: the metric store full (while shard 0
-- can still take a record about it), then shard 0, then both shards.

-- The metric store is first found corrupt (garbage in metrics.db at
-- start), which is a storage error, and then made full, which is not:
-- a write refused for space takes the capacity path, requesting
-- retention and emitting nothing.
test("a corrupt metric store is recorded as a storage error; a full one is not", {
    spec = "eventd *synthetic.a-store-found-corrupt-and-quarantined-emits-eventd-store-quarantined"
        .. " eventd *storagefail.a-write-refused-for-want-of-space-emits-no-storage-error",
}, function(t)
    local f = full_vm()
    local function metric_errors()
        local n = 0
        for _, r in ipairs(eventd.rows(f, "EVENTS " .. eventd.T.storage_error .. " SINCE 1h ago")) do
            if r["store.kind"] == "metric" then n = n + 1 end
        end
        return n
    end
    local before = metric_errors()
    eventd.stop(vm)
    vm:write_file(eventd.DB.metrics, string.rep("this is not a database. ", 400))
    vm:run("rm -f " .. eventd.DB.metrics .. "-wal " .. eventd.DB.metrics .. "-shm"):assert_ok()
    eventd.start(vm)
    local _, corrupt = eventd.wait_rows(vm, "EVENTS " .. eventd.T.storage_error .. ' WHERE store.kind == "metric" SINCE 1h ago',
        function(rs) return #rs >= before + 1 end)
    t:assert(corrupt, "the corrupt metric store is recorded as a storage_error naming the metric store")
    local after_corrupt = metric_errors()

    eventd.stop(vm)
    small_tmpfs(eventd.STORE.metrics, "2m")
    eventd.start(vm)
    fill(eventd.STORE.metrics)
    -- New series need new pages, and there are none.
    local stem = eventd.marker("fill")
    local last
    for d = 1, 20 do
        local records = {}
        for i = 1, 100 do
            last = string.format("%s.series%03d.%03d", stem, d, i)
            records[i] = { name = last, type = "gauge", value = i }
        end
        eventd.send_metric(f, records)
    end
    wait_until(function() return eventd_said(f, "metric store is full") end,
        { timeout = 30, interval = 0.5, desc = "the metric store to fill" })
    local tail = eventd.rows(f, "METRIC " .. last .. " SINCE 10m ago")
    t:assert_eq(#tail, 0, "the last series was not stored: the metric writer's commits failed")
    f:run("sleep 2")
    t:assert_eq(metric_errors(), after_corrupt, "and no storage_error records the writes refused for space")
end)

-- Every daemon-wide record, not only eventd.daemon.stopped, falls back from a
-- full shard 0 to the lowest-numbered writable active shard.
test("with shard 0 full, a configuration change is recorded in shard 1", {
    spec = "eventd *synthetic.daemon-wide-events-go-to-shard-0-else-the-lowest-numbered-writable-active-shard",
}, function(t)
    -- First, the ordinary case, on the file VM: every daemon-wide record
    -- is in shard 0 while shard 0 is writable.
    local seen = 0
    for _, r in ipairs(synthetic_rows(vm)) do
        if r.type ~= eventd.T.gap then
            seen = seen + 1
            t:assert_eq(r.shard, "shard-0000", r.type .. " is in shard 0 while shard 0 is writable")
        end
    end
    t:assert(seen >= 3, "startup, shutdown and config_change records were checked: " .. seen)

    local f = full_vm()
    eventd.stop(vm)
    full_wal(0)
    eventd.start(vm)
    t:assert_eq(config_changes_in(f, 1), 0, "shard 1 holds no config_change yet")
    -- CPU 0's first stripe after the start goes to shard 0, which is full.
    fill_events(f, 2)
    wait_until(function() return eventd_said(f, "event store is full") end,
        { timeout = 30, interval = 0.5, desc = "shard 0's writes to fail" })

    eventd.set(f, "LogRetentionDays", "dword:9"):assert_ok()
    f:run("sleep 3")
    t:assert(config_changes_in(f, 1) >= 1,
        "with shard 0 unwritable, the config_change went to shard 1, the lowest writable shard")
end)

test("with every shard full a daemon-wide record is skipped and the failure goes to stderr", {
    spec = "eventd *synthetic.with-no-writable-shard-a-daemon-wide-event-is-skipped-and-logged-to-stderr",
}, function(t)
    local f = full_vm()
    eventd.stop(vm)
    for i = 0, 1 do
        -- The previous test filled shard 0's; this one does not rely on it.
        if not vm:read_file("/proc/mounts"):find(" /run/pt-wal" .. i .. " ", 1, true) then full_wal(i) end
    end
    eventd.start(vm)
    f:run("sleep 2")
    local before = { config_changes_in(f, 0), config_changes_in(f, 1) }
    local complaints = 0
    for _, l in ipairs(eventd.rows(f, "LOGS FROM eventd SINCE 10m ago TAKE 500")) do
        if l.message:find("event store is full", 1, true) then complaints = complaints + 1 end
    end
    eventd.set(f, "LogRetentionDays", "dword:8"):assert_ok()
    local now
    wait_until(function()
        now = 0
        for _, l in ipairs(eventd.rows(f, "LOGS FROM eventd SINCE 10m ago TAKE 500")) do
            if l.message:find("event store is full", 1, true) then now = now + 1 end
        end
        return now > complaints
    end, { timeout = 30, interval = 0.5, desc = "eventd to report the failed write" })
    t:assert_eq(config_changes_in(f, 0), before[1], "the config_change is not in shard 0")
    t:assert_eq(config_changes_in(f, 1), before[2], "nor in shard 1: it was skipped")
    t:assert(now > complaints, "and the failure was written to standard error")
end)

