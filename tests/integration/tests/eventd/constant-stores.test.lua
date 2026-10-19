-- eventd TRM Appendix B — Constants: the five event types eventd writes
-- about itself (eventd.daemon.started and the rest), the metric
-- types, log severity, series hashing and the schema versions.
--
-- One file-scope VM, whose boot seed adds a one-shot service, `pt-sev`,
-- that writes one line to standard output and one to standard error, so
-- that peinit's forwarding of service output is the source of the
-- severity under test. The other values are read where they are stored —
-- the stores, copied to the host by `eventd.sql` — and where they are
-- served, through evctl.
--
-- Several tests stop eventd and change a store while it is down: the
-- database is copied to the host, edited with sqlite there, checkpointed
-- out of its WAL, and written back, and eventd is started on it. A store
-- change that must make startup fail, and the full disk under the log
-- store, each get a VM of their own, booted inside the test.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local sys = require("helpers.sys")
peinit.claim(2) -- the file-scope VM, plus one of a test's own at a time

local SEV_OUT, SEV_ERR = "pt-sev-stdout-line", "pt-sev-stderr-line"

local SERVICE = peinit.seed("pt-eventd-sev", {
    { path = [[Machine\System]] },
    { path = [[Machine\System\Services]] },
    { path = [[Machine\System\Services\pt-sev]], values = {
        { name = "ImagePath", type = "sz", data = "/bin/sh" },
        { name = "Arguments", type = "multi", data = {
            "-c", "echo " .. SEV_OUT .. "; echo " .. SEV_ERR .. " >&2" } },
        { name = "Type", type = "dword", data = 1 },
        { name = "Identity", type = "sz", data = "SYSTEM" },
        { name = "Readiness", type = "dword", data = 1 },
    } },
})

local vm = eventd.boot({ name = "ev-const-stores", files = SERVICE })

local function q(s) return '"' .. s .. '"' end

-- ---------------------------------------------------------------------------
-- Working on a store while eventd is down
-- ---------------------------------------------------------------------------

local function schema_version(db)
    local rows = eventd.sql(vm, db, "SELECT value FROM metadata WHERE key = 'schema_version'")
    return rows[1] and rows[1][1]
end

-- ---------------------------------------------------------------------------
-- Synthetic event types
-- ---------------------------------------------------------------------------

local function count(text) return #eventd.rows(vm, text) end

test("eventd.daemon.started is written when eventd starts and attaches to KMES", {
    spec = "eventd *constant.eventd-daemon-started-is-emitted-when-eventd-starts-and-attaches-to-kmes",
}, function(t)
    local before = count("EVENTS " .. eventd.T.startup .. " SINCE 1h ago")
    eventd.restart(vm)
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago")
    t:assert_eq(#rows, before + 1, "one more startup record after a restart")
    local fds = eventd.fd_listing(vm, eventd.pid(vm))
    t:assert(fds:find("anon_inode:kmes-cpu", 1, true), "the new eventd holds a KMES buffer")
    t:assert_eq(#(rows[1]["store.resume.cpus"] or {}), 1,
        "and the record names the one CPU it attached: " .. json.encode(rows[1]))
end)

test("eventd.daemon.stopped is written on a graceful shutdown", {
    spec = "eventd *constant.eventd-daemon-stopped-is-emitted-on-graceful-shutdown",
}, function(t)
    local before = count("EVENTS " .. eventd.T.shutdown .. " SINCE 1h ago")
    local stopping_at = eventd.guest_ns(vm)
    eventd.restart(vm)
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.shutdown .. " SINCE 1h ago")
    t:assert_eq(#rows, before + 1, "svctl's graceful stop left one shutdown record")
    local startup = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1")[1]
    t:assert(rows[1]["event.time"] >= stopping_at and rows[1]["event.time"] < startup["event.time"],
        "written after the stop was asked for and before the next start")
end)

test("eventd.events.lost is written when a CPU's sequence has a gap, naming the CPU and the lost sequences", {
    spec = "eventd *constant.eventd-events-lost-is-emitted-when-a-cpu-sequence-gap-is-detected"
        .. " eventd *term.a-gap-record-names-the-cpu-and-the-missing-sequence-numbers",
}, function(t)
    -- With eventd stopped, overrun the 4 MiB ring on CPU 0 (the only one):
    -- 120 events of 60 KB. The oldest are overwritten before anyone reads
    -- them, and the restarted eventd finds the hole.
    local before = count("EVENTS " .. eventd.T.gap .. " SINCE 1h ago")
    eventd.stop(vm)
    local flood = "pt.flood" .. eventd.marker()
    local big = string.rep("x", 60000)
    for i = 1, 120 do eventd.emit(vm, flood, { i = i, pad = big }) end
    eventd.start(vm)
    local gaps = eventd.wait_rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 1h ago",
        function(rs) return #rs > before end)
    t:assert_eq(#gaps, before + 1, "one gap record")
    local g = gaps[1]
    t:assert_eq(g["buffer.cpu"], 0, "naming CPU 0: " .. json.encode(g))
    t:assert_eq(g["event.cpu"], 0, "the CPU it is stored under too")
    local first, last = g["loss.sequence"], g["loss.sequence-last"]
    t:assert(math.type(first) == "integer" and math.type(last) == "integer" and last >= first,
        "and the range of sequence numbers lost")
    local survivors = eventd.rows(vm, "EVENTS " .. flood .. " SINCE 1h ago")
    local lowest = math.huge
    for _, r in ipairs(survivors) do lowest = math.min(lowest, r["event.sequence"]) end
    t:assert_eq(last, lowest - 1, "ending just before the oldest surviving event")
    t:assert(#survivors < 120, "which is fewer than were emitted: " .. #survivors)
end)

test("eventd.config.changed is written when a value is applied at runtime", {
    spec = "eventd *constant.eventd-config-changed-is-emitted-when-a-value-is-applied-at-runtime",
}, function(t)
    eventd.set(vm, "LogRetentionDays", "dword:21"):assert_ok()
    local rows, ok = eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change ..
        ' WHERE config.name == "LogRetentionDays" AND config.value == 21 SINCE 1h ago',
        function(rs) return #rs == 1 end)
    eventd.unset(vm, "LogRetentionDays")
    t:assert(ok, "applying LogRetentionDays=21 is recorded")
    t:assert_eq(rows[1] and rows[1]["event.type"], eventd.T.config_change, "as eventd.config.changed")
end)

test("eventd.store.quarantined is written when a store is found corrupt, not when it is full", {
    spec = "eventd *constant.eventd-store-quarantined-is-emitted-when-a-store-is-found-corrupt-and-quarantined",
}, function(t)
    -- First the log store is found corrupt at start: garbage in logs.db.
    -- Then it is put on a 256 KiB tmpfs of its own, so that its writes fail
    -- for space while the event store, which would hold a record, has room.
    local fvm = eventd.boot({ name = "ev-full" })
    local function log_errors()
        local n = 0
        for _, r in ipairs(eventd.rows(fvm, "EVENTS " .. eventd.T.storage_error .. " SINCE 1h ago")) do
            if r["store.kind"] == "log" then n = n + 1 end
        end
        return n
    end
    eventd.stop(fvm)
    fvm:write_file(eventd.DB.logs, string.rep("this is not a database. ", 400))
    fvm:run("rm -f " .. eventd.DB.logs .. "-wal " .. eventd.DB.logs .. "-shm"):assert_ok()
    eventd.start(fvm)
    local _, corrupt = eventd.wait_rows(fvm, "EVENTS " .. eventd.T.storage_error
        .. ' WHERE store.kind == "log" SINCE 1h ago', function(rs) return #rs >= 1 end, { timeout = 20 })
    t:assert(corrupt, "the corrupt log store is recorded as a quarantine naming the log store")
    local after_corrupt = log_errors()

    eventd.stop(fvm)
    local r = sys.mount(fvm, { source = "pt-full", target = eventd.STORE.logs, fstype = "tmpfs",
        data = "size=256k" })
    t:assert_eq(r.ret, 0, "mounted a small tmpfs on the log store (errno " .. tostring(r.errno) .. ")")
    fvm:run("sd set '" .. eventd.STORE.logs .. "' 'O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)" ..
        "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)'"):assert_ok()
    eventd.start(fvm)
    local origin = eventd.marker("full")
    for i = 1, 600 do
        eventd.send_log(fvm, { origin = origin, is_error = false, message = string.rep("f", 1000) .. i })
    end
    local full = fvm:run("df -k " .. eventd.STORE.logs).stdout
    vm:run("sleep 2")
    local kept = #eventd.rows(fvm, "LOGS FROM " .. origin .. " SINCE 10m ago")
    t:assert(kept < 600, "the full store refused some of the writes: " .. kept .. " of 600 kept; " .. full)
    fvm:run("sleep 2")
    t:assert_eq(log_errors(), after_corrupt, "no quarantine record for the writes refused for space; the store: " .. full)
end)

-- ---------------------------------------------------------------------------
-- Metric types
-- ---------------------------------------------------------------------------

--- Wait until metric `name` has `n` samples in the store. Read from
--- metrics.db rather than queried: a raw histogram, or a name spanning two
--- series, is not something METRIC answers without a transform or window.
local function stored(name, n)
    local ok = pcall(wait_until, function()
        return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM samples s JOIN series r " ..
            "ON r.id = s.series_id WHERE r.name = '" .. name .. "'")[1][1] == n
    end, { timeout = 30, interval = 0.5, desc = n .. " samples of " .. name })
    assert(ok, n .. " samples of " .. name .. " stored")
end

local HIST = { boundaries = { 1, 5, 10 }, counts = { 1, 2, 3 }, total_count = 4, sum = eventd.float(12.5) }

local function metric_types()
    local names = {
        counter = eventd.marker("mc"), gauge = eventd.marker("mg"), histogram = eventd.marker("mh"),
    }
    eventd.send_metric(vm, { name = names.counter, type = "counter", value = 3 })
    eventd.send_metric(vm, { name = names.gauge, type = "gauge", value = -2 })
    eventd.send_metric(vm, { name = names.histogram, type = "histogram", value = HIST })
    for _, n in pairs(names) do stored(n, 1) end
    return names
end

local function series_type(name)
    return eventd.sql(vm, eventd.DB.metrics, "SELECT type FROM series WHERE name = '" .. name .. "'")[1][1]
end

test("metric type 0 is counter", { spec = "eventd *constant.metric-type-0-is-counter" }, function(t)
    t:assert_eq(series_type(metric_types().counter), 0, "a counter's series has type 0")
end)

test("metric type 1 is gauge", { spec = "eventd *constant.metric-type-1-is-gauge" }, function(t)
    t:assert_eq(series_type(metric_types().gauge), 1, "a gauge's series has type 1")
end)

test("metric type 2 is histogram", { spec = "eventd *constant.metric-type-2-is-histogram" }, function(t)
    t:assert_eq(series_type(metric_types().histogram), 2, "a histogram's series has type 2")
end)

test("the metric type is stored in series.type, and only there", {
    spec = "eventd *constant.the-metric-type-is-stored-in-series-type",
}, function(t)
    local schema = eventd.schema(vm, eventd.DB.metrics)
    t:assert(schema.series:find("type INTEGER NOT NULL CHECK %(type IN %(0, 1, 2%)%)"),
        "series has an integer type column holding 0, 1 or 2: " .. schema.series)
    t:assert(not schema.samples:find("type", 1, true), "samples has none: " .. schema.samples)
end)

test("the query language shows and takes the type names, not the numbers", {
    spec = "eventd *constant.the-query-language-exposes-metric-type-names-not-numbers",
}, function(t)
    local names = metric_types()
    -- A histogram is not served raw; counter and gauge show the field.
    for _, name in ipairs({ "counter", "gauge" }) do
        local metric = names[name]
        local rows = eventd.rows(vm, "METRIC " .. metric .. " SINCE 10m ago")
        t:assert_eq(rows[1].type, name, metric .. " is shown as " .. name)
        t:assert_eq(count("METRIC " .. metric .. " WHERE type == " .. q(name) .. " SINCE 10m ago"), 1,
            "and matches type == \"" .. name .. "\"")
    end
    t:assert_eq(count("METRIC " .. names.gauge .. " WHERE type == 1 SINCE 10m ago"), 0,
        "while type == 1 matches nothing: the number is not the type's name")
end)

-- ---------------------------------------------------------------------------
-- Log severity
-- ---------------------------------------------------------------------------

local function service_lines()
    vm:run("svctl start pt-sev")
    local rows = eventd.wait_rows(vm, "LOGS FROM pt-sev SINCE 1h ago",
        function(rs) return #rs >= 2 end, { timeout = 20 })
    local by = {}
    for _, r in ipairs(rows) do by[r.message] = r end
    return by
end

local function stored_is_error(message)
    return eventd.sql(vm, eventd.DB.logs, "SELECT is_error, typeof(is_error) FROM logs WHERE message = '" ..
        message .. "' ORDER BY id DESC LIMIT 1")[1]
end

test("severity 0 is normal: what a service writes to standard output", {
    spec = "eventd *constant.log-severity-0-is-normal-standard-output",
}, function(t)
    local by = service_lines()
    t:assert(by[SEV_OUT], "the service's stdout line was forwarded: " .. json.encode(by))
    t:assert_eq(stored_is_error(SEV_OUT)[1], 0, "and is stored with is_error 0")
end)

test("severity 1 is error: standard error, or a record marked so", {
    spec = "eventd *constant.log-severity-1-is-error-standard-error-or-explicitly-marked",
}, function(t)
    local by = service_lines()
    t:assert(by[SEV_ERR], "the service's stderr line was forwarded")
    t:assert_eq(stored_is_error(SEV_ERR)[1], 1, "and is stored with is_error 1")
    local marked = "pt-marked-" .. eventd.marker()
    eventd.send_log(vm, { origin = eventd.marker("sev"), is_error = true, message = marked })
    eventd.wait_rows(vm, "LOGS CONTAINING " .. q(marked) .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(stored_is_error(marked)[1], 1, "a record sent with is_error true is stored with 1")
end)

-- §4.2 and a2: a query may compare is_error against true/false or 1/0.
test("is_error is stored as an integer and served as a boolean, and queries take either", {
    spec = "eventd *constant.is-error-is-stored-as-an-integer-and-exposed-as-a-boolean",
}, function(t)
    local origin = eventd.marker("ie")
    eventd.send_log(vm, { origin = origin, is_error = true, message = "e" })
    eventd.send_log(vm, { origin = origin, is_error = false, message = "n" })
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local stored = stored_is_error("e")
    t:assert_eq(stored[2], "integer", "the column holds an integer")
    for _, r in ipairs(rows) do
        t:assert(type(r.is_error) == "boolean", "served as a boolean: " .. json.encode(r))
    end
    local base = "LOGS FROM " .. origin .. " WHERE is_error == "
    t:assert_eq(count(base .. "true SINCE 10m ago"), 1, "is_error == true matches the error")
    t:assert_eq(count(base .. "false SINCE 10m ago"), 1, "is_error == false the other")
    t:assert_eq(count(base .. "1 SINCE 10m ago"), 1, "is_error == 1 matches the error too")
    t:assert_eq(count(base .. "0 SINCE 10m ago"), 1, "and is_error == 0 the other")
end)

-- ---------------------------------------------------------------------------
-- Series hashing
-- ---------------------------------------------------------------------------

--- A label value whose canonical string's FNV-1a has its top bit set.
local function high_bit_value()
    for i = 1, 1000 do
        local v = "v" .. i
        if eventd.fnv1a64("k=" .. v) < 0 then return v end
    end
end

local function series_row(name)
    return eventd.sql(vm, eventd.DB.metrics, "SELECT labels, label_hash, boundaries_hash, hex(boundaries) " ..
        "FROM series WHERE name = '" .. name .. "'")[1]
end

test("series hashes are FNV-1a 64 over the exact canonical label bytes or boundary blob", {
    spec = "eventd *constant.series-hashes-are-64-bit-fnv-1a-over-the-exact-label-string-or-boundary-bytes",
}, function(t)
    local name = eventd.marker("h")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1,
        labels = eventd.map{ zone = "b", app = "a" } })
    local hist = eventd.marker("hh")
    eventd.send_metric(vm, { name = hist, type = "histogram", value = HIST })
    stored(hist, 1)
    stored(name, 1)
    local row = series_row(name)
    t:assert_eq(row[1], "app=a,zone=b", "the canonical label string")
    t:assert_eq(row[2], eventd.fnv1a64(row[1]) & 0x7fffffffffffffff, "label_hash is FNV-1a 64 of exactly those bytes")
    local h = series_row(hist)
    local blob = eventd.unhex(h[4])
    t:assert_eq(blob, string.pack("<I4ddd", 3, 1, 5, 10), "the boundary blob: a count, then each boundary")
    t:assert_eq(h[3], eventd.fnv1a64(blob) & 0x7fffffffffffffff, "boundaries_hash is FNV-1a 64 of the blob")
end)

test("the FNV offset basis is 0xcbf29ce484222325", {
    spec = "eventd *constant.the-fnv-offset-basis-is-0xcbf29ce484222325",
}, function(t)
    -- The hash of the empty label string is the offset basis itself.
    local name = eventd.marker("e")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local row = series_row(name)
    t:assert_eq(row[1], "", "a series with no labels has the empty label string")
    t:assert_eq(row[2], 0xcbf29ce484222325 & 0x7fffffffffffffff,
        "and its hash, over no bytes, is the offset basis with the top bit cleared")
end)

test("the FNV prime is 0x100000001b3", {
    spec = "eventd *constant.the-fnv-prime-is-0x100000001b3",
}, function(t)
    -- One byte: (basis ^ byte) * prime. Labels are key=value, so the
    -- shortest label string is three bytes; the prime is seen in each step.
    local name = eventd.marker("p")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = eventd.map{ a = "b" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local h = -3750763034362895579
    for _, c in ipairs({ ("a"):byte(), ("="):byte(), ("b"):byte() }) do h = (h ~ c) * 0x100000001b3 end
    t:assert_eq(series_row(name)[2], h & 0x7fffffffffffffff, "\"a=b\" hashes with the prime 0x100000001b3")
end)

test("the stored hash has its high bit cleared", {
    spec = "eventd *constant.the-stored-series-hash-has-its-high-bit-cleared",
}, function(t)
    local v = high_bit_value()
    t:assert(v, "found a label value whose hash has the high bit set")
    local name = eventd.marker("hb")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = eventd.map{ k = v } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local stored = series_row(name)[2]
    t:assert(eventd.fnv1a64("k=" .. v) < 0, "the full hash of k=" .. v .. " has bit 63 set")
    t:assert(stored >= 0, "the stored one is non-negative: " .. tostring(stored))
    t:assert_eq(stored, eventd.fnv1a64("k=" .. v) & 0x7fffffffffffffff, "it is the hash with only bit 63 cleared")
end)

test("a series is identified by its full label string, never by its hash", {
    spec = "eventd *constant.series-identity-is-always-confirmed-against-the-full-string-or-blob",
}, function(t)
    -- Make a collision: series A's stored hash becomes that of B's labels.
    -- A sample for B then finds A by hash and must still not join it.
    local name = eventd.marker("id")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = eventd.map{ s = "a" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.metrics, "UPDATE series SET label_hash = " ..
        (eventd.fnv1a64("s=b") & 0x7fffffffffffffff) .. " WHERE name = '" .. name .. "';")
    eventd.start(vm)
    eventd.send_metric(vm, { name = name, type = "gauge", value = 2, labels = eventd.map{ s = "b" } })
    stored(name, 2)
    local series = eventd.sql(vm, eventd.DB.metrics, "SELECT labels, (SELECT count(*) FROM samples " ..
        "WHERE series_id = series.id) FROM series WHERE name = '" .. name .. "' ORDER BY labels")
    t:assert_eq(#series, 2, "two series, though they share a hash: " .. json.encode(series))
    t:assert_eq(json.encode(series), json.encode({ { "s=a", 1 }, { "s=b", 1 } }),
        "one sample each: s=a was not given s=b's sample")
end)

-- ---------------------------------------------------------------------------
-- Schema versions
-- ---------------------------------------------------------------------------

test("the event shard schema version is 1", { spec = "eventd *constant.the-event-shard-schema-version-is-1" },
    function(t)
        for _, s in ipairs(eventd.shards(vm)) do t:assert_eq(schema_version(s), "1", s) end
    end)

test("the log store schema version is 1", { spec = "eventd *constant.the-log-store-schema-version-is-1" },
    function(t) t:assert_eq(schema_version(eventd.DB.logs), "1", "logs.db") end)

test("the metric store schema version is 3", { spec = "eventd *constant.the-metric-store-schema-version-is-3" },
    function(t) t:assert_eq(schema_version(eventd.DB.metrics), "3", "metrics.db") end)

test("the metadata database schema version is 1", {
    spec = "eventd *constant.the-metadata-database-schema-version-is-1",
}, function(t)
    local rows = eventd.sql(vm, eventd.DB.meta, "SELECT CAST(value AS TEXT) FROM meta WHERE key = 'schema_version'")
    t:assert_eq(rows[1] and rows[1][1], "1", "eventd-meta.db")
end)

test("a version eventd does not know is never migrated: a historical shard is left as it is, and excluded", {
    spec = "eventd *constant.an-unrecognised-schema-version-is-never-migrated",
}, function(t)
    -- A historical shard of an unknown version (shard-0007, beyond the one
    -- active shard), holding the only copy of an event.
    local etype = "pt.hist" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local shard = eventd.shards(vm)[1]
    local hist = eventd.STORE.events .. "/shard-0007.db"
    eventd.stop(vm)
    eventd.edit_store(vm, shard, "UPDATE metadata SET value = '99' WHERE key = 'schema_version';", { to = hist })
    eventd.edit_store(vm, shard, "DELETE FROM events WHERE event_type = '" .. etype .. "';")
    eventd.start(vm)
    vm:run("sleep 1")
    t:assert_eq(schema_version(hist), "99", "the historical shard is still at version 99")
    t:assert_eq(count("EVENTS " .. etype .. " SINCE 10m ago"), 0, "and its event is not served")
    vm:run("rm -f " .. hist .. "*")
end)

test("an unknown version fails a required store, excludes a historical shard and recreates the metadata", {
    spec = "eventd *constant.an-unrecognised-version-fails-a-required-store-excludes-a-historical-shard-and-recreates-metadata",
}, function(t)
    -- Historical shard: as above, excluded rather than served or failing.
    local etype = "pt.hist" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local shard = eventd.shards(vm)[1]
    local hist = eventd.STORE.events .. "/shard-0006.db"
    eventd.stop(vm)
    eventd.edit_store(vm, shard, "UPDATE metadata SET value = '99' WHERE key = 'schema_version';", { to = hist })
    eventd.edit_store(vm, shard, "DELETE FROM events WHERE event_type = '" .. etype .. "';")
    -- Metadata: an unknown version, and a desired index to show what goes.
    eventd.edit_store(vm, eventd.DB.meta, "UPDATE meta SET value = CAST('99' AS BLOB) WHERE key = 'schema_version';" ..
        "INSERT OR REPLACE INTO desired_indexes VALUES ('ptmarkerfield', 0, 1);")
    eventd.start(vm)
    t:assert_eq(count("EVENTS " .. etype .. " SINCE 10m ago"), 0, "the historical shard's event is excluded")
    local said = eventd.wait_rows(vm, "LOGS FROM eventd CONTAINING " .. q("excluding historical shard") ..
        " SINCE 10m ago", function(rs) return #rs >= 1 end, { timeout = 10 })
    t:assert(#said >= 1, "and eventd says so")
    local meta = eventd.sql(vm, eventd.DB.meta, "SELECT CAST(value AS TEXT) FROM meta WHERE key = 'schema_version'")
    t:assert_eq(meta[1] and meta[1][1], "1", "eventd-meta.db is back at version 1")
    t:assert_eq(#eventd.sql(vm, eventd.DB.meta, "SELECT * FROM desired_indexes WHERE field_path = 'ptmarkerfield'"),
        0, "recreated from defaults: the old desired index is gone")
    vm:run("rm -f " .. hist .. "*")

    -- Required store: its own VM, since eventd will not start.
    local fvm = eventd.boot({ name = "ev-badver" })
    eventd.stop(fvm)
    eventd.edit_store(fvm, eventd.DB.logs, "UPDATE metadata SET value = 99 WHERE key = 'schema_version';")
    fvm:run("svctl start eventd")
    local cause
    local deadline = os.time() + 25
    while os.time() < deadline do
        local ok, r = pcall(function() return fvm:run("svctl --json status eventd") end)
        if not ok then break end
        local okj, s = pcall(json.decode, r.stdout)
        if okj and s then cause = s.cause end
        if cause == "process_crash" then break end
        pcall(function() fvm:run("sleep 1") end)
    end
    t:assert_eq(cause, "process_crash", "with logs.db at version 99, eventd fails startup")
end)
