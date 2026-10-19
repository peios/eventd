-- eventd TRM §9.2 — storage failure: a full disk under each kind of
-- store, and corruption found at startup, at write time and at query
-- time.
--
-- One VM, with eventd made Normal and RestartPolicy=Never by the seed so
-- that a start that is meant to fail (a bad schema version) fails once
-- and stays failed rather than spending a Critical restart budget.
--
-- The databases are prepared on the host: eventd is stopped (which
-- checkpoints and closes every file), a database is copied out, changed
-- with Python's sqlite3 — or, for a corrupt page, by overwriting one
-- B-tree page in place — and written back. A full disk is a small tmpfs
-- mounted over a store directory with the descriptor eventd requires,
-- then filled from the agent.
--
-- Tests run in order and share state through the tables declared beside
-- them: the corrupt-page tests are four looks at one prepared logs.db,
-- the disk-full tests three at one filled event store.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

local vm = eventd.boot({
    name = "ev-storagefail",
    noncritical = true,
})

--- Run a Python body on the host against a copy of guest database `db`
--- (available as `path`), then write the copy back. Returns stdout.
local function host_edit(db, body)
    local dir = eventd.host_tmpdir()
    local f = assert(io.open(dir .. "/db", "wb")); f:write(vm:read_file(db)); f:close()
    f = assert(io.open(dir .. "/run.py", "wb"))
    f:write("import sqlite3, sys\npath = sys.argv[1] + '/db'\n" .. body); f:close()
    local p = io.popen("python3 " .. dir .. "/run.py " .. dir .. " 2>&1")
    local out = p:read("a")
    assert(p:close(), "host edit failed: " .. out)
    f = assert(io.open(dir .. "/db", "rb")); local bytes = f:read("a"); f:close()
    os.execute("rm -rf '" .. dir .. "'")
    vm:write_file(db, bytes)
    return out
end

local function sql_exec(db, statements)
    return host_edit(db, "c = sqlite3.connect(path)\nc.executescript('''" .. statements
        .. "''')\nc.commit()\nc.execute('PRAGMA wal_checkpoint(TRUNCATE)')\nc.close()\n")
end

--- Overwrite the B-tree leaf page of `table` that holds `rowid` with
--- 0xff bytes: an invalid page type, which SQLite reports as corruption
--- when — and only when — something reads that page.
local CORRUPT_LEAF = [[
c = sqlite3.connect(path)
ps = c.execute('PRAGMA page_size').fetchone()[0]
root = c.execute("SELECT rootpage FROM sqlite_master WHERE name = ?", (TABLE,)).fetchone()[0]
c.close()
f = open(path, 'r+b')
def page(n):
    f.seek((n - 1) * ps)
    return f.read(ps)
def varint(b, i):
    v = 0
    for k in range(9):
        c = b[i + k]
        if k == 8:
            return (v << 8) | c
        v = (v << 7) | (c & 0x7f)
        if c < 0x80:
            return v
def leaf(n):
    p = page(n)
    off = 100 if n == 1 else 0
    if p[off] == 13:
        return n
    assert p[off] == 5, 'not a table b-tree page'
    cells = int.from_bytes(p[off + 3:off + 5], 'big')
    for i in range(cells):
        cp = int.from_bytes(p[off + 12 + 2 * i:off + 14 + 2 * i], 'big')
        if ROWID <= varint(p, cp + 4):
            return leaf(int.from_bytes(p[cp:cp + 4], 'big'))
    return leaf(int.from_bytes(p[off + 8:off + 12], 'big'))
n = leaf(root)
assert n != root, 'the table is one page'
f.seek((n - 1) * ps)
f.write(b'\xff' * ps)
f.close()
print(n)
]]

local function corrupt_leaf(db, tbl, rowid)
    return host_edit(db, "TABLE = '" .. tbl .. "'\nROWID = " .. rowid .. "\n" .. CORRUPT_LEAF)
end

--- "ok", or what SQLite says is wrong (it may raise rather than report).
--- The copy is left as it was: nothing is written back that differs.
local function integrity(db)
    local dir = eventd.host_tmpdir()
    local f = assert(io.open(dir .. "/db", "wb")); f:write(vm:read_file(db)); f:close()
    f = assert(io.open(dir .. "/run.py", "wb"))
    f:write("import sqlite3, sys\ntry:\n    c = sqlite3.connect('file:' + sys.argv[1] + '/db?mode=ro', uri=True)\n"
        .. "    print(c.execute('PRAGMA integrity_check').fetchone()[0])\nexcept Exception as e:\n    print('error: %s' % e)\n")
    f:close()
    local p = io.popen("python3 " .. dir .. "/run.py " .. dir .. " 2>&1")
    local out = p:read("a")
    p:close()
    os.execute("rm -rf '" .. dir .. "'")
    return (out:gsub("%s+$", ""))
end

local function storage_errors(since)
    local out = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 30m ago")) do
        if r["event.time"] >= since then out[#out + 1] = r end
    end
    return out
end

local function quarantined(dir)
    local out = {}
    for name in vm:run("ls " .. dir).stdout:gmatch("[^\n]+") do
        if name:find(".corrupt.", 1, true) then out[#out + 1] = name end
    end
    return out
end

-- ---------------------------------------------------------------------------
-- Schema versions and structure, at startup
-- ---------------------------------------------------------------------------

test("a required store with an unknown schema version fails startup and is neither quarantined nor changed", {
    spec = "eventd *storagefail.a-required-store-with-a-bad-schema-version-fails-startup"
        .. " eventd *storagefail.a-missing-or-unrecognised-schema-version-is-not-treated-as-corruption",
}, function(t)
    eventd.stop(vm)
    local original = vm:read_file(eventd.DB.logs)
    sql_exec(eventd.DB.logs, "UPDATE metadata SET value = '99' WHERE key = 'schema_version';")
    local changed = vm:read_file(eventd.DB.logs)
    vm:run("svctl start eventd")
    local status = eventd.settle(vm)
    local after = vm:read_file(eventd.DB.logs)
    local corrupt = quarantined(eventd.STORE.logs)
    vm:write_file(eventd.DB.logs, original)
    eventd.start(vm)
    t:assert_eq(status.state, "failed", "the start failed: " .. json.encode(status))
    t:assert_eq(#corrupt, 0, "nothing was quarantined: " .. table.concat(corrupt, " "))
    t:assert(after == changed, "the database was left exactly as it was: not repaired, not migrated")
end)

test("startup checks that the required tables and indexes exist", {
    spec = "eventd *storagefail.startup-corruption-detection-checks-that-required-tables-and-indexes-exist",
}, function(t)
    eventd.stop(vm)
    local original = vm:read_file(eventd.DB.logs)
    sql_exec(eventd.DB.logs, "DROP INDEX idx_logs_origin;")
    local since = eventd.guest_ns(vm)
    vm:run("svctl start eventd")
    local status = eventd.settle(vm)
    local corrupt = quarantined(eventd.STORE.logs)
    -- Detected either way: eventd refused it, or set it aside.
    t:assert(status.state == "failed" or #corrupt > 0,
        "a logs.db missing a required index was not accepted: " .. json.encode(status))
    if status.state == "failed" then
        vm:write_file(eventd.DB.logs, original)
        eventd.start(vm)
        t:assert(eventd.stderr_line(vm,"required schema object is missing", since), "and the missing object was reported")
    end
end)

test("a required store SQLite cannot read is quarantined, replaced, logged and reported", {
    spec = "eventd *storagefail.a-corrupt-required-store-at-startup-is-quarantined-and-replaced-with-an-empty-database"
        .. " eventd *storagefail.startup-corruption-is-logged-and-reported-by-a-storage-error-event",
}, function(t)
    eventd.stop(vm)
    local name = eventd.marker("gone")
    local garbage = string.rep("this is not an SQLite database\n", 512)
    vm:write_file(eventd.DB.metrics, garbage)
    local since = eventd.guest_ns(vm)
    eventd.start(vm)
    local corrupt = quarantined(eventd.STORE.metrics)
    t:assert(#corrupt >= 1, "the unreadable metrics.db was moved aside: " .. table.concat(corrupt, " "))
    t:assert_eq(vm:read_file(eventd.STORE.metrics .. "/" .. corrupt[1]), garbage,
        "and kept as it was")
    t:assert(eventd.schema(vm, eventd.DB.metrics).series, "a fresh metrics.db took its place")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 3 })
    local _, ok = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(r) return #r == 1 end)
    t:assert(ok, "and is in use")
    t:assert(eventd.stderr_line(vm,"quarantined corrupt metric store", since), "the corruption was logged")
    local errs = eventd.wait_rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 10m ago", function(r)
        for _, e in ipairs(r) do if e["event.time"] >= since and e["store.kind"] == "metric" then return true end end
        return false
    end)
    local found
    for _, e in ipairs(errs) do if e["event.time"] >= since and e["store.kind"] == "metric" then found = e end end
    t:assert(found, "and reported as eventd.store.quarantined naming the metric store")
end)

-- ---------------------------------------------------------------------------
-- A corrupt page: invisible at startup, found by a query and by a write
-- ---------------------------------------------------------------------------

local page = {}

test("startup does not run an integrity check: a database with a bad page opens", {
    spec = "eventd *storagefail.startup-does-not-run-integrity-check",
}, function(t)
    -- Old records, many pages of them, so one leaf in their middle can go
    -- bad while the schema, the metadata and the newest pages stay good.
    page.origin = eventd.marker("old")
    local old = eventd.guest_ns(vm) - 10 * 86400 * 1000000000
    for d = 1, 40 do
        local batch = {}
        for i = 1, 50 do
            batch[i] = { origin = page.origin, is_error = false, timestamp = old + d * 1000 + i,
                message = string.rep("o", 400) }
        end
        eventd.send_log(vm, batch)
    end
    eventd.wait_rows(vm, "LOGS FROM " .. page.origin .. " SINCE 30d ago TAKE 5000",
        function(r) return #r == 2000 end, { timeout = 60 })
    eventd.stop(vm)
    local mid = eventd.sql(vm, eventd.DB.logs, "SELECT id FROM logs WHERE origin = '" .. page.origin
        .. "' ORDER BY id LIMIT 1 OFFSET 1000")[1][1]
    page.leaf = corrupt_leaf(eventd.DB.logs, "logs", mid)
    t:assert(integrity(eventd.DB.logs) ~= "ok", "logs.db now fails an integrity check")
    local since = eventd.guest_ns(vm)
    eventd.start(vm)
    t:assert_eq(#quarantined(eventd.STORE.logs), 0, "yet eventd opened it without quarantining it")
    t:assert(not eventd.stderr_line(vm,"corrupt log store", since), "or noticing anything")
end)

test("a query that reaches the corrupt page fails with an error", {
    spec = "eventd *storagefail.query-time-corruption-fails-the-affected-query",
}, function(t)
    page.query = eventd.query(vm, "LOGS FROM " .. page.origin .. " SINCE 30d ago TAKE 5000")
    t:assert(not page.query.ok, "the query failed: exit " .. tostring(page.query.exit_code))
    t:assert((page.query.stderr or ""):lower():find("malformed", 1, true)
        or (page.query.stderr or ""):lower():find("corrupt", 1, true),
        "with SQLite's corruption error: " .. tostring(page.query.stderr))
end)

test("a query failed by corruption returns none of the rows it read", {
    spec = "eventd *storagefail.a-query-failed-by-corruption-returns-no-partial-rows",
}, function(t)
    t:assert(page.query, "the failed query from the previous test")
    local lines = 0
    for _ in (page.query.stdout or ""):gmatch("[^\n]+") do lines = lines + 1 end
    t:assert_eq(lines, 0, "the client was given no rows: " .. (page.query.stdout or ""):sub(1, 300))
end)

test("corruption met at write time is quarantined, reported, and writing resumes on a new database", {
    spec = "eventd *storagefail.write-time-corruption-quarantines-and-replaces-the-database-and-writes-resume"
        .. " eventd *storagefail.eventd-never-attempts-automatic-repair-of-a-quarantined-file",
}, function(t)
    -- Retention is a write, and the oldest rows are the corrupt page's:
    -- any applied configuration change asks for a retention pass.
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "LogRetentionDays", "dword:1"):assert_ok()
    local ok = pcall(wait_until, function() return #quarantined(eventd.STORE.logs) > 0 end,
        { timeout = 30, interval = 0.5, desc = "logs.db to be quarantined" })
    eventd.unset(vm, "LogRetentionDays")
    t:assert(ok, "the corrupt logs.db was quarantined when a write met it")
    local errs = eventd.wait_rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 10m ago", function(r)
        for _, e in ipairs(r) do if e["event.time"] >= since and e["store.kind"] == "log" then return true end end
        return false
    end)
    t:assert(#errs > 0, "an eventd.store.quarantined for the log store was emitted")
    local origin = eventd.marker("resume")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "after" })
    local _, resumed = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(r) return #r == 1 end)
    t:assert(resumed, "writes resumed on the replacement")
    -- The quarantined file is the only copy; eventd left it corrupt.
    local q = eventd.STORE.logs .. "/" .. quarantined(eventd.STORE.logs)[1]
    t:assert(integrity(q) ~= "ok", "the quarantined file was not repaired")
end)

-- ---------------------------------------------------------------------------
-- Disk full
-- ---------------------------------------------------------------------------

local full = {}

local function mount_small(dir)
    eventd.stop(vm)
    vm:run("mount -t tmpfs -o size=2m,policy=synth-ephemeral --synth-sddl '" .. STORE_SDDL
        .. "' none " .. dir):assert_ok()
    eventd.start(vm)
end

--- Fill the small tmpfs at `dir`. Refuses anything that is not one: the
--- root is RAM-backed, and filling it would take the whole VM down.
local function fill(dir)
    local mounts = vm:read_file("/proc/mounts")
    assert(mounts:find(" " .. dir .. " tmpfs ", 1, true), "refusing to fill " .. dir .. ": not a test tmpfs")
    vm:run("dd if=/dev/zero of=" .. dir .. "/pt-fill bs=4k 2>/dev/null; true")
end

local function unfill(dir)
    vm:run("rm -f " .. dir .. "/pt-fill"):assert_ok()
end

test("a failed event commit on a full disk does not stop the writer", {
    spec = "eventd *storagefail.a-failed-event-insert-or-commit-does-not-crash-the-writer-thread",
}, function(t)
    -- Setup for this and the next three tests as well. No scheduled
    -- retention pass for a day, and a log retention of one day; each
    -- change itself asks for a pass, so let those run first.
    eventd.set(vm, "RetentionCheckIntervalMinutes", "dword:1440"):assert_ok()
    eventd.set(vm, "LogRetentionDays", "dword:1"):assert_ok()
    -- The passes those changes asked for have run once an aged probe
    -- record sent now is gone again.
    local probe = eventd.marker("probe")
    eventd.send_log(vm, { origin = probe, is_error = false, message = "probe",
        timestamp = eventd.guest_ns(vm) - 10 * 86400 * 1000000000 })
    vm:run("sleep 1")
    wait_until(function() return #eventd.rows(vm, "LOGS FROM " .. probe .. " SINCE 30d ago") == 0 end,
        { timeout = 30, interval = 0.5, desc = "the requested retention passes to finish" })
    vm:run("sleep 3")
    -- A log record ten days old, in the (healthy) log store. Nothing but
    -- a retention pass removes it.
    full.old = eventd.marker("aged")
    eventd.send_log(vm, { origin = full.old, is_error = false, message = "aged",
        timestamp = eventd.guest_ns(vm) - 10 * 86400 * 1000000000 })
    eventd.wait_rows(vm, "LOGS FROM " .. full.old .. " SINCE 30d ago", function(r) return #r == 1 end)
    vm:run("sleep 2")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. full.old .. " SINCE 30d ago"), 1,
        "the aged record stays while no pass runs")
    mount_small(eventd.STORE.events)
    full.pid = eventd.pid(vm)
    full.since = eventd.guest_ns(vm)
    fill(eventd.STORE.events)
    full.tag = eventd.marker("lost")
    for i = 1, 10 do eventd.emit(vm, "pt.lostbatch", { tag = full.tag, i = i }) end
    t:assert(eventd.stderr_line(vm,"event store is full", full.since), "the event write failed for want of space")
    vm:run("sleep 1")
    t:assert_eq(eventd.pid(vm), full.pid, "eventd is still the same process")
    local names = select(2, eventd.thread_names(vm, full.pid))
    t:assert(names:find("eventd-writer-0", 1, true), "with its writer thread alive: " .. names)
end)

-- The disk-full write requests an immediate pass, and the pass reaches the
-- log store though the event store's own retention fails for want of
-- space (§3.6: each store is processed whether or not the one before it
-- failed, and the pass reports every store that did).
test("a full disk under the event store triggers a retention run across every store", {
    spec = "eventd *storagefail.a-disk-full-or-quota-write-failure-triggers-an-immediate-retention-run-on-every-enabled-store"
        .. " eventd *eventretain.a-store-whose-retention-fails-does-not-stop-the-others",
}, function(t)
    t:assert(full.old, "the aged log record and the full event store from the previous test")
    local gone = pcall(wait_until, function()
        return #eventd.rows(vm, "LOGS FROM " .. full.old .. " SINCE 30d ago") == 0
    end, { timeout = 15, interval = 0.5, desc = "the aged log record to be retained away" })
    t:assert(gone, "the retention run the full disk triggered reached the log store and removed the aged record")
    local failed = eventd.stderr_line(vm, "retention pass failed", full.since)
    t:assert(failed, "the pass itself failed, on the full event store")
    local m = failed and failed.message or ""
    t:assert(m:find("event store:", 1, true), "and its failure names the event store: " .. m)
    t:assert(not m:find("log store:", 1, true), "not the log store, whose retention succeeded: " .. m)
end)

-- §3.6: "After a failed pass, requests are held for one second, doubling to
-- a minute while passes keep failing". The full event store fails every
-- pass, and every failed write into it requests another; each failure's
-- line names the hold before the next, and the passes are that far apart.
test("after a failed retention pass, requested passes back off from one second, doubling", {
    spec = "eventd *eventretain.after-a-failed-pass-requested-passes-back-off",
}, function(t)
    t:assert(full.since, "the full event store from the earlier test")
    local lines = eventd.stderr(vm, "retention pass failed and will be retried in",
        { since = full.since, count = 4, timeout = 30 })
    t:assert(#lines >= 4, "four failed passes in a row: " .. #lines)
    local holds = {}
    for i, l in ipairs(lines) do
        holds[i] = tonumber(l.message:match("will be retried in (%d+)s"))
    end
    t:assert_eq(holds[1], 1, "the first failure holds requests for one second: " .. json.encode(holds))
    for i = 2, #holds do
        t:assert_eq(holds[i], math.min(holds[i - 1] * 2, 60), "each further failure doubles the hold, "
            .. "to at most a minute: " .. json.encode(holds))
        local gap = (lines[i].timestamp - lines[i - 1].timestamp) / 1e9
        t:assert(gap >= holds[i - 1] * 0.9, "and no pass ran inside the previous hold (" .. holds[i - 1]
            .. "s): " .. string.format("%.2f", gap) .. "s apart")
    end
end)

test("a failed event batch is logged to stderr with its CPUs and sequence ranges", {
    spec = "eventd *storagefail.a-failed-event-batch-is-logged-to-stderr-with-its-cpus-and-sequence-ranges",
}, function(t)
    local line = eventd.stderr_line(vm,"event store is full", full.since)
    t:assert(line, "the failure reached stderr")
    local m = line and line.message or ""
    t:assert(m:lower():find("cpu", 1, true) and m:find("%d+%s*%-%s*%d+"),
        "naming the CPU and the sequence range of the lost batch: " .. m)
end)

test("the failed batch is lost, and on the writer's next commit the loss is recorded first", {
    spec = "eventd *storagefail.a-failed-event-batch-is-lost"
        .. " eventd *storagefail.failed-batch-ranges-are-held-in-an-in-memory-lost-batch-list"
        .. " eventd *storagefail.lost-batch-gap-records-are-written-before-new-events-on-the-next-successful-commit",
}, function(t)
    t:assert_eq(eventd.pid(vm), full.pid, "the same eventd that met the full disk")
    unfill(eventd.STORE.events)
    local tag = eventd.marker("next")
    eventd.emit(vm, "pt.nextbatch", { tag = tag })
    eventd.wait_rows(vm, 'EVENTS pt.nextbatch WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 1 end)
    t:assert_eq(#eventd.rows(vm, 'EVENTS pt.lostbatch WHERE tag == "' .. full.tag .. '" SINCE 10m ago'), 0,
        "the events of the failed batch are not in the store")
    -- The lost sequences, from the shard: gaps written after the fill
    -- began, and the first new event after them.
    local shard = eventd.shards(vm)[1]
    local next_id = eventd.sql(vm, shard,
        "SELECT min(id) FROM events WHERE event_type = 'pt.nextbatch'")[1][1]
    local gaps = eventd.sql(vm, shard, "SELECT id FROM events WHERE event_type = '" .. eventd.T.gap
        .. "' AND timestamp >= " .. full.since)
    t:assert(#gaps >= 1, "the failed batches were recorded as gaps, from the list held in memory")
    for _, g in ipairs(gaps) do
        t:assert(g[1] < next_id, "each gap record was written before the new event")
    end
    local gap_rows = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")
    local covered = 0
    for _, g in ipairs(gap_rows) do
        if g["event.time"] >= full.since then covered = covered + (g["loss.count"] or 0) end
    end
    t:assert(covered >= 10, "covering at least the ten lost events: " .. covered)
end)

test("a crash before the disk recovers still records the loss, through restart reconciliation", {
    spec = "eventd *storagefail.a-crash-before-disk-recovery-still-records-the-loss-through-restart-reconciliation",
}, function(t)
    local since = eventd.guest_ns(vm)
    fill(eventd.STORE.events)
    local tag = eventd.marker("crashfull")
    for i = 1, 10 do eventd.emit(vm, "pt.crashfull", { tag = tag, i = i }) end
    eventd.stderr_line(vm,"event store is full", since)
    eventd.crash(vm, { pid = eventd.pid(vm) })
    unfill(eventd.STORE.events)
    eventd.start(vm)
    -- Each lost event is either back (it survived in the ring and no
    -- receipt covered it) or inside a gap record.
    local stored = {}
    for _, r in ipairs(eventd.rows(vm, 'EVENTS pt.crashfull WHERE tag == "' .. tag .. '" SINCE 10m ago SELECT i, event.sequence')) do
        stored[r.i] = r["event.sequence"]
    end
    local missing = 0
    for i = 1, 10 do if not stored[i] then missing = missing + 1 end end
    if missing > 0 then
        local gaps = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")
        local spanned = 0
        for _, g in ipairs(gaps) do if g["event.time"] >= since then spanned = spanned + g["loss.count"] end end
        t:assert(spanned >= missing, "every event not recovered is inside a gap record")
    end
    t:assert(true, "recovered " .. (10 - missing) .. " of 10 from the ring")
    eventd.stop(vm)
    vm:run("umount " .. eventd.STORE.events):assert_ok()
    eventd.start(vm)
end)

test("a failed log commit loses that batch and the log writer carries on, with no lost-batch record", {
    spec = "eventd *storagefail.a-failed-log-or-metric-commit-loses-the-batch-and-the-writer-carries-on"
        .. " eventd *storagefail.the-log-and-metric-stores-have-no-lost-batch-accounting",
}, function(t)
    mount_small(eventd.STORE.logs)
    local pid = eventd.pid(vm)
    local since = eventd.guest_ns(vm)
    fill(eventd.STORE.logs)
    local lost = eventd.marker("loglost")
    for i = 1, 5 do eventd.send_log(vm, { origin = lost, is_error = false, message = "l" .. i }) end
    vm:run("sleep 2")
    unfill(eventd.STORE.logs)
    local kept = eventd.marker("logkept")
    eventd.send_log(vm, { origin = kept, is_error = false, message = "k" })
    local _, ok = eventd.wait_rows(vm, "LOGS FROM " .. kept .. " SINCE 10m ago", function(r) return #r == 1 end)
    local lost_rows = #eventd.rows(vm, "LOGS FROM " .. lost .. " SINCE 10m ago")
    local errs = storage_errors(since)
    local gaps = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")
    local new_gaps = 0
    for _, g in ipairs(gaps) do if g["event.time"] >= since then new_gaps = new_gaps + 1 end end
    local alive = eventd.alive(vm, pid)
    eventd.stop(vm)
    vm:run("umount " .. eventd.STORE.logs):assert_ok()
    eventd.start(vm)
    eventd.unset(vm, "RetentionCheckIntervalMinutes")
    eventd.unset(vm, "LogRetentionDays")
    t:assert_eq(lost_rows, 0, "the records written into a full disk are gone")
    t:assert(ok, "and the next record after space returned was stored")
    t:assert(alive, "by the same eventd")
    t:assert_eq(new_gaps, 0, "no gap record stands for the lost logs")
    t:assert_eq(#errs, 0, "and no storage error either: logs have nothing to account against")
end)
