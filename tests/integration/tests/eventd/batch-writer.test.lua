-- eventd TRM §2.4 — the batch writer: explicit transactions with their
-- receipts, adaptive batch sizing against MaxBatchSize and
-- MaxBatchLatencyMs, passive WAL checkpointing, the event-type catalogue,
-- and maintenance arriving as commands to the writer.
--
-- The writer's transactions have one external trace: every commit inserts
-- one `receipt_ranges` row per contiguous sequence run it holds (§3.1), and
-- consecutive commits are not merged into each other. So the receipt rows
-- covering a flood of consecutive sequences are the batch boundaries, read
-- straight out of the shard with the host-side sqlite copy. To get a
-- backlog for the writer to batch, eventd is SIGSTOPped while a flood is
-- emitted into the (default, 4 MiB) ring, then resumed.
--
-- Two vCPUs: a batch only outgrows the handoff channel, and only meets
-- the latency cap rather than the drained-input condition, when the drain
-- thread refills the channel *while* the writer inserts; on one vCPU the
-- two alternate and the writer always finds the channel empty first.
-- Events are emitted from a worker pinned to CPU 0, so they all route to
-- shard 0 (default configuration: one shard per CPU).
--
-- One file-scope VM. The checkpoint tests run first, because a WAL file
-- never shrinks and they reason about its size.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local sys = require("helpers.sys")
local unixsock = require("helpers.unixsock")
peinit.claim(1, { cpus = 2 })

local SHARD0 = eventd.STORE.events .. "/shard-0000.db"
local PAGE = 4096

local vm = eventd.boot({ name = "ev-batch", cpus = 2 })

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

--- Emit `n` events of `event_type` from a worker pinned to CPU 0.
local function emit0(event_type, n)
    local worker = vm:spawn_worker()
    local ok, err = pcall(function()
        assert(worker:syscall(sys.NR.sched_setaffinity, {
            args = { 0, 8, 0 }, bufs = { string.pack("<I8", 1) }, ptrs = { 2 },
        }).ret == 0, "pin to cpu 0")
        local sent = 0
        while sent < n do
            local k = math.min(256, n - sent)
            local batch = {}
            for i = 1, k do batch[i] = { type = event_type, payload = kmes.PAYLOAD } end
            local r = kmes.emit_batch(worker, batch)
            assert(r.ret == 0 and r.emitted == k, "emit_batch: errno " .. tostring(r.errno))
            sent = sent + k
        end
    end)
    worker:kill(); worker:join()
    if not ok then error(err, 0) end
end

--- `eventd.sql`, retried: a copy taken while the writer is mid-commit can
--- be inconsistent (helpers/eventd.lua says as much), and these tests read
--- shard 0 while it is busy.
local function sql(db, query)
    local last
    for _ = 1, 20 do
        local ok, rows = pcall(eventd.sql, vm, db, query)
        if ok then return rows end
        last = rows
        vm:run("sleep 0.5")
    end
    error(last, 0)
end

--- A flood emitted while eventd is stopped, so the writer meets it as one
--- backlog. Returns the first and last sequence once all are stored.
local function backlog(event_type, n)
    eventd.freeze(vm)
    local ok, err = pcall(emit0, event_type, n)
    eventd.thaw(vm)
    if not ok then error(err, 0) end
    local r
    wait_until(function()
        r = sql(SHARD0, string.format(
            "SELECT count(*), min(sequence), max(sequence) FROM events WHERE event_type = '%s'",
            event_type))[1]
        return r[1] >= n
    end, { timeout = 90, interval = 0.5, desc = n .. " rows of " .. event_type })
    return r[2], r[3], r[1]
end

--- Receipt rows of CPU 0 inside [lo, hi], sorted, as {first, last, len}.
local function receipts_in(lo, hi)
    local out = {}
    for _, r in ipairs(sql(SHARD0, string.format(
        "SELECT first_sequence, last_sequence FROM receipt_ranges " ..
        "WHERE cpu_id = 0 AND last_sequence >= %d AND first_sequence <= %d " ..
        "ORDER BY first_sequence", lo, hi))) do
        out[#out + 1] = { first = r[1], last = r[2], len = r[2] - r[1] + 1 }
    end
    return out
end

local function file_size(path)
    local r = vm:run("stat -c %s " .. path)
    if r.exit_code == 0 then return tonumber(r.stdout:match("%d+")) end
    -- No stat(1): fall back to reading the file.
    local ok, data = pcall(vm.read_file, vm, path)
    return ok and #data or 0
end

--- Wait for the record of `key` being changed to `new_value` (written as
--- the registry decimal; the record carries it as an integer).
local function config_change(key, new_value)
    local found
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago TAKE 1000",
        function(rs)
            for _, r in ipairs(rs) do
                if r["config.name"] == key and r["config.value"] == tonumber(new_value) then
                    found = r; return true
                end
            end
            return false
        end, { desc = "a config change of " .. key .. " to " .. new_value })
    return found
end

--- Run `fn` with shard 0's writer at SCHED_IDLE under four busy loops and
--- the drain threads at SCHED_FIFO, so input keeps arriving while the
--- writer barely runs. Writers are spawned in shard order, so the lowest
--- writer tid is shard 0's (comm is truncated and identical for all).
---
--- With `niced`, the writer stays SCHED_OTHER at nice 19 under two busy
--- loops instead: it then runs in short, regular slices (about 1.5% of a
--- CPU) rather than almost never, which is what a latency-cap comparison
--- needs.
local function starved(fn, niced)
    local writers, drains = {}, {}
    for _, th in ipairs(eventd.threads(vm)) do
        if th.comm:find("^eventd%-writer") then writers[#writers + 1] = th.tid end
        if th.comm:find("^eventd%-drain") then drains[#drains + 1] = th.tid end
    end
    table.sort(writers)
    local hogs = {}
    local ok, err = pcall(function()
        if niced then
            -- setpriority(PRIO_PROCESS, tid, 19)
            assert(vm:syscall(141, 0, writers[1], 19).ret == 0, "setpriority on the writer")
        else
            eventd.set_policy(vm, writers[1], 5)
        end
        for _, d in ipairs(drains) do eventd.set_policy(vm, d, 1, 1) end
        for _ = 1, (niced and 2 or 4) do
            local r = vm:run("sh -c 'while :; do :; done' >/dev/null 2>&1 & echo $!")
            hogs[#hogs + 1] = r.stdout:match("(%d+)")
        end
        fn()
    end)
    for _, h in ipairs(hogs) do vm:run("kill -9 " .. h) end
    eventd.set_policy(vm, writers[1], 0)
    vm:syscall(141, 0, writers[1], 0)
    for _, d in ipairs(drains) do eventd.set_policy(vm, d, 0) end
    if not ok then error(err, 0) end
end

local function writes_by_thread()
    local ths, pid = eventd.threads(vm)
    local out = {}
    for _, th in ipairs(ths) do
        local ok, io = pcall(vm.read_file, vm, "/proc/" .. pid .. "/task/" .. th.tid .. "/io")
        if ok then
            out[th.tid] = { comm = th.comm, syscw = tonumber(io:match("syscw: (%d+)")),
                            wchar = tonumber(io:match("wchar: (%d+)")) }
        end
    end
    return out
end

local function store_writer(comm)
    return comm:find("^eventd%-writer") or comm == "eventd-log" or comm == "eventd-metric"
end

--- Wait until no thread but a store writer has made a write syscall for a
--- whole second. An applied configuration change requests a retention
--- pass, and a pass rings the log and metric threads' wake eventfds (a
--- write(2) of eight bytes each) as it hands them its commands; that pass
--- is over before a measurement starts.
local function others_quiet()
    local last = writes_by_thread()
    wait_until(function()
        vm:clock():sleep("1s")
        local now, moved = writes_by_thread(), false
        for tid, a in pairs(now) do
            local b = last[tid]
            if b and not store_writer(a.comm) and a.syscw > b.syscw then moved = true end
        end
        last = now
        return not moved
    end, { timeout = 30, interval = 0.05, desc = "every thread but the store writers to stop writing" })
end

-- ---------------------------------------------------------------------------
-- WAL checkpointing (first: the WAL file's size is a high-water mark)
-- ---------------------------------------------------------------------------

test("the writer checkpoints passively once its WAL reaches WalCheckpointPages", {
    spec = "eventd *batch.a-passive-checkpoint-is-triggered-when-the-wal-reaches-wal-checkpoint-pages"
        .. " eventd *batch.checkpointing-runs-on-the-writer-thread",
}, function(t)
    eventd.set(vm, "WalCheckpointPages", "dword:100"):assert_ok()
    config_change("WalCheckpointPages", "100")
    others_quiet()
    local db0 = file_size(SHARD0)
    local before = writes_by_thread()

    -- ~30000 rows: several times the 100-page threshold, written in many
    -- small commits as eventd keeps up with the emitter.
    local event_type = "pt.batch." .. eventd.marker("ckpt")
    emit0(event_type, 30000)
    wait_until(function()
        return sql(SHARD0, string.format(
            "SELECT count(*) FROM events WHERE event_type = '%s'", event_type))[1][1] >= 30000
    end, { timeout = 90, interval = 0.5, desc = "the flood to be stored" })
    local wal = file_size(SHARD0 .. "-wal")
    local db1 = file_size(SHARD0)
    t:assert(db1 - db0 > 300 * PAGE,
        "the rows reached the main database file, which only a checkpoint does: " ..
        db0 .. " -> " .. db1 .. " bytes")
    t:assert(wal < 400 * PAGE,
        "and the WAL never grew far past the threshold: " .. math.floor(wal / PAGE) .. " pages")

    -- No thread other than a store writer wrote anything meanwhile.
    local after = writes_by_thread()
    for tid, a in pairs(after) do
        local b = before[tid]
        if b and a.syscw > b.syscw then
            t:assert(store_writer(a.comm), a.comm .. " made write syscalls during the flood: "
                .. (a.syscw - b.syscw) .. " of them, " .. (a.wchar - b.wchar) .. " bytes")
        end
    end
end)

test("a checkpoint held up by a reader does not block the writer and completes after a later commit", {
    spec = "eventd *batch.a-stalled-checkpoint-never-blocks-the-writer-and-is-retried-after-a-later-commit",
}, function(t)
    -- A query whose client never reads its answer: eventd sends the
    -- initial result set as it reads it (query/mod.rs:346), so once the
    -- socket buffer is full it sits mid-statement on every shard, holding a
    -- read snapshot, until the client goes away. evctl cannot be that
    -- client (it reads the whole result before printing), so the agent
    -- speaks the query framing itself: a u32 length, then {query = text}.
    local fd = assert(unixsock.socket(vm, unixsock.AF_UNIX, unixsock.SOCK.STREAM))
    t:assert_eq(unixsock.connect(vm, fd, eventd.SOCKET.query).ret, 0, "connected to the query socket")
    local frame = eventd.msgpack(eventd.map({ query = "EVENTS SINCE 1h ago TAKE 50000" }))
    local sent = unixsock.sendmsg(vm, fd, string.pack("<I4", #frame) .. frame)
    t:assert_eq(sent.ret, 4 + #frame, "sent the query")
    vm:run("sleep 2")
    local peek = unixsock.recvmsg(vm, fd, 64, { flags = unixsock.MSG.PEEK | unixsock.MSG.DONTWAIT, cmsg = 0 })
    t:assert(peek.ret and peek.ret > 0 and peek.data:find("records", 1, true),
        "eventd has begun sending the result, and the client is not reading it")
    local db0 = file_size(SHARD0)
    local wal0 = file_size(SHARD0 .. "-wal")

    local held = "pt.batch." .. eventd.marker("held")
    emit0(held, 20000)
    local stored
    wait_until(function()
        stored = sql(SHARD0, string.format(
            "SELECT count(*) FROM events WHERE event_type = '%s'", held))[1][1]
        return stored >= 20000
    end, { timeout = 60, interval = 0.5, desc = "the writer to keep committing" })
    local db1 = file_size(SHARD0)
    local wal1 = file_size(SHARD0 .. "-wal")
    t:assert_eq(stored, 20000, "the writer kept committing while the reader held its snapshot")
    t:assert(wal1 - wal0 > 100 * PAGE,
        "the WAL grew past the threshold: the checkpoint could not reset it (" ..
        math.floor(wal0 / PAGE) .. " -> " .. math.floor(wal1 / PAGE) .. " pages)")
    t:assert(db1 - db0 < 100 * PAGE,
        "and the main file took in almost none of the new rows (" .. db0 .. " -> " .. db1 .. ")")

    -- Release the reader, then commit again.
    sys.close(vm, fd)
    vm:run("sleep 2")
    emit0("pt.batch." .. eventd.marker("later"), 10)
    local db2
    wait_until(function()
        db2 = file_size(SHARD0)
        return db2 - db1 > 300 * PAGE
    end, { timeout = 30, interval = 0.5, desc = "a later commit's checkpoint to copy the held rows" })
    t:assert(db2 - db1 > 300 * PAGE, "after a later commit the held rows reached the main file")
    eventd.unset(vm, "WalCheckpointPages")
end)

-- ---------------------------------------------------------------------------
-- Transactions and adaptive sizing
-- ---------------------------------------------------------------------------

test("a lone event commits at once in its own transaction, without waiting out the latency cap", {
    spec = "eventd *batch.a-batch-commits-when-no-assigned-drain-thread-has-an-event-available",
}, function(t)
    eventd.set(vm, "MaxBatchLatencyMs", "dword:5000"):assert_ok()
    config_change("MaxBatchLatencyMs", "5000")
    local ok, err = pcall(function()
        local event_type = "pt.batch." .. eventd.marker("lone")
        local r = eventd.emit(vm, event_type, { n = 1 })
        t:assert_eq(r.ret, 0, "emitted")
        local waited = pcall(eventd.wait_rows, vm, "EVENTS " .. event_type .. " SINCE 10m ago",
            function(rs) return #rs == 1 end, { timeout = 2, interval = 0.05 })
        t:assert(waited, "stored within 2 s although a batch may stay open for 5 s")
        local seq = eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 10m ago")[1]["event.sequence"]
        local cpu = eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 10m ago")[1]["event.cpu"]
        local rows = {}
        for _, shard in ipairs(eventd.shards(vm)) do
            for _, x in ipairs(sql(shard, string.format(
                "SELECT first_sequence, last_sequence FROM receipt_ranges " ..
                "WHERE cpu_id = %d AND first_sequence <= %d AND last_sequence >= %d", cpu, seq, seq))) do
                rows[#rows + 1] = x
            end
        end
        t:assert_eq(#rows, 1, "one receipt covers it")
    end)
    eventd.unset(vm, "MaxBatchLatencyMs")
    if not ok then error(err, 0) end
end)

test("with MaxBatchSize 100 a backlog commits in runs of at most 100, one row and one receipt per run", {
    spec = "eventd *batch.a-batch-is-an-explicit-transaction-with-one-insert-per-event"
        .. " eventd *batch.each-maximal-contiguous-sequence-run-in-a-transaction-gets-a-receipt-row"
        .. " eventd *batch.a-batch-commits-when-it-holds-max-batch-size-events"
        .. " eventd *batch.an-insert-group-never-takes-a-batch-past-either-cap",
}, function(t)
    eventd.set(vm, "MaxBatchSize", "dword:100"):assert_ok()
    config_change("MaxBatchSize", "100")
    eventd.set(vm, "MaxBatchLatencyMs", "dword:5000"):assert_ok()
    config_change("MaxBatchLatencyMs", "5000")

    local ok, err = pcall(function()
        local event_type = "pt.batch." .. eventd.marker("b100")
        local lo, hi, n = backlog(event_type, 1000)
        t:assert_eq(n, 1000, "one row per event")
        t:assert_eq(hi - lo + 1, 1000, "with consecutive sequences")
        local runs = receipts_in(lo, hi)
        local full, covered, prev = 0, 0, nil
        for _, r in ipairs(runs) do
            t:assert(r.len <= 100, "no commit held more than 100 events: " .. json.encode(r))
            if r.len == 100 then full = full + 1 end
            if prev then
                t:assert(r.first > prev.last, "receipt rows do not overlap: " .. json.encode(runs))
            end
            prev = r
            covered = covered + r.len
        end
        t:assert(full >= 5,
            "most commits stopped at exactly 100, the size cap, with input still waiting: " ..
            full .. " of " .. #runs)
        t:assert(#runs <= 20,
            "and each commit's run is one receipt row, not one per event: " .. #runs .. " rows")
        t:assert(covered >= 1000, "the receipts account for every event")
    end)
    eventd.unset(vm, "MaxBatchSize")
    eventd.unset(vm, "MaxBatchLatencyMs")
    if not ok then error(err, 0) end
end)

test("a sustained backlog commits when MaxBatchLatencyMs runs out, and a commit can outgrow the channel", {
    spec = "eventd *batch.a-batch-commits-once-max-batch-latency-ms-has-elapsed-since-its-first-event"
        .. " eventd *batch.the-first-available-event-opens-a-transaction-and-records-the-start-time"
        .. " eventd *shard.a-transaction-may-hold-more-events-than-the-channel-can-at-once",
}, function(t)
    local ok, err = pcall(function()
        eventd.set(vm, "MaxBatchSize", "dword:100000"):assert_ok()
        config_change("MaxBatchSize", "100000")

        -- A long latency cap and a 40000-event backlog: the drain thread
        -- refills the channel while the writer inserts, so a batch runs on.
        eventd.set(vm, "MaxBatchLatencyMs", "dword:5000"):assert_ok()
        config_change("MaxBatchLatencyMs", "5000")
        local lo, hi = backlog("pt.batch." .. eventd.marker("long"), 40000)
        local biggest = 0
        for _, r in ipairs(receipts_in(lo, hi)) do biggest = math.max(biggest, r.len) end
        -- The channel holds 4096 events (config.rs:19); one commit took
        -- more than that while the drain thread kept refilling it.
        t:assert(biggest > 4096,
            "one transaction held " .. biggest .. " events, far more than the channel holds at once")

        -- The writer inserts an unstarved backlog in a few milliseconds, so
        -- to watch the latency cap the writer is slowed instead: nice 19
        -- under busy loops, with the drain threads at SCHED_FIFO keeping its
        -- channel full. A batch then stays open across long stretches with
        -- the writer off the CPU, and only the clock (or the size cap,
        -- 100000 here) can end it. Commits are counted after 6 s.
        local function starved_commits(latency_ms)
            eventd.set(vm, "MaxBatchLatencyMs", "dword:" .. latency_ms):assert_ok()
            config_change("MaxBatchLatencyMs", tostring(latency_ms))
            local event_type = "pt.batch." .. eventd.marker("lat" .. latency_ms)
            local commits, stored
            starved(function()
                emit0(event_type, 20000)
                vm:run("sleep 6")
                local r = sql(SHARD0, string.format(
                    "SELECT count(*), min(sequence), max(sequence) FROM events WHERE event_type = '%s'",
                    event_type))[1]
                stored = r[1]
                commits = stored > 0 and #receipts_in(r[2], r[3]) or 0
            end, true)
            wait_until(function()
                return sql(SHARD0, string.format(
                    "SELECT count(*) FROM events WHERE event_type = '%s'", event_type))[1][1] >= 20000
            end, { timeout = 90, interval = 0.5, desc = "the starved backlog to drain" })
            return commits, stored
        end
        local slow_commits, slow_stored = starved_commits(5000)
        local fast_commits, fast_stored = starved_commits(10)
        t:assert(fast_commits >= 5 and fast_commits >= 3 * math.max(slow_commits, 1),
            "with the writer slowed, a 10 ms cap commits far more often than a 5 s cap: " ..
            fast_commits .. " commits (" .. fast_stored .. " rows) vs " .. slow_commits ..
            " (" .. slow_stored .. " rows) in 6 s")
    end)
    eventd.unset(vm, "MaxBatchSize")
    eventd.unset(vm, "MaxBatchLatencyMs")
    if not ok then error(err, 0) end
end)

test("the default caps are 10000 events and 100 ms", {
    spec = "eventd *batch.the-default-caps-are-10000-events-and-100-milliseconds",
}, function(t)
    -- eventd records a configuration change only when the value in force
    -- changes (config.rs:676-722 compares the applied settings). Writing
    -- the default explicitly changes nothing in force, so it is not
    -- recorded; any other value is. A later, different change is the
    -- marker that eventd has processed the writes before it.
    local function recorded(key)
        local n = 0
        for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago TAKE 1000")) do
            if r["config.name"] == key then n = n + 1 end
        end
        return n
    end
    local ok, err = pcall(function()
        local size0, latency0 = recorded("MaxBatchSize"), recorded("MaxBatchLatencyMs")
        eventd.set(vm, "MaxBatchSize", "dword:10000"):assert_ok()
        eventd.set(vm, "MaxBatchLatencyMs", "dword:100"):assert_ok()
        eventd.set(vm, "LogRetentionDays", "dword:13"):assert_ok()
        config_change("LogRetentionDays", "13")
        t:assert_eq(recorded("MaxBatchSize"), size0,
            "setting MaxBatchSize to 10000 changed nothing in force: 10000 is the default")
        t:assert_eq(recorded("MaxBatchLatencyMs"), latency0,
            "setting MaxBatchLatencyMs to 100 changed nothing in force: 100 ms is the default")

        -- The control: one more is a change.
        eventd.set(vm, "MaxBatchSize", "dword:10001"):assert_ok()
        eventd.set(vm, "MaxBatchLatencyMs", "dword:101"):assert_ok()
        config_change("MaxBatchSize", "10001")
        config_change("MaxBatchLatencyMs", "101")
    end)
    eventd.unset(vm, "MaxBatchSize")
    eventd.unset(vm, "MaxBatchLatencyMs")
    eventd.unset(vm, "LogRetentionDays")
    if not ok then error(err, 0) end
end)

test("a committed event survives the writer's process being killed", {
    spec = "eventd *batch.the-commit-is-the-durability-boundary",
}, function(t)
    local event_type = "pt.batch." .. eventd.marker("durable")
    eventd.emit(vm, event_type, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. event_type .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local pid = eventd.pid(vm)
    eventd.signal(vm, pid, "KILL")
    wait_until(function()
        local now = eventd.pid(vm)
        return now ~= nil and now ~= pid
    end, { timeout = 90, interval = 0.5, desc = "peinit to restart eventd" })
    eventd.ready(vm)
    local rows = eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 10m ago")
    t:assert_eq(#rows, 1, "once queryable it was committed: it outlived a SIGKILL, exactly once")
end)

test("a new event type is catalogued with its first event, once", {
    spec = "eventd *batch.only-a-new-event-type-is-catalogued-in-the-transaction-of-its-first-event",
}, function(t)
    local event_type = "pt.batch." .. eventd.marker("cat")
    local function catalogued()
        return sql(SHARD0, string.format(
            "SELECT count(*) FROM event_types WHERE event_type = '%s'", event_type))[1][1]
    end
    t:assert_eq(catalogued(), 0, "not catalogued before it is seen")
    eventd.freeze(vm)
    local ok, err = pcall(emit0, event_type, 1)
    eventd.thaw(vm)
    if not ok then error(err, 0) end
    wait_until(function()
        return sql(SHARD0, string.format(
            "SELECT count(*) FROM events WHERE event_type = '%s'", event_type))[1][1] == 1
    end, { timeout = 30, interval = 0.25, desc = "the first event" })
    t:assert_eq(catalogued(), 1, "catalogued by the time its first event is visible")
    emit0(event_type, 500)
    wait_until(function()
        return sql(SHARD0, string.format(
            "SELECT count(*) FROM events WHERE event_type = '%s'", event_type))[1][1] == 501
    end, { timeout = 30, interval = 0.25, desc = "the rest" })
    t:assert_eq(catalogued(), 1, "and still exactly one catalogue row after 500 more")
end)

test("event shards are in WAL mode", {
    spec = "eventd *batch.shards-run-in-wal-mode-with-synchronous-full",
}, function(t)
    -- WAL is in the file header: bytes 18 and 19 (the write and read
    -- format versions) are 2 in WAL mode. synchronous is a per-connection
    -- setting with no trace in the file; tracefs is closed to the agent, so
    -- FULL's fsync-per-commit cannot be watched (see the report).
    for _, shard in ipairs(eventd.shards(vm)) do
        local header = vm:read_file(shard):sub(1, 100)
        t:assert_eq(header:sub(1, 16), "SQLite format 3\0", shard .. " is a SQLite database")
        t:assert_eq(header:byte(19), 2, shard .. " has WAL write format")
        t:assert_eq(header:byte(20), 2, shard .. " has WAL read format")
        t:assert(file_size(shard .. "-wal") > 0, "and a write-ahead log")
    end
end)

-- ---------------------------------------------------------------------------
-- Maintenance
-- ---------------------------------------------------------------------------

test("retention reaches an active shard only through its writer, a bounded step at a time", {
    spec = "eventd *batch.the-writer-owns-the-shards-only-read-write-connection"
        .. " eventd *batch.maintenance-reaches-a-shard-only-as-bounded-commands-to-its-writer"
        .. " eventd *batch.low-priority-maintenance-runs-at-transaction-boundaries-one-bounded-action-at-a-time"
        .. " eventd *batch.ingestion-takes-priority-over-maintenance-under-sustained-pressure"
        .. " eventd *synthetic.synthetic-events-share-the-shards-batching-retention-and-queries-of-kmes-events",
}, function(t)
    -- Retention is made long and its progress visible: a flood in ordered
    -- segments, the oldest a small `head`, then nine of 30000 events, all
    -- on CPU 0 and so in shard 0. With a 1 MiB limit and 100 rows a step,
    -- retention deletes oldest-first through every segment but the
    -- newest, some 2700 bounded steps through shard 0's writer. Progress is
    -- read through queries, which cost the same under any load: the head
    -- gone means the run has begun, segment 8 present means it has not
    -- finished. An event emitted once the run has begun, to the same
    -- writer, must be committed while segment 8 is still there — that is,
    -- not behind the run but between its steps. No clock is involved.
    local stem = "pt.batch." .. eventd.marker("ret")
    local head = stem .. ".head"
    local function seg(k) return stem .. ".seg" .. k end
    local function present(event_type)
        return #eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 1h ago TAKE 1") > 0
    end
    emit0(head, 500)
    for k = 1, 9 do emit0(seg(k), 30000) end
    wait_until(function() return present(seg(9)) end,
        { timeout = 120, interval = 0.5, desc = "the flood to be stored" })
    t:assert(present(head) and present(seg(8)), "the head and segment 8 are stored")
    local startups0 = sql(SHARD0,
        "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.startup .. "'")[1][1]
    t:assert(startups0 >= 1, "shard 0 holds this boot's startup records: " .. startups0)

    local ok, err = pcall(function()
        eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
        config_change("RetentionDeleteBatchRows", "100")
        eventd.set(vm, "EventRetentionMaxBytes", "qword:1048576"):assert_ok()

        wait_until(function() return not present(head) end,
            { timeout = 120, interval = 0.2, desc = "retention to begin deleting" })
        t:assert(present(seg(8)), "retention has begun and has not reached segment 8")
        local rw_during = (eventd.fds_on(vm, nil, SHARD0))

        local during = "pt.batch." .. eventd.marker("during")
        emit0(during, 1)
        wait_until(function() return present(during) end,
            { timeout = 120, interval = 0.2, desc = "the event emitted mid-retention" })
        local seg8_when_committed = present(seg(8))

        wait_until(function() return not present(seg(8)) end,
            { timeout = 300, interval = 1, desc = "retention to delete segment 8" })
        t:assert_eq(rw_during, 1,
            "while retention ran, the shard had only the writer's read-write connection")
        t:assert(seg8_when_committed,
            "the event emitted mid-retention was committed while the run still had segment 8 " ..
            "to delete: the writer took it between bounded retention steps, not after the run")
        local startups1 = sql(SHARD0,
            "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.startup .. "'")[1][1]
        t:assert(startups1 < startups0,
            "synthetic records live in the same shard and age out with the events around them: " ..
            startups0 .. " -> " .. startups1 .. " startup records")
    end)
    eventd.unset(vm, "EventRetentionMaxBytes")
    eventd.unset(vm, "RetentionDeleteBatchRows")
    if not ok then error(err, 0) end
end)

-- Documented skip: when a statement is prepared is invisible to any caller
-- — no query, file or timing distinguishes a statement prepared at startup
-- from one prepared per commit. (For the record, the shard prepares its
-- INSERTs through rusqlite's per-connection statement cache on first use,
-- shard.rs:146-156, not at startup.)
test("the event insert is prepared once and reused", {
    spec = "eventd *batch.the-event-insert-is-prepared-once-at-startup-and-reused",
    skip = true,
}, function() end)

-- Documented skip: as above, statement preparation leaves no observable
-- trace (shard.rs:155,210 use the same statement cache).
test("the receipt and catalogue inserts are prepared and reused", {
    spec = "eventd *batch.the-receipt-and-catalogue-inserts-are-prepared-and-reused",
    skip = true,
}, function() end)

-- Documented skip: a permission ("may"), not a behaviour; nothing is wrong
-- whether or not a size-urgent delete shares an ingestion transaction.
-- (eventd sends retention as separate Maintenance commands, writer.rs:521.)
test("a size-urgent retention delete may join an ingestion transaction", {
    spec = "eventd *batch.a-size-urgent-retention-delete-may-join-an-ingestion-transaction",
    skip = true,
}, function() end)
