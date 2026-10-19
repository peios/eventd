-- eventd TRM §8.4 — shutdown: the nine-step sequence, and the peinit stop
-- timeout that bounds it.
--
-- One VM. A clean stop is fast — well under a second — so most of the
-- sequence is read from what it leaves behind: the stores copied out
-- while eventd is down (the final drain and commit, the checkpoint rows,
-- the shutdown record, the WAL files gone), and the socket directory.
--
-- The middle of a shutdown is made observable by holding it open. An
-- idle query connection — connected, never sending — keeps one query
-- handler blocked reading its request for up to QueryTimeoutMs, and the
-- shutdown waits for every handler before it gets past step 1. With the
-- query timeout raised to its maximum and eventd's StopTimeout seeded to
-- a few seconds, a stop sits in that state until peinit gives up and
-- kills it: long enough to look at a shutdown in progress, and the
-- abort at the timeout is the second half of the section.
--
-- eventd is Normal in this VM so that a stop that ends in a kill is not
-- also a Critical failure; it is never restarted by policy here, only by
-- the tests.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local us = require("helpers.unixsock")
peinit.claim(1)

local STOP_TIMEOUT = 4

local STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

local vm = eventd.boot({
    name = "ev-shutdown",
    noncritical = { restart = false, values = {
        { name = "StopTimeout", type = "dword", data = STOP_TIMEOUT },
        -- A SIGKILLed eventd with unread events in its ring has been
        -- seen to take longer to die than peinit's default post-kill
        -- deadline, which then calls it unkillable and abandons the
        -- service. The abort is what is under test, not that.
        { name = "PostKillTimeout", type = "dword", data = 30 },
    } },
})

-- `eventd.start` waits until peinit has finished whatever operation it has
-- on eventd first: after a stop that ended in a kill, the stop operation
-- outlives the process by peinit's post-kill check, and a start issued
-- inside that window is a different test.

--- Hold a shutdown open: an idle query connection that will not send
--- its request, under the longest query timeout. Returns the client fd.
local function hold_open(t)
    eventd.set(vm, "QueryTimeoutMs", "dword:300000"):assert_ok()
    wait_until(function()
        for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago")) do
            if r["config.name"] == "QueryTimeoutMs" and r["config.value"] == 300000 then return true end
        end
        return false
    end, { timeout = 20, interval = 0.25, desc = "the long query timeout to apply" })
    local fd = assert(us.socket(vm, us.AF_UNIX, us.SOCK.STREAM))
    t:assert_eq(us.connect(vm, fd, eventd.SOCKET.query).ret, 0, "the idle client connected")
    return fd
end

--- `svctl stop` without waiting, and the moment it was asked.
local function begin_stop()
    local at = os.time()
    vm:run("svctl --no-wait stop eventd")
    return at
end

-- Shared by the held-open tests: the pid of the eventd being held, the
-- idle client and a datagram socket connected to the log socket before
-- the stop began.
local held = {}

test("a held shutdown has unlinked all three socket paths and accepts no new query", {
    spec = "eventd *shutdown.all-three-socket-paths-are-unlinked-and-query-connections-stop-being-accepted"
        .. " eventd *shutdown.the-log-and-metric-socket-descriptors-stay-open-after-unlinking",
}, function(t)
    held.pid = eventd.pid(vm)
    held.idle = hold_open(t)
    -- Connected before the stop, so it reaches the log socket's
    -- descriptor without its pathname.
    held.log = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    t:assert_eq(us.connect(vm, held.log, eventd.SOCKET.log).ret, 0, "a sender connected to the log socket")
    held.metric = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    t:assert_eq(us.connect(vm, held.metric, eventd.SOCKET.metric).ret, 0, "and one to the metric socket")
    held.asked = begin_stop()
    wait_until(function() return vm:run("ls /run/eventd").stdout:find("sock", 1, true) == nil end,
        { timeout = 3, interval = 0.1, desc = "the socket paths to go" })
    t:assert(eventd.alive(vm, held.pid), "eventd is still running its shutdown")
    t:assert_eq(vm:run("ls /run/eventd").stdout:gsub("%s", ""), "", "and every socket path is gone")
    local q = eventd.query(vm, "EVENTS TAKE 1")
    t:assert(not q.ok, "a new query cannot reach it")
    -- The descriptors behind the unlinked log and metric paths are open:
    -- a send on a connection made earlier is still accepted.
    local r = us.sendmsg(vm, held.log, eventd.msgpack({ origin = "ptheld", is_error = false, message = "x" }),
        { flags = us.MSG.DONTWAIT })
    t:assert(r.ret and r.ret > 0, "the log socket still receives (errno " .. tostring(r.errno) .. ")")
    r = us.sendmsg(vm, held.metric, eventd.msgpack({ name = "ptheld", type = "gauge", value = 1 }),
        { flags = us.MSG.DONTWAIT })
    t:assert(r.ret and r.ret > 0, "and so does the metric socket (errno " .. tostring(r.errno) .. ")")
end)

test("a shutdown still running at the stop timeout is ended there", {
    spec = "eventd *shutdown.an-unfinished-shutdown-aborts-at-the-peinit-stop-timeout",
}, function(t)
    t:assert(held.pid, "the held shutdown from the previous test")
    local gone = eventd.wait_gone(vm, held.pid, STOP_TIMEOUT + 10)
    local took = os.time() - held.asked
    t:assert(gone, "eventd did not outlive its stop timeout")
    t:assert(took <= STOP_TIMEOUT + 3, "it ended at the timeout, not at the query timeout: " .. took .. "s")
    -- It never reached step 6.
    vm:syscall(3, held.idle); vm:syscall(3, held.log); vm:syscall(3, held.metric)
    eventd.start(vm)
    local after = eventd.rows(vm, "EVENTS " .. eventd.T.shutdown .. " SINCE 10m ago")
    for _, r in ipairs(after) do
        t:assert(r["event.time"] < held.asked * 1000000000,
            "no shutdown record was written by the aborted shutdown")
    end
    eventd.unset(vm, "QueryTimeoutMs")
end)

-- §8.4 step 2: "Shut the log and metric sockets for reading, so that a
-- later send fails with EPIPE and its sender keeps the datagram … Then read
-- and process the datagrams still in the receive queues, and close those
-- descriptors." A sender connected before the stop has its sends accepted
-- (the held test above shows that through step 1); once step 2 begins they
-- fail with EPIPE, never again succeeding, until the descriptors close.
-- The drain is widened so the agent can see inside it: with eventd frozen,
-- both receive queues are filled with large datagrams (the metric ones of
-- new, authorized series, the slowest to process), the stop is asked
-- (SIGTERM waits for the thaw), and eventd is thawed.
test("once the drain begins, a send to the log socket fails with EPIPE", {
    spec = "eventd *shutdown.a-send-after-the-log-and-metric-sockets-are-shut-for-reading-fails-with-epipe",
}, function(t)
    local pid = eventd.pid(vm)
    local log = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    t:assert_eq(us.connect(vm, log, eventd.SOCKET.log).ret, 0, "a sender connected to the log socket")
    local metric = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    t:assert_eq(us.connect(vm, metric, eventd.SOCKET.metric).ret, 0, "and one to the metric socket")
    t:assert_eq(us.set_pass_token(vm, metric, true).ret, 0, "which passes its token")
    local tag = eventd.marker("epipe")
    local function log_bytes(n)
        local recs = {}
        for i = 1, n do recs[i] = { origin = tag, is_error = false, message = string.rep("p", 40) .. i } end
        return eventd.msgpack(eventd.array(recs))
    end
    local one = log_bytes(1)
    local r = us.sendmsg(vm, log, one, { flags = us.MSG.DONTWAIT })
    t:assert(r.ret and r.ret > 0, "before the stop the log socket takes a send (errno " .. tostring(r.errno) .. ")")

    eventd.freeze(vm, pid)
    local big = log_bytes(500)
    for _ = 1, 64 do
        local s = us.sendmsg(vm, log, big, { flags = us.MSG.DONTWAIT })
        if not (s.ret and s.ret > 0) then break end
    end
    for d = 1, 64 do
        local recs = {}
        for i = 1, 900 do recs[i] = { name = tag .. "." .. d .. "." .. i, type = "gauge", value = i } end
        local s = us.sendmsg(vm, metric, eventd.msgpack(eventd.array(recs)), { flags = us.MSG.DONTWAIT })
        if not (s.ret and s.ret > 0) then break end
    end
    begin_stop()
    eventd.thaw(vm, pid)

    local seen, epipe_at, ok_after = {}, nil, false
    local deadline = os.time() + 20
    while os.time() < deadline do
        local s = us.sendmsg(vm, log, one, { flags = us.MSG.DONTWAIT })
        local what = (s.ret and s.ret > 0) and "ok" or tostring(s.errno)
        if seen[#seen] ~= what then seen[#seen + 1] = what end
        if what == tostring(us.E.PIPE) then epipe_at = epipe_at or #seen
        elseif what == "ok" and epipe_at then ok_after = true end
        if what ~= "ok" and what ~= tostring(us.E.PIPE) and what ~= "11" then break end
    end
    vm:syscall(3, log); vm:syscall(3, metric)
    local gone = eventd.wait_gone(vm, pid, 30)
    eventd.start(vm)
    t:assert(gone, "the stop finished")
    t:assert(epipe_at, "once the drain began a send failed with EPIPE: " .. table.concat(seen, " → "))
    t:assert(not ok_after, "and none succeeded after it: " .. table.concat(seen, " → "))
end)

test("a stop ends streaming queries with an error", {
    spec = "eventd *shutdown.existing-streaming-queries-are-terminated-with-an-error",
}, function(t)
    local stream = vm:run_async("/usr/bin/evctl",
        { args = { "--format", "jsonl", "EVENTS pt.never.emitted STREAM" } })
    vm:run("sleep 1")
    eventd.stop(vm)
    local r = stream:wait("10s")
    eventd.start(vm)
    t:assert(r.exit_code ~= 0, "the streaming client ended with a failure: exit " .. tostring(r.exit_code))
    t:assert((r.stderr or ""):find("shutting down", 1, true),
        "told that eventd is shutting down: " .. tostring(r.stderr))
end)

test("events and datagrams queued at the stop are read, committed and closed out", {
    spec = "eventd *shutdown.queued-log-and-metric-datagrams-are-processed-before-their-sockets-close"
        .. " eventd *shutdown.each-drain-thread-performs-one-final-drain-cycle"
        .. " eventd *shutdown.every-writer-commits-its-current-batch-whatever-its-size",
}, function(t)
    -- The longest batch latency, so a batch is never closed by time alone.
    for _, k in ipairs({ "MaxBatchLatencyMs", "LogMaxBatchLatencyMs", "MetricMaxBatchLatencyMs" }) do
        eventd.set(vm, k, "dword:5000"):assert_ok()
    end
    vm:run("sleep 1")
    local pid = eventd.pid(vm)
    -- Frozen, so what follows sits in the ring and the receive queues
    -- when the stop arrives: SIGTERM is delivered on SIGCONT.
    eventd.freeze(vm, pid)
    local tag = eventd.marker("q")
    for i = 1, 7 do eventd.emit(vm, "pt.final", { tag = tag, i = i }) end
    for i = 1, 5 do eventd.send_log(vm, { origin = tag, is_error = false, message = "m" .. i }) end
    for i = 1, 3 do eventd.send_metric(vm, { name = tag, type = "gauge", value = i }) end
    eventd.signal(vm, pid, "TERM"); eventd.thaw(vm, pid)
    assert(eventd.wait_gone(vm, pid, 30), "eventd to finish its shutdown")
    -- Read from the files while eventd is down: what the shutdown itself
    -- committed, not what a later start recovers.
    local events = 0
    for _, shard in ipairs(eventd.shards(vm)) do
        events = events + eventd.sql(vm, shard,
            "SELECT count(*) FROM events WHERE event_type = 'pt.final'")[1][1]
    end
    t:assert(events >= 7, "the frozen ring's events were drained and committed: " .. events)
    t:assert_eq(eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE origin = '" .. tag .. "'")[1][1],
        5, "the queued log datagrams were processed")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT count(*) FROM samples JOIN series ON samples.series_id = series.id WHERE series.name = '" .. tag .. "'")[1][1],
        3, "and the queued metric datagrams")
    eventd.start(vm)
    for _, k in ipairs({ "MaxBatchLatencyMs", "LogMaxBatchLatencyMs", "MetricMaxBatchLatencyMs" }) do
        eventd.unset(vm, k)
    end
end)

test("a clean stop records each CPU's covered sequence, checkpoints it, and closes the WAL", {
    spec = "eventd *shutdown.each-cpus-highest-contiguously-covered-sequence-is-written-to-sequence-checkpoints"
        .. " eventd *shutdown.an-eventd-daemon-stopped-event-carries-the-per-cpu-sequences"
        .. " eventd *shutdown.every-database-connection-is-closed-checkpointing-its-wal",
}, function(t)
    for i = 1, 10 do eventd.emit(vm, "pt.stop", { i = i }) end
    vm:run("sleep 1")
    local asked = eventd.guest_ns(vm)
    eventd.stop(vm)
    local boot = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT hex(boot_id), payload FROM events WHERE event_type = '" .. eventd.T.shutdown
        .. "' ORDER BY timestamp DESC LIMIT 1")
    t:assert(#boot == 1, "the shutdown record is in the shard")
    local cps = eventd.sql(vm, eventd.DB.meta,
        "SELECT hex(boot_id), cpu_id, sequence, updated_at FROM sequence_checkpoints")
    local mine
    for _, r in ipairs(cps) do if r[1] == boot[1][1] and r[2] == 0 then mine = r end end
    t:assert(mine, "a checkpoint row for this boot's CPU 0")
    t:assert(mine and mine[4] >= asked, "written by this stop")
    -- Highest contiguously covered: the receipts end there.
    local receipts = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT max(last_sequence) FROM receipt_ranges WHERE cpu_id = 0 AND hex(boot_id) = '" .. boot[1][1] .. "'")
    t:assert_eq(mine and mine[3], receipts[1][1], "the checkpoint is the receipts' contiguous end")
    -- No -wal left beside any database: each was checkpointed on close.
    local wal = vm:run("ls /var/state/eventd/*/ | grep -- '-wal$' || true").stdout
    t:assert_eq(wal:gsub("%s", ""), "", "no write-ahead log survives a clean stop: " .. wal)
    eventd.start(vm)
    local sd = eventd.rows(vm, "EVENTS " .. eventd.T.shutdown .. " SINCE 10m ago")
    table.sort(sd, function(a, b) return a["event.time"] < b["event.time"] end)
    local last = sd[#sd]
    local cpus = last and last["store.committed.cpus"]
    local seqs = last and last["store.committed.sequences"]
    t:assert(type(cpus) == "table" and #cpus == 1 and type(seqs) == "table" and #seqs == 1,
        "the record carries one sequence per CPU: " .. json.encode(last))
    t:assert_eq(cpus and cpus[1], 0, "for CPU 0")
    t:assert_eq(seqs and seqs[1], mine and mine[3], "the same sequence as the checkpoint")
end)

test("with no writable shard the shutdown record is skipped and the failure logged", {
    spec = "eventd *shutdown.with-no-writable-shard-the-shutdown-event-is-skipped-and-the-failure-logged",
}, function(t)
    eventd.stop(vm)
    vm:run("mount -t tmpfs -o size=1m,policy=synth-ephemeral --synth-sddl '" .. STORE_SDDL
        .. "' none " .. eventd.STORE.events):assert_ok()
    eventd.start(vm)
    vm:run("dd if=/dev/zero of=" .. eventd.STORE.events .. "/pt-fill bs=4k 2>/dev/null; true")
    local since = eventd.guest_ns(vm)
    vm:run("svctl stop eventd")
    local records = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.shutdown .. "'")[1][1]
    vm:run("umount " .. eventd.STORE.events):assert_ok()
    eventd.start(vm)
    t:assert_eq(records, 0, "the full shard holds no shutdown record")
    -- The writer reports the refused commit (writer.rs request_retention)
    -- and the supervisor a failed one (pipeline.rs commit_synthetic_fallback);
    -- either is the failure on stderr.
    local line = eventd.stderr_line(vm, "cannot persist " .. eventd.T.shutdown, since)
        or eventd.stderr_line(vm, "event store is full", since)
    t:assert(line, "and the failed write was reported on stderr")
end)

-- Route closed: eventd unmaps its rings and closes their descriptors as
-- the last thing before it exits, and the kernel does the same for any
-- it left at exit. Nothing outside the process can tell the two apart.
test("every ring buffer is unmapped and its descriptor closed", {
    spec = "eventd *shutdown.every-ring-buffer-is-unmapped-and-its-descriptor-closed",
    skip = true,
    covered_by = "cargo:eventd eventd pipeline::tests::shutdown_joins_every_drain_and_releases_each_ring_it_returns",
}, function() end)

-- A fatal SQL error with control requests already queued, and a producer
-- holding an unpublished permit, need deterministic scheduling inside the
-- daemon. The host tests exercise the real writer and bounded handoff.
test("a stopped event writer releases pending control requests", {
    spec = "eventd *shutdown.a-stopped-event-writer-releases-pending-control-requests",
    skip = true,
    covered_by = "cargo:eventd eventd writer::tests::a_failed_writer_releases_all_pending_control_replies",
}, function() end)

test("writer exit handles blocked producers and late handoff publication", {
    spec = "eventd *shutdown.a-stopped-event-writer-releases-pending-control-requests",
    skip = true,
    covered_by = "cargo:eventd eventd writer::tests::writer_exit_wakes_blocked_producers_and_releases_late_control_replies",
}, function() end)

-- Last: after a held shutdown has been killed with events left unread in
-- the ring, the next eventd has twice ended Abandoned (ProcessUnkillable)
-- when it was itself stopped (reported). Nothing follows it.
test("an aborted shutdown: events are recovered from KMES, uncommitted logs are lost, stale checkpoints do no harm", {
    spec = "eventd *shutdown.event-batches-lost-to-an-aborted-shutdown-are-recovered-from-kmes-at-the-next-start"
        .. " eventd *shutdown.an-aborted-shutdown-loses-uncommitted-log-and-metric-batches"
        .. " eventd *shutdown.stale-sequence-metadata-after-an-aborted-shutdown-does-not-affect-recovery",
}, function(t)
    -- A clean stop first, so there are checkpoints for the abort to leave
    -- stale.
    eventd.stop(vm)
    eventd.start(vm)
    local pid = eventd.pid(vm)
    local idle = hold_open(t)
    local log = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    us.connect(vm, log, eventd.SOCKET.log)
    local asked = eventd.guest_ns(vm)
    begin_stop()
    wait_until(function() return vm:run("ls /run/eventd").stdout:find("sock", 1, true) == nil end,
        { timeout = 3, interval = 0.1, desc = "the shutdown to be under way" })
    -- The drains see `stopping` at once, do their one final cycle and
    -- finish; the held query handler keeps the shutdown from reaching its
    -- final commit. Events produced now stay in KMES. The log thread goes
    -- on reading through the held step 1 and commits what it reads at
    -- once, so eventd is frozen for the log record: it is still queued,
    -- uncommitted, when the kill comes.
    vm:run("sleep 2")
    eventd.freeze(vm, pid)
    local tag = eventd.marker("abort")
    for i = 1, 20 do eventd.emit(vm, "pt.abort", { tag = tag, i = i }) end
    us.sendmsg(vm, log, eventd.msgpack({ origin = tag, is_error = false, message = "never committed" }),
        { flags = us.MSG.DONTWAIT })
    -- The abort itself is the previous test's; here the SIGKILL that ends
    -- the held shutdown is sent directly. (Left to peinit, a stop that
    -- kills an eventd with unread events in its ring has ended with the
    -- service Abandoned as ProcessUnkillable even though the process was
    -- gone within seconds — reported, not under test here.)
    eventd.crash(vm, { pid = pid, timeout = STOP_TIMEOUT + 10 })
    vm:syscall(3, idle); vm:syscall(3, log)
    -- The checkpoints are now whatever the previous clean stop wrote.
    local cp = eventd.sql(vm, eventd.DB.meta, "SELECT max(updated_at) FROM sequence_checkpoints")[1][1]
    eventd.start(vm)
    eventd.unset(vm, "QueryTimeoutMs")
    local rows = eventd.wait_rows(vm, 'EVENTS pt.abort WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r >= 20 end)
    t:assert_eq(#rows, 20, "all twenty events were recovered from the ring, once each")
    local gaps = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")
    t:assert_eq(#gaps, 0, "and no gap was recorded: nothing was truly missed")
    t:assert(type(cp) == "number" and cp < asked,
        "the checkpoints recovery ran beside were stale, from before this run: " .. tostring(cp))
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. tag .. " SINCE 10m ago"), 0,
        "the uncommitted log record died with the aborted shutdown")
end)
