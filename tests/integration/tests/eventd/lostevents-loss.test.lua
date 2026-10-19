-- eventd TRM §9.1 — losing events: ring overrun (the one unrecoverable
-- loss), query timeouts, and datagrams dropped at a full receive queue.
--
-- One VM. Overrun is produced live: eventd is frozen with SIGSTOP, so its
-- drain stops reading, and a burst of ~60 KiB events laps the 4 MiB ring;
-- on SIGCONT the drain finds its position overwritten. The same freeze
-- makes the other two cases deterministic: a query whose deadline passes
-- while eventd is frozen mid-execution, and a log socket whose receive
-- queue fills because nobody is reading it.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local us = require("helpers.unixsock")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-lostevents" })

local function set_and_wait(name, value)
    eventd.set(vm, name, "dword:" .. value):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago", function(rows)
        for _, r in ipairs(rows) do
            if r["config.name"] == name and r["config.value"] == value then return true end
        end
        return false
    end, { desc = name .. " to apply" })
end

-- Shared by the three overrun tests.
local overrun = {}

test("an overrun is seen as a sequence gap on the CPU that overran", {
    spec = "eventd *lostevents.an-overrun-is-detected-as-a-sequence-gap-on-the-affected-cpu",
}, function(t)
    overrun.since = eventd.guest_ns(vm)
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    overrun.tag = eventd.marker("lap")
    local big = eventd.bin(string.rep("y", 60000))
    for i = 1, 120 do eventd.emit(vm, "pt.lap", { tag = overrun.tag, i = i, b = big }) end
    eventd.thaw(vm, pid)
    local rows = eventd.wait_rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago", function(r)
        for _, g in ipairs(r) do if g["event.time"] >= overrun.since then return true end end
        return false
    end)
    for _, g in ipairs(rows) do if g["event.time"] >= overrun.since then overrun.gap = g end end
    t:assert(overrun.gap, "the overrun produced a gap record")
    t:assert_eq(overrun.gap and overrun.gap["event.cpu"], 0, "on CPU 0, the one that overran")
    t:assert_eq(eventd.pid(vm), pid, "and the same eventd carried on: no restart was involved")
end)

test("the gap record names the missing range", {
    spec = "eventd *lostevents.an-overrun-writes-an-eventd-events-lost-record-naming-the-missing-range",
}, function(t)
    local g = overrun.gap
    t:assert(g, "the gap from the previous test")
    t:assert_eq(g["event.type"], eventd.T.gap, "the record is an eventd.events.lost")
    local first, last = g["loss.sequence"], g["loss.sequence-last"]
    t:assert(first and last and last >= first,
        "a first and last sequence: " .. json.encode(g))
    t:assert_eq(g["loss.count"], last - first + 1, "and the count they span")
    -- None of the range is stored: those sequences are gone.
    local inside = eventd.rows(vm, "EVENTS WHERE event.cpu == 0 AND event.sequence >= " .. first
        .. " AND event.sequence <= " .. last .. " SINCE 10m ago")
    local real = 0
    for _, r in ipairs(inside) do if r["event.type"] ~= eventd.T.gap then real = real + 1 end end
    t:assert_eq(real, 0, "no event in the named range was stored")
end)

test("after an overrun draining resumes from the oldest survivor", {
    spec = "eventd *lostevents.after-an-overrun-draining-resumes-from-the-oldest-survivor-at-tail-pos",
}, function(t)
    local g = overrun.gap
    t:assert(g, "the gap from the first test")
    local kept = eventd.rows(vm, 'EVENTS pt.lap WHERE tag == "' .. overrun.tag .. '" SINCE 10m ago SELECT event.sequence, i')
    t:assert(#kept > 0, "some of the burst survived")
    local lowest, highest_i = math.huge, 0
    for _, r in ipairs(kept) do
        lowest = math.min(lowest, r["event.sequence"])
        highest_i = math.max(highest_i, r.i)
    end
    t:assert_eq(lowest, g["loss.sequence-last"] + 1, "the first stored after the gap is the next sequence")
    t:assert_eq(highest_i, 120, "and everything from there to the end of the burst was read")
    local tag = eventd.marker("after")
    eventd.emit(vm, "pt.after", { tag = tag })
    local _, ok = eventd.wait_rows(vm, 'EVENTS pt.after WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 1 end)
    t:assert(ok, "and draining continues normally afterwards")
end)

test("a query past QueryTimeoutMs is cancelled with an error, and its connections are released", {
    spec = "eventd *lostevents.a-query-exceeding-querytimeoutms-is-cancelled-with-an-error"
        .. " eventd *lostevents.a-timed-out-query-releases-its-read-only-connections",
}, function(t)
    -- Enough rows that a payload scan takes a few hundred milliseconds.
    local entries = {}
    for i = 1, 256 do
        entries[i] = { type = "pt.load", payload = eventd.msgpack({ n = i, s = string.rep("x", 200) }) }
    end
    local emitted = 0
    for _ = 1, 100 do
        local r = kmes.emit_batch(vm, entries)
        emitted = emitted + (r.emitted or 0)
        vm:run("sleep 0.03") -- under KMES's per-process emit rate
    end
    t:assert(emitted >= 20000, "the load was emitted: " .. emitted)
    wait_until(function()
        return eventd.sql(vm, eventd.shards(vm)[1],
            "SELECT count(*) FROM events WHERE event_type = 'pt.load'")[1][1] >= emitted
    end, { timeout = 60, interval = 1, desc = "the load to be stored" })
    set_and_wait("QueryTimeoutMs", 1000)
    local pid = eventd.pid(vm)
    --- How many descriptors eventd holds on a store database, and which.
    local function db_fds()
        local n, which = 0, {}
        for line in eventd.fd_listing(vm, pid):gmatch("[^\n]+") do
            if line:find("shard%-%d+%.db$") or line:find("logs%.db$") or line:find("metrics%.db$") then
                n = n + 1
                which[#which + 1] = line
            end
        end
        return n, table.concat(which, "; ")
    end
    -- The QueryTimeoutMs change also requested a retention pass, which
    -- measures each store through a read-only connection of its own. When
    -- one closes while the store's writer still holds its lock on the
    -- file, SQLite keeps the descriptor for reuse rather than closing it,
    -- so the pass can leave one more descriptor than it found. The
    -- baseline is taken once that has settled: the same for three
    -- seconds running.
    local baseline, baseline_which = db_fds()
    local steady = 0
    wait_until(function()
        vm:clock():sleep("1s")
        local n, which = db_fds()
        if which == baseline_which then steady = steady + 1 else steady = 0 end
        baseline, baseline_which = n, which
        return steady >= 3
    end, { timeout = 60, interval = 0.05, desc = "eventd's store descriptors to settle" })
    -- Start the scan, then freeze eventd across its one-second deadline.
    -- Whether the freeze lands inside execution depends on timing, so
    -- try a few offsets.
    local q = eventd.guest_tmp(vm, 'EVENTS pt.load WHERE s CONTAINS "never" SINCE 10m ago', "slow")
    local out
    for _, delay in ipairs({ "0.05", "0.1", "0.15", "0.2", "0.03", "0.25" }) do
        local p = vm:run_async("/usr/bin/evctl", { args = { "--format", "jsonl", "--file", q } })
        vm:clock():sleep(tonumber(delay))
        eventd.freeze(vm, pid)
        vm:clock():sleep(1.5)
        eventd.thaw(vm, pid)
        local r = p:wait("60s")
        out = (r.stdout or "") .. (r.stderr or "") .. "\nrc=" .. tostring(r.exit_code)
        if out:find("timed out", 1, true) then break end
    end
    t:assert(out:find("timed out", 1, true), "the query was cancelled with a timeout error: " .. out)
    t:assert(not out:find("rc=0", 1, true), "and the client was told it failed")
    -- The handler closes its connections as it unwinds; under load that
    -- can trail the client's error by a moment.
    local now, now_which = db_fds()
    pcall(wait_until, function() now, now_which = db_fds(); return now <= baseline end,
        { timeout = 20, interval = 0.5, desc = "the query's connections to close" })
    t:assert_eq(now, baseline, "the query's read-only connections were closed: before [" .. baseline_which
        .. "], after [" .. now_which .. "]")
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "and queries carry on")
    eventd.unset(vm, "QueryTimeoutMs")
end)

test("a streaming query's watch phase is not bound by the query timeout", {
    spec = "eventd *lostevents.the-watch-phase-of-a-streaming-query-is-not-time-limited",
}, function(t)
    set_and_wait("QueryTimeoutMs", 1000)
    local etype = "pt.watch" .. eventd.marker()
    local stream = vm:run_async("/usr/bin/evctl", { args = { "--format", "jsonl", "EVENTS " .. etype .. " STREAM" } })
    -- Three query timeouts later, the stream is still watching.
    vm:run("sleep 3")
    eventd.emit(vm, etype, { late = true })
    vm:run("sleep 2")
    stream:kill("kill")
    local r = stream:wait("5s")
    eventd.unset(vm, "QueryTimeoutMs")
    t:assert((r.stdout or ""):find('"late":true', 1, true),
        "the record emitted after three timeouts was delivered: " .. tostring(r.stdout) .. tostring(r.stderr))
    t:assert(not (r.stderr or ""):find("timed out", 1, true), "and the stream was never timed out")
end)

test("a datagram refused at a full receive queue is counted nowhere", {
    spec = "eventd *lostevents.a-datagram-dropped-on-a-full-receive-queue-is-not-counted",
}, function(t)
    local origin = eventd.marker("full")
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    local fd = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    local payload = eventd.msgpack({ origin = origin, is_error = false, message = string.rep("m", 60000) })
    local accepted, dropped = 0, 0
    for _ = 1, 400 do
        local r = us.sendmsg(vm, fd, payload, { to = eventd.SOCKET.log, flags = us.MSG.DONTWAIT })
        if r.ret and r.ret > 0 then accepted = accepted + 1 else dropped = dropped + 1 end
        if dropped >= 20 then break end
    end
    vm:syscall(3, fd)
    eventd.thaw(vm, pid)
    t:assert(dropped > 0 and accepted > 0, "the queue filled: " .. accepted .. " in, " .. dropped .. " refused")
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
        function(r) return #r >= accepted end)
    t:assert_eq(#rows, accepted, "exactly what the queue held was stored")
    -- eventd's own counters, after at least one health interval (15 s by
    -- default): nothing names a socket-level drop.
    vm:run("sleep 16")
    local names = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics,
        "SELECT DISTINCT name FROM series WHERE name LIKE 'eventd.%'")) do
        names[r[1]] = true
    end
    for name in pairs(names) do
        t:assert(not (name:find("drop") or name:find("discard") or name:find("overflow")),
            "no health metric counts dropped datagrams: " .. name)
    end
end)

test("the log and metric sockets are each drained by the thread that commits them", {
    spec = "eventd *lostevents.a-socket-queue-is-not-drained-while-its-batch-commits",
}, function(t)
    -- One thread per channel and no other: eventd-log receives and
    -- commits logs, eventd-metric metrics. There is no separate reader
    -- that could empty a queue while its writer is inside a commit.
    local names = eventd.thread_names(vm, eventd.pid(vm))
    local log, metric = 0, 0
    for _, n in ipairs(names) do
        if n:find("log", 1, true) then log = log + 1 end
        if n:find("metric", 1, true) then metric = metric + 1 end
    end
    t:assert_eq(log, 1, "one thread touches logs: " .. table.concat(names, " "))
    t:assert_eq(metric, 1, "one thread touches metrics")
end)
