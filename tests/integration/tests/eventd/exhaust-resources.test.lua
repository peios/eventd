-- eventd TRM §9.4 — resource exhaustion: memory (an OOM kill, and what
-- bounds each long-lived consumer), query slots, descriptors, and the two
-- bounded writer stalls.
--
-- One VM. eventd is made Normal by the seed (its restart policy is left
-- as the image's, OnFailure, because the OOM test is about peinit
-- bringing it back): a Critical eventd has oom_score_adj -1000 and the
-- OOM killer would never choose it. The OOM kill is the kernel's own,
-- requested through sysrq with eventd made the most attractive victim.
--
-- Query slots are held open with streaming queries (each holds a slot
-- for as long as it runs) and, for the per-user bound, idle connections
-- made by a worker as a minted non-SYSTEM user: a connection takes its
-- user's slot before its request is read.
--
-- Most of the memory table, the index-build cancellation and the
-- maintenance scheduling are internal to the writer threads; those are
-- homed on unit tests or left as TODOs at the bottom.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local client = require("helpers.peinit_client")
local us = require("helpers.unixsock")
peinit.claim(1)

local vm = eventd.boot({
    name = "ev-exhaust",
    noncritical = { restart = false },
})

local function set_and_wait(name, value)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, name, "dword:" .. value):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago", function(rows)
        for _, r in ipairs(rows) do
            if r["event.time"] >= since and r["config.name"] == name
                and r["config.value"] == tonumber(value) then
                return true
            end
        end
        return false
    end, { desc = name .. " to apply" })
end

local function stream(etype)
    local s = vm:run_async("/usr/bin/evctl", { args = { "--format", "jsonl", "EVENTS " .. etype .. " STREAM" } })
    vm:run("sleep 1")
    return s
end

local function close_stream(s)
    s:kill("kill")
    pcall(function() s:wait("5s") end)
end

test("an OOM kill is recovered exactly like a crash", {
    spec = "eventd *exhaust.an-oom-kill-is-recovered-exactly-like-a-crash",
}, function(t)
    local pid = eventd.pid(vm)
    local since = eventd.guest_ns(vm)
    -- Frozen, so these are in the ring and in no receipt when it dies.
    eventd.freeze(vm, pid)
    local tag = eventd.marker("oom")
    for i = 1, 10 do eventd.emit(vm, "pt.oom", { tag = tag, i = i }) end
    vm:write_file("/proc/" .. pid .. "/oom_score_adj", "1000\n")
    eventd.thaw(vm, pid); vm:run("echo f > /proc/sysrq-trigger"):assert_ok()
    local died = eventd.wait_gone(vm, pid, 15)
    t:assert(died, "the OOM killer took eventd")
    wait_until(function()
        local now = eventd.pid(vm)
        return now ~= nil and now ~= pid
    end, { timeout = 60, interval = 0.5, desc = "peinit to restart eventd" })
    eventd.ready(vm)
    local rows = eventd.wait_rows(vm, 'EVENTS pt.oom WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r >= 10 end)
    t:assert_eq(#rows, 10, "the events in the ring were recovered, once each")
    local starts = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 10m ago")
    local s
    for _, r in ipairs(starts) do if r["event.time"] >= since then s = r end end
    t:assert(s and s["store.restarted"] == true, "and the new start knew itself a restart")
    local gaps = 0
    for _, g in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")) do
        if g["event.time"] >= since then gaps = gaps + 1 end
    end
    t:assert_eq(gaps, 0, "with nothing recorded lost")
end)

test("the series cache never holds more than MetricSeriesCacheSize", {
    spec = "eventd *exhaust.series-cache-memory-is-bounded-by-metricseriescachesize",
}, function(t)
    set_and_wait("MetricSeriesCacheSize", 1000)
    local base = eventd.marker("card")
    for d = 0, 29 do
        local batch = {}
        for i = 1, 50 do batch[i] = { name = base .. "." .. (d * 50 + i), type = "gauge", value = i } end
        eventd.send_metric(vm, batch)
    end
    wait_until(function()
        return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name LIKE '"
            .. base .. ".%'")[1][1] >= 1500
    end, { timeout = 60, interval = 1, desc = "1500 distinct series to be stored" })
    local d = eventd.quit_dump(vm)
    local line
    for _, m in ipairs(d.messages) do if m:find("metric_series_cache:", 1, true) then line = m end end
    local held = tonumber(line and line:match("metric_series_cache:%s*(%d+)") or "")
    eventd.unset(vm, "MetricSeriesCacheSize")
    t:assert(held and held >= 1 and held <= 1000,
        "1500 series went through a cache of 1000, which held " .. tostring(held))
end)

test("a query beyond a concurrency limit is refused at once, not queued", {
    spec = "eventd *exhaust.a-query-beyond-the-concurrency-limits-is-rejected-not-queued",
}, function(t)
    set_and_wait("MaxStreamingQueries", 1)
    local first = stream("pt.never.one")
    local started = os.time()
    local r = vm:run("timeout 20 evctl --format jsonl 'EVENTS pt.never.two STREAM'")
    local took = os.time() - started
    close_stream(first)
    eventd.unset(vm, "MaxStreamingQueries")
    t:assert((r.stderr or ""):find("too many concurrent streaming queries", 1, true),
        "the second stream was refused: " .. tostring(r.stderr))
    t:assert(took <= 3, "straight away rather than after waiting for a slot: " .. took .. "s")
end)

test("with every query slot taken, ingestion carries on", {
    spec = "eventd *exhaust.query-slot-exhaustion-does-not-affect-ingestion",
}, function(t)
    local first = stream("pt.never.slot")
    -- (Applied when a query is refused: the config-change record cannot
    -- be read to confirm it, reading being a query.)
    eventd.set(vm, "MaxConcurrentQueries", "dword:1"):assert_ok()
    pcall(wait_until, function() return not eventd.query(vm, "EVENTS TAKE 1").ok end, { timeout = 10 })
    local refused = not eventd.query(vm, "EVENTS TAKE 1").ok
    local tag = eventd.marker("slot")
    for i = 1, 10 do eventd.emit(vm, "pt.slot", { tag = tag, i = i }) end
    eventd.send_log(vm, { origin = tag, is_error = false, message = "while full" })
    vm:run("sleep 2")
    close_stream(first)
    eventd.unset(vm, "MaxConcurrentQueries")
    t:assert(refused, "no query slot was free")
    local _, ev = eventd.wait_rows(vm, 'EVENTS pt.slot WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 10 end)
    t:assert(ev, "events emitted meanwhile were stored")
    local _, lg = eventd.wait_rows(vm, "LOGS FROM " .. tag .. " SINCE 10m ago", function(r) return #r == 1 end)
    t:assert(lg, "and the log record")
end)

test("one user cannot take every query slot; SYSTEM still can", {
    spec = "eventd *exhaust.one-user-cannot-occupy-every-query-slot",
}, function(t)
    set_and_wait("MaxQueriesPerUser", 1)
    local since = eventd.guest_ns(vm)
    local answer, detail = "", ""
    local system_ok
    client.with_worker(vm, function(w)
        local tok = assert(client.mint_admin(w, token.SID.TEST_USER))
        local held = assert(client.connect_as(w, eventd.SOCKET.query, us.SOCK.STREAM, tok))
        vm:run("sleep 1")
        local second = assert(client.connect_as(w, eventd.SOCKET.query, us.SOCK.STREAM, tok))
        -- The refusal is one frame, a four-byte length and its payload, and
        -- the stream may deliver the two separately: under load the first
        -- read can return the length alone. Read until the frame is whole.
        local buf, r = "", nil
        for _ = 1, 50 do
            r = us.recvmsg(w, second, 4096, { cmsg = 0 })
            if not r.ret or r.ret <= 0 then break end
            buf = buf .. (r.data or "")
            if #buf >= 4 and #buf >= 4 + string.unpack("<I4", buf) then break end
        end
        answer = buf
        detail = "read " .. #buf .. " bytes; last recvmsg ret " .. tostring(r and r.ret) ..
            " errno " .. tostring(r and r.errno)
        system_ok = eventd.query(vm, "EVENTS TAKE 1").ok
        us.sendmsg(w, held, "") -- `held` stays open until here
    end)
    eventd.unset(vm, "MaxQueriesPerUser")
    if not answer:find("too many concurrent queries from this user", 1, true) then
        for _, r in ipairs(eventd.rows(vm, 'LOGS FROM eventd CONTAINING "query" SINCE 10m ago')) do
            if r.timestamp >= since then detail = detail .. " | " .. r.message end
        end
    end
    t:assert(answer:find("too many concurrent queries from this user", 1, true),
        "the user's second connection was refused while its first held a slot: " .. detail)
    t:assert(system_ok, "while SYSTEM, which the per-user bound does not count, was still served")
end)

-- Route closed: what eventd does when it cannot allocate for an admitted
-- query (a descriptor, a thread, memory) needs a process limit lowered
-- under the running daemon; the image ships no prlimit, and eventd's
-- limits are peinit's to set at start.
test("a query whose resources cannot be allocated fails without blocking a writer", {
    spec = "eventd *exhaust.a-query-whose-resources-cannot-be-allocated-fails-without-blocking-a-writer",
    skip = true,
    covered_by = "cargo:eventd eventd query::executor::tests::a_query_whose_store_cannot_be_opened_fails_without_blocking_a_writer",
}, function() end)

test("event handoff channels are bounded by fixed slot and byte limits", {
    spec = "eventd *exhaust.handoff-channel-memory-is-bounded-by-startup-fixed-slot-and-byte-limits",
    skip = true,
    covered_by = "cargo:eventd eventd-core queue::tests::enforces_slot_and_byte_bounds_before_publish",
}, function() end)

-- Route closed: the channels' limits are constants inside the process
-- (HANDOFF_SLOTS, HANDOFF_BYTES) and nothing reports them.
test("the handoff limits do not follow a live MaxBatchSize change", {
    spec = "eventd *exhaust.handoff-limits-do-not-follow-live-maxbatchsize-changes",
    skip = true,
    covered_by = "cargo:eventd eventd pipeline::tests::a_live_max_batch_size_change_leaves_the_handoff_limits_unchanged",
}, function() end)

-- Not runtime behaviour: these rows of the memory table say which
-- setting, if any, bounds a consumer. There is no key to vary for the
-- intern sets, the index counters or the page caches, and their memory
-- is not reported anywhere a caller can read.
test("the intern sets have no bound of their own beyond catalogue cardinality", {
    spec = "eventd *exhaust.intern-set-memory-has-no-independent-bound-beyond-catalogue-cardinality",
    skip = true,
}, function() end)

test("adaptive index counters are bounded by the event query surface", {
    spec = "eventd *exhaust.adaptive-index-counter-memory-is-bounded-by-the-event-query-surface",
    skip = true,
}, function() end)

test("SQLite page caches are bounded per connection", {
    spec = "eventd *exhaust.sqlite-page-cache-memory-is-bounded-per-connection",
    skip = true,
}, function() end)

-- Route closed: an index build is cancelled from the drain threads
-- between SQLite progress callbacks; catching one mid-build needs a
-- shard large enough that CREATE INDEX outlasts a burst, and the
-- outcome (the index absent, writing resumed) is indistinguishable from
-- the build never having started.
test("rising write pressure cancels an index build and writing resumes", {
    spec = "eventd *exhaust.rising-write-pressure-cancels-an-index-build-and-event-writing-resumes",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::a_cancelled_index_build_leaves_no_index_and_the_writer_takes_the_next_batch",
}, function() end)

test("maintenance runs only as bounded low-priority commands at transaction boundaries", {
    spec = "eventd *exhaust.maintenance-runs-only-as-bounded-low-priority-commands-at-transaction-boundaries",
    skip = true,
    covered_by = "cargo:eventd eventd writer::tests::maintenance_runs_after_the_batch_before_it_commits_and_deletes_at_most_its_limit",
}, function() end)

test("ingestion wins before the next maintenance command", {
    spec = "eventd *exhaust.ingestion-takes-priority-over-the-next-maintenance-command",
    skip = true,
    covered_by = "cargo:eventd eventd writer::tests::events_handed_off_while_a_command_runs_commit_before_the_next_command",
}, function() end)
