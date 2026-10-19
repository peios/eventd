-- eventd TRM §8.5 — crash recovery and signals: what an ungraceful end
-- leaves behind and what the next start makes of it, the four handled
-- signals, and the SIGQUIT diagnostic dump.
--
-- One VM. A crash is `kill -9`; "while eventd is down" is the time
-- between that and a test's own `svctl start`, because the seed sets
-- eventd's RestartPolicy to Never (and ErrorControl to Normal, so that
-- killing it repeatedly is not a Critical failure). The one test about
-- peinit restarting it unprompted switches the policy back on for its
-- own duration. SIGSTOP is the other tool: a frozen eventd has read
-- nothing, so what is in the ring or a receive queue when it is then
-- killed is exactly what the crash cost.
--
-- The dump goes to eventd's standard error, which peinit forwards to the
-- log store under origin `eventd` once an eventd is running again.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local client = require("helpers.peinit_client")
peinit.claim(1)

local vm = eventd.boot({
    name = "ev-crash",
    noncritical = true,
})

local function latest(event_type, since)
    local out
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 30m ago")) do
        if r["event.time"] >= since and (not out or r["event.time"] > out["event.time"]) then out = r end
    end
    return out
end

local function gaps_since(since)
    local out = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 30m ago")) do
        if r["event.time"] >= since then out[#out + 1] = r end
    end
    return out
end

test("events emitted while eventd is down wait in the ring and are ingested at the next start", {
    spec = "eventd *crash.events-emitted-while-eventd-is-down-accumulate-in-the-ring-buffers",
}, function(t)
    eventd.crash(vm)
    local tag = eventd.marker("down")
    for i = 1, 25 do eventd.emit(vm, "pt.down", { tag = tag, i = i }) end
    eventd.start(vm)
    local rows = eventd.wait_rows(vm, 'EVENTS pt.down WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r >= 25 end)
    t:assert_eq(#rows, 25, "all twenty-five were waiting and were read")
end)

test("committed rows survive a crash and the databases come back consistent", {
    spec = "eventd *crash.committed-transactions-survive-a-crash-and-the-in-flight-batch-rolls-back",
}, function(t)
    local tag = eventd.marker("commit")
    for i = 1, 10 do eventd.emit(vm, "pt.commit", { tag = tag, i = i }) end
    eventd.wait_rows(vm, 'EVENTS pt.commit WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 10 end)
    -- Events flowing as the kill lands, so a batch may be in flight,
    -- then the files checked as the crash left them.
    for i = 1, 30 do eventd.emit(vm, "pt.inflight", { i = i }) end
    eventd.crash(vm)
    for _, db in ipairs({ eventd.shards(vm)[1], eventd.DB.logs, eventd.DB.metrics, eventd.DB.meta }) do
        t:assert_eq(eventd.sql(vm, db, "PRAGMA integrity_check")[1][1], "ok", db .. " is consistent")
    end
    eventd.start(vm)
    local rows = eventd.rows(vm, 'EVENTS pt.commit WHERE tag == "' .. tag .. '" SINCE 10m ago')
    t:assert_eq(#rows, 10, "every committed event is still there, once")
end)

test("a restart re-ingests uncovered survivors and records a gap only for what neither source has", {
    spec = "eventd *crash.restart-re-ingests-uncovered-survivors-and-gaps-only-sequences-in-neither-source",
}, function(t)
    -- Part 1: survivors. Frozen, eventd reads none of these; killed, it
    -- never commits them. They are in the ring and in no receipt.
    local since = eventd.guest_ns(vm)
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    local tag = eventd.marker("surv")
    for i = 1, 15 do eventd.emit(vm, "pt.surv", { tag = tag, i = i }) end
    eventd.crash(vm)
    eventd.start(vm)
    local rows = eventd.wait_rows(vm, 'EVENTS pt.surv WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r >= 15 end)
    t:assert_eq(#rows, 15, "each uncovered survivor was ingested once")
    t:assert_eq(#gaps_since(since), 0, "and nothing was recorded lost")

    -- Part 2: a sequence in neither. While eventd is down, overrun the
    -- 4 MiB ring with ~60 KiB events, so its oldest entries are gone.
    since = eventd.guest_ns(vm)
    eventd.crash(vm)
    local big = eventd.bin(string.rep("z", 60000))
    local otag = eventd.marker("over")
    for i = 1, 120 do eventd.emit(vm, "pt.over", { tag = otag, i = i, b = big }) end
    eventd.start(vm)
    local gaps = eventd.wait_rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago", function(r)
        for _, g in ipairs(r) do if g["event.time"] >= since then return true end end
        return false
    end)
    local gap
    for _, g in ipairs(gaps) do if g["event.time"] >= since then gap = g end end
    t:assert(gap, "the overwritten sequences were recorded as a gap")
    local kept = eventd.rows(vm, 'EVENTS pt.over WHERE tag == "' .. otag .. '" SINCE 10m ago SELECT i, event.sequence')
    t:assert(#kept > 0 and #kept < 120, "some of the burst survived, not all: " .. #kept)
    local lowest = math.huge
    for _, r in ipairs(kept) do
        local seq = r["event.sequence"]
        t:assert(seq > gap["loss.sequence-last"], "no stored survivor falls inside the gap")
        lowest = math.min(lowest, seq)
    end
    t:assert_eq(lowest, gap["loss.sequence-last"] + 1, "the gap ends where the oldest survivor begins")
end)

test("logs and metrics waiting in the receive queues are lost in a crash", {
    spec = "eventd *crash.socket-buffered-logs-and-metrics-are-lost-in-a-crash",
}, function(t)
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    local tag = eventd.marker("sock")
    for i = 1, 5 do eventd.send_log(vm, { origin = tag, is_error = false, message = "q" .. i }) end
    for i = 1, 5 do eventd.send_metric(vm, { name = tag, type = "gauge", value = i }) end
    eventd.crash(vm)
    eventd.start(vm)
    vm:run("sleep 2")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. tag .. " SINCE 10m ago"), 0, "the queued logs are gone")
    t:assert_eq(#eventd.rows(vm, "METRIC " .. tag .. " SINCE 10m ago"), 0, "and the queued metrics")
end)

test("after a crash peinit restarts eventd and it carries on with nothing done by hand", {
    spec = "eventd *crash.no-manual-recovery-is-needed-or-offered"
        .. " eventd *crash.a-restart-is-recognised-from-committed-rows-or-receipts-not-an-advance-flag",
}, function(t)
    vm:run("reg set '" .. eventd.SERVICE .. "' RestartPolicy dword:1"):assert_ok()
    vm:run("svctl reload-config"):assert_ok()
    local since = eventd.guest_ns(vm)
    local old = eventd.crash(vm)
    local new
    local ok = pcall(wait_until, function()
        new = eventd.pid(vm)
        return new ~= nil and new ~= old
    end, { timeout = 30, interval = 0.25, desc = "peinit to restart eventd" })
    vm:run("reg set '" .. eventd.SERVICE .. "' RestartPolicy dword:0"):assert_ok()
    vm:run("svctl reload-config")
    t:assert(ok, "a new eventd is running without anyone starting it")
    eventd.ready(vm)
    local s = latest(eventd.T.startup, since)
    t:assert(s, "it wrote a startup record")
    -- The crash wrote nothing on its way out; the start knew it was a
    -- restart from what the store already held for this boot.
    t:assert_eq(s and s["store.restarted"], true, "and recognised itself as a restart within the boot")
    t:assert_eq(latest(eventd.T.shutdown, since), nil, "though no shutdown record announced it")
end)

local function signal_stops_gracefully(t, sig)
    local since = eventd.guest_ns(vm)
    local pid = eventd.pid(vm)
    eventd.signal(vm, pid, sig)
    assert(eventd.wait_gone(vm, pid, 30), "eventd to stop")
    local status = eventd.status(vm)
    eventd.start(vm)
    t:assert_eq(status.cause, "clean_exit", "SIG" .. sig .. ": eventd exited cleanly: " .. json.encode(status))
    t:assert(latest(eventd.T.shutdown, since), "SIG" .. sig .. ": having written its shutdown record")
end

test("SIGTERM begins a graceful shutdown", {
    spec = "eventd *crash.sigterm-begins-graceful-shutdown",
}, function(t) signal_stops_gracefully(t, "TERM") end)

test("SIGINT begins a graceful shutdown", {
    spec = "eventd *crash.sigint-begins-graceful-shutdown",
}, function(t) signal_stops_gracefully(t, "INT") end)

-- Shared by the dump tests: the dump's lines, keyed by their label.
local dump = {}

local function need_dump(t)
    t:assert(dump.captured, "dump never captured: the SIGQUIT test above failed to read it")
end

test("SIGQUIT writes a diagnostic dump to stderr, taken before the shutdown starts, then stops gracefully", {
    spec = "eventd *crash.sigquit-writes-a-diagnostic-dump-then-begins-graceful-shutdown"
        .. " eventd *crash.the-diagnostic-dump-is-written-to-stderr-before-shutdown-step-one",
}, function(t)
    -- The dump reaches the log store as eventd's stderr, forwarded by
    -- peinit while that same eventd is shutting down: a line peinit
    -- delivers after the log thread's final drain of its queue is lost
    -- when the socket closes (seen under host load; reported). So the
    -- whole procedure is tried up to three times until one dump arrives
    -- complete, and every try is a full, graceful SIGQUIT shutdown.
    local d = eventd.quit_dump(vm, { attempts = 3, prepare = function()
        -- Material for the dump's counters, which are per process and so
        -- are made again each try: a log record whose origin is outside
        -- the grammar, a metric datagram with no identity, one whose
        -- publish descriptor grants nobody anything, a type conflict, and
        -- a KMES event claiming one of the five types eventd writes itself
        -- (the event after it, once stored, shows the writer has been past
        -- it).
        local reserved = eventd.marker("rsv")
        eventd.emit(vm, eventd.T.startup, { tag = reserved })
        eventd.emit(vm, "pt.crash.after." .. reserved, { tag = reserved })
        eventd.wait_rows(vm, "EVENTS pt.crash.after." .. reserved .. " SINCE 10m ago",
            function(r) return #r == 1 end)
        dump.origin = "pt bad origin " .. eventd.marker()
        eventd.send_log(vm, { origin = dump.origin, is_error = false, message = "x" })
        eventd.send_metric(vm, { name = eventd.marker("noid"), type = "gauge", value = 1 }, { pass_token = false })
        local denied = eventd.marker("deny")
        local key = eventd.SECURITY .. [[\Metrics\]] .. denied
        vm:run("reg new '" .. key .. "' -p"):assert_ok()
        vm:run("reg set '" .. key .. "' '' hex:" .. client.descriptor_hex(token.SID.LOCAL_SYSTEM, {})):assert_ok()
        vm:run("sleep 1")
        eventd.send_metric(vm, { name = denied, type = "gauge", value = 1 })
        dump.conflict = eventd.marker("mm")
        eventd.send_metric(vm, { name = dump.conflict, type = "gauge", value = 1 })
        eventd.wait_rows(vm, "METRIC " .. dump.conflict .. " SINCE 10m ago", function(r) return #r == 1 end)
        eventd.send_metric(vm, { name = dump.conflict, type = "counter", value = 2 })
        vm:run("sleep 1")
        -- A streaming query open when the signal lands: step 1 ends it, so
        -- a dump taken after step 1 would count no streams.
        local stream = vm:run_async("/usr/bin/evctl",
            { args = { "--format", "jsonl", "EVENTS pt.never.emitted STREAM" } })
        vm:run("sleep 1")
        return function() pcall(function() stream:wait("5s") end) end
    end })
    t:assert_eq(d.status.cause, "clean_exit", "the shutdown after the dump was graceful")
    t:assert(latest(eventd.T.shutdown, d.since), "and wrote its shutdown record")
    t:assert(d.headers == 1, "one dump was written to stderr")
    t:assert(d.lines.last_write_errors, "a whole dump reached the log store: " .. table.concat(d.partial, "; "))
    for k, v in pairs(d.lines) do dump[k] = v end
    dump.captured = true
    t:assert(dump.queries and dump.queries:find("streaming=1", 1, true),
        "the dump saw the stream still open — it was taken before step 1: " .. tostring(dump.queries))
end)

test("the dump names the boot ID", {
    spec = "eventd *crash.the-dump-includes-the-current-boot-id",
}, function(t)
    need_dump(t)
    local id = "{" .. eventd.boot_id(vm) .. "}"
    t:assert_eq(dump.boot_id, id, "boot_id, in the braced canonical form")
end)

test("the dump counts active and readable historical shards", {
    spec = "eventd *crash.the-dump-includes-the-active-and-readable-historical-shard-counts",
}, function(t)
    need_dump(t)
    t:assert(dump.shards and dump.shards:match("active=1") and dump.shards:match("historical_readable=%d+"),
        "shards: " .. tostring(dump.shards))
end)

test("the dump gives each CPU's receipt coverage and highest covered sequence", {
    spec = "eventd *crash.the-dump-includes-per-cpu-receipt-coverage-and-highest-covered-sequence",
}, function(t)
    need_dump(t)
    local line = dump["cpu[0]"]
    t:assert(line and line:match("receipt_ranges=%d+") and line:match("highest_contiguous=%d+"),
        "cpu[0]: " .. tostring(line))
end)

test("the dump gives the non-streaming and streaming query counts", {
    spec = "eventd *crash.the-dump-includes-the-non-streaming-and-streaming-query-counts",
}, function(t)
    need_dump(t)
    t:assert(dump.queries and dump.queries:match("active=%d+") and dump.queries:match("streaming=%d+"),
        "queries: " .. tostring(dump.queries))
end)

test("the dump gives the series cache occupancy", {
    spec = "eventd *crash.the-dump-includes-the-series-cache-occupancy",
}, function(t)
    need_dump(t)
    t:assert(dump.metric_series_cache and dump.metric_series_cache:match("^%d+$")
        and tonumber(dump.metric_series_cache) >= 1,
        "metric_series_cache holds the series used: " .. tostring(dump.metric_series_cache))
end)

test("the dump counts KMES events discarded for claiming a type eventd writes itself", {
    spec = "eventd *crash.the-dump-includes-kmes-events-discarded-for-a-reserved-type",
}, function(t)
    need_dump(t)
    t:assert(dump.event_ingress and tonumber(dump.event_ingress:match("reserved_types=(%d+)") or "0") >= 1,
        "event_ingress: " .. tostring(dump.event_ingress))
end)

test("the dump counts invalid-origin log discards and names the latest such origin", {
    spec = "eventd *crash.the-dump-includes-invalid-origin-log-discards-and-the-latest-such-origin",
}, function(t)
    need_dump(t)
    t:assert(dump.log_ingress and tonumber(dump.log_ingress:match("rejected_origins=(%d+)") or "0") >= 1,
        "log_ingress: " .. tostring(dump.log_ingress))
    t:assert(dump.last_rejected_log_origin and dump.last_rejected_log_origin:find(dump.origin, 1, true),
        "last_rejected_log_origin: " .. tostring(dump.last_rejected_log_origin))
end)

test("the dump counts metric datagrams rejected for identity, truncation, policy and authorization", {
    spec = "eventd *crash.the-dump-includes-metric-datagrams-rejected-for-identity-truncation-policy-or-authorization",
}, function(t)
    need_dump(t)
    local m = dump.metric_ingress or ""
    t:assert(tonumber(m:match("missing_identity=(%d+)") or "0") >= 1, "missing identity counted: " .. m)
    t:assert(m:match("truncated=%d+"), "truncated reported: " .. m)
    t:assert(tonumber(m:match("unauthorized_records=(%d+)") or "0") >= 1, "a policy denial counted: " .. m)
    t:assert(m:match("authorization_errors=%d+"), "authorization errors reported: " .. m)
end)

test("the dump counts type mismatches and names the latest conflict", {
    spec = "eventd *crash.the-dump-includes-the-metric-type-mismatch-count-and-latest-conflict",
}, function(t)
    need_dump(t)
    local m = dump.metric_ingress or ""
    t:assert(tonumber(m:match("type_mismatches=(%d+)") or "0") >= 1, "a mismatch counted: " .. m)
    local c = dump.last_metric_type_mismatch or ""
    t:assert(c:find("name=" .. dump.conflict, 1, true) and c:find("expected=gauge", 1, true)
        and c:find("received=counter", 1, true), "latest conflict: " .. c)
end)

test("the dump reports the last write error of each store", {
    spec = "eventd *crash.the-dump-includes-the-last-write-error-per-store",
}, function(t)
    need_dump(t)
    local w = dump.last_write_errors or ""
    for _, store in ipairs({ "event=", "log=", "metric=" }) do
        t:assert(w:find(store, 1, true), store .. " reported: " .. w)
    end
end)

test("SIGPIPE is ignored, and every other signal keeps its default action", {
    spec = "eventd *crash.every-other-signal-keeps-its-default-behaviour"
        .. " eventd *crash.sigpipe-is-ignored",
}, function(t)
    -- SIGUSR1 and SIGPIPE both terminate a process by default; eventd
    -- ignores SIGPIPE and leaves SIGUSR1 alone.
    local pid = eventd.pid(vm)
    eventd.signal(vm, pid, "PIPE")
    local piped = eventd.wait_gone(vm, pid, 5)
    if piped then eventd.start(vm) end
    t:assert(not piped, "SIGPIPE did not end eventd: it is ignored")
    t:assert_eq(eventd.pid(vm), pid, "the same process is still running")

    pid = eventd.pid(vm)
    eventd.signal(vm, pid, "USR1")
    local died = eventd.wait_gone(vm, pid, 5)
    if died then eventd.start(vm) end
    t:assert(died, "SIGUSR1 ended eventd, as its default action does")
end)
