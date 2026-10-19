-- eventd TRM §9.3 — losing dependencies after startup: the registry, KACS,
-- KMES and peinit.
--
-- The registry is the one that can really be taken away. One VM, and one
-- outage in the middle of the file: before it, a test leaves eventd with
-- a configuration value of its own and an event type it has queried
-- (so its descriptor is cached) beside one it never has; then
-- `svctl stop registryd` (an explicit stop, so not a failure of anything
-- Critical), the tests that read eventd's behaviour without a registry,
-- `svctl start registryd`, and the tests about its return. SIGHUP is
-- here too: during the outage it is the only way to make eventd try to
-- re-read, and its failure to is what shows the attempt was made.
--
-- KACS is part of the kernel and cannot be taken away from a running
-- system; KMES's only post-startup change is a ring resize, which is
-- made through the registry; peinit cannot be removed while the machine
-- runs. Each is homed with what can and cannot be shown.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-lostdeps" })

local function visible(etype)
    local r = eventd.query(vm, "EVENTS " .. etype .. " SINCE 30m ago")
    return r.ok and #r.rows or 0, r
end

local function wait_change(key, value, since)
    local _, ok = eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 30m ago", function(rows)
        for _, r in ipairs(rows) do
            if r["event.time"] >= since and r["config.name"] == key
                and r["config.value"] == tonumber(value) then
                return true
            end
        end
        return false
    end, { desc = key .. "=" .. value .. " to apply" })
    return ok
end

local state = {}

--- The second of two streams, while one is open: refused under
--- MaxStreamingQueries=1, admitted under the default.
local function second_stream_refused()
    local first = vm:run_async("/usr/bin/evctl", { args = { "--format", "jsonl", "EVENTS pt.never.a STREAM" } })
    vm:run("sleep 1")
    local r = vm:run("timeout 3 evctl --format jsonl 'EVENTS pt.never.b STREAM'")
    first:kill("kill")
    pcall(function() first:wait("5s") end)
    return (r.stderr or ""):find("too many concurrent streaming queries", 1, true) ~= nil
end

test("before the outage: a configured limit in force, one descriptor cached and one not", {}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "MaxStreamingQueries", "dword:1"):assert_ok()
    t:assert(wait_change("MaxStreamingQueries", "1", since), "the limit applied")
    t:assert(second_stream_refused(), "and holds")
    state.cached = "ptc" .. eventd.marker()
    state.fresh = "ptf" .. eventd.marker()
    eventd.emit(vm, state.cached, { n = 1 })
    eventd.emit(vm, state.fresh, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. state.cached .. " SINCE 10m ago", function(r) return #r == 1 end)
    state.pid = eventd.pid(vm)
    vm:run("svctl stop registryd")
    t:assert(vm:run("reg get '" .. eventd.KEY .. "'").exit_code ~= 0, "the registry is gone")
end)

test("without the registry eventd keeps its last configuration, even when told to re-read", {
    spec = "eventd *lostdeps.without-the-registry-the-last-known-configuration-is-kept",
}, function(t)
    -- SIGHUP asks for a re-read now, during the outage; that the attempt
    -- was made is read back from stderr once the registry has returned
    -- (peinit's delivery of the line waits on it), in a test further down.
    state.hup = eventd.guest_ns(vm)
    eventd.signal(vm, state.pid, "HUP")
    vm:run("sleep 2")
    t:assert_eq(eventd.pid(vm), state.pid, "SIGHUP did not end it")
    t:assert(second_stream_refused(), "the configured limit, not the default, is still in force")
end)

test("without the registry a cached descriptor still answers", {
    spec = "eventd *lostdeps.without-the-registry-descriptor-lookups-fall-back-to-the-cache",
}, function(t)
    local n, r = visible(state.cached)
    t:assert_eq(n, 1, "the type queried before the outage is still readable: " .. tostring(r.stderr))
end)

test("without the registry an uncached descriptor is denied", {
    spec = "eventd *lostdeps.without-the-registry-an-uncached-descriptor-pattern-is-denied",
}, function(t)
    local n, r = visible(state.fresh)
    t:assert_eq(n, 0, "the type never queried before is not shown: " .. tostring(r.stdout))
end)

test("without the registry eventd keeps ingesting and answering, and does not exit", {
    spec = "eventd *lostdeps.without-the-registry-ingestion-and-queries-on-cached-descriptors-continue-indefinitely"
        .. " eventd *lostdeps.registry-loss-does-not-make-eventd-exit-or-stop-collecting",
}, function(t)
    eventd.emit(vm, state.cached, { n = 2 })
    local origin = eventd.marker("noreg")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "during" })
    vm:run("sleep 20")
    local _, ok = eventd.wait_rows(vm, "EVENTS " .. state.cached .. " SINCE 10m ago", function(r) return #r == 2 end)
    t:assert(ok, "an event emitted during the outage was stored and is readable")
    t:assert_eq(eventd.pid(vm), state.pid, "the same eventd, twenty seconds into the outage")
    vm:run("svctl start registryd")
    wait_until(function() return vm:run("reg get '" .. eventd.KEY .. "'").exit_code == 0 end,
        { timeout = 30, interval = 0.5, desc = "the registry to return" })
    local _, logged = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(r) return #r == 1 end)
    t:assert(logged, "and the log record sent during the outage was stored too")
end)

test("when the registry returns eventd reads its configuration again", {
    spec = "eventd *lostdeps.when-the-registry-returns-eventd-re-reads-its-configuration",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.unset(vm, "MaxStreamingQueries"):assert_ok()
    local _, applied = eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 30m ago", function(rows)
        for _, r in ipairs(rows) do
            -- A removal leaves config.type and config.value out entirely.
            if r["event.time"] >= since and r["config.name"] == "MaxStreamingQueries"
                and r["config.type"] == nil and r["config.value"] == nil then
                return true
            end
        end
        return false
    end, { desc = "the removal to apply" })
    t:assert(applied, "a change made after the return is applied by the watch")
    local n = 0
    pcall(wait_until, function() n = visible(state.fresh); return n == 1 end, { timeout = 10, interval = 0.5 })
    t:assert_eq(n, 1, "and the descriptor that could not be resolved during the outage now resolves")
    t:assert_eq(eventd.pid(vm), state.pid, "all without a restart")
end)

test("SIGHUP made eventd re-read its configuration from the registry", {
    spec = "eventd *runtime.sighup-re-reads-configuration-like-a-watch-notification"
        .. " eventd *crash.sighup-re-reads-configuration-from-the-registry",
}, function(t)
    -- The watch had nothing to deliver during the outage — the case
    -- SIGHUP exists for. Its re-read failed (no registry) and said so;
    -- the failure is the evidence that the read was attempted on the
    -- signal and not on any notification.
    t:assert(state.hup, "the SIGHUP sent during the outage")
    local _, tried = eventd.wait_rows(vm, 'LOGS FROM eventd CONTAINING "configuration reload ignored" SINCE 30m ago',
        function(rows)
            for _, r in ipairs(rows) do if r.timestamp >= state.hup then return true end end
            return false
        end, { desc = "eventd's SIGHUP re-read to be on record" })
    t:assert(tried, "on SIGHUP eventd went to the registry for its configuration")
end)

-- Route closed: the security watch is an LCS watch held by eventd; with
-- the registry reachable nothing outside the process can break it (a
-- registryd stop, above, does not — eventd logs no watch degradation).
test("a failed security watch discards the descriptor cache and fails closed", {
    spec = "eventd *lostdeps.a-failed-watch-discards-the-descriptor-cache-and-fails-closed-until-re-established",
    skip = true,
    covered_by = "cargo:eventd eventd query::security::tests::default_descriptors_are_valid_and_cache_generation_advances",
}, function() end)

test("a KMES ring resize is taken as a new generation, not a failure", {
    spec = "eventd *lostdeps.a-ring-buffer-resize-is-a-generation-change-not-a-failure",
}, function(t)
    local key = [[Machine\System\KMES]]
    local before = vm:run("reg get '" .. key .. "'").stdout
    vm:run("reg new '" .. key .. "' -p")
    local set = vm:run("reg set '" .. key .. "' BufferCapacity qword:8388608")
    t:assert_eq(set.exit_code, 0, "the ring capacity was changed: " .. set.stderr)
    vm:run("sleep 2")
    local tag = eventd.marker("resize")
    for i = 1, 20 do eventd.emit(vm, "pt.resize", { tag = tag, i = i }) end
    local _, ok = eventd.wait_rows(vm, 'EVENTS pt.resize WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 20 end)
    t:assert(ok, "events emitted into the resized ring are ingested")
    t:assert_eq(eventd.pid(vm), state.pid, "by the same eventd")
    local cap = before:match("BufferCapacity = REG_QWORD (%d+)")
    if cap then
        vm:run("reg set '" .. key .. "' BufferCapacity qword:" .. cap)
    else
        vm:run("reg del '" .. key .. "' BufferCapacity")
    end
end)

-- Route closed: KACS is the kernel's access-control subsystem. It cannot
-- be unloaded or made to fail on a running system, so "after KACS goes
-- away" is not a state any VM can be put in.
test("without KACS new query connections are denied", {
    spec = "eventd *lostdeps.without-kacs-new-query-connections-are-denied",
    skip = true,
    covered_by = "cargo:eventd eventd query::security::tests::a_failed_peer_token_read_ends_the_connection_without_evaluating_its_query",
}, function() end)

test("without KACS a query needing a fresh access check is denied", {
    spec = "eventd *lostdeps.without-kacs-a-query-needing-a-fresh-access-check-is-denied",
    skip = true,
    covered_by = "cargo:eventd eventd query::executor::tests::a_query_whose_fresh_access_check_fails_is_denied_not_allowed",
}, function() end)

test("cached access-check results last for the query that obtained them", {
    spec = "eventd *lostdeps.cached-access-check-results-stay-valid-for-the-query-that-obtained-them",
    skip = true,
    covered_by = "cargo:eventd eventd query::executor::tests::a_verdict_is_reused_for_the_rest_of_its_query_without_a_second_check",
}, function() end)

test("without KACS event ingestion is unaffected", {
    spec = "eventd *lostdeps.without-kacs-event-ingestion-is-unaffected",
    skip = true,
    covered_by = "cargo:eventd eventd retention::tests::the_write_and_retention_paths_run_without_kacs",
}, function() end)

test("without KACS log ingestion is unaffected, and a metric needing a fresh check is refused", {
    spec = "eventd *lostdeps.without-kacs-log-ingestion-is-unaffected"
        .. " eventd *lostdeps.without-kacs-a-metric-record-needing-a-fresh-publish-check-is-refused",
    skip = true,
    covered_by = "cargo:eventd eventd log_ingest::tests::without_kacs_log_ingestion_commits_and_a_failed_metric_check_stops_nothing",
}, function() end)

test("query service resumes when KACS returns", {
    spec = "eventd *lostdeps.query-service-resumes-when-kacs-returns",
    skip = true,
    covered_by = "cargo:eventd eventd query::security::tests::a_connection_after_a_failed_peer_token_read_is_served_once_the_read_succeeds",
}, function() end)

-- Not runtime behaviour: a statement that eventd uses no runtime
-- interface of peinit's. peinit is PID 1 and cannot be taken away while
-- the machine runs, so the absence has no observable consequence.
test("peinit supplies no runtime service to eventd", {
    spec = "eventd *lostdeps.peinit-supplies-no-runtime-service-to-eventd",
    skip = true,
}, function() end)
