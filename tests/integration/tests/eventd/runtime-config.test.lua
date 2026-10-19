-- eventd TRM §8.3 — configuration at runtime: what eventd's registry
-- watch applies at once, what it defers to a restart, what is not
-- configuration at all, and the `eventd.config.changed` record of each
-- applied change (`config.name`, and `config.value` / `config.type` and
-- their `-previous` pair as integers, TRM §3.2).
--
-- One VM, and every test drives the live system: `reg set` / `reg del`
-- under `Machine\System\eventd`, then reads what eventd did about it —
-- a config-change record, a deferral line on its stderr (in the log store
-- under origin `eventd`), or a behaviour that changed. Each test puts the
-- values it touched back, so the next one starts from the defaults. One
-- test restarts eventd to show a deferred socket path taking effect, and
-- restores it with a second restart.
--
-- SIGHUP (`*runtime.sighup-…`) needs a registry that is not delivering
-- notifications, so it lives with the registry outage in
-- lostdeps-registry.test.lua. Three statements about writer batch
-- thresholds describe state inside the writer threads that nothing
-- outside the process can read; they are homed at the bottom.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local client = require("helpers.peinit_client")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-runtime" })

--- Config-change records for `key` written at or after `since`.
local function changes(key, since)
    local out = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 30m ago")) do
        if r["config.name"] == key and r["event.time"] >= since then out[#out + 1] = r end
    end
    table.sort(out, function(a, b) return a["event.time"] < b["event.time"] end)
    return out
end

local function wait_change(key, since, pred, desc)
    local found
    pcall(wait_until, function()
        for _, r in ipairs(changes(key, since)) do
            if pred(r) then found = r; return true end
        end
        return false
    end, { timeout = 20, interval = 0.25, desc = desc or ("a config change to " .. key) })
    return found
end

test("a change is applied by the watch, without a restart", {
    spec = "eventd *runtime.eventd-watches-its-configuration-subtree-and-applies-changes-without-restarting",
}, function(t)
    local pid = eventd.pid(vm)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "MetricRetentionDays", "dword:17"):assert_ok()
    t:assert(wait_change("MetricRetentionDays", since, function(r) return r["config.value"] == 17 end),
        "the change was applied")
    t:assert_eq(eventd.pid(vm), pid, "by the same process")
    eventd.unset(vm, "MetricRetentionDays")
end)

test("every applied change is recorded with its key and its old and new values", {
    spec = "eventd *runtime.every-applied-change-emits-a-config-change-event-with-key-and-old-and-new-values",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "EventRetentionDays", "dword:41"):assert_ok()
    local first = wait_change("EventRetentionDays", since, function(r) return r["config.value"] == 41 end)
    t:assert(first, "a record for the first change")
    eventd.set(vm, "EventRetentionDays", "dword:42"):assert_ok()
    local second = wait_change("EventRetentionDays", since, function(r) return r["config.value"] == 42 end)
    t:assert(second, "and for the second")
    t:assert_eq(second and second["config.value-previous"], 41, "whose old value is the first's new one")
    t:assert_eq(second and second["config.type-previous"], 4, "with its registry type, REG_DWORD's number")
    t:assert_eq(second and second["config.type"], 4, "both sides")
    t:assert_eq(second and math.type(second["config.value"]), "integer", "the values are integers, not text")
    t:assert_eq(second and second["config.key.path"], [[Machine\System\eventd]], "under eventd's key")
    eventd.unset(vm, "EventRetentionDays")
end)

test("an invalid value is ignored and the value in use, not the default, is kept", {
    spec = "eventd *runtime.an-invalid-value-is-ignored-and-the-value-in-use-is-kept",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "LogRetentionDays", "dword:13"):assert_ok()
    t:assert(wait_change("LogRetentionDays", since, function(r) return r["config.value"] == 13 end),
        "13 applied")
    -- 1..3650 is the range; 99999 is outside it.
    eventd.set(vm, "LogRetentionDays", "dword:99999"):assert_ok()
    eventd.set(vm, "LogRetentionDays", "dword:14"):assert_ok()
    local last = wait_change("LogRetentionDays", since, function(r) return r["config.value"] == 14 end)
    t:assert(last, "14 applied")
    for _, r in ipairs(changes("LogRetentionDays", since)) do
        t:assert(r["config.value"] ~= 99999, "the out-of-range value was never applied")
    end
    t:assert_eq(last and last["config.value-previous"], 13,
        "and what 14 replaced was the retained 13, not the compiled-in default")
    eventd.unset(vm, "LogRetentionDays")
end)

test("an unknown key in the subtree is ignored", {
    spec = "eventd *runtime.unknown-keys-are-ignored",
}, function(t)
    local pid = eventd.pid(vm)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "PtNoSuchSetting", "dword:5"):assert_ok()
    -- A known change straight after, so "nothing for the unknown key" is
    -- read once eventd has demonstrably handled what came before it.
    eventd.set(vm, "MetricRetentionDays", "dword:18"):assert_ok()
    t:assert(wait_change("MetricRetentionDays", since, function(r) return r["config.value"] == 18 end),
        "the following known change was applied")
    t:assert_eq(#changes("PtNoSuchSetting", since), 0, "nothing was recorded for the unknown key")
    t:assert_eq(eventd.pid(vm), pid, "and eventd carried on")
    eventd.unset(vm, "PtNoSuchSetting")
    eventd.unset(vm, "MetricRetentionDays")
end)

test("a tuning parameter applies at once: MaxConcurrentQueries", {
    spec = "eventd *runtime.tuning-parameters-apply-immediately",
}, function(t)
    -- A streaming query holds a query slot for as long as it runs.
    local stream = vm:run_async("/usr/bin/evctl",
        { args = { "--format", "jsonl", "EVENTS pt.never.emitted STREAM" } })
    vm:run("sleep 1")
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "a second query is admitted under the default")
    eventd.set(vm, "MaxConcurrentQueries", "dword:1"):assert_ok()
    -- Nothing restarts; the next query after the watch has applied the
    -- change is held to it. (The config-change record cannot be read to
    -- confirm the apply: reading it is a query, and is refused.)
    local last
    local refused = pcall(wait_until, function()
        last = eventd.query(vm, "EVENTS TAKE 1")
        return not last.ok and (last.stderr or ""):find("too many concurrent queries", 1, true) ~= nil
    end, { timeout = 10, interval = 0.1, desc = "a query to be refused under the new limit" })
    t:assert(refused, "the running daemon refuses a second query under the new limit: "
        .. tostring(last and last.stderr))
    eventd.unset(vm, "MaxConcurrentQueries")
    eventd.wait_rows(vm, "EVENTS TAKE 1", function(rows) return #rows == 1 end,
        { desc = "queries to be admitted again under the default" })
    stream:kill("kill")
    pcall(function() stream:wait("5s") end)
end)

test("changes to the socket paths wait for a restart", {
    spec = "eventd *runtime.a-socket-path-change-waits-for-a-restart",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "MetricSocketPath", "sz:/run/eventd/pt-metric.sock"):assert_ok()
    t:assert(eventd.stderr_line(vm,"MetricSocketPath is deferred until restart", since),
        "eventd noticed the change and deferred it")
    t:assert(not vm:run("ls /run/eventd").stdout:find("pt-metric.sock", 1, true),
        "nothing was bound at the new path")
    local name = eventd.marker("sp")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    t:assert(#eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(r) return #r == 1 end) == 1,
        "the old socket still takes metrics")
    eventd.restart(vm)
    local listing = vm:run("ls /run/eventd").stdout
    t:assert(listing:find("pt-metric.sock", 1, true) and not listing:find("^metric.sock", 1),
        "after a restart the new path is the metric socket: " .. listing)
    eventd.set(vm, "MetricSocketPath", "sz:/run/eventd/metric.sock"):assert_ok()
    eventd.restart(vm)
end)

test("a change to a store path waits for a restart", {
    spec = "eventd *runtime.a-store-path-change-waits-for-a-restart",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/pt-elsewhere"):assert_ok()
    t:assert(eventd.stderr_line(vm,"LogStorePath is deferred until restart", since),
        "eventd noticed the change and deferred it")
    local origin = eventd.marker("store")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "still here" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(r) return #r == 1 end)
    local stored = eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'")
    t:assert_eq(stored[1][1], 1, "logs still go to the logs.db that is open")
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/logs/"):assert_ok()
end)

test("a StorageShards change waits for a restart", {
    spec = "eventd *runtime.a-storageshards-change-waits-for-a-restart",
}, function(t)
    local since = eventd.guest_ns(vm)
    local before = #eventd.shards(vm)
    eventd.set(vm, "StorageShards", "dword:4"):assert_ok()
    t:assert(eventd.stderr_line(vm,"StorageShards is deferred until restart", since),
        "eventd noticed the change and deferred it")
    for i = 1, 50 do eventd.emit(vm, "pt.shards", { i = i }) end
    eventd.wait_rows(vm, "EVENTS pt.shards SINCE 10m ago", function(r) return #r >= 50 end)
    t:assert_eq(#eventd.shards(vm), before, "no shard was added while eventd ran")
    t:assert_eq(#changes("StorageShards", since), 0, "and no change was recorded as applied")
    eventd.unset(vm, "StorageShards")
end)

test("restart-only changes are deferred, not migrated or recorded as applied", {
    spec = "eventd *runtime.restart-only-changes-are-deferred-not-migrated-live",
}, function(t)
    local pid = eventd.pid(vm)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "EventStorePath", "sz:/var/state/eventd/pt-moved"):assert_ok()
    eventd.set(vm, "QuerySocketPath", "sz:/run/eventd/pt-q.sock"):assert_ok()
    t:assert(eventd.stderr_line(vm,"EventStorePath is deferred until restart", since), "the store move is deferred")
    t:assert(eventd.stderr_line(vm,"QuerySocketPath is deferred until restart", since), "the socket move is deferred")
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "queries are still answered where they were")
    t:assert(vm:run("test -e /var/state/eventd/pt-moved").exit_code ~= 0, "nothing was migrated")
    t:assert_eq(#changes("EventStorePath", since) + #changes("QuerySocketPath", since), 0,
        "neither is recorded as an applied change")
    t:assert_eq(eventd.pid(vm), pid, "and eventd did not restart itself")
    eventd.set(vm, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    eventd.set(vm, "QuerySocketPath", "sz:/run/eventd/query.sock"):assert_ok()
end)

test("changes arriving while eventd starts are applied after it is ready", {
    spec = "eventd *runtime.notifications-arriving-during-startup-are-processed-after-readiness",
}, function(t)
    local since = eventd.guest_ns(vm)
    -- Restart without waiting, then change a value every 50 ms for two
    -- seconds: some changes land before the watch is armed (read as the
    -- starting configuration), some between arming and readiness, some
    -- after. Every one that is recorded as applied must be recorded after
    -- the startup record, and the last one must win.
    vm:run("svctl --no-wait restart eventd; i=100; while [ $i -lt 140 ]; do "
        .. "reg set '" .. eventd.KEY .. "' MetricRetentionDays dword:$i >/dev/null; "
        .. "i=$((i + 1)); sleep 0.05; done; "
        .. "reg set '" .. eventd.KEY .. "' MetricRetentionDays dword:140 >/dev/null")
    eventd.ready(vm)
    local start, stop
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 10m ago")) do
        if r["event.time"] >= since then start = r end
    end
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.shutdown .. " SINCE 10m ago")) do
        if r["event.time"] >= since then stop = r end
    end
    t:assert(start and stop, "the old process's shutdown record and the new one's startup record")
    local last = wait_change("MetricRetentionDays", since, function(r) return r["config.value"] == 140 end)
    t:assert(last, "the final value was applied")
    -- The old process, still running when the first changes landed, may
    -- apply some itself; those precede its shutdown record. What the new
    -- process applies must all follow its startup record: nothing falls
    -- between the two.
    for _, r in ipairs(changes("MetricRetentionDays", since)) do
        t:assert(r["event.time"] < stop["event.time"] or r["event.time"] > start["event.time"],
            "no change was applied by the new process before its startup record ("
            .. tostring(r["config.value"]) .. ")")
    end
    t:assert(last["event.time"] > start["event.time"], "and the last one came after readiness")
    eventd.unset(vm, "MetricRetentionDays")
end)

test("a descriptor change under Security takes effect on the next query", {
    spec = "eventd *runtime.a-security-descriptor-change-takes-effect-on-the-next-query",
}, function(t)
    local etype = eventd.marker("acl")
    local key = eventd.SECURITY .. [[\Events\]] .. etype
    eventd.emit(vm, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(r) return #r == 1 end)
    -- A descriptor for exactly this type that grants nobody anything.
    vm:run("reg new '" .. key .. "' -p"):assert_ok()
    vm:run("reg set '" .. key .. "' '' hex:"
        .. client.descriptor_hex(token.SID.LOCAL_SYSTEM, {})):assert_ok()
    local hidden = wait_until(function()
        return #eventd.rows(vm, "EVENTS " .. etype .. " SINCE 10m ago") == 0
    end, { timeout = 5, interval = 0.1, desc = "the revocation to apply" })
    t:assert(hidden, "the revocation applied to the next query, with nothing reloaded")
    -- And a grant to SYSTEM brings it back the same way.
    vm:run("reg set '" .. key .. "' '' hex:" .. client.descriptor_hex(token.SID.LOCAL_SYSTEM,
        { { sid = token.SID.LOCAL_SYSTEM, mask = 0x1 } })):assert_ok()
    local shown = wait_until(function()
        return #eventd.rows(vm, "EVENTS " .. etype .. " SINCE 10m ago") == 1
    end, { timeout = 5, interval = 0.1, desc = "the grant to apply" })
    t:assert(shown, "and the grant likewise")
    vm:run("reg del '" .. key .. "' ''")
end)

-- Route closed: the event writers' commit threshold, the handoff
-- channels between drains and writers, and the log and metric writers'
-- transaction thresholds are all state inside eventd's threads. A batch
-- boundary leaves no mark in the stores (receipt ranges are merged as
-- they commit), and the channels' capacity is not reported anywhere.
test("a live MaxBatchSize change becomes the next commit threshold", {
    spec = "eventd *runtime.a-live-maxbatchsize-change-atomically-updates-the-next-commit-threshold",
    skip = true,
    covered_by = "cargo:eventd eventd writer::tests::a_max_batch_size_change_between_batches_bounds_the_next_batch",
}, function() end)

test("a live MaxBatchSize change does not touch the handoff channels", {
    spec = "eventd *runtime.a-live-maxbatchsize-change-leaves-the-handoff-channels-untouched",
    skip = true,
    covered_by = "cargo:eventd eventd pipeline::tests::a_live_max_batch_size_change_leaves_the_handoff_limits_unchanged",
}, function() end)

test("log and metric batch changes affect only later transactions", {
    spec = "eventd *runtime.log-and-metric-batch-changes-affect-only-subsequent-transactions",
    skip = true,
    covered_by = "cargo:eventd eventd log_ingest::tests::a_log_batch_size_change_applies_only_to_later_transactions",
}, function() end)
