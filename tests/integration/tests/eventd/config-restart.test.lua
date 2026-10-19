-- eventd TRM Appendix A — Configuration Keys: the six required keys, the
-- keys that wait for a restart, and what eventd does with a configuration
-- it cannot use at startup.
--
-- One file-scope VM with two vCPUs, so that the default shard count — one
-- per attached KMES buffer — is 2 and cannot be mistaken for a fixed 1.
-- Three spare store directories are made beside the standard ones
-- (`/var/state/eventd/pt-*`, with the descriptor eventd insists a store
-- directory has), and the boot seeds one out-of-range tuning value, for
-- the startup half of "an invalid value is ignored".
--
-- The tests move eventd's stores and sockets live, show that nothing moves
-- until a restart, restart it, and read the result. They depend on running
-- in order, and the last one puts the standard configuration back.
--
-- A configuration that makes eventd fail startup trips peinit's Critical
-- policy and reboots the machine, so those cases each boot a VM of their
-- own, watch eventd fail through `svctl` while the agent is still up, and
-- let the VM go.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(2, { cpus = 2 }) -- the file-scope VM, plus one failing boot at a time

local CC = eventd.T.config_change

-- The descriptor eventd requires of a store directory (directory.rs:12),
-- which eventd-config.reg's ProvisionedPaths give the standard three.
local STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)" ..
    "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

local ALT = {
    events = "/var/state/eventd/pt-events",
    logs = "/var/state/eventd/pt-logs",
    metrics = "/var/state/eventd/pt-metrics",
    query = "/run/eventd/pt-query.sock",
    log = "/run/eventd/pt-log.sock",
    metric = "/run/eventd/pt-metric.sock",
}

local vm = eventd.boot({
    name = "ev-restart",
    cpus = 2, -- the default shard count is the attached buffer count, one per CPU
    config = {
        -- Out of range (100-100000): ignored at startup.
        { name = "MaxBatchSize", type = "dword", data = 5 },
    },
})

-- The spare store directories, made as path provisioning makes the
-- standard ones.
for _, d in ipairs({ ALT.events, ALT.logs, ALT.metrics }) do
    vm:run("mkdir -p '" .. d .. "'"):assert_ok()
    vm:run("sd set '" .. d .. "' '" .. STORE_SDDL .. "'"):assert_ok()
end

local function q(s) return '"' .. s .. '"' end

local barrier_seq = 0
local function barrier()
    barrier_seq = barrier_seq + 1
    local v = 200000 + barrier_seq
    eventd.set(vm, "CrossTypeMaxLookbackSeconds", "dword:" .. v):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "CrossTypeMaxLookbackSeconds" AND config.value == ' ..
        v .. " SINCE 1h ago", function(rs) return #rs >= 1 end)
end

local function changes(key, socket)
    return eventd.query(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) .. " SINCE 1h ago",
        { socket = socket }).rows
end

local function exists(path)
    return vm:run("test -e '" .. path .. "'").exit_code == 0
end

local function names_in(dir)
    local r = vm:run("ls -1 '" .. dir .. "'")
    local out = {}
    for n in r.stdout:gmatch("[^\n]+") do out[n] = true end
    return out
end

local MOVED = {
    { "EventStorePath", "sz:" .. ALT.events .. "/" },
    { "LogStorePath", "sz:" .. ALT.logs .. "/" },
    { "MetricStorePath", "sz:" .. ALT.metrics .. "/" },
    { "QuerySocketPath", "sz:" .. ALT.query },
    { "LogSocketPath", "sz:" .. ALT.log },
    { "MetricSocketPath", "sz:" .. ALT.metric },
    { "StorageShards", "dword:3" },
}

-- ---------------------------------------------------------------------------
-- The keys that wait for a restart
-- ---------------------------------------------------------------------------

test("store paths, socket paths and StorageShards are deferred until a restart, then applied", {
    spec = "eventd *config.a-socket-path-change-waits-for-a-restart"
        .. " eventd *config.a-store-path-change-waits-for-a-restart"
        .. " eventd *config.a-storage-shards-change-waits-for-a-restart",
}, function(t)
    local before = {}
    for _, kv in ipairs(MOVED) do before[kv[1]] = #changes(kv[1]) end
    local pid = eventd.pid(vm)
    for _, kv in ipairs(MOVED) do eventd.set(vm, kv[1], kv[2]):assert_ok() end
    barrier()

    -- Nothing is applied: no change is recorded, and eventd says why.
    for _, kv in ipairs(MOVED) do
        t:assert_eq(#changes(kv[1]), before[kv[1]], kv[1] .. " is not recorded as applied")
        local said = eventd.wait_rows(vm, "LOGS FROM eventd CONTAINING " ..
            q("change to " .. kv[1] .. " is deferred until restart") .. " SINCE 10m ago",
            function(rs) return #rs >= 1 end, { timeout = 10 })
        t:assert(#said >= 1, "eventd reports " .. kv[1] .. " deferred until restart")
    end
    t:assert_eq(eventd.pid(vm), pid, "eventd did not restart itself")
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "it still answers on the old query socket")
    for _, s in ipairs({ ALT.query, ALT.log, ALT.metric }) do
        t:assert(not exists(s), "no socket at " .. s .. " yet")
    end
    for _, d in ipairs({ ALT.events, ALT.logs, ALT.metrics }) do
        t:assert(next(names_in(d)) == nil, d .. " is still empty")
    end
    t:assert_eq(#eventd.shards(vm), 2, "and the event store still has its two shards")

    -- After a restart, all of it is in force.
    eventd.restart(vm, { socket = ALT.query })
    for _, s in ipairs({ ALT.query, ALT.log, ALT.metric }) do
        t:assert(exists(s), "a socket at " .. s .. " after the restart")
    end
    local startup = eventd.wait_rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1m ago",
        function(rs) return #rs >= 1 end, { socket = ALT.query })
    t:assert_eq(startup[1]["store.shard-count"], 3, "the restarted eventd runs three shards: " .. json.encode(startup[1]))
    local files = names_in(ALT.events)
    t:assert(files["shard-0000.db"] and files["shard-0001.db"] and files["shard-0002.db"],
        "three shards in the new event store: " .. json.encode(files))
end)

-- ---------------------------------------------------------------------------
-- The required keys, by what eventd does with each once it is in force
-- ---------------------------------------------------------------------------

test("EventStorePath is the directory holding the shards and eventd-meta.db", {
    spec = "eventd *config.event-store-path-is-the-directory-for-event-shards-and-eventd-meta-db",
}, function(t)
    local files = names_in(ALT.events)
    t:assert(files["eventd-meta.db"], "eventd-meta.db is in EventStorePath: " .. json.encode(files))
    local etype = "pt.esp" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { socket = ALT.query })
    local found = 0
    for name in pairs(names_in(ALT.events)) do
        if name:match("^shard%-%d+%.db$") then
            local n = eventd.sql(vm, ALT.events .. "/" .. name,
                "SELECT count(*) FROM events WHERE event_type = '" .. etype .. "'")[1][1]
            found = found + n
        end
    end
    t:assert_eq(found, 1, "the new event is in a shard there")
end)

test("LogStorePath is the directory holding logs.db", {
    spec = "eventd *config.log-store-path-is-the-directory-for-logs-db",
}, function(t)
    local origin = eventd.marker("lsp")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "here" }, { path = ALT.log })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { socket = ALT.query })
    local n = eventd.sql(vm, ALT.logs .. "/logs.db",
        "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'")[1][1]
    t:assert_eq(n, 1, "the record is in LogStorePath/logs.db")
end)

test("MetricStorePath is the directory holding metrics.db", {
    spec = "eventd *config.metric-store-path-is-the-directory-for-metrics-db",
}, function(t)
    local name = eventd.marker("msp")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 }, { path = ALT.metric })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { socket = ALT.query })
    local n = eventd.sql(vm, ALT.metrics .. "/metrics.db",
        "SELECT count(*) FROM series WHERE name = '" .. name .. "'")[1][1]
    t:assert_eq(n, 1, "the series is in MetricStorePath/metrics.db")
end)

test("QuerySocketPath is where eventd answers queries", {
    spec = "eventd *config.query-socket-path-is-the-query-socket",
}, function(t)
    t:assert(eventd.query(vm, "EVENTS TAKE 1", { socket = ALT.query }).ok, "a query on QuerySocketPath is answered")
    t:assert(not eventd.query(vm, "EVENTS TAKE 1").ok, "and nothing answers on the standard path")
end)

test("LogSocketPath is where eventd takes log datagrams", {
    spec = "eventd *config.log-socket-path-is-the-log-ingestion-socket",
}, function(t)
    local origin = eventd.marker("lsock")
    local r = eventd.send_log(vm, { origin = origin, is_error = false, message = "x" }, { path = ALT.log })
    t:assert(r.ret and r.ret > 0, "the datagram was accepted at LogSocketPath")
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { socket = ALT.query })
    local old = eventd.send_log(vm, { origin = origin, is_error = false, message = "x" },
        { allow_error = true })
    t:assert(not old.ret or old.ret < 0, "and nothing takes one at the standard path")
end)

test("MetricSocketPath is where eventd takes metric datagrams", {
    spec = "eventd *config.metric-socket-path-is-the-metric-ingestion-socket",
}, function(t)
    local name = eventd.marker("msock")
    local r = eventd.send_metric(vm, { name = name, type = "gauge", value = 2 }, { path = ALT.metric })
    t:assert(r.ret and r.ret > 0, "the datagram was accepted at MetricSocketPath")
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end,
        { socket = ALT.query })
    local old = eventd.send_metric(vm, { name = name, type = "gauge", value = 2 },
        { allow_error = true })
    t:assert(not old.ret or old.ret < 0, "and nothing takes one at the standard path")
end)

test("(restore) the standard stores and sockets, after one more restart", {}, function(t)
    for _, kv in ipairs(MOVED) do eventd.unset(vm, kv[1]) end
    eventd.set(vm, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/logs/"):assert_ok()
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/metrics/"):assert_ok()
    eventd.set(vm, "QuerySocketPath", "sz:/run/eventd/query.sock"):assert_ok()
    eventd.set(vm, "LogSocketPath", "sz:/run/eventd/log.sock"):assert_ok()
    eventd.set(vm, "MetricSocketPath", "sz:/run/eventd/metric.sock"):assert_ok()
    eventd.restart(vm)
    t:assert(eventd.query(vm, "EVENTS TAKE 1").ok, "back on the standard query socket")
end)

-- ---------------------------------------------------------------------------
-- StorageShards' default
-- ---------------------------------------------------------------------------

test("StorageShards defaults to 0, which is one shard per attached KMES buffer", {
    spec = "eventd *config.storage-shards-defaults-to-0-meaning-the-attached-kmes-buffer-count",
}, function(t)
    local function started()
        local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1")
        return rows[1]["store.shard-count"]
    end
    local fds = eventd.fd_listing(vm, eventd.pid(vm))
    local buffers = select(2, fds:gsub("anon_inode:kmes%-cpu", ""))
    t:assert_eq(buffers, 2, "eventd attached two KMES buffers on two CPUs")
    t:assert_eq(started(), 2, "with StorageShards absent it runs two shards")
    eventd.set(vm, "StorageShards", "dword:0"):assert_ok()
    eventd.restart(vm)
    t:assert_eq(started(), 2, "an explicit 0 is the same: two")
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    eventd.restart(vm)
    t:assert_eq(started(), 1, "1 is one shard, so the 2 was the buffer count rather than a fixed value")
    eventd.set(vm, "StorageShards", "dword:257"):assert_ok()
    eventd.restart(vm)
    t:assert_eq(started(), 2, "257, above the range, is ignored")
    eventd.unset(vm, "StorageShards")
    eventd.restart(vm)
end)

-- ---------------------------------------------------------------------------
-- Startup
-- ---------------------------------------------------------------------------

--- Boot a VM whose eventd is expected not to come up, and report what it
--- did: whether it ever answered, and the service's last state and cause.
local function failing_boot(name, opts)
    local fvm = eventd.boot({ name = name, wait = false, config = opts.config })
    local answered, state, cause = false, nil, nil
    local deadline = os.time() + (opts.seconds or 25)
    while os.time() < deadline do
        local ok, r = pcall(function() return fvm:run("svctl --json status eventd") end)
        if not ok then break end
        local okj, s = pcall(json.decode, r.stdout)
        if okj and s then state, cause = s.state, s.cause end
        local okq, qr = pcall(eventd.query, fvm, "EVENTS TAKE 1")
        if okq and qr.ok then answered = true; break end
        if cause == "process_crash" and opts.until_crash then break end
        pcall(function() fvm:run("sleep 1") end)
    end
    return answered, state, cause
end

test("an invalid required key fails eventd's startup", {
    spec = "eventd *config.a-missing-or-invalid-required-key-fails-startup",
}, function(t)
    local answered, state, cause = failing_boot("ev-badpath", {
        config = { { name = "EventStorePath", type = "dword", data = 1 } },
        until_crash = true,
    })
    t:assert(not answered, "with EventStorePath a REG_DWORD, eventd never answers")
    t:assert_eq(cause, "process_crash", "it exits during startup: state " .. tostring(state))
end)

test("so does a missing one", {
    spec = "eventd *config.a-missing-or-invalid-required-key-fails-startup",
}, function(t)
    -- A seed cannot delete a value, so this one is deleted from a
    -- running system and eventd restarted onto it.
    local fvm = eventd.boot({ name = "ev-nopath" })
    eventd.unset(fvm, "MetricSocketPath"):assert_ok()
    fvm:run("svctl restart eventd")
    local cause, answered = nil, false
    local deadline = os.time() + 25
    while os.time() < deadline do
        local ok, r = pcall(function() return fvm:run("svctl --json status eventd") end)
        if not ok then break end
        local okj, s = pcall(json.decode, r.stdout)
        if okj and s then cause = s.cause end
        if cause == "process_crash" then break end
        pcall(function() fvm:run("sleep 1") end)
    end
    local okq, qr = pcall(eventd.query, fvm, "EVENTS TAKE 1")
    answered = okq and qr.ok
    t:assert_eq(cause, "process_crash", "without MetricSocketPath, the restarted eventd exits during startup")
    t:assert(not answered, "and does not answer")
end)

-- An invalid tuning value is ignored and the value in use retained, at
-- startup as on reload: a seeded value outside its range, or a drop
-- threshold not below the create threshold, does not fail startup.
test("an invalid value is ignored and the value in use kept, live and at startup", {
    spec = "eventd *config.an-invalid-value-is-ignored-and-the-value-in-use-is-retained",
}, function(t)
    -- At startup the file-scope VM was seeded with MaxBatchSize=5: eventd
    -- started, on the default, so setting the default now is no change.
    local before = #changes("MaxBatchSize")
    eventd.set(vm, "MaxBatchSize", "dword:10000"):assert_ok()
    barrier()
    t:assert_eq(#changes("MaxBatchSize"), before,
        "the seeded out-of-range 5 was ignored at startup: 10000 was already in use")
    eventd.unset(vm, "MaxBatchSize")

    -- Live: a valid value goes into use, invalid ones after it are ignored,
    -- and deleting the key is a change from the value eventd kept.
    eventd.set(vm, "LogMaxBatchSize", "dword:7000"):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "LogMaxBatchSize" AND config.value == 7000 SINCE 1h ago',
        function(rs) return #rs >= 1 end)
    local n = #changes("LogMaxBatchSize")
    for _, bad in ipairs({ "dword:5", "sz:7000", "qword:7000" }) do
        eventd.set(vm, "LogMaxBatchSize", bad):assert_ok()
        barrier()
        t:assert_eq(#changes("LogMaxBatchSize"), n, "LogMaxBatchSize=" .. bad .. " is ignored")
    end
    eventd.unset(vm, "LogMaxBatchSize")
    -- A removed value leaves `config.type` and `config.value` out (§3.2).
    local rows = eventd.wait_rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "LogMaxBatchSize"' ..
        ' AND config.value-previous == 7000 SINCE 1h ago', function(rs)
            for _, r in ipairs(rs) do if r["config.type"] == nil then return true end end
            return false
        end)
    local removed
    for _, r in ipairs(rows) do if r["config.type"] == nil then removed = r end end
    t:assert(removed and removed["config.value"] == nil,
        "deleting it is a change with no value now: " .. json.encode(rows))
    t:assert_eq(removed and removed["config.value-previous"], 7000,
        "from the 7000 eventd kept: " .. json.encode(removed))

    -- At startup, a drop threshold that is not below the create threshold.
    -- Both values are in range; the pair is not. Ignored, eventd starts.
    local answered, state, cause = failing_boot("ev-thresh", {
        config = { { name = "AdaptiveIndexDropThreshold", type = "dword", data = 500 } },
        seconds = 40,
    })
    t:assert(answered, "with AdaptiveIndexDropThreshold=500 over the default create threshold 100, " ..
        "eventd still starts: state " .. tostring(state) .. " cause " .. tostring(cause))
end)
