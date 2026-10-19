-- eventd TRM §1.1 — Overview, and §1.4 — Prior Art: what eventd is, and
-- what it is not.
--
-- One file-scope VM. The overview's statements are about the daemon as a
-- whole — where it sits in boot, what it stores and answers, and the things
-- it deliberately does not do — so each is checked against the running
-- system: its service definition and process, its sockets, its stores, and
-- a KMES consumer of the test's own attached beside it.
--
-- Prior Art has one anchored statement, that the stores are SQLite; it is
-- checked here too, since it is one more property of the daemon's files.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-overview" })

local CC = eventd.T.config_change
local function q(s) return '"' .. s .. '"' end

test("eventd stores events, logs and metrics, and answers queries for all three", {
    spec = "eventd *overview.eventd-stores-and-answers-queries-for-events-logs-and-metrics",
}, function(t)
    local etype, origin, metric = "pt.ov" .. eventd.marker(), eventd.marker("ov"), eventd.marker("ovm")
    eventd.emit(vm, etype, { n = 1 })
    eventd.send_log(vm, { origin = origin, is_error = false, message = "a log" })
    eventd.send_metric(vm, { name = metric, type = "gauge", value = 9 })
    local e = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local l = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local m = eventd.wait_rows(vm, "METRIC " .. metric .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(e[1].n, 1, "the event is stored and answered")
    t:assert_eq(l[1].message, "a log", "the log record is")
    t:assert_eq(m[1].value, 9, "and the metric sample is, all through the one query socket")
    t:assert_eq(eventd.sql(vm, eventd.shards(vm)[1], "SELECT count(*) FROM events WHERE event_type = '" .. etype .. "'")[1][1],
        1, "held in eventd's own event store")
end)

test("eventd is a boot-started, Critical platform daemon signed at TCB level", {
    spec = "eventd *overview.eventd-is-a-boot-started-critical-platform-daemon-signed-at-tcb-level",
}, function(t)
    local def = vm:run([[reg get 'Machine\System\Services\eventd']]).stdout
    t:assert(def:find('Triggers = REG_MULTI_SZ%s*%[?"?boot') or def:find("boot", 1, true),
        "the service is boot-triggered: " .. def)
    t:assert(def:find("ErrorControl = REG_DWORD 1", 1, true) or def:match("ErrorControl[^\n]*1\n"),
        "with ErrorControl 1, Critical")
    local st = eventd.status(vm)
    t:assert_eq(st.state, "active", "it is running")
    local psb = vm:read_file("/proc/" .. eventd.pid(vm) .. "/psb")
    local ptype, trust = psb:match("pip_type=(%d+) pip_trust=(%d+)")
    t:assert_eq(tonumber(trust), 8192, "its process carries the PeiosTcb trust its signature confers: " .. psb)
    t:assert_eq(tonumber(ptype), 512, "at the protected tier")
end)

test("eventd stores a log line as text, without parsing it", {
    spec = "eventd *overview.eventd-stores-log-lines-without-parsing-them",
}, function(t)
    local origin = eventd.marker("raw")
    local line = '{"level":"error","msg":"disk on fire","code":7} severity=CRITICAL'
    eventd.send_log(vm, { origin = origin, is_error = false, message = line })
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local r = rows[1]
    t:assert_eq(r.message, line, "the message is the bytes sent")
    t:assert_eq(r.is_error, false, "its \"error\" and CRITICAL do not make it an error: one flag, as sent")
    local fields = {}
    for k in pairs(r) do fields[#fields + 1] = k end
    table.sort(fields)
    t:assert_eq(table.concat(fields, ","), "boot_id,is_error,message,origin,timestamp",
        "and nothing was extracted from the JSON into fields")
    t:assert(not eventd.query(vm, "LOGS FROM " .. origin .. " WHERE level == \"error\" SINCE 10m ago").ok,
        "there is no level field to query")
end)

test("eventd reads no /proc, scrapes no endpoint and polls no service", {
    spec = "eventd *overview.eventd-reads-no-proc-scrapes-no-endpoint-and-polls-no-service",
}, function(t)
    local pid = eventd.pid(vm)
    -- No network sockets: everything eventd holds that is a socket is
    -- AF_UNIX.
    local inodes = {}
    for ino in eventd.fd_listing(vm, pid):gmatch("socket:%[(%d+)%]") do inodes[#inodes + 1] = ino end
    local unix = vm:run("cat /proc/net/unix").stdout
    for _, ino in ipairs(inodes) do
        t:assert(unix:find(" " .. ino .. " ", 1, true) or unix:find(" " .. ino .. "\n", 1, true),
            "socket " .. ino .. " is an AF_UNIX socket")
    end
    local inet = vm:run("cat /proc/net/tcp /proc/net/tcp6 /proc/net/udp /proc/net/udp6 2>/dev/null").stdout
    t:assert(#inodes >= 3, "eventd holds its sockets: " .. #inodes)
    for _, ino in ipairs(inodes) do
        t:assert(not inet:find(" " .. ino .. " ", 1, true), "socket " .. ino .. " is not TCP or UDP")
    end
    -- And over 20 s, it opens nothing under /proc and no metric series
    -- appears that something did not push to it.
    local opened = {}
    for _ = 1, 20 do
        for target in eventd.fd_listing(vm, pid):gmatch("%-> (/proc/%S+)") do
            if not target:find("^/proc/self/") then opened[#opened + 1] = target end
        end
        vm:run("sleep 1")
    end
    t:assert_eq(#opened, 0, "eventd held nothing under /proc open: " .. table.concat(opened, ","))
    local names = eventd.sql(vm, eventd.DB.metrics, "SELECT DISTINCT name FROM series")
    for _, r in ipairs(names) do
        t:assert(r[1]:sub(1, 7) == "eventd." or r[1]:sub(1, 2) == "pt",
            "every series is eventd's own health or one a test pushed: " .. r[1])
    end
end)

test("eventd implements no distributed tracing", {
    spec = "eventd *overview.eventd-implements-no-distributed-tracing",
}, function(t)
    for _, mode in ipairs({ "TRACES", "SPANS", "TRACE" }) do
        local r = eventd.query(vm, mode .. " SINCE 1h ago")
        t:assert(not r.ok, mode .. " is not a query mode: " .. tostring(r.stderr))
    end
    local socks = vm:run("ls /run/eventd").stdout
    local n = select(2, socks:gsub("%.sock", ""))
    t:assert_eq(n, 3, "and eventd listens on three sockets — query, log, metric — and no span intake: " .. socks)
end)

test("a query sees an event a batch commit interval after KMES delivered it", {
    spec = "eventd *overview.eventd-costs-a-batch-commit-interval-of-latency-over-kmes",
}, function(t)
    eventd.set(vm, "MaxBatchLatencyMs", "dword:4000"):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "MaxBatchLatencyMs" AND config.value == 4000 SINCE 1h ago',
        function(rs) return #rs >= 1 end)
    -- A KMES consumer of the test's own has the event at once.
    local ring = assert(kmes.attach(vm, 0))
    local etype = "pt.lat" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    local direct = #kmes.of_type(kmes.drain(ring), etype)
    kmes.detach(ring)
    local started = os.time()
    local _, later = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago",
        function(rs) return #rs == 1 end, { timeout = 15, interval = 0.1 })
    local took = os.time() - started
    eventd.unset(vm, "MaxBatchLatencyMs")
    t:assert_eq(direct, 1, "the ring had the event as soon as it was emitted")
    t:assert(later, "eventd has it once its batch commits")
    t:assert(took <= 5, "within the 4 s commit interval (plus a second of slack): " .. took .. " s")
end)

test("eventd is one KMES consumer among others, with nothing taken from them", {
    spec = "eventd *overview.eventd-holds-no-privileged-position-among-kmes-consumers",
}, function(t)
    local ring, errno = kmes.attach(vm, 0)
    t:assert(ring, "another consumer attaches to the ring eventd drains: errno " .. tostring(errno))
    local etype = "pt.both" .. eventd.marker()
    eventd.emit(vm, etype, { n = 1 })
    local mine = #kmes.of_type(kmes.drain(ring), etype)
    kmes.detach(ring)
    t:assert_eq(mine, 1, "it reads the event")
    t:assert_eq(#eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end), 1,
        "and so does eventd: neither consumer took it from the other")
end)

test("eventd's stores are SQLite databases, not a binary format of its own", {
    spec = "eventd *priorart.eventd-stores-in-sqlite-not-a-proprietary-binary-format",
}, function(t)
    local files = { eventd.DB.logs, eventd.DB.metrics, eventd.DB.meta }
    for _, s in ipairs(eventd.shards(vm)) do files[#files + 1] = s end
    for _, f in ipairs(files) do
        t:assert_eq(vm:read_file(f):sub(1, 16), "SQLite format 3\0", f .. " has the SQLite header")
        t:assert(#eventd.sql(vm, f, "SELECT name FROM sqlite_master") > 0, "and stock sqlite reads it")
    end
end)
