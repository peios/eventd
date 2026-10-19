-- eventd TRM §8.2 — the bootstrap sequence, the half of it that is about
-- failing: which inputs fail startup, that a failure is total, and what a
-- failed start leaves behind.
--
-- One VM, broken and repaired in turn. Each test breaks one input (a
-- registry value, a directory, a file at a socket path, the boot ID, the
-- service's privileges), asks peinit to start eventd, reads the failed
-- start back from peinit's status, puts the input right and starts eventd
-- again. The failure's stderr line is read afterwards from the log store
-- under origin `eventd`: peinit holds service output while the log
-- socket is gone and delivers it once the next eventd is up.
--
-- eventd is Critical with RestartPolicy=OnFailure in the image, so a
-- failed start would be retried and, once the budget ran out, would
-- reboot the machine out from under the file. The seed makes it Normal
-- and Never for this VM only: every failure here is then one attempt that
-- stays Failed until the test starts it again. Nothing under test reads
-- either value.
--
-- The healthy, observational half of §8.2 is bootstrap-sequence.test.lua.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local unixsock = require("helpers.unixsock")
peinit.claim(1)

--- The descriptor eventd requires on a store directory (directory.rs),
--- for the tmpfs a test mounts where it needs a fresh, valid one.
local STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

--- A directory eventd's service identity may create sockets in.
local SOCKET_DIR_SDDL = "O:SYG:SYD:(A;OICI;GA;;;SY)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

local vm = eventd.boot({
    name = "ev-boot-fail",
    noncritical = true,
})

local function tmpfs(path, sddl, size)
    vm:run("mkdir -p " .. path):assert_ok()
    vm:run("mount -t tmpfs -o size=" .. (size or "1m") .. ",policy=synth-ephemeral --synth-sddl '"
        .. sddl .. "' none " .. path):assert_ok()
end

--- Ask peinit for a fresh eventd and return its status once the attempt
--- has settled. `restart` covers both a running eventd and a failed one.
local function attempt()
    vm:run("svctl restart eventd")
    return eventd.settle(vm)
end

--- Whether anything answers on the default query socket.
local function answering()
    return eventd.query(vm, "EVENTS TAKE 1").ok
end

--- Assert one failed start: Failed, by a non-zero exit, never ready.
local function assert_failed(t, status, why)
    t:assert_eq(status.state, "failed", why .. ": the start failed: " .. json.encode(status))
    t:assert_eq(status.cause, "process_crash",
        why .. ": eventd exited non-zero rather than cleanly (peinit's cause)")
    t:assert(not answering(), why .. ": and nothing answers on the query socket")
end

test("a missing required key fails startup, which is logged to stderr and exits non-zero", {
    spec = "eventd *bootstrap.a-missing-or-invalid-required-key-fails-startup"
        .. " eventd *bootstrap.a-startup-failure-is-logged-to-stderr-and-exits-non-zero"
        .. " eventd *bootstrap.a-failed-phase-means-readiness-is-never-signalled",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.unset(vm, "EventStorePath"):assert_ok()
    local status = attempt()
    -- Readiness is Notify: peinit moves a service to Active only on its
    -- READY=1. Settling in Failed with no Active in between is the
    -- readiness signal never having been sent.
    assert_failed(t, status, "EventStorePath deleted")
    eventd.set(vm, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    eventd.start(vm)
    local line = eventd.stderr_line(vm,"EventStorePath", since)
    t:assert(line, "the failure reached standard error, which peinit captured")
    t:assert(line and line.message:find("missing", 1, true),
        "and it names the missing value: " .. json.encode(line))

    -- Invalid rather than missing: the right type, but not absolute.
    since = eventd.guest_ns(vm)
    eventd.set(vm, "QuerySocketPath", "sz:relative.sock"):assert_ok()
    assert_failed(t, attempt(), "QuerySocketPath relative")
    eventd.set(vm, "QuerySocketPath", "sz:/run/eventd/query.sock"):assert_ok()
    eventd.start(vm)
    -- And the wrong type entirely.
    eventd.set(vm, "LogStorePath", "dword:7"):assert_ok()
    assert_failed(t, attempt(), "LogStorePath a DWORD")
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/logs/"):assert_ok()
    eventd.start(vm)
    local invalid = eventd.stderr_line(vm,"invalid type or value", since)
    t:assert(invalid, "an invalid required value is reported as invalid")
end)

test("the six required keys are the three store paths and the three socket paths", {
    spec = "eventd *bootstrap.the-six-required-keys-are-the-three-store-paths-and-three-socket-paths",
}, function(t)
    local paths = {
        EventStorePath = "/var/state/eventd/events/",
        LogStorePath = "/var/state/eventd/logs/",
        MetricStorePath = "/var/state/eventd/metrics/",
        QuerySocketPath = "/run/eventd/query.sock",
        LogSocketPath = "/run/eventd/log.sock",
        MetricSocketPath = "/run/eventd/metric.sock",
    }
    for _, name in ipairs({ "EventStorePath", "LogStorePath", "MetricStorePath",
                            "QuerySocketPath", "LogSocketPath", "MetricSocketPath" }) do
        eventd.unset(vm, name):assert_ok()
        local status = attempt()
        t:assert_eq(status.state, "failed", name .. " is required: without it the start fails")
        eventd.set(vm, name, "sz:" .. paths[name]):assert_ok()
        eventd.start(vm)
    end
    -- Every other value is optional: the image sets none of them, and an
    -- eventd started with only the six is the one answering now.
    local listed = vm:run("reg get '" .. eventd.KEY .. "'").stdout
    local names = {}
    for name in listed:gmatch("([%w]+) = REG_") do names[#names + 1] = name end
    table.sort(names)
    t:assert_eq(table.concat(names, ","),
        "EventStorePath,LogSocketPath,LogStorePath,MetricSocketPath,MetricStorePath,QuerySocketPath",
        "the six are all the key holds")
    t:assert(answering(), "and eventd runs on them alone")
end)

test("a missing store directory, or one with the wrong descriptor, fails startup", {
    spec = "eventd *bootstrap.a-missing-or-unsafe-store-directory-fails-startup"
        .. " eventd *bootstrap.there-is-no-degraded-mode-without-a-store-or-kmes",
}, function(t)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-absent"):assert_ok()
    assert_failed(t, attempt(), "metric store directory missing")
    -- No degraded mode: the event and log stores are fine, yet neither
    -- ingestion socket exists either — the whole daemon failed.
    local listing = vm:run("ls /run/eventd").stdout
    t:assert(not listing:find("log.sock", 1, true) and not listing:find("query.sock", 1, true),
        "no socket of a partial eventd exists: " .. listing)

    -- A directory that exists but carries the inherited descriptor of its
    -- parent rather than the protected one.
    vm:run("mkdir -p /var/state/eventd/pt-plain"):assert_ok()
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-plain"):assert_ok()
    assert_failed(t, attempt(), "metric store directory with the wrong descriptor")
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/metrics/"):assert_ok()
    eventd.start(vm)
    t:assert(eventd.stderr_line(vm,"state directory", since),
        "the directory failure was reported on stderr")
    t:assert(eventd.stderr_line(vm,"required protected descriptor", since),
        "and the descriptor mismatch named as such")
end)

test("store directories are opened without following symbolic links", {
    spec = "eventd *bootstrap.store-directories-are-opened-without-following-symbolic-links",
}, function(t)
    -- The final component a link to the real, correctly protected store.
    vm:run("ln -sfn /var/state/eventd/logs /var/state/eventd/pt-loglink"):assert_ok()
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/pt-loglink"):assert_ok()
    assert_failed(t, attempt(), "log store reached through a final-component symlink")
    -- An intermediate component a link: /run/pt-varlink -> /var/state/eventd.
    vm:run("ln -sfn /var/state/eventd /run/pt-varlink"):assert_ok()
    eventd.set(vm, "LogStorePath", "sz:/run/pt-varlink/logs"):assert_ok()
    assert_failed(t, attempt(), "log store reached through an intermediate symlink")
    eventd.set(vm, "LogStorePath", "sz:/var/state/eventd/logs/"):assert_ok()
    eventd.start(vm)
end)

test("a non-socket at a socket path fails startup and is left in place", {
    spec = "eventd *bootstrap.a-non-socket-at-a-socket-path-fails-startup",
}, function(t)
    tmpfs("/run/pt-sock-file", SOCKET_DIR_SDDL)
    local defaults = {
        QuerySocketPath = eventd.SOCKET.query,
        LogSocketPath = eventd.SOCKET.log,
        MetricSocketPath = eventd.SOCKET.metric,
    }
    for _, name in ipairs({ "QuerySocketPath", "LogSocketPath", "MetricSocketPath" }) do
        local path = "/run/pt-sock-file/" .. name
        vm:write_file(path, "not eventd's\n")
        local before = defaults[name]
        eventd.set(vm, name, "sz:" .. path):assert_ok()
        assert_failed(t, attempt(), name .. " pointing at a regular file")
        t:assert_eq(vm:read_file(path), "not eventd's\n",
            name .. ": the file that was there is untouched")
        eventd.set(vm, name, "sz:" .. before):assert_ok()
        eventd.start(vm)
    end
end)

test("a stale socket at each socket path is unlinked and replaced, and the query socket is the configured one", {
    spec = "eventd *bootstrap.a-stale-socket-at-a-socket-path-is-unlinked-first"
        .. " eventd *bootstrap.the-query-socket-is-created-at-querysocketpath",
}, function(t)
    tmpfs("/run/pt-sock-stale", SOCKET_DIR_SDDL)
    local paths = {
        QuerySocketPath = "/run/pt-sock-stale/q.sock",
        LogSocketPath = "/run/pt-sock-stale/l.sock",
        MetricSocketPath = "/run/pt-sock-stale/m.sock",
    }
    -- A socket bound and then abandoned, exactly what a crash leaves: the
    -- pathname stays, nothing listens behind it.
    local inode = {}
    for name, path in pairs(paths) do
        local fd = assert(unixsock.socket(vm, unixsock.AF_UNIX, unixsock.SOCK.DGRAM))
        t:assert_eq(unixsock.bind(vm, fd, path).ret, 0, "a stale socket bound at " .. path)
        vm:syscall(3, fd)
        inode[name] = vm:run("stat -c %i " .. path).stdout:match("%d+")
        eventd.set(vm, name, "sz:" .. path):assert_ok()
    end
    local status = attempt()
    t:assert_eq(status.state, "active", "eventd started over the stale sockets: " .. json.encode(status))
    for name, path in pairs(paths) do
        local now = vm:run("stat -c '%i %F' " .. path).stdout
        t:assert(now:find("socket", 1, true) and now:match("%d+") ~= inode[name],
            name .. ": a new socket replaced the stale one: " .. now)
    end
    local q = eventd.query(vm, "EVENTS " .. eventd.T.startup .. " TAKE 1",
        { socket = paths.QuerySocketPath })
    t:assert(q.ok and #q.rows == 1, "queries are answered at the configured QuerySocketPath: "
        .. tostring(q.stderr))
    t:assert(not answering(), "and not at the old default")
    for name, _ in pairs(paths) do
        eventd.set(vm, name, "sz:/run/eventd/" .. ({ QuerySocketPath = "query.sock",
            LogSocketPath = "log.sock", MetricSocketPath = "metric.sock" })[name]):assert_ok()
    end
    attempt()
    eventd.ready(vm)
end)

test("an unavailable or malformed boot ID fails startup", {
    spec = "eventd *bootstrap.an-unavailable-or-malformed-boot-id-fails-startup",
}, function(t)
    local since = eventd.guest_ns(vm)
    -- A tmpfs eventd may read, so that the bind below is malformed rather
    -- than merely unreadable; then a file eventd may not read, which is
    -- the unavailable case.
    tmpfs("/run/pt-bid", "O:SYG:SYD:(A;OICI;GA;;;SY)(A;OICI;GR;;;WD)", "64k")
    vm:write_file("/run/pt-bid/malformed", "not-a-uuid\n")
    vm:run("mount --bind /run/pt-bid/malformed /proc/sys/kernel/random/boot_id"):assert_ok()
    local status = attempt()
    vm:run("umount /proc/sys/kernel/random/boot_id"):assert_ok()
    assert_failed(t, status, "malformed boot ID")
    eventd.start(vm)
    t:assert(eventd.stderr_line(vm,"not a canonical UUID", since), "the malformed boot ID was reported")

    since = eventd.guest_ns(vm)
    tmpfs("/run/pt-bid-closed", "O:SYG:SYD:(A;OICI;GA;;;SY)", "64k")
    vm:write_file("/run/pt-bid-closed/id", vm:read_file("/proc/sys/kernel/random/boot_id"))
    vm:run("mount --bind /run/pt-bid-closed/id /proc/sys/kernel/random/boot_id"):assert_ok()
    status = attempt()
    vm:run("umount /proc/sys/kernel/random/boot_id"):assert_ok()
    assert_failed(t, status, "unreadable boot ID")
    eventd.start(vm)
    t:assert(eventd.stderr_line(vm,"cannot read kernel boot ID", since), "the unreadable boot ID was reported")
end)

test("KMES attachment needs SeSecurityPrivilege, and without KMES nothing starts", {
    spec = "eventd *bootstrap.kmes-attachment-requires-sesecurityprivilege-in-the-effective-token",
}, function(t)
    -- The service's privileges are what peinit's RequiredPrivileges
    -- leaves in the token. Keeping only SeChangeNotifyPrivilege (traverse)
    -- removes SeSecurityPrivilege, and nothing before Phase 2 needs it.
    local since = eventd.guest_ns(vm)
    vm:run("reg set '" .. eventd.SERVICE .. "' RequiredPrivileges 'multi:SeChangeNotifyPrivilege'"):assert_ok()
    vm:run("svctl reload-config"):assert_ok()
    local status = attempt()
    vm:run("reg del '" .. eventd.SERVICE .. "' RequiredPrivileges"):assert_ok()
    vm:run("svctl reload-config"):assert_ok()
    assert_failed(t, status, "no SeSecurityPrivilege")
    t:assert(not vm:run("ls /run/eventd").stdout:find(".sock", 1, true),
        "and no socket was created: there is no mode without KMES")
    eventd.start(vm)
    local line = eventd.stderr_line(vm,"eventd:", since)
    t:assert(line and (line.message:lower():find("kmes", 1, true)
        or line.message:lower():find("permission", 1, true)
        or line.message:lower():find("operation not permitted", 1, true)),
        "the failure is KMES attachment being refused: " .. json.encode(line))
end)

test("the phases do not fix the order: the boot ID is read before any store is opened", {
    spec = "eventd *bootstrap.the-seven-phases-group-the-work-rather-than-order-it-and-startup-completes-or-fails-entirely",
}, function(t)
    -- A fresh, correctly protected event store: opening the stores would
    -- create shard-0000.db and eventd-meta.db in it. A malformed boot ID
    -- fails startup first, because it is read before any store is
    -- opened, so neither file exists when it fails.
    tmpfs("/run/pt-fresh-events", STORE_SDDL)
    tmpfs("/run/pt-bid-order", "O:SYG:SYD:(A;OICI;GA;;;SY)(A;OICI;GR;;;WD)", "64k")
    vm:write_file("/run/pt-bid-order/malformed", "not-a-uuid\n")
    eventd.set(vm, "EventStorePath", "sz:/run/pt-fresh-events"):assert_ok()
    vm:run("mount --bind /run/pt-bid-order/malformed /proc/sys/kernel/random/boot_id"):assert_ok()
    local since = eventd.guest_ns(vm)
    local status = attempt()
    vm:run("umount /proc/sys/kernel/random/boot_id"):assert_ok()
    local listing = vm:run("ls /run/pt-fresh-events").stdout
    eventd.set(vm, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    eventd.start(vm)
    t:assert_eq(status.state, "failed", "the malformed boot ID failed startup")
    t:assert(eventd.stderr_line(vm,"not a canonical UUID", since), "and it was the boot ID that failed it")
    t:assert(not listing:find("eventd-meta.db", 1, true) and not listing:find("shard-0000.db", 1, true),
        "no store had been opened or created when the boot ID was read: "
        .. (listing == "" and "(empty)" or listing))
end)
