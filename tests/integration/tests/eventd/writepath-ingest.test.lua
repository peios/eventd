-- eventd TRM §7.6 — the write path: events admitted by KMES and stamped by
-- the kernel, logs brokered by peinit through a protected socket, and
-- metrics authorized per name against the token each datagram carries,
-- behind a bounded thread-local verdict cache.
--
-- One file-scope eventd serves every test. Each test publishes under its
-- own marker-named metric names and writes descriptors only for those.
-- What was stored is read out of the stores directly (`eventd.sql`), so a
-- test about publication does not depend on being allowed to read what it
-- published. A datagram's fate is decided before the next one's, so each
-- rejected datagram is followed by a witness that must be stored; once
-- the witness is there, the rejection has happened.
--
-- AccessChecks are counted through KACS's own audit: a descriptor with a
-- SACL success/failure audit ACE makes every check against it emit a
-- `kacs.audit.access.checked` event naming the object eventd passed as the
-- check's audit context (`object.kind` `metric-namespace`,
-- `object.metric-namespace.pattern` the pattern), with `access.requested`
-- EVENTD_PUBLISH for a publication check rather than a read; eventd stores
-- those like any other event.
--
-- The rejection counters live only in eventd's memory and are read from
-- its diagnostic dump (§8.5), which SIGQUIT writes to standard error on the
-- way out and peinit forwards into the log store. That test stops eventd
-- twice and so runs last but three. The last three narrow a socket and
-- start eventd again, each on a machine of its own, so that a start that
-- failed would not take the file's machine with it.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
local kmes = require("helpers.kmes")
local us = require("helpers.unixsock")
local sys = require("helpers.sys")
peinit.claim(2) -- the file's machine, and one the last test may lose

local vm = eventd.boot({ name = "ev-write" })

local SY, BA, AU = token.SID.LOCAL_SYSTEM, token.SID.ADMINISTRATORS, token.SID.AUTHENTICATED_USERS
local EVERYONE, SERVICE = token.SID.EVERYONE, token.sid(5, 6)
local NOBODY = token.SID.TEST_GROUP_2
local READ, PUBLISH = 0x1, 0x8

local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)

local function allow(mask, sid) return access.ace(access.ACE.ALLOWED, mask, sid or SY) end
local AUDIT = access.acl({ access.ace(access.ACE.AUDIT, 0xf, EVERYONE,
    access.ACE_FLAG.SUCCESSFUL_ACCESS | access.ACE_FLAG.FAILED_ACCESS) })
local DENY_ALL = access.simple({ allow(READ | PUBLISH, NOBODY) })

local function groups_of(sids)
    local list = {}
    for i, sid in ipairs(sids) do list[i] = { sid = sid, attributes = ENABLED } end
    return list
end

--- The DACL part of `sd show --sddl` for a path (on `on`, default the
--- file's machine).
local function dacl(path, on)
    local r = (on or vm):run("sd show --sddl " .. path)
    r:assert_ok()
    return r.stdout:match("(D:[^\n]*)"), r.stdout
end

-- ---------------------------------------------------------------------------
-- Stores
-- ---------------------------------------------------------------------------

local function series(name)
    return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name = '" .. name .. "'")[1][1]
end

local function logs_of(origin)
    return eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'")[1][1]
end

local function wait_series(name, n, desc)
    return wait_until(function() return series(name) == n end,
        { timeout = 15, desc = desc or (n .. " series named " .. name) })
end

local function record(name, value, labels)
    return { name = name, type = "gauge", value = value or 1, labels = labels }
end

--- Send one metric datagram of `records` as `who`, with its token.
local function publish(who, records, opts)
    local r = eventd.send_metric(who, #records == 1 and records[1] or eventd.array(records), opts)
    assert(r.ret and r.ret > 0, "sendto metric socket: errno " .. tostring(r.errno))
    return r
end

--- A witness from the console, stored once every earlier datagram is decided.
local function witness()
    local name = eventd.marker("ptwit")
    publish(vm, { record(name) })
    wait_series(name, 1, "the witness " .. name)
end

-- ---------------------------------------------------------------------------
-- Counting checks
-- ---------------------------------------------------------------------------

--- Publication checks against the metric-namespace pattern `ctx`.
local function audits(ctx)
    local r = eventd.query(vm, 'EVENTS kacs.audit.access.checked WHERE object.kind == "metric-namespace"'
        .. ' WHERE object.metric-namespace.pattern == "' .. ctx .. '" WHERE access.requested == ' .. PUBLISH
        .. ' SINCE 1h ago TAKE 100000 SELECT event.sequence')
    assert(r.ok, "audit query: " .. tostring(r.stderr))
    return #r.rows
end

--- The audit count for `ctx`, once it has stopped moving.
local function settled(ctx)
    local last, steady = -1, 0
    wait_until(function()
        local n = audits(ctx)
        if n == last then steady = steady + 1 else steady, last = 0, n end
        return steady >= 3
    end, { timeout = 30, interval = 0.5, desc = "the audit count for " .. ctx })
    return last
end

--- An audited publication descriptor for `pattern`; returns the pattern
--- its checks are audited against.
local function audited(pattern, aces)
    eventd.put_descriptor(vm, "Metrics", pattern, access.simple(aces, { sacl = AUDIT }))
    -- Prove it is in force: a publish under it is audited.
    local ctx = pattern
    local ok = wait_until(function()
        publish(vm, { record(pattern .. "." .. eventd.marker("probe")) })
        witness()
        return audits(ctx) > 0
    end, { timeout = 30, desc = "the audited descriptor on " .. pattern })
    assert(ok, "no audited check against " .. pattern)
    return ctx
end

-- ---------------------------------------------------------------------------
-- Events
-- ---------------------------------------------------------------------------

test("eventd is not in the admission path: KMES admits events while eventd is stopped", {
    spec = "eventd *writepath.eventd-is-not-in-the-event-admission-path",
}, function(t)
    local ty = eventd.marker("ptadmit")
    vm:run("svctl stop eventd", { timeout = 60 }):assert_ok()
    local ok, err = pcall(function()
        t:assert_eq(eventd.pid(vm), nil, "eventd is not running")
        local r = eventd.emit(vm, ty, { n = 1 })
        t:assert_eq(r.ret, 0, "kmes_emit succeeds with no eventd at all: errno " .. tostring(r.errno))
        -- And refuses a caller without SeAuditPrivilege, with no eventd to ask.
        token.as_principal(t, vm, { groups = groups_of({ EVERYONE, AU }) }, function(w)
            local denied = eventd.emit(w, ty, { n = 2 })
            t:assert(denied.ret ~= 0, "kmes_emit refuses a caller without SeAuditPrivilege")
        end)
    end)
    eventd.start(vm)
    if not ok then error(err, 0) end
end)

test("an event's identity stamps are the kernel's, whatever the payload says", {
    spec = "eventd *writepath.event-identity-stamps-are-the-kernels-and-the-emitter-cannot-alter-them",
}, function(t)
    local ty = eventd.marker("ptstamp")
    local forged = "{11111111-2222-3333-4444-555555555555}"
    -- A payload written at every header path it can name.
    local lie = { emitter = { process = { guid = forged }, token = { guid = forged },
                              ["true-token"] = { guid = forged }, class = 7 },
                  event = { cpu = 99 }, tag = "agent" }
    eventd.emit(vm, ty, lie)
    eventd.emit(vm, ty, { tag = "agent2" })
    local audit = token.bit(token.PRIV.AUDIT)
    token.as_principal(t, vm, {
        groups = groups_of({ EVERYONE, AU }), privs_present = audit | NOTIFY, privs_enabled = audit | NOTIFY,
    }, function(w)
        local r = eventd.emit(w, ty, { tag = "worker", emitter = { process = { guid = forged } } })
        t:assert_eq(r.ret, 0, "the worker emits: errno " .. tostring(r.errno))
    end)
    local rows = eventd.wait_rows(vm, "EVENTS " .. ty .. " SINCE 1h ago", function(rs) return #rs == 3 end)
    local by = {}
    for _, r in ipairs(rows) do by[r.tag] = r end
    for _, f in ipairs({ "emitter.process.guid", "emitter.token.guid", "emitter.true-token.guid" }) do
        t:assert(by.agent[f] ~= forged, f .. " is not the forged value: " .. tostring(by.agent[f]))
        t:assert_eq(by.agent[f], by.agent2[f], f .. " is the same for two emits from one process and token")
    end
    t:assert(by.agent["event.cpu"] ~= 99 and by.agent["emitter.class"] ~= 7,
        "nor are event.cpu and emitter.class the payload's")
    t:assert(by.worker["emitter.process.guid"] ~= by.agent["emitter.process.guid"],
        "another process's event carries its own emitter.process.guid")
    t:assert(by.worker["emitter.token.guid"] ~= by.agent["emitter.token.guid"],
        "and its own token's identity")
end)

-- ---------------------------------------------------------------------------
-- Logs
-- ---------------------------------------------------------------------------

test("the log socket carries the protected peinit-only DACL §7.6 gives, and eventd's service SID owns it", {
    spec = "eventd *writepath.the-log-socket-gets-a-protected-dacl-before-the-first-receive"
        .. " eventd *writepath.the-log-socket-stays-owned-by-eventds-service-sid",
}, function(t)
    local d, all = dacl(eventd.SOCKET.log)
    t:assert_eq(d, "D:P(D;;0x2;;;SU)(A;;GA;;;SY)(A;;GA;;;OW)", "its DACL is the one §7.6 gives: " .. all)
    t:assert_eq(all:match("O:(S%-[%d%-]+)"), "S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124",
        "and its owner is eventd's service SID: " .. all)
end)

test("a service cannot reach the log socket, even as SYSTEM, while peinit's own token can", {
    spec = "eventd *writepath.service-processes-cannot-reach-the-log-socket"
        .. " eventd *writepath.a-system-service-cannot-write-to-the-log-socket-but-peinit-can",
}, function(t)
    local origin = eventd.marker("ptbroker")
    -- An ordinary service, and a service running as SYSTEM: both carry SU.
    for _, spec in ipairs({
        { user_sid = token.sid(5, 80, 9, 8, 7, 6, 5), groups = groups_of({ EVERYONE, AU, SERVICE }) },
        { user_sid = SY, groups = groups_of({ EVERYONE, AU, BA, SERVICE }) },
    }) do
        token.as_principal(t, vm, spec, function(w)
            local r = eventd.send_log(w, { origin = origin, is_error = false, message = "forged" },
                { allow_error = true })
            t:assert_eq(r.errno, sys.E.ACCES, token.sid_string(spec.user_sid)
                .. " with the Service logon group is refused at sendto")
        end)
    end
    -- The agent runs on peinit's bootstrap SYSTEM token, which has no SU.
    local r = eventd.send_log(vm, { origin = origin, is_error = false, message = "brokered" })
    t:assert(r.ret and r.ret > 0, "peinit's token sends: errno " .. tostring(r.errno))
    wait_until(function() return logs_of(origin) == 1 end, { timeout = 15, desc = "the brokered line" })
    t:assert_eq(logs_of(origin), 1, "and only the brokered line is stored")
end)

test("one log datagram may carry records of several origins", {
    spec = "eventd *writepath.a-log-datagram-may-carry-records-from-several-origins",
}, function(t)
    local a, b = eventd.marker("ptorigina"), eventd.marker("ptoriginb")
    local r = eventd.send_log(vm, eventd.array({
        { origin = a, is_error = false, message = "one" },
        { origin = b, is_error = true, message = "two" },
        { origin = a, is_error = false, message = "three" },
    }))
    t:assert(r.ret and r.ret > 0, "one datagram is sent: errno " .. tostring(r.errno))
    wait_until(function() return logs_of(a) == 2 and logs_of(b) == 1 end,
        { timeout = 15, desc = "every record of the datagram" })
    t:assert_eq(logs_of(a), 2, "both of " .. a .. "'s records are stored")
    t:assert_eq(logs_of(b), 1, "and " .. b .. "'s")
end)

test("a log datagram is stored with no token, and no read descriptor stands in the way", {
    spec = "eventd *writepath.eventd-requests-and-checks-no-token-for-a-log-datagram",
}, function(t)
    local bare, tokened = eventd.marker("ptnotok"), eventd.marker("pttok")
    -- Nobody may read either origin: the read side has no say in storage.
    eventd.put_descriptor(vm, "Logs", bare, DENY_ALL)
    eventd.put_descriptor(vm, "Logs", tokened, DENY_ALL)
    eventd.send_log(vm, { origin = bare, is_error = false, message = "m" })
    eventd.send_log(vm, { origin = tokened, is_error = false, message = "m" }, { pass_token = true })
    wait_until(function() return logs_of(bare) == 1 and logs_of(tokened) == 1 end,
        { timeout = 15, desc = "both lines" })
    t:assert_eq(logs_of(bare), 1, "a datagram with no token is stored")
    t:assert_eq(logs_of(tokened), 1, "and one with a token is stored the same")
end)

-- ---------------------------------------------------------------------------
-- The metric socket
-- ---------------------------------------------------------------------------

test("every authenticated caller may send to the metric socket", {
    spec = "eventd *writepath.every-authenticated-caller-may-send-to-the-metric-socket",
}, function(t)
    t:assert_eq(dacl(eventd.SOCKET.metric), "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;FW;;;AU)",
        "the metric socket's DACL is the one §7.6 gives")
    token.as_principal(t, vm, { groups = groups_of({ EVERYONE, AU }), privs_present = NOTIFY,
        privs_enabled = NOTIFY }, function(w)
        local r = eventd.send_metric(w, record(eventd.marker("ptau")))
        t:assert(r.ret and r.ret > 0, "an ordinary signed-in user sends: errno " .. tostring(r.errno))
    end)
end)

--- A raw metric datagram from the console with KACS_SO_PASS_TOKEN on and
--- ancillary data `control`, or, when `control` is nil, an SCM_RIGHTS
--- message passing the sending socket's own descriptor.
local function send_with_control(bytes, control)
    local fd = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    us.set_pass_token(vm, fd, true)
    control = control or (string.pack("<I8I4I4i4", 20, 1, 1, fd) .. "\0\0\0\0")
    local r = us.sendmsg(vm, fd, bytes, { raw_control = control, to = eventd.SOCKET.metric })
    sys.close(vm, fd)
    return r
end

test("metric recvmsg has room for one token and no descriptors: anything more truncates and is discarded", {
    spec = "eventd *writepath.metric-recvmsg-has-room-for-one-token-and-no-ordinary-fds"
        .. " eventd *writepath.a-metric-datagram-with-truncated-ancillary-data-is-discarded",
}, function(t)
    local with_fd, with_two = eventd.marker("ptfd"), eventd.marker("pttwotok")
    -- SCM_RIGHTS carrying a descriptor, beside the token KACS attaches.
    local r = send_with_control(eventd.msgpack(record(with_fd)))
    t:assert(r.ret and r.ret > 0, "the datagram with a descriptor is sent: errno " .. tostring(r.errno))
    -- A second token, explicit, beside the automatic one.
    local self_tok = assert(token.open_self(vm, token.RIGHT.QUERY))
    r = send_with_control(eventd.msgpack(record(with_two)), us.token_cmsg(self_tok))
    sys.close(vm, self_tok)
    witness()
    t:assert_eq(series(with_fd), 0, "a datagram carrying an ordinary descriptor is discarded whole")
    if r.ret and r.ret > 0 then
        t:assert_eq(series(with_two), 0, "and one carrying two tokens")
    end
end)

test("a metric datagram that arrives without a token is discarded", {
    spec = "eventd *writepath.a-metric-datagram-without-a-token-is-discarded",
}, function(t)
    local name = eventd.marker("ptanon")
    eventd.send_metric(vm, record(name), { pass_token = false })
    witness()
    t:assert_eq(series(name), 0, "nothing of it is stored")
end)

test("a metric datagram larger than the receive buffer is discarded whole", {
    spec = "eventd *writepath.a-metric-datagram-with-truncated-data-is-discarded",
}, function(t)
    local big, small = eventd.marker("ptbig"), eventd.marker("ptsmall")
    local function datagram(name, n)
        local list = {}
        for i = 1, n do list[i] = record(name, i, { i = tostring(i) }) end
        return eventd.msgpack(eventd.array(list))
    end
    local fd = assert(us.socket(vm, us.AF_UNIX, us.SOCK.DGRAM))
    us.set_pass_token(vm, fd, true)
    -- SO_SNDBUFFORCE, so the sender can send more than the 262144-byte
    -- MaxMetricDatagramBytes eventd receives into.
    vm:syscall(us.NR.setsockopt, { args = { fd, 1, 32, 0, 4 },
        bufs = { string.pack("<i4", 4 * 1024 * 1024) }, ptrs = { 3 } })
    local bytes = datagram(big, 6000)
    t:assert(#bytes > 262144, "the datagram is over the ceiling: " .. #bytes)
    local r = us.sendto(vm, fd, bytes, eventd.SOCKET.metric)
    t:assert_eq(r.ret, #bytes, "and is sent whole: errno " .. tostring(r.errno))
    local s = us.sendto(vm, fd, datagram(small, 50), eventd.SOCKET.metric)
    t:assert(s.ret and s.ret > 0, "a small one of the same shape is sent")
    sys.close(vm, fd)
    wait_series(small, 50, "the small datagram's fifty series")
    witness()
    t:assert_eq(series(big), 0, "none of the oversized datagram's records is stored")
end)

test("a policy failure discards the whole datagram, authorized records with it, and is reported sparingly", {
    spec = "eventd *writepath.a-metric-datagram-is-discarded-when-token-query-or-policy-resolution-fails"
        .. " eventd *writepath.policy-errors-may-produce-rate-limited-standard-error-text",
}, function(t)
    local broken, fine = eventd.marker("ptbroken"), eventd.marker("ptfine")
    -- A descriptor that is not REG_BINARY cannot be resolved.
    vm:run("reg set -p '" .. eventd.key_of("Metrics",broken) .. "' @ sz:not-a-descriptor"):assert_ok()
    vm:run("sleep 1")
    local function rejected_lines()
        local r = eventd.query(vm, 'LOGS FROM eventd WHERE message CONTAINS "metric datagram rejected"'
            .. " SINCE 1h ago TAKE 10000")
        return r.ok and #r.rows or -1
    end
    local before = rejected_lines()
    local started = os.time()
    for i = 1, 10 do
        publish(vm, { record(fine .. "." .. i), record(broken .. ".x"), record(fine .. ".after" .. i) })
    end
    local elapsed = os.time() - started
    witness()
    for i = 1, 10 do
        t:assert_eq(series(fine .. "." .. i), 0, "the authorized record before the failing one is discarded")
        t:assert_eq(series(fine .. ".after" .. i), 0, "and the one after it")
    end
    local lines
    wait_until(function() lines = rejected_lines() - before; return lines >= 1 end,
        { timeout = 15, desc = "the rejection on standard error" })
    t:assert(lines >= 1, "the policy failure is reported on standard error")
    t:assert(lines <= elapsed + 2, lines .. " reports for ten failures in " .. elapsed
        .. "s: rate-limited, not one per datagram")
    vm:run("reg del -r -y '" .. eventd.key_of("Metrics",broken) .. "'")
end)

test("each record is checked for EVENTD_PUBLISH against the token its datagram carried", {
    spec = "eventd *writepath.each-metric-record-is-checked-for-eventd-publish-against-the-conveyed-token"
        .. " eventd *writepath.a-denied-metric-record-is-discarded-and-its-authorized-siblings-continue",
}, function(t)
    local owned, open = eventd.marker("ptowned"), eventd.marker("ptopen")
    eventd.put_descriptor(vm, "Metrics", owned, access.simple({ allow(READ, SY), allow(PUBLISH, token.SID.TEST_USER) }))
    vm:run("sleep 1")
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        -- Administrators too, so the image's socket admits it; the owned
        -- name's descriptor grants Administrators nothing.
        local user = assert(token.mint(w, {
            user_sid = token.SID.TEST_USER,
            token_type = token.TYPE.IMPERSONATION,
            impersonation_level = token.LEVEL.IMPERSONATION,
            groups = groups_of({ EVERYONE, AU, BA }),
            privs_present = NOTIFY, privs_enabled = NOTIFY,
        }))
        -- As SYSTEM: the owned name is refused, its siblings are not.
        publish(w, { record(owned .. ".sys"), record(open .. ".a"), record(owned .. ".sys2"), record(open .. ".b") })
        -- As the owner: the same name is accepted.
        assert(token.impersonate(w, user).ret == 0)
        publish(w, { record(owned .. ".user") })
        token.revert(w)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
    witness()
    t:assert_eq(series(open .. ".a") + series(open .. ".b"), 2, "the authorized siblings are stored")
    t:assert_eq(series(owned .. ".sys") + series(owned .. ".sys2"), 0, "the records SYSTEM may not publish are not")
    t:assert_eq(series(owned .. ".user"), 1, "and the same name sent under the owner's token is")
end)

test("publication authorizes the name, not its labels or value", {
    spec = "eventd *writepath.publication-authorizes-the-metric-name-not-its-labels-or-value",
}, function(t)
    local name = eventd.marker("ptlabels")
    -- Descriptors named like the labels, refusing everyone: not consulted.
    eventd.put_descriptor(vm, "Metrics", "core", DENY_ALL)
    eventd.put_descriptor(vm, "Metrics", "device", DENY_ALL)
    local list = {}
    for i = 1, 12 do
        list[i] = { name = name, type = "gauge", value = (i % 2 == 0) and -i * 1.5 or i * 1e9,
                    labels = { core = tostring(i), device = "d" .. i } }
    end
    publish(vm, list)
    wait_series(name, 12, "twelve label sets under one authorized name")
    t:assert_eq(series(name), 12, "every label set and value under the authorized name is stored")
    vm:run("reg del -r -y '" .. eventd.key_of("Metrics","core") .. "'")
    vm:run("reg del -r -y '" .. eventd.key_of("Metrics","device") .. "'")
end)

-- ---------------------------------------------------------------------------
-- The publication cache
-- ---------------------------------------------------------------------------

-- A persistent sending socket (`eventd.sender`), KACS_SO_PASS_TOKEN set
-- once, as §7.6 has a metric producer do: KACS reuses the captured token
-- object while the socket keeps one effective identity, so its datagrams
-- share a token_id. (A fresh socket per datagram, as `eventd.send_metric`
-- makes, is a fresh capture each time.)

test("a recurring name is checked once per token and name, denials included", {
    spec = "eventd *writepath.publication-is-not-checked-per-sample-or-per-datagram"
        .. " eventd *writepath.the-publication-cache-is-keyed-by-token-id-modified-id-and-metric-name"
        .. " eventd *writepath.a-publication-cache-hit-performs-no-allocation-and-no-accesscheck"
        .. " eventd *writepath.the-publication-cache-stores-denials-as-well-as-grants",
}, function(t)
    local p = eventd.marker("ptpcache")
    local ctx = audited(p, { allow(READ | PUBLISH, SY), allow(PUBLISH, BA) })
    local s = eventd.sender(vm)
    local base = settled(ctx)

    for _ = 1, 5 do s.send({ record(p .. ".a"), record(p .. ".a") }) end
    witness()
    t:assert_eq(settled(ctx) - base, 1, "ten samples in five datagrams of one name: one check")
    for _ = 1, 3 do s.send({ record(p .. ".b") }) end
    witness()
    t:assert_eq(settled(ctx) - base, 2, "another name: one more")

    -- Denials: a name nobody may publish, sent five times.
    eventd.put_descriptor(vm, "Metrics", p .. ".d", access.simple({ allow(PUBLISH, NOBODY) }, { sacl = AUDIT }))
    local dctx = p .. ".d"
    wait_until(function()
        publish(vm, { record(p .. ".d.probe" .. eventd.marker("x")) })
        witness()
        return audits(dctx) > 0
    end, { timeout = 30, desc = "the denying descriptor" })
    local dbase = settled(dctx)
    for _ = 1, 5 do s.send({ record(p .. ".d.same") }) end
    witness()
    t:assert_eq(settled(dctx) - dbase, 1, "five denied datagrams of one name: one check")
    t:assert_eq(series(p .. ".d.same"), 0, "and none stored")
    s.close()

    -- Another token, and the same token once modified: each a new key.
    local now = settled(ctx)
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local shutdown = token.bit(token.PRIV.SHUTDOWN)
        local other = assert(token.mint(w, {
            user_sid = token.SID.TEST_USER, token_type = token.TYPE.IMPERSONATION,
            impersonation_level = token.LEVEL.IMPERSONATION,
            groups = groups_of({ EVERYONE, AU, BA }),
            privs_present = NOTIFY | shutdown, privs_enabled = NOTIFY,
        }))
        local ws = eventd.sender(w)
        local function as_other()
            assert(token.impersonate(w, other).ret == 0)
            ws.send({ record(p .. ".a") })
            token.revert(w)
            witness()
        end
        as_other(); as_other()
        t:assert_eq(settled(ctx) - now, 1, "a second token publishing the same name: one check, then hits")
        -- modified_id is not exercised: enabling a privilege on the
        -- impersonated token between sends on the same socket did not
        -- produce a new check, because KACS went on conveying the capture
        -- it already held for that socket, so eventd never saw a changed
        -- modified_id.
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

test("MetricAuthorizationCacheSize bounds the cache, and a full cache is cleared, not evicted", {
    spec = "eventd *writepath.metricauthorizationcachesize-bounds-the-publication-cache-entry-count"
        .. " eventd *writepath.a-full-publication-cache-is-cleared-rather-than-evicted",
}, function(t)
    local p = eventd.marker("ptbound")
    eventd.set(vm, "MetricAuthorizationCacheSize", "dword:256"):assert_ok()
    local ok, err = pcall(function()
        vm:run("sleep 2")
        local ctx = audited(p, { allow(READ | PUBLISH, SY) })
        -- From here on nothing but these names is published under the
        -- console's token, so the cache holds only what this test puts in
        -- it; each step waits for the audit count to settle instead of
        -- sending a witness, which would be one more entry.
        local s = eventd.sender(vm)
        local function n(i) return p .. ".n" .. i end
        local fill = {}
        for i = 0, 255 do fill[#fill + 1] = record(n(i)) end
        -- The first fill may straddle a clear caused by entries made before
        -- it; after the second, the cache is exactly n0..n255.
        s.send(fill); settled(ctx)
        s.send(fill)
        local base = settled(ctx)
        s.send(fill)
        t:assert_eq(settled(ctx) - base, 0, "256 names fit in a cache of 256: all hits")

        s.send({ record(n(0)) })
        t:assert_eq(settled(ctx) - base, 0, "n0, cached and just used: a hit")
        s.send({ record(n(256)) })
        t:assert_eq(settled(ctx) - base, 1, "a 257th name is checked; the cache was full")
        s.send({ record(n(0)) })
        t:assert_eq(settled(ctx) - base, 2,
            "n0, the most recently used, was dropped too: the cache was cleared, not evicted")
        s.send({ record(n(1)) })
        t:assert_eq(settled(ctx) - base, 3, "and so was everything else")
        s.close()
    end)
    eventd.unset(vm, "MetricAuthorizationCacheSize")
    if not ok then error(err, 0) end
end)

test("any change under the security subtree clears the publication verdicts before reuse", {
    spec = "eventd *writepath.a-security-registry-change-clears-publication-verdicts-before-reuse",
}, function(t)
    local p = eventd.marker("ptgen")
    local ctx = audited(p, { allow(READ | PUBLISH, SY) })
    local s = eventd.sender(vm)
    s.send({ record(p .. ".r") }); witness()
    local base = settled(ctx)
    s.send({ record(p .. ".r") }); witness()
    t:assert_eq(settled(ctx) - base, 0, "a cached name is a hit")
    -- An unrelated descriptor, in another namespace.
    eventd.put_descriptor(vm, "Events", eventd.marker("ptunrelated"), access.simple({ allow(READ, SY) }))
    vm:run("sleep 1")
    s.send({ record(p .. ".r") }); witness()
    t:assert_eq(settled(ctx) - base, 1, "after an unrelated security change the name is checked again")
    s.close()
end)

-- ---------------------------------------------------------------------------
-- Rejected input
-- ---------------------------------------------------------------------------

test("rejected metric input leaves no event and no log record behind", {
    spec = "eventd *writepath.rejected-input-produces-no-durable-event-or-log-record",
}, function(t)
    local function eventd_logs()
        return #eventd.rows(vm, "LOGS FROM eventd SINCE 1h ago TAKE 100000")
    end
    local function eventd_events()
        return #eventd.rows(vm, 'EVENTS eventd.* SINCE 1h ago TAKE 100000')
    end
    local logs, events = eventd_logs(), eventd_events()
    local name = eventd.marker("ptreject")
    eventd.put_descriptor(vm, "Metrics", name .. ".denied", DENY_ALL)
    for _ = 1, 20 do
        eventd.send_metric(vm, record(name .. ".anon"), { pass_token = false })
        publish(vm, { record(name .. ".denied") })
        send_with_control(eventd.msgpack(record(name .. ".fd")))
    end
    witness()
    vm:run("sleep 2")
    t:assert_eq(eventd_logs(), logs, "sixty rejected datagrams wrote no log line")
    t:assert_eq(eventd_events(), events, "and no event")
end)

--- SIGQUIT eventd, start it again, and return the metric_ingress counters
--- of the dump it wrote on the way out.
local function dump_counters()
    local d = eventd.quit_dump(vm)
    local line = assert(d.lines.metric_ingress,
        "the dump's metric_ingress line: " .. table.concat(d.messages, " | "))
    local out = {}
    for k, v in line:gmatch("([%w_]+)=(%d+)") do out[k] = tonumber(v) end
    return out, line
end

test("rejected metric input is counted in memory, and only there", {
    spec = "eventd *writepath.rejected-metric-input-increments-in-memory-diagnostic-counters",
}, function(t)
    -- A clean slate: a start's counters are zero.
    dump_counters()
    local name = eventd.marker("ptcount")
    eventd.put_descriptor(vm, "Metrics", name .. ".denied", DENY_ALL)
    for _ = 1, 3 do eventd.send_metric(vm, record(name .. ".anon"), { pass_token = false }) end
    for _ = 1, 4 do publish(vm, { record(name .. ".denied") }) end
    for _ = 1, 2 do
        send_with_control(eventd.msgpack(record(name .. ".fd")))
    end
    witness()
    local c, line = dump_counters()
    t:assert_eq(c.missing_identity, 3, "three datagrams without a token: " .. line)
    t:assert_eq(c.unauthorized_records, 4, "four denied records: " .. line)
    t:assert_eq(c.truncated, 2, "two truncated datagrams: " .. line)
    local after, line2 = dump_counters()
    t:assert(after.missing_identity == 0 and after.unauthorized_records == 0 and after.truncated == 0,
        "and a restart starts them from zero again: " .. line2)
end)

--- Boot a machine of its own (`name`; a machine booted in a test is shut
--- down when the test ends), narrow its metric socket to `narrowed`,
--- restart eventd, and assert it comes back with its own descriptor.
local function narrow_and_restart(t, name, narrowed)
    local own = eventd.boot({ name = name })
    local before = dacl(eventd.SOCKET.metric, own)
    own:run("sd set " .. eventd.SOCKET.metric .. " '" .. narrowed .. "'"):assert_ok()
    t:assert_eq(dacl(eventd.SOCKET.metric, own), narrowed, "an operator narrowed the socket")
    local pid = eventd.pid(own)
    own:run("svctl restart eventd", { timeout = 60 })
    local up = wait_until(function()
        local now = eventd.pid(own)
        return now ~= nil and now ~= pid and eventd.query(own, "EVENTS TAKE 1").ok
    end, { timeout = 45, interval = 1, desc = "eventd to start again" })
    t:assert(up, "eventd starts again")
    t:assert_eq(dacl(eventd.SOCKET.metric, own), before, "and has put its own descriptor back")
end

test("eventd sets the metric socket's descriptor again each time it starts", {
    spec = "eventd *writepath.eventd-sets-the-metric-socket-descriptor-again-at-each-start",
}, function(t)
    -- Narrowed to SYSTEM, keeping the owner's ACE eventd manages its
    -- socket by (eventd/src/datagram.rs:22-27).
    narrow_and_restart(t, "ev-write-sock", "D:P(A;;GA;;;SY)(A;;GA;;;OW)")
end)

--- On a machine of its own (`name`), with eventd made Normal / RestartPolicy
--- Never: narrow `socket` to `narrowed`, kill eventd so the socket is left
--- behind as it is, start eventd again, and assert it comes up with its own
--- descriptor back on the socket.
local function narrow_stale_and_start(t, name, socket, narrowed)
    local own = eventd.boot({ name = name, noncritical = true })
    local before = dacl(socket, own)
    own:run("sd set " .. socket .. " '" .. narrowed .. "'"):assert_ok()
    t:assert_eq(dacl(socket, own), narrowed, "an operator narrowed the socket")
    eventd.crash(own)
    eventd.settle(own)
    t:assert(own:run("test -S " .. socket).exit_code == 0, "the killed eventd left the socket behind")
    t:assert_eq(dacl(socket, own), narrowed, "still narrowed")
    own:run("svctl start eventd")
    local up = pcall(wait_until, function() return eventd.query(own, "EVENTS TAKE 1").ok end,
        { timeout = 45, interval = 1, desc = "eventd to start again" })
    t:assert(up, "eventd starts again over the narrowed leftover: " .. own:run("svctl --json status eventd").stdout)
    t:assert_eq(dacl(socket, own), before, "and its own descriptor is on the socket")
end

-- §8.2: "Recognising a leftover socket needs no right on it. If an operator
-- has narrowed its descriptor so that eventd may no longer delete it,
-- eventd, which still owns it, first puts its own descriptor back, so a
-- narrowed socket never stops the next start." Narrowed so that eventd's
-- own service SID is not in the DACL (no OW ACE): eventd, the socket's
-- owner, keeps only the owner's implicit READ_CONTROL | WRITE_DAC.
test("a metric socket narrowed past eventd's own access does not stop eventd starting", {
    spec = "eventd *bootstrap.a-stale-socket-eventd-may-no-longer-delete-is-reclaimed-first",
}, function(t)
    narrow_stale_and_start(t, "ev-write-sock2", eventd.SOCKET.metric, "D:P(A;;GA;;;SY)")
end)

test("so does a query socket narrowed the same way", {
    spec = "eventd *bootstrap.a-stale-socket-eventd-may-no-longer-delete-is-reclaimed-first",
}, function(t)
    narrow_stale_and_start(t, "ev-write-sock3", eventd.SOCKET.query, "D:P(A;;GA;;;SY)")
end)
