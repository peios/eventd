-- eventd TRM §7.2 — patterns and descriptors: the three namespaces,
-- dot-delimited prefix matching, the walk from an identifier to `*`, where
-- descriptors are stored and who may change them, the defaults eventd
-- provisions (each with a failure-audit SACL for Everyone, PEI-1279), the
-- upgrade of a former default, conditional ACEs, and the administrative
-- descriptor that governs INDEX.
--
-- One file-scope eventd serves every test. Most tests write descriptors
-- only for their own marker-named identifiers. The ones that must change
-- a default (`*`, Admin) save its bytes first and put them back whatever
-- the outcome, and the ones about provisioning restart the service, which
-- re-runs `eventd --prepare-security` (the SYSTEM ExecStartPre hook that
-- writes missing defaults).
--
-- The agent is SYSTEM and an Administrator. A descriptor granting only a
-- group nobody holds refuses it, which is how every denial here is made.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
local sys = require("helpers.sys")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-pattern" })

-- ---------------------------------------------------------------------------
-- Descriptors
-- ---------------------------------------------------------------------------

local SY, BA, AU = token.SID.LOCAL_SYSTEM, token.SID.ADMINISTRATORS, token.SID.AUTHENTICATED_USERS
local EVERYONE = token.SID.EVERYONE
local NOBODY = token.SID.TEST_GROUP_2
local GROUP = token.SID.TEST_GROUP
local READ, ADMINISTER, PUBLISH = 0x1, 0x4, 0x8
local ALLOWED = access.ACE.ALLOWED

local function allow(mask, sid) return access.ace(ALLOWED, mask, sid) end
local function descriptor(aces, opts) return access.simple(aces, opts) end
local DENY_ALL = descriptor({ allow(READ | PUBLISH, NOBODY) })

local function put(ns, pattern, sd) eventd.put_descriptor(vm, ns, pattern, sd) end
local function put_hex(ns, pattern, h) eventd.put_descriptor(vm, ns, pattern, eventd.unhex(h)) end
local function drop(ns, pattern) eventd.drop_descriptor(vm, ns, pattern) end
local function get_hex(ns, pattern) return eventd.descriptor_hex(vm, ns, pattern) end

--- Run `body`, then `restore` whatever happened, then re-raise.
local function finally(body, restore)
    local ok, err = pcall(body)
    restore()
    if not ok then error(err, 0) end
end

local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)

local function groups_of(sids)
    local list = {}
    for i, sid in ipairs(sids) do list[i] = { sid = sid, attributes = ENABLED } end
    return list
end

local function mint(who, sids)
    local fd, e = token.mint(who, {
        user_sid = token.SID.TEST_USER,
        token_type = token.TYPE.IMPERSONATION,
        impersonation_level = token.LEVEL.IMPERSONATION,
        groups = groups_of(sids), privs_present = NOTIFY, privs_enabled = NOTIFY,
    })
    assert(fd, "mint: " .. sys.errname(e or 0))
    return fd
end

-- ---------------------------------------------------------------------------
-- Records
-- ---------------------------------------------------------------------------

local function emit(ty, payload)
    local r = eventd.emit(vm, ty, payload or { n = 1 })
    assert(r.ret == 0, "kmes_emit " .. ty .. ": errno " .. tostring(r.errno))
end

local function log(origin)
    local r = eventd.send_log(vm, { origin = origin, is_error = false, message = "m " .. origin })
    assert(r.ret and r.ret > 0, "send_log " .. origin .. ": errno " .. tostring(r.errno))
end

local function metric(name)
    local r = eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    assert(r.ret and r.ret > 0, "send_metric " .. name .. ": errno " .. tostring(r.errno))
end

local Q = {
    events = function(ty) return "EVENTS " .. ty .. " SINCE 1h ago TAKE 1000" end,
    logs = function(origin) return "LOGS FROM " .. origin .. " SINCE 1h ago TAKE 1000" end,
    metric = function(name) return "METRIC " .. name .. " SINCE 1h ago" end,
}

--- How many records of `ty`/`origin`/`name` the console sees.
local function seen(kind, id)
    local r = eventd.query(vm, Q[kind](id))
    assert(r.ok, "query failed: " .. r.stderr)
    return #r.rows
end

--- Wait for the console to see exactly `n`; true when it does.
local function settles(kind, id, n, desc)
    return wait_until(function() return seen(kind, id) == n end,
        { timeout = 15, interval = 0.25, desc = desc or (kind .. " " .. id .. " = " .. n) })
end

-- ---------------------------------------------------------------------------
-- Namespaces and matching
-- ---------------------------------------------------------------------------

test("events, logs and metrics are three namespaces, keyed by type, origin and name", {
    spec = "eventd *pattern.each-data-type-has-an-independent-pattern-namespace"
        .. " eventd *pattern.event-patterns-match-the-event-type"
        .. " eventd *pattern.log-patterns-match-the-log-origin"
        .. " eventd *pattern.metric-patterns-match-the-metric-name",
}, function(t)
    -- One string, used as an event type, a log origin and a metric name.
    local id = eventd.marker("ptns")
    emit(id); log(id); metric(id)
    settles("events", id, 1); settles("logs", id, 1); settles("metric", id, 1)

    put("Events", id, DENY_ALL)
    settles("events", id, 0, "Events\\" .. id .. " to govern the event type")
    t:assert_eq(seen("logs", id), 1, "a descriptor under Events does not touch the log origin")
    t:assert_eq(seen("metric", id), 1, "nor the metric name")

    put("Logs", id, DENY_ALL)
    drop("Events", id)
    settles("logs", id, 0, "Logs\\" .. id .. " to govern the origin")
    settles("events", id, 1, "the event type to answer to * again")
    t:assert_eq(seen("metric", id), 1, "the metric is still readable")

    put("Metrics", id, DENY_ALL)
    drop("Logs", id)
    settles("metric", id, 0, "Metrics\\" .. id .. " to govern the name")
    settles("logs", id, 1, "the origin to answer to * again")
    t:assert_eq(seen("events", id), 1, "and the event type is untouched")
    drop("Metrics", id)
end)

test("a pattern matches its own string and dotted descendants, never a longer word", {
    spec = "eventd *pattern.a-pattern-matches-by-dot-delimited-prefix"
        .. " eventd *pattern.a-prefix-match-must-end-at-a-dot-or-the-end-of-the-string",
}, function(t)
    local base = eventd.marker("ptpfx")
    local types = {
        [base] = 0, [base .. ".sub"] = 0, [base .. ".sub.deeper"] = 0,
        [base .. "_extended"] = 1, [base .. "foo"] = 1,
    }
    for ty in pairs(types) do emit(ty) end
    for ty in pairs(types) do settles("events", ty, 1) end
    put("Events", base, DENY_ALL)
    settles("events", base, 0, "the pattern to apply to its own string")
    for ty, n in pairs(types) do
        t:assert_eq(seen("events", ty), n, ty .. (n == 0
            and " is matched by the pattern " or " is not matched by the pattern ") .. base)
    end
    drop("Events", base)
end)

test("the wildcard pattern matches every identifier no other pattern does", {
    spec = "eventd *pattern.the-wildcard-pattern-matches-everything",
}, function(t)
    local saved = assert(get_hex("Events", "*"))
    local loose, kept = eventd.marker("ptwild"), eventd.marker("ptwildkept")
    emit(loose); emit(kept)
    settles("events", loose, 1); settles("events", kept, 1)
    put("Events", kept, descriptor({ allow(READ, SY) }))
    finally(function()
        put("Events", "*", DENY_ALL)
        settles("events", loose, 0, "the narrowed * to reach a type no other pattern names")
        t:assert_eq(seen("events", eventd.T.startup), 0, "nor any other unclaimed type")
        t:assert_eq(seen("events", kept), 1, "while a type with its own pattern is unaffected")
    end, function()
        put_hex("Events", "*", saved)
        drop("Events", kept)
    end)
    settles("events", loose, 1, "the default * to be back")
end)

test("an origin's producer is ignored: service producers and every job answer to one descriptor", {
    spec = "eventd *pattern.an-origin-with-a-producer-resolves-from-the-part-before-the-slash"
        .. " eventd *pattern.service-producers-use-the-service-descriptor-and-all-jobs-use-logs-jobs"
        .. " eventd *pattern.a-slash-never-enters-a-descriptor-path",
}, function(t)
    local svc = eventd.marker("ptsvc")
    local producer = svc .. "/HealthCheck"
    log(svc); log(producer)
    settles("logs", svc, 1); settles("logs", producer, 1)

    put("Logs", svc, DENY_ALL)
    settles("logs", producer, 0, "the service's descriptor to govern its producer")
    t:assert_eq(seen("logs", svc), 0, "and the service itself")

    -- Grants written where a producer would land if the slash entered
    -- the path — a subkey, or a key whose name carries the slash — are
    -- never consulted.
    put("Logs", svc .. "\\HealthCheck", descriptor({ allow(READ, SY) }))
    put("Logs", svc .. "/HealthCheck", descriptor({ allow(READ, SY) }))
    vm:run("sleep 1")
    t:assert_eq(seen("logs", producer), 0,
        "the producer still answers to " .. svc .. ", not to a key below or beside it")
    drop("Logs", svc .. "/HealthCheck")
    drop("Logs", svc)
    settles("logs", producer, 1, "the producer to answer to * again")

    -- Every submitted job logs as jobs/<id>, and all of them answer to
    -- Logs\jobs.
    local word = eventd.marker("ptjob")
    vm:run("svctl --json job submit /bin/echo " .. word, { timeout = 60 }):assert_ok()
    local origin
    eventd.wait_rows(vm, 'LOGS WHERE origin STARTS_WITH "jobs/" SINCE 1h ago TAKE 1000',
        function(rs)
            for _, r in ipairs(rs) do
                if r.message == word then origin = r.origin; return true end
            end
            return false
        end, { desc = "the job's output" })
    t:assert(origin and origin:match("^jobs/"), "the job's origin is jobs/<id>: " .. tostring(origin))
    finally(function()
        put("Logs", "jobs", DENY_ALL)
        settles("logs", origin, 0, "Logs\\jobs to govern " .. origin)
    end, function() drop("Logs", "jobs") end)
end)

test("resolution tries the identifier, then each shorter prefix, then *, and the first match wins", {
    spec = "eventd *pattern.resolution-walks-up-the-hierarchy-to-the-wildcard"
        .. " eventd *pattern.resolution-tries-the-full-identifier-first"
        .. " eventd *pattern.resolution-then-drops-the-last-dot-separated-component"
        .. " eventd *pattern.resolution-falls-back-to-the-wildcard-last"
        .. " eventd *pattern.the-most-specific-matching-pattern-wins",
}, function(t)
    local base = eventd.marker("ptwalk")
    local ty = base .. ".a.b"
    emit(ty)
    settles("events", ty, 1, "no pattern of its own: the wildcard grants it")

    put("Events", base, DENY_ALL)
    settles("events", ty, 0, "a pattern two components up is found")
    put("Events", base .. ".a", descriptor({ allow(READ, SY) }))
    settles("events", ty, 1, "dropping one component finds " .. base .. ".a before " .. base)
    put("Events", ty, DENY_ALL)
    settles("events", ty, 0, "the full identifier is tried before any prefix")

    drop("Events", ty)
    settles("events", ty, 1, "with the exact pattern gone, the next prefix decides again")
    drop("Events", base .. ".a")
    settles("events", ty, 0, "and then the next")
    drop("Events", base)
    settles("events", ty, 1, "and with none left, * decides last")
end)

test("only the default value of a key under Machine\\System\\eventd\\Security is a descriptor", {
    spec = "eventd *pattern.descriptors-are-registry-values-under-the-eventd-security-subtree",
}, function(t)
    local ty = eventd.marker("ptstore")
    emit(ty)
    settles("events", ty, 1)
    -- A named value beside where the descriptor would go, and a default
    -- value at the same relative path outside the Security subtree.
    vm:run("reg set -p '" .. eventd.key_of("Events", ty) .. "' Descriptor hex:" .. eventd.hex(DENY_ALL)):assert_ok()
    vm:run("reg set -p '" .. eventd.KEY .. "\\Events\\" .. ty .. "' @ hex:" .. eventd.hex(DENY_ALL)):assert_ok()
    vm:run("sleep 1")
    t:assert_eq(seen("events", ty), 1, "neither a named value nor a key outside Security is consulted")
    put("Events", ty, DENY_ALL)
    settles("events", ty, 0, "the default value of Security\\Events\\" .. ty .. " is")
    drop("Events", ty)
    vm:run("reg del -r -y '" .. eventd.KEY .. "\\Events'")
end)

-- ---------------------------------------------------------------------------
-- Defaults
-- ---------------------------------------------------------------------------

test("a missing wildcard default denies every identifier of its type that has no pattern", {
    spec = "eventd *pattern.a-missing-wildcard-default-denies-all-data-of-that-type",
}, function(t)
    local id = eventd.marker("ptnodef")
    emit(id); log(id); metric(id)
    settles("events", id, 1); settles("logs", id, 1); settles("metric", id, 1)
    for _, case in ipairs({ { "Events", "events" }, { "Logs", "logs" }, { "Metrics", "metric" } }) do
        local ns, kind = case[1], case[2]
        local saved = assert(get_hex(ns, "*"))
        finally(function()
            drop(ns, "*")
            settles(kind, id, 0, "without " .. ns .. "\\* the " .. kind .. " " .. id .. " is denied")
        end, function() put_hex(ns, "*", saved) end)
        settles(kind, id, 1, ns .. "\\* to be back")
    end
end)

--- The default descriptors, as §7.2's table gives them.
local DEFAULTS = {
    { "Events", "*", { { SY, READ }, { BA, READ } } },
    { "Logs", "*", { { SY, READ }, { BA, READ }, { AU, READ } } },
    { "Metrics", "*", { { SY, READ | PUBLISH }, { BA, READ | PUBLISH }, { AU, READ } } },
    { "Metrics", "eventd", { { SY, READ }, { BA, READ }, { AU, READ } } },
    { nil, "Admin", { { SY, ADMINISTER }, { BA, ADMINISTER } } },
}

--- `nil` when the value at ns\pattern is exactly an allow-ACE DACL
--- granting `want` ({sid, mask} pairs, in any order); otherwise why not.
local function differs(ns, pattern, want)
    local h = get_hex(ns, pattern)
    if not h then return "no descriptor at " .. eventd.key_of(ns, pattern) end
    local sd = access.parse_sd(eventd.unhex(h))
    if not sd.dacl then return "no DACL" end
    local got = {}
    for _, ace in ipairs(sd.dacl.aces) do
        if ace.type ~= ALLOWED then return "an ACE of type " .. ace.type end
        got[#got + 1] = token.sid_string(ace.sid) .. "=" .. string.format("0x%x", ace.mask)
    end
    local expect = {}
    for _, pair in ipairs(want) do
        expect[#expect + 1] = token.sid_string(pair[1]) .. "=" .. string.format("0x%x", pair[2])
    end
    table.sort(got); table.sort(expect)
    local a, b = table.concat(got, " "), table.concat(expect, " ")
    if a ~= b then return "grants " .. a .. ", not " .. b end
    return nil
end

test("eventd creates the default descriptors when they do not exist", {
    spec = "eventd *pattern.missing-default-descriptors-are-created",
}, function(t)
    local saved = {}
    for _, d in ipairs(DEFAULTS) do saved[d[2] .. tostring(d[1])] = get_hex(d[1], d[2]) end
    finally(function()
        for _, d in ipairs(DEFAULTS) do drop(d[1], d[2]) end
        eventd.restart(vm)
        for _, d in ipairs(DEFAULTS) do
            local why = differs(d[1], d[2], d[3])
            t:assert(not why, eventd.key_of(d[1], d[2]) .. " was recreated with its default: " .. tostring(why))
        end
    end, function()
        for _, d in ipairs(DEFAULTS) do
            local h = saved[d[2] .. tostring(d[1])]
            if h and not get_hex(d[1], d[2]) then put_hex(d[1], d[2], h) end
        end
    end)
end)

test("the default Events descriptor grants EVENTD_READ to SYSTEM and Administrators", {
    spec = "eventd *pattern.the-default-events-descriptor-grants-read-to-system-and-administrators",
}, function(t)
    local d = DEFAULTS[1]
    local why = differs(d[1], d[2], d[3])
    t:assert(not why, "Events\\*: " .. tostring(why))
    -- And as a caller sees it: an Administrator who is not SYSTEM reads
    -- an event type that has no pattern of its own.
    local ty = eventd.marker("ptdefev")
    emit(ty)
    settles("events", ty, 1)
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local out = eventd.rq.ask(w, Q.events(ty), { as = mint(w, { EVERYONE, AU, BA }), timeout = 20 })
        t:assert_eq(#out.records, 1, "an Administrator reads it: " .. tostring(out.error)
            .. " " .. tostring(out.connect_error))
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

test("the default Logs descriptor also grants EVENTD_READ to Authenticated Users", {
    spec = "eventd *pattern.the-default-logs-descriptor-also-grants-read-to-authenticated-users",
}, function(t)
    local d = DEFAULTS[2]
    local why = differs(d[1], d[2], d[3])
    t:assert(not why, "Logs\\*: " .. tostring(why))
end)

test("the default Metrics descriptor grants EVENTD_PUBLISH only to SYSTEM and Administrators", {
    spec = "eventd *pattern.the-default-metrics-descriptor-grants-publish-only-to-system-and-administrators",
}, function(t)
    local d = DEFAULTS[3]
    local why = differs(d[1], d[2], d[3])
    t:assert(not why, "Metrics\\*: " .. tostring(why))
end)

test("the default Metrics\\eventd descriptor grants EVENTD_PUBLISH to nobody", {
    spec = "eventd *pattern.the-default-eventd-metrics-descriptor-grants-publish-to-nobody",
}, function(t)
    local d = DEFAULTS[4]
    local why = differs(d[1], d[2], d[3])
    t:assert(not why, "Metrics\\eventd: " .. tostring(why))
end)

test("the default Admin descriptor grants EVENTD_ADMINISTER to SYSTEM and Administrators", {
    spec = "eventd *pattern.the-default-admin-descriptor-grants-administer-to-system-and-administrators",
}, function(t)
    local d = DEFAULTS[5]
    local why = differs(d[1], d[2], d[3])
    t:assert(not why, "Admin: " .. tostring(why))
end)

--- Metric series by name, read out of the store itself.
local function series(name)
    return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name = '"
        .. name .. "'")[1][1]
end

test("a service may publish nothing until a more specific pattern grants its service SID", {
    spec = "eventd *pattern.publication-is-fail-closed-for-services-without-a-specific-grant",
}, function(t)
    local service_sid = token.sid(5, 80, 11, 22, 33, 44, 55)
    local name = eventd.marker("ptsvcpub")
    local function publish(n)
        -- A fresh spec each time: token.mint stamps its logon session in.
        local spec = {
            user_sid = service_sid,
            groups = groups_of({ EVERYONE, AU, token.sid(5, 6) }),
            privs_present = NOTIFY, privs_enabled = NOTIFY,
        }
        token.as_principal(t, vm, spec, function(w)
            local r = eventd.send_metric(w, { name = n, type = "gauge", value = 1 })
            t:assert(r.ret and r.ret > 0, "the service reaches the metric socket: errno "
                .. tostring(r.errno))
        end)
    end
    publish(name .. ".before")
    -- A witness from SYSTEM, sent after: once it is stored, the service's
    -- sample has been decided.
    local witness = eventd.marker("ptsvcw")
    metric(witness)
    wait_until(function() return series(witness) == 1 end, { timeout = 15, desc = "the witness" })
    t:assert_eq(series(name .. ".before"), 0, "under the wildcard alone the service publishes nothing")

    put("Metrics", name, descriptor({ allow(PUBLISH, service_sid) }))
    local ok = wait_until(function()
        publish(name .. ".after")
        return series(name .. ".after") == 1
    end, { timeout = 20, desc = "the service's own grant to apply" })
    t:assert(ok, "a pattern granting its service SID EVENTD_PUBLISH lets it publish")
    drop("Metrics", name)
end)

--- `bytes` (a self-relative descriptor laid out owner, group, SACL, DACL,
--- as libpeios writes one) without its SACL: what the same SDDL minus its
--- `S:` part parses to.
local function strip_sacl(bytes)
    local rev, sbz, control, owner_off, group_off, sacl_off, dacl_off =
        string.unpack("<I1I1I2I4I4I4I4", bytes)
    if sacl_off == 0 then return bytes end
    assert(dacl_off > sacl_off, "the SACL sits before the DACL")
    return string.pack("<I1I1I2I4I4I4I4", rev, sbz, control & ~access.CONTROL.SACL_PRESENT,
        owner_off, group_off, 0, sacl_off) .. bytes:sub(21, sacl_off) .. bytes:sub(dacl_off + 1)
end

-- PEI-1279 item 5: (AU;FA;0xd;;;WD) on every default, so a denial under
-- one is audited whoever asked, and nothing else in the SACL.
test("every default descriptor audits every failed access, by anyone, at every eventd right", {
    spec = "eventd *pattern.every-default-descriptor-audits-every-failed-access-by-everyone",
}, function(t)
    for _, d in ipairs(DEFAULTS) do
        local where = eventd.key_of(d[1], d[2])
        local h = get_hex(d[1], d[2])
        t:assert(h, where .. " exists")
        local sd = h and access.parse_sd(eventd.unhex(h))
        local aces = sd and sd.sacl and sd.sacl.aces or {}
        t:assert_eq(#aces, 1, where .. " has a SACL of exactly one ACE")
        local ace = aces[1] or {}
        t:assert_eq(ace.type, access.ACE.AUDIT, where .. ": a SYSTEM_AUDIT ACE")
        t:assert_eq(ace.flags, access.ACE_FLAG.FAILED_ACCESS, where .. ": for failure only")
        t:assert_eq(ace.mask, READ | ADMINISTER | PUBLISH, where .. ": at every eventd right")
        t:assert(ace.sid == EVERYONE, where .. ": for Everyone")
    end
end)

test("a former default is upgraded only when it matches byte for byte", {
    spec = "eventd *pattern.a-former-default-is-replaced-only-on-an-exact-binary-match"
        .. " eventd *pattern.an-administrator-changed-descriptor-is-never-rewritten",
}, function(t)
    local metrics_now = assert(get_hex("Metrics", "*"))
    local events_now = assert(get_hex("Events", "*"))
    local logs_now = assert(get_hex("Logs", "*"))
    -- The defaults before PEI-1279 item 5 were today's without the SACL.
    local logs_former = eventd.hex(strip_sacl(eventd.unhex(logs_now)))
    t:assert(logs_former ~= logs_now, "today's Logs\\* differs from its former default")
    -- The Metrics wildcard's older default also differs in its masks: read
    -- (0x1) where SYSTEM and Administrators now hold read|publish (0x9).
    -- Patch exactly those two masks in the SACL-less bytes.
    local bytes = strip_sacl(eventd.unhex(metrics_now))
    local sd = access.parse_sd(bytes)
    t:assert_eq(#sd.dacl.aces, 3, "today's Metrics\\* has three ACEs")
    local legacy, edits = bytes, 0
    local dacl_at = string.unpack("<I4", bytes, 17)
    local at = dacl_at + 9
    for _ = 1, 3 do
        local size = string.unpack("<I2", legacy, at + 2)
        if string.unpack("<I4", legacy, at + 4) == 0x9 then
            legacy = legacy:sub(1, at + 3) .. string.pack("<I4", 0x1) .. legacy:sub(at + 8)
            edits = edits + 1
        end
        at = at + size
    end
    t:assert_eq(edits, 2, "two masks were the publish-granting ones")

    -- An administrator's Events\*: the default plus a grant of its own.
    local events_sd = access.parse_sd(eventd.unhex(events_now))
    local aces = {}
    for _, ace in ipairs(events_sd.dacl.aces) do aces[#aces + 1] = allow(ace.mask, ace.sid) end
    aces[#aces + 1] = allow(READ, GROUP)
    local custom_events = eventd.hex(access.sd({ owner = SY, group = SY, dacl = access.acl(aces),
        control = access.CONTROL.DACL_PROTECTED }))

    finally(function()
        put_hex("Metrics", "*", eventd.hex(legacy))
        put_hex("Logs", "*", logs_former)
        put_hex("Events", "*", custom_events)
        eventd.restart(vm)
        t:assert_eq(get_hex("Metrics", "*"), metrics_now,
            "the Metrics wildcard's older read-only default is replaced by today's")
        t:assert_eq(get_hex("Logs", "*"), logs_now,
            "a default without its SACL is replaced by today's, SACL and all")
        t:assert_eq(get_hex("Events", "*"), custom_events,
            "an administrator's Events\\* is left as written, though it has no SACL")

        -- One ACE more than the former default: not a byte-exact match.
        local near = eventd.unhex(eventd.hex(legacy))
        local nsd = access.parse_sd(near)
        local naces = {}
        for _, ace in ipairs(nsd.dacl.aces) do naces[#naces + 1] = allow(ace.mask, ace.sid) end
        naces[#naces + 1] = allow(READ, GROUP)
        local near_hex = eventd.hex(access.sd({ owner = SY, group = SY, dacl = access.acl(naces),
            control = access.CONTROL.DACL_PROTECTED }))
        put_hex("Metrics", "*", near_hex)
        eventd.restart(vm)
        t:assert_eq(get_hex("Metrics", "*"), near_hex,
            "a changed read-only wildcard is never treated as the default")
    end, function()
        put_hex("Metrics", "*", metrics_now)
        put_hex("Logs", "*", logs_now)
        put_hex("Events", "*", events_now)
    end)
end)

--- The ACEs of `reg sd --dacl` output as a sorted "flags;rights;sid" list,
--- with the protected flag; flags letter-sorted so "CIOI" and "OICI" agree.
local function dacl_of(key)
    local r = vm:run("reg sd '" .. key .. "' --dacl")
    if r.exit_code ~= 0 then return nil, r.stderr end
    local text = r.stdout:gsub("%s", "")
    local aces = {}
    for ty, flags, rights, sid in text:gmatch("%((%a+);([^;]*);([^;]*);[^;]*;[^;]*;([^)]*)%)") do
        local f = {}
        for i = 1, #flags, 2 do f[#f + 1] = flags:sub(i, i + 1) end
        table.sort(f)
        aces[#aces + 1] = ty .. ";" .. table.concat(f) .. ";" .. rights .. ";" .. sid
    end
    table.sort(aces)
    return { protected = text:find("D:P", 1, true) ~= nil, aces = aces, text = text }
end

--- A key-rights field as `reg sd` may print it, normalised to its code.
local function key_right(r)
    local codes = { ["0xf003f"] = "KA", ["0x20019"] = "KR" }
    return codes[r:lower()] or r
end

test("the security root gets a protected DACL while its own is wholly inherited, and keeps a chosen one", {
    spec = "eventd *pattern.the-security-root-gets-a-protected-dacl-while-its-dacl-is-wholly-inherited",
}, function(t)
    -- The image's Security key was created by prepare-security, inheriting
    -- from Machine\System\eventd, so eventd has restricted it.
    local d, err = dacl_of(eventd.SECURITY)
    t:assert(d, "reg sd reads the Security key: " .. tostring(err))
    t:assert(d and d.protected, "its DACL is protected: " .. tostring(d and d.text))
    local got = {}
    for _, a in ipairs(d and d.aces or {}) do
        local ty, flags, rights, sid = a:match("^([^;]*);([^;]*);([^;]*);(.*)$")
        got[#got + 1] = ty .. ";" .. flags .. ";" .. key_right(rights) .. ";" .. sid
    end
    table.sort(got)
    t:assert_eq(table.concat(got, " "), "A;CIOI;KA;BA A;CIOI;KA;SY A;CIOI;KR;AU",
        "SYSTEM and Administrators full control, Authenticated Users read, all inheritable")

    -- A DACL somebody chose is never replaced: one more read grant.
    local saved = d and d.text
    local chosen = "D:P(A;OICI;KA;;;SY)(A;OICI;KA;;;BA)(A;OICI;KR;;;AU)(A;OICI;KR;;;"
        .. token.sid_string(GROUP) .. ")"
    finally(function()
        vm:run("reg sd '" .. eventd.SECURITY .. "' --dacl --set '" .. chosen .. "'"):assert_ok()
        eventd.restart(vm)
        local after = dacl_of(eventd.SECURITY)
        t:assert_eq(after and #after.aces, 4, "prepare-security left the chosen DACL as it was: "
            .. tostring(after and after.text))
    end, function()
        if saved then vm:run("reg sd '" .. eventd.SECURITY .. "' --dacl --set '" .. saved .. "'") end
    end)
end)

-- The caveat of §7.2: the SACL travels inside the value, so the right to
-- set the value is the right to drop it. An Administrator's token without
-- SeSecurityPrivilege does exactly that.
test("whoever may write a descriptor value may also drop its SACL, with no SeSecurityPrivilege", {
    spec = "eventd *pattern.whoever-may-write-a-descriptor-value-may-also-drop-its-sacl",
}, function(t)
    local p = eventd.marker("ptsacldrop")
    put("Events", p, descriptor({ allow(READ, SY) }, {
        sacl = access.acl({ access.ace(access.ACE.AUDIT, READ, EVERYONE, access.ACE_FLAG.FAILED_ACCESS) }) }))
    local before = access.parse_sd(eventd.unhex(assert(get_hex("Events", p))))
    t:assert(before.sacl and #before.sacl.aces == 1, "the pattern starts with a SACL")
    local bare = eventd.hex(descriptor({ allow(READ, SY) }))
    local written
    token.as_principal(t, vm, {
        user_sid = token.SID.TEST_USER,
        groups = groups_of({ EVERYONE, AU, BA }),
        integrity_level = token.INTEGRITY.HIGH,
        privs_present = NOTIFY, privs_enabled = NOTIFY,
    }, function(w)
        written = w:run("/bin/reg", { args = { "set", "-p", eventd.key_of("Events", p), "@", "hex:" .. bare } })
    end)
    t:assert_eq(written and written.exit_code, 0, "an Administrator without SeSecurityPrivilege may set the value: "
        .. tostring(written and written.stderr))
    local after = access.parse_sd(eventd.unhex(assert(get_hex("Events", p))))
    t:assert(not after.sacl, "and the descriptor now has no SACL at all")
    drop("Events", p)
end)

-- ---------------------------------------------------------------------------
-- Conditional ACEs
-- ---------------------------------------------------------------------------

local function pad4(s) return s .. string.rep("\0", (-#s) % 4) end
local function utf16(s)
    local out = {}
    for i = 1, #s do out[i] = string.pack("<I2", s:byte(i)) end
    return table.concat(out)
end
--- `Member_of({sid})`.
local function member_of(sid)
    return pad4("artx" .. string.pack("<I1I4", 0x51, #sid) .. sid .. string.pack("<I1", 0x89))
end
--- `Exists @Local.<name>`, or its negation.
local function local_exists(name, negate)
    return pad4("artx" .. string.pack("<I1I4", 0xf8, #utf16(name)) .. utf16(name)
        .. string.pack("<I1", 0x87) .. (negate and string.pack("<I1", 0xa2) or ""))
end
local CALLBACK_ALLOWED = 0x09
local function allow_if(mask, sid, condition)
    return access.ace(CALLBACK_ALLOWED, mask, sid, 0, { condition = condition })
end

test("a conditional ACE is evaluated by KACS as it would be anywhere", {
    spec = "eventd *pattern.conditional-aces-are-evaluated-by-kacs-as-anywhere",
}, function(t)
    local ty = eventd.marker("ptcond")
    emit(ty)
    settles("events", ty, 1)
    put("Events", ty, descriptor({ allow_if(READ, SY, member_of(NOBODY)) }))
    settles("events", ty, 0, "Member_of a group SYSTEM lacks: the ACE does not apply")
    put("Events", ty, descriptor({ allow_if(READ, SY, member_of(BA)) }))
    settles("events", ty, 1, "Member_of Administrators, which SYSTEM is in: it does")
    drop("Events", ty)
end)

test("eventd passes no local claims, so a condition on one sees it absent", {
    spec = "eventd *pattern.no-local-claims-are-passed-to-accesscheck"
        .. " eventd *pattern.conditions-on-eventd-local-claims-observe-them-as-absent",
}, function(t)
    local ty = eventd.marker("ptlocal")
    emit(ty)
    settles("events", ty, 1)
    -- The claims §7.2 calls a plausible later addition: if eventd passed
    -- any of them, one of these would exist.
    for _, claim in ipairs({ "event_type", "origin", "time_of_day", "pattern" }) do
        put("Events", ty, descriptor({ allow_if(READ, SY, local_exists(claim)) }))
        settles("events", ty, 0, "Exists @Local." .. claim .. " is false")
        put("Events", ty, descriptor({ allow_if(READ, SY, local_exists(claim, true)) }))
        settles("events", ty, 1, "and !(Exists @Local." .. claim .. ") is true")
    end
    drop("Events", ty)
end)

-- ---------------------------------------------------------------------------
-- The administrative descriptor
-- ---------------------------------------------------------------------------

local ADMIN_DEFAULT = get_hex(nil, "Admin")

local function index(field)
    local r = eventd.query(vm, "EVENTS INDEX " .. field)
    return r.ok, r.stderr
end

local function index_settles(expect, desc)
    return wait_until(function() return (index("ptadminprobe")) == expect end,
        { timeout = 15, desc = desc })
end

local function restore_admin()
    put_hex(nil, "Admin", ADMIN_DEFAULT)
    index_settles(true, "the default Admin descriptor to be back")
end

test("INDEX is checked against Security\\Admin for EVENTD_ADMINISTER", {
    spec = "eventd *pattern.index-is-checked-against-the-admin-descriptor-for-eventd-administer",
}, function(t)
    local events_now = assert(get_hex("Events", "*"))
    finally(function()
        t:assert((index("ptadminfield")), "the default Admin descriptor permits SYSTEM to INDEX")
        -- Every right on the data itself, and none on Admin.
        put("Events", "*", descriptor({ allow(0x000F000F, SY), allow(READ, BA) }))
        put(nil, "Admin", descriptor({ allow(ADMINISTER, NOBODY) }))
        index_settles(false, "an Admin descriptor naming nobody to refuse INDEX")
        local ok, err = index("ptadminfield")
        t:assert(not ok, "INDEX is refused though Events\\* grants everything")
        t:assert(err:find("EVENTD_ADMINISTER", 1, true), "for want of EVENTD_ADMINISTER: " .. err)
        put(nil, "Admin", descriptor({ allow(ADMINISTER, SY) }))
        index_settles(true, "Admin granting SYSTEM administer to permit it")
    end, function()
        put_hex("Events", "*", events_now)
        restore_admin()
    end)
end)

test("INDEX is refused when there is no administrative descriptor", {
    spec = "eventd *pattern.index-is-refused-when-no-admin-descriptor-exists",
}, function(t)
    finally(function()
        drop(nil, "Admin")
        index_settles(false, "the missing Admin descriptor to refuse INDEX")
        t:assert(not (index("ptadminfield")), "INDEX is refused with no Admin descriptor")
    end, restore_admin)
end)

test("recreating eventd-meta.db does not reset the administrative policy", {
    spec = "eventd *pattern.recreating-eventd-meta-db-does-not-reset-the-admin-policy",
}, function(t)
    local narrowed = eventd.hex(descriptor({ allow(ADMINISTER, NOBODY), allow(READ, SY) }))
    finally(function()
        put_hex(nil, "Admin", narrowed)
        index_settles(false, "the narrowed Admin descriptor")
        vm:run("svctl stop eventd", { timeout = 60 }):assert_ok()
        vm:run("rm -f " .. eventd.DB.meta .. " " .. eventd.DB.meta .. "-wal "
            .. eventd.DB.meta .. "-shm"):assert_ok()
        eventd.start(vm)
        t:assert(vm:run("test -f " .. eventd.DB.meta).exit_code == 0, "eventd-meta.db was recreated")
        t:assert_eq(get_hex(nil, "Admin"), narrowed, "the Admin descriptor is as it was")
        t:assert(not (index("ptadminfield")), "and INDEX is still refused")
    end, restore_admin)
end)
