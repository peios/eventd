-- eventd TRM §7.1 — the access model: enforcement on the read path at
-- query time, the caller's identity, and the four rights.
--
-- One file-scope eventd serves every test. Each test names its own event
-- types with a `marker()` prefix and writes a descriptor for exactly that
-- prefix under `Machine\System\eventd\Security\Events\…`, so the defaults
-- every other test relies on are never touched (the Admin descriptor is
-- the one exception, and is put back byte for byte).
--
-- A denial does not need a second principal: the agent is SYSTEM and an
-- Administrator, and a descriptor that grants only a group nobody holds
-- refuses it as readily as anyone. Where a test is about *which* identity
-- eventd evaluates, a worker mints one, connects while impersonating it,
-- and speaks the query channel itself (PSPU §3.15–§3.17), because evctl
-- can only ever be the console's own identity.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
local sys = require("helpers.sys")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-access" })

-- ---------------------------------------------------------------------------
-- Descriptors
-- ---------------------------------------------------------------------------

local SY, BA, AU = token.SID.LOCAL_SYSTEM, token.SID.ADMINISTRATORS, token.SID.AUTHENTICATED_USERS
local EVERYONE, ANONYMOUS = token.SID.EVERYONE, token.SID.ANONYMOUS
--- A group the test principals hold and SYSTEM does not.
local GROUP = token.SID.TEST_GROUP
--- A group nobody holds: a descriptor granting only it refuses everyone.
local NOBODY = token.SID.TEST_GROUP_2
local READ, CLEAR, ADMINISTER, PUBLISH = 0x1, 0x2, 0x4, 0x8
local GENERIC_READ, GENERIC_WRITE = 0x80000000, 0x40000000

local function allow(mask, sid) return access.ace(access.ACE.ALLOWED, mask, sid) end
local function descriptor(aces, opts) return access.simple(aces, opts) end

-- ---------------------------------------------------------------------------
-- The query channel, spoken directly (PSPU §3.15–§3.17): `eventd.rq`,
-- each connection with a 20 s SO_RCVTIMEO so an answer that never comes
-- fails the read.
-- ---------------------------------------------------------------------------

local rq = eventd.rq

local function describe(out)
    return string.format("status=%s error=%s closed=%s connect=%s records=%d",
        tostring(out.status), tostring(out.error), tostring(out.closed),
        tostring(out.connect_error), #out.records)
end

local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)

--- An impersonation token for TEST_USER in `groups` (each a binary SID),
--- holding only SeChangeNotifyPrivilege, without which the walk to
--- /run/eventd fails on traverse before the socket is reached.
local function mint(who, groups, spec)
    local list = {}
    for i, sid in ipairs(groups) do list[i] = { sid = sid, attributes = ENABLED } end
    local s = {
        user_sid = token.SID.TEST_USER,
        token_type = token.TYPE.IMPERSONATION,
        impersonation_level = token.LEVEL.IMPERSONATION,
        groups = list, privs_present = NOTIFY, privs_enabled = NOTIFY,
    }
    for k, v in pairs(spec or {}) do s[k] = v end
    local fd, e = token.mint(who, s)
    assert(fd, "mint: " .. sys.errname(e or 0))
    return fd
end

local function with_worker(fn)
    local w = vm:spawn_worker()
    local ok, err = pcall(fn, w)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end

-- ---------------------------------------------------------------------------
-- Records
-- ---------------------------------------------------------------------------

local function emit(ty, payload)
    local r = eventd.emit(vm, ty, payload)
    assert(r.ret == 0, "kmes_emit " .. ty .. ": errno " .. tostring(r.errno))
end

local function events(ty) return "EVENTS " .. ty .. " SINCE 1h ago TAKE 1000" end

--- Wait until the console sees exactly `n` events of `ty`.
local function wait_seen(ty, n, desc)
    return eventd.wait_rows(vm, events(ty), function(rs) return #rs == n end,
        { timeout = 15, desc = desc })
end

-- ---------------------------------------------------------------------------
-- Enforcement at query time
-- ---------------------------------------------------------------------------

test("a record nobody may read is stored, and a later grant or revocation applies to it", {
    spec = "eventd *access.access-is-enforced-at-query-time"
        .. " eventd *access.every-record-is-stored-regardless-of-who-may-read-it"
        .. " eventd *access.a-grant-or-revocation-applies-retroactively-to-stored-records",
}, function(t)
    local ty = eventd.marker("ptacc")
    -- The descriptor is in place before the event exists: storage-time
    -- filtering would have to drop it here.
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, NOBODY) }))
    emit(ty, { n = 1 })
    wait_until(function() return eventd.stored_count(vm, ty) == 1 end,
        { timeout = 15, desc = "the unreadable event to be committed" })
    t:assert_eq(eventd.stored_count(vm, ty), 1, "the event is in the store although nobody may read it")
    t:assert_eq(#eventd.rows(vm, events(ty)), 0, "and the console's query does not return it")

    -- A grant made after the fact reaches the record already stored.
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, SY) }))
    local rows = wait_seen(ty, 1, "the later grant to reach the stored event")
    t:assert_eq(rows[1].n, 1, "the stored event is returned once SYSTEM is granted")

    -- Two callers, one copy: an Administrator who is not SYSTEM asks the
    -- same store the same question and is answered differently.
    with_worker(function(w)
        local admin = mint(w, { EVERYONE, AU, BA })
        local out = rq.ask(w, events(ty), { as = admin, timeout = 20 })
        t:assert_eq(out.status, "end", "the administrator's query succeeds: " .. describe(out))
        t:assert_eq(#out.records, 0, "and returns nothing, where SYSTEM's returned the event")
    end)

    -- And a revocation takes it away again.
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, NOBODY) }))
    wait_seen(ty, 0, "the revocation to hide the stored event")
    t:assert_eq(eventd.stored_count(vm, ty), 1, "while it stays in the store")
end)

-- ---------------------------------------------------------------------------
-- Who may connect, and who the caller is
-- ---------------------------------------------------------------------------

test("every authenticated caller may connect to the query socket", {
    spec = "eventd *access.every-authenticated-caller-may-connect-to-the-query-socket",
}, function(t)
    local shown = vm:run("sd show --sddl " .. eventd.SOCKET.query)
    shown:assert_ok()
    local dacl = shown.stdout:match("(D:[^\n]*)")
    t:assert_eq(dacl, "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;FW;;;AU)",
        "the query socket's DACL is the one §7.1 gives: " .. shown.stdout)

    with_worker(function(w)
        -- An ordinary signed-in user: no Administrators, no SYSTEM.
        local user = mint(w, { EVERYONE, AU })
        local out = rq.ask(w, "LOGS SINCE 1h ago TAKE 1", { as = user, timeout = 20 })
        t:assert_eq(out.connect_error, nil, "an Authenticated User connects: " .. describe(out))
        t:assert_eq(out.status, "end", "and is answered: " .. describe(out))
    end)
end)

test("the query is evaluated as the identity captured at connect(), not the sender's", {
    spec = "eventd *access.the-caller-token-is-read-from-the-peer-token-socket-option"
        .. " eventd *access.the-caller-token-is-captured-at-connection-time",
}, function(t)
    local ty = eventd.marker("ptwho")
    emit(ty, { n = 1 })
    wait_seen(ty, 1, "the event")
    -- Readable by GROUP only: the minted principal, not SYSTEM (which is
    -- what the worker is whenever it is not impersonating).
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, GROUP) }))
    wait_seen(ty, 0, "SYSTEM to lose the event")

    with_worker(function(w)
        -- Administrators as well, so the image's socket admits it; the
        -- descriptor above grants Administrators nothing.
        local member = mint(w, { EVERYONE, AU, BA, GROUP })

        -- Connect as the member, then send and read as SYSTEM.
        local c = assert(rq.connect(w, { as = member, timeout = 20 }))
        rq.send(c, events(ty))
        local got = {}
        while true do
            local m = assert(rq.frame(c))
            if m.status ~= "ok" then t:assert_eq(m.status, "end", tostring(m.error)); break end
            for _, r in ipairs(m.records) do got[#got + 1] = r end
        end
        rq.close(c)
        t:assert_eq(#got, 1, "the member's connection reads the event, though SYSTEM sent the query")

        -- Connect as SYSTEM, then send and read while impersonating the member.
        c = assert(rq.connect(w, { timeout = 20 }))
        assert(token.impersonate(w, member).ret == 0)
        rq.send(c, events(ty))
        local m = assert(rq.frame(c))
        local records = 0
        while m and m.status == "ok" do records = records + #m.records; m = rq.frame(c) end
        token.revert(w)
        rq.close(c)
        t:assert_eq(records, 0, "and SYSTEM's connection does not, whoever sends on it")
    end)
end)

-- Route closed: KACS captures an identity on every connect(), so no
-- client can make eventd's KACS_SO_PEER_TOKEN read fail. Tried here: a
-- client at Anonymous level conveys the anonymous token, one at
-- Identification level conveys an Identification token (the read
-- succeeds; AccessCheck then refuses it), and a token whose own
-- descriptor admits only its user is still conveyed and evaluated. The
-- refusal branch is `Authorizer::from_peer(...)?` in `handle`
-- (eventd/src/query/mod.rs:269-271), which drops the connection.
test("a failed peer-token read ends the connection without evaluating a query", {
    spec = "eventd *access.a-failed-peer-token-read-denies-the-query",
    skip = true,
    covered_by = "cargo:eventd eventd query::security::tests::a_failed_peer_token_read_ends_the_connection_without_evaluating_its_query",
}, function() end)

test("a connection whose identity cannot be evaluated is refused, never served as someone else", {
    spec = "eventd *access.there-is-no-fallback-identification-or-anonymous-mode"
        .. " eventd *access.an-anonymous-level-caller-is-refused",
}, function(t)
    local ty = eventd.marker("ptanon")
    emit(ty, { n = 1 })
    wait_seen(ty, 1, "the event")
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, SY), allow(READ, EVERYONE), allow(READ, ANONYMOUS) }))
    wait_seen(ty, 1, "the widened descriptor")

    -- Identification level: an identity eventd may look at but not act
    -- as. AccessCheck refuses it, and eventd answers with the failure
    -- rather than falling back to anything.
    local ident = rq.ask(vm, events(ty), { level = token.LEVEL.IDENTIFICATION, timeout = 20 })
    t:assert_eq(#ident.records, 0, "an Identification-level caller gets nothing: " .. describe(ident))
    t:assert_eq(ident.status, "error", "and is refused: " .. describe(ident))

    -- Anonymous level: the caller conveys no identity of its own, and
    -- eventd answers with an error whatever the descriptor grants.
    local anon = rq.ask(vm, events(ty), { level = token.LEVEL.ANONYMOUS, timeout = 20 })
    t:assert_eq(#anon.records, 0,
        "an anonymous caller is not served, even where Everyone may read: " .. describe(anon))
    t:assert_eq(anon.status, "error", "and is answered with an error: " .. describe(anon))
end)

-- ---------------------------------------------------------------------------
-- Rights
-- ---------------------------------------------------------------------------

test("EVENTD_READ is 0x0001: that bit alone reads records, and no other bit does", {
    spec = "eventd *access.eventd-read-is-bit-0-value-0x0001",
}, function(t)
    local ty = eventd.marker("ptread")
    emit(ty, { n = 1 })
    wait_seen(ty, 1, "the event")
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(CLEAR | ADMINISTER | PUBLISH, SY) }))
    wait_seen(ty, 0, "every right but read to leave the event unreadable")
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, SY) }))
    wait_seen(ty, 1, "0x0001 alone to read it")
end)

local ADMIN_DEFAULT = eventd.descriptor_hex(vm, nil, "Admin")

--- INDEX as the console; true when eventd accepted it.
local function index(field)
    local r = eventd.query(vm, "EVENTS INDEX " .. field)
    return r.ok, r.stderr
end

--- Put the Admin descriptor, then wait until INDEX answers `expect`.
local function admin_is(sd, expect, what)
    eventd.put_descriptor(vm, nil, "Admin", sd)
    local ok = wait_until(function() return (index("ptadminprobe")) == expect end,
        { timeout = 15, desc = what })
    return ok
end

test("EVENTD_ADMINISTER is 0x0004, and EVENTD_READ alone does not grant it", {
    spec = "eventd *access.eventd-administer-is-bit-2-value-0x0004"
        .. " eventd *access.eventd-read-alone-does-not-grant-eventd-administer",
}, function(t)
    admin_is(descriptor({ allow(ADMINISTER, SY) }), true, "0x0004 to permit INDEX")
    t:assert((index("ptadminfield")), "an Admin descriptor granting 0x0004 permits INDEX")

    -- Read, as a bit and as GENERIC_READ (EVENTD_READ | READ_CONTROL).
    admin_is(descriptor({ allow(READ, SY) }), false, "0x0001 to refuse INDEX")
    local ok, err = index("ptadminfield")
    t:assert(not ok, "EVENTD_READ alone does not permit INDEX")
    t:assert(err:find("EVENTD_ADMINISTER", 1, true), "refused for want of administer: " .. err)
    admin_is(descriptor({ allow(GENERIC_READ, SY) }), false, "GENERIC_READ to refuse INDEX")
    t:assert(not (index("ptadminfield")), "nor does GENERIC_READ")

    -- Every other right together still is not 0x0004.
    admin_is(descriptor({ allow(READ | CLEAR | PUBLISH, SY) }), false, "the other bits to refuse INDEX")
    t:assert(not (index("ptadminfield")), "read, clear and publish together do not permit INDEX")

    vm:run("reg set '" .. eventd.key_of(nil, "Admin") .. "' @ hex:" .. ADMIN_DEFAULT):assert_ok()
    wait_until(function() return (index("ptadminprobe")) end,
        { timeout = 15, desc = "the default Admin descriptor to be back" })
end)

--- Metric series by name, read out of the store itself, so the test does
--- not depend on being allowed to read what it is testing publication of.
local function series(name)
    return eventd.sql(vm, eventd.DB.metrics, "SELECT count(*) FROM series WHERE name = '"
        .. name .. "'")[1][1]
end

test("EVENTD_PUBLISH is 0x0008: that bit alone publishes a metric name, and no other bit does", {
    spec = "eventd *access.eventd-publish-is-bit-3-value-0x0008",
}, function(t)
    local p = eventd.marker("ptpub")
    local witness = eventd.marker("ptpubw")
    eventd.put_descriptor(vm, "Metrics", p, descriptor({ allow(PUBLISH, SY) }))
    -- A sample under the default wildcard rides in the same datagram,
    -- after the one under test: once it is stored, the one before it has
    -- been decided.
    local function publish(name, w)
        local r = eventd.send_metric(vm, eventd.array({
            { name = name, type = "gauge", value = 1 },
            { name = w, type = "gauge", value = 1 },
        }))
        assert(r.ret and r.ret > 0, "sendto: errno " .. tostring(r.errno))
        wait_until(function() return series(w) == 1 end,
            { timeout = 15, desc = "the witness " .. w })
    end
    wait_until(function()
        publish(p .. ".one", eventd.marker("ptpubw"))
        return series(p .. ".one") == 1
    end, { timeout = 20, desc = "0x0008 to publish" })
    t:assert_eq(series(p .. ".one"), 1, "a name whose descriptor grants only 0x0008 is published")

    eventd.put_descriptor(vm, "Metrics", p, descriptor({ allow(READ | CLEAR | ADMINISTER, SY) }))
    -- The change reaches the metric thread through the descriptor
    -- generation; give it the same settling a query would need.
    wait_until(function()
        local probe = p .. "." .. eventd.marker("probe")
        publish(probe, eventd.marker("ptpubw"))
        return series(probe) == 0
    end, { timeout = 20, desc = "the narrower descriptor to apply" })
    publish(p .. ".two", witness)
    t:assert_eq(series(p .. ".two"), 0, "every other right together does not publish")
end)

-- Route closed: EVENTD_CLEAR is reserved and nothing uses it (§7.1), so no
-- operation turns on whether a caller holds bit 1, and no VM test can
-- tell 0x0002 from any other unused bit.
test("EVENTD_CLEAR is bit 1, value 0x0002", {
    spec = "eventd *access.eventd-clear-is-bit-1-value-0x0002",
    skip = true,
    covered_by = "cargo:eventd eventd-client access::tests::eventd_clear_is_bit_1_value_0x0002",
}, function() end)

-- Route closed: the only right GENERIC_WRITE could be seen to grant through
-- EVENTD_CLEAR is a right nothing checks; what is observable of the
-- mapping (administer and publish from GENERIC_WRITE) is the a2 constant
-- chapter's.
test("GENERIC_WRITE maps to a mask that includes EVENTD_CLEAR", {
    spec = "eventd *access.generic-write-already-grants-eventd-clear",
    skip = true,
    covered_by = "cargo:eventd eventd-client access::tests::generic_write_already_grants_eventd_clear",
}, function() end)

test("no operation deletes records on a caller's behalf", {
    spec = "eventd *access.no-operation-uses-eventd-clear",
}, function(t)
    local ty = eventd.marker("ptclear")
    emit(ty, { n = 1 })
    wait_seen(ty, 1, "the event")
    -- Every right there is, so a deleting operation, if there were one,
    -- would be permitted.
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(0x000F000F, SY) }))
    for _, text in ipairs({ "CLEAR EVENTS " .. ty, "DELETE EVENTS " .. ty,
                            "EVENTS " .. ty .. " CLEAR", "EVENTS " .. ty .. " DELETE" }) do
        local r = eventd.query(vm, text)
        t:assert(not r.ok, "`" .. text .. "` is not an operation eventd performs: " .. r.stdout)
    end
    t:assert_eq(eventd.stored_count(vm, ty), 1, "and the record is still stored")
    t:assert_eq(#eventd.rows(vm, events(ty)), 1, "and still read")
end)

-- ---------------------------------------------------------------------------
-- KACS decides
-- ---------------------------------------------------------------------------

test("every verdict is KACS AccessCheck's: its audit walk, integrity and restricted-SID passes all apply", {
    spec = "eventd *access.eventd-delegates-every-decision-to-kacs-accesscheck",
}, function(t)
    local ty = eventd.marker("ptkacs")
    emit(ty, { n = 1 })
    wait_seen(ty, 1, "the event")

    -- The SACL audit walk: a success-audit ACE makes AccessCheck itself
    -- emit kacs.audit.access.checked, from inside eventd's query thread.
    local sacl = access.acl({ access.ace(access.ACE.AUDIT, READ, EVERYONE,
        access.ACE_FLAG.SUCCESSFUL_ACCESS) })
    eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, SY), allow(READ, GROUP) }, { sacl = sacl }))
    wait_seen(ty, 1, "the audited descriptor")
    local audits = eventd.wait_rows(vm,
        'EVENTS kacs.audit.access.checked WHERE object.event-namespace.pattern == "' .. ty
            .. '" SINCE 1h ago TAKE 100',
        function(rs) return #rs >= 1 end, { timeout = 15, desc = "a kacs.audit.access.checked record" })
    t:assert_eq(audits[1]["emitter.process.executable"], "/usr/sbin/eventd",
        "the audit record was emitted by eventd's own AccessCheck call")
    t:assert_eq(audits[1]["object.kind"], "event-namespace", "against an event-namespace pattern")
    t:assert_eq(audits[1]["fields.attestation.userspace"], true, "which eventd asserted, not KACS")
    t:assert_eq(audits[1]["access.requested"], READ, "asking for EVENTD_READ")

    with_worker(function(w)
        -- Restricted SIDs: a second pass that only the restricting SIDs
        -- may satisfy. Restricted to Administrators, the token still
        -- reaches the socket but not the GROUP-only grant.
        local member = mint(w, { EVERYONE, AU, BA, GROUP })
        local plain = rq.ask(w, events(ty), { as = member, timeout = 20 })
        t:assert_eq(#plain.records, 1, "the member reads the event: " .. describe(plain))
        local restricted, e = token.restrict(w, member, { restrict_sids = { BA } })
        t:assert(restricted, "restrict: " .. tostring(e))
        local out = rq.ask(w, events(ty), { as = restricted, timeout = 20 })
        t:assert_eq(out.status, "end", "the restricted member is answered: " .. describe(out))
        t:assert_eq(#out.records, 0, "and the restricted pass hides the event")

        -- Integrity: a High mandatory label with no-read-up refuses a
        -- Medium caller whatever the DACL grants it.
        local label = access.acl({ access.label_ace(token.INTEGRITY.HIGH, access.LABEL.NO_READ_UP) })
        eventd.put_descriptor(vm, "Events", ty, descriptor({ allow(READ, SY), allow(READ, GROUP) }, { sacl = label }))
        wait_until(function() return #rq.ask(w, events(ty), { as = member, timeout = 20 }).records == 0 end,
            { timeout = 15, desc = "the label to refuse the Medium member" })
        t:assert_eq(#rq.ask(w, events(ty), { as = member, timeout = 20 }).records, 0,
            "a Medium caller does not read up past a High no-read-up label")
        t:assert_eq(#eventd.rows(vm, events(ty)), 1, "while SYSTEM, at System integrity, still does")
    end)
end)
