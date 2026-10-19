-- eventd TRM Appendix B — Constants: the access rights, the generic
-- mapping, the field GUID namespace and the data type root GUIDs.
--
-- None of these numbers is reported by any interface; each is seen through
-- what eventd lets a caller do. Every test writes a descriptor of its own
-- for a pattern only it uses — a log origin, an event type or a metric name
-- made from `eventd.marker()` — as the default value of its key under
-- `Machine\System\eventd\Security`, and then asks as TEST_USER whether the
-- record is visible, the metric is accepted, or INDEX is allowed. The
-- descriptor is the most specific pattern for that identifier, so it alone
-- decides (§7.2); the default `*` descriptors never come into it.
--
-- TEST_USER is minted in Administrators, as config-keys' is. That does not
-- leak into the answers: the test descriptors name TEST_USER and SYSTEM
-- only. The Security\Admin tests rewrite Admin for the length of the
-- test and put the original back.
--
-- GUIDs are computed on the host with Python's uuid module from the
-- namespace and names written out here, and written into object ACEs in
-- PCDS byte order (`bytes_le`).

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-const-access" })

local ROOT = {
    events = "a1b2c3d4-0001-4000-8000-000000000001",
    logs = "a1b2c3d4-0001-4000-8000-000000000002",
    metrics = "a1b2c3d4-0001-4000-8000-000000000003",
}

local A = access.ACE
local SY, USER = token.SID.LOCAL_SYSTEM, token.SID.TEST_USER
local GENERIC_READ, GENERIC_WRITE = 0x80000000, 0x40000000
local GENERIC_EXECUTE, GENERIC_ALL = 0x20000000, 0x10000000

local function host(py)
    local p = assert(io.popen("python3 -c '" .. py .. "'", "r"))
    local out = p:read("a")
    p:close()
    return (out:gsub("%s+$", ""))
end

--- A GUID string's 16 bytes in PCDS order.
local function guid_bytes(s)
    return eventd.unhex(host("import uuid; print(uuid.UUID(\"" .. s .. "\").bytes_le.hex())"))
end

local function sd(aces) return access.simple(aces) end
local function allow(mask, sid) return access.ace(A.ALLOWED, mask, sid) end
local function allow_obj(mask, sid, guid) return access.ace(A.ALLOWED_OBJECT, mask, sid, 0, { object_type = guid }) end
local function deny_obj(mask, sid, guid) return access.ace(A.DENIED_OBJECT, mask, sid, 0, { object_type = guid }) end

local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local function reader()
    return {
        user_sid = USER,
        privs_present = NOTIFY, privs_enabled = NOTIFY,
        groups = {
            { sid = token.SID.EVERYONE, attributes = ENABLED },
            { sid = token.SID.AUTHENTICATED_USERS, attributes = ENABLED },
            { sid = token.SID.ADMINISTRATORS, attributes = ENABLED },
        },
    }
end

local function as_user(t, fn) token.as_principal(t, vm, reader(), fn) end

local function user_query(w, text)
    local r = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", text } })
    local out = { ok = r.exit_code == 0, stderr = r.stderr, rows = {} }
    if out.ok then
        for line in r.stdout:gmatch("[^\n]+") do out.rows[#out.rows + 1] = json.decode(line) end
    end
    return out
end

--- A log record of a fresh origin, stored and waited for.
local function log_origin()
    local origin = eventd.marker("ca")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "m" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    return origin
end

--- An event of a fresh type, stored and waited for.
local function event_type(payload)
    local etype = "pt.ca" .. eventd.marker()
    eventd.emit(vm, etype, payload or { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    return etype
end

--- What TEST_USER sees of `query` once the descriptor at `key` is `bytes`.
--- Descriptor changes reach eventd through its registry watch, so a short
--- settle precedes the read.
local function user_sees(t, key, bytes, query)
    eventd.write_descriptor(vm, key, bytes):assert_ok()
    vm:run("sleep 0.5")
    local out
    as_user(t, function(w) out = user_query(w, query) end)
    return out
end

--- Whether TEST_USER may INDEX while Security\Admin is `bytes`.
local function user_may_index(t, bytes)
    local key = eventd.SECURITY .. [[\Admin]]
    local original = eventd.read_descriptor(vm, key)
    eventd.write_descriptor(vm, key, bytes):assert_ok()
    vm:run("sleep 0.5")
    local ok, msg
    as_user(t, function(w)
        local r = user_query(w, "EVENTS INDEX " .. eventd.marker("f"))
        ok, msg = r.ok, r.stderr
    end)
    eventd.write_descriptor(vm, key, original):assert_ok()
    return ok, msg
end

--- Whether a metric TEST_USER publishes under a name governed by `bytes`
--- is stored.
local function user_may_publish(t, bytes)
    local name = eventd.marker("pub")
    eventd.write_descriptor(vm, eventd.key_of("Metrics", name), bytes):assert_ok()
    vm:run("sleep 0.5")
    as_user(t, function(w)
        eventd.send_metric(w, { name = name, type = "gauge", value = 1 })
    end)
    local ok = pcall(wait_until, function()
        return #eventd.rows(vm, "METRIC " .. name .. " SINCE 10m ago") == 1
    end, { timeout = 4, interval = 0.25 })
    return ok
end

local function user_may_read(t, mask)
    local origin = log_origin()
    local r = user_sees(t, eventd.key_of("Logs", origin), sd({ allow(0x1, SY), allow(mask, USER) }),
        "LOGS FROM " .. origin .. " SINCE 10m ago")
    return #r.rows == 1
end

-- ---------------------------------------------------------------------------
-- Access rights
-- ---------------------------------------------------------------------------

test("EVENTD_READ is 0x0001: it, and no other eventd right, lets a caller read", {
    spec = "eventd *constant.eventd-read-is-bit-0-value-0x0001",
}, function(t)
    t:assert(user_may_read(t, 0x0001), "0x0001 granted: the record is visible")
    t:assert(not user_may_read(t, 0x000E), "0x000E, every other eventd right, granted: it is not")
end)

test("EVENTD_CLEAR is 0x0002 and nothing uses it yet", {
    spec = "eventd *constant.eventd-clear-is-bit-1-value-0x0002-and-reserved",
}, function(t)
    t:assert(not user_may_read(t, 0x0002), "0x0002 does not grant reading")
    t:assert(not user_may_index(t, sd({ allow(0x4, SY), allow(0x0002, USER) })), "nor INDEX")
    t:assert(not user_may_publish(t, sd({ allow(0x9, SY), allow(0x0002, USER) })), "nor publishing")
    -- and no query deletes anything, so there is nothing for it to govern
    for _, text in ipairs({ "LOGS FROM x CLEAR", "LOGS FROM x DELETE", "EVENTS pt.x CLEAR", "EVENTS pt.x DELETE" }) do
        t:assert(not eventd.query(vm, text).ok, "no deletion command: '" .. text .. "' is refused")
    end
end)

test("EVENTD_ADMINISTER is 0x0004 and is what INDEX needs", {
    spec = "eventd *constant.eventd-administer-is-bit-2-value-0x0004-and-governs-index",
}, function(t)
    local ok = user_may_index(t, sd({ allow(0x4, SY), allow(0x0004, USER) }))
    t:assert(ok, "with 0x0004 on Security\\Admin, TEST_USER may INDEX")
    local ok2, msg = user_may_index(t, sd({ allow(0x4, SY), allow(0x000B, USER) }))
    t:assert(not ok2, "with every other eventd right and not 0x0004, it may not")
    t:assert(tostring(msg):find("EVENTD_ADMINISTER", 1, true), "refused for EVENTD_ADMINISTER: " .. tostring(msg))
end)

test("EVENTD_PUBLISH is 0x0008 and is what publishing a metric needs", {
    spec = "eventd *constant.eventd-publish-is-bit-3-value-0x0008-and-governs-metric-publication",
}, function(t)
    t:assert(user_may_publish(t, sd({ allow(0x9, SY), allow(0x0008, USER) })),
        "with 0x0008 on the metric's name, TEST_USER's sample is stored")
    t:assert(not user_may_publish(t, sd({ allow(0x9, SY), allow(0x0007, USER) })),
        "with every other eventd right and not 0x0008, it is not")
end)

-- ---------------------------------------------------------------------------
-- The generic mapping
--
-- The standard bits in each mapping (READ_CONTROL and the rest) govern the
-- descriptor rather than the data, and nothing eventd serves shows them;
-- what a caller can see is which eventd rights each generic right becomes.
-- ---------------------------------------------------------------------------

test("GENERIC_READ becomes EVENTD_READ: eventd passes its own mapping to AccessCheck", {
    spec = "eventd *constant.generic-read-maps-to-0x00020001"
        .. " eventd *constant.eventd-passes-its-generic-mapping-to-accesscheck",
}, function(t)
    -- An ACE carrying only the generic bit grants nothing until a mapping
    -- turns it into specific rights; reading succeeds, so eventd's did.
    t:assert(user_may_read(t, GENERIC_READ), "a GENERIC_READ ACE lets TEST_USER read")
end)

test("GENERIC_EXECUTE becomes EVENTD_READ", {
    spec = "eventd *constant.generic-execute-maps-to-0x00020001",
}, function(t)
    t:assert(user_may_read(t, GENERIC_EXECUTE), "a GENERIC_EXECUTE ACE lets TEST_USER read")
end)

test("GENERIC_WRITE becomes clear, administer and publish, and not read", {
    spec = "eventd *constant.generic-write-maps-to-0x0002000e",
}, function(t)
    t:assert(not user_may_read(t, GENERIC_WRITE), "GENERIC_WRITE does not read")
    t:assert(user_may_index(t, sd({ allow(0x4, SY), allow(GENERIC_WRITE, USER) })), "it administers")
    t:assert(user_may_publish(t, sd({ allow(0x9, SY), allow(GENERIC_WRITE, USER) })), "it publishes")
end)

test("GENERIC_ALL becomes every eventd right", {
    spec = "eventd *constant.generic-all-maps-to-0x000f000f",
}, function(t)
    t:assert(user_may_read(t, GENERIC_ALL), "GENERIC_ALL reads")
    t:assert(user_may_index(t, sd({ allow(0x4, SY), allow(GENERIC_ALL, USER) })), "administers")
    t:assert(user_may_publish(t, sd({ allow(0x9, SY), allow(GENERIC_ALL, USER) })), "and publishes")
end)

test("neither GENERIC_READ nor GENERIC_EXECUTE administers or publishes", {
    spec = "eventd *constant.administer-and-publish-are-not-in-generic-read-or-generic-execute",
}, function(t)
    for name, g in pairs({ GENERIC_READ = GENERIC_READ, GENERIC_EXECUTE = GENERIC_EXECUTE }) do
        t:assert(not user_may_index(t, sd({ allow(0x4, SY), allow(g, USER) })), name .. " does not INDEX")
        t:assert(not user_may_publish(t, sd({ allow(0x9, SY), allow(g, USER) })), name .. " does not publish")
    end
end)

-- ---------------------------------------------------------------------------
-- Field GUIDs
-- ---------------------------------------------------------------------------

-- A deny object ACE naming a field's GUID is the witness. Whether a denied
-- field is cut from the record or the whole record withheld is §7.3's
-- business; either way the ACE took effect only if eventd put that GUID in
-- the object type list, and an ACE for any other GUID changes nothing.

--- Whether TEST_USER's view of a fresh log record is the whole record
--- when its descriptor grants the record and denies the object `guid`.
local function log_untouched_by(t, guid)
    local origin = log_origin()
    local r = user_sees(t, eventd.key_of("Logs", origin), sd({
        deny_obj(0x1, USER, guid), allow(0x1, SY), allow(0x1, USER) }),
        "LOGS FROM " .. origin .. " SINCE 10m ago")
    local row = r.rows[1]
    return row ~= nil and row.origin == origin and row.message == "m", row
end

test("field GUIDs are in the namespace {e7d3a1b0-5c2f-4e8a-9b1d-0a6f3c8e2d4b}", {
    spec = "eventd *constant.the-field-guid-namespace-is-e7d3a1b0-5c2f-4e8a-9b1d-0a6f3c8e2d4b",
}, function(t)
    local whole, row = log_untouched_by(t, eventd.field_guid("message", eventd.FIELD_NAMESPACE))
    t:assert(not whole, "a deny on uuid_v5({e7d3a1b0-…}, \"message\") takes effect: " .. json.encode(row))
    local whole2, row2 = log_untouched_by(t, eventd.field_guid("message", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
    t:assert(whole2, "the same name in the DNS namespace is not message's GUID: " .. json.encode(row2))
end)

test("a field's GUID is uuid_v5 of its exact UTF-8 name", {
    spec = "eventd *constant.a-field-guid-is-uuid-v5-of-the-utf-8-field-name-in-the-eventd-namespace",
}, function(t)
    local whole, row = log_untouched_by(t, eventd.field_guid("origin"))
    t:assert(not whole, "uuid_v5(ns, \"origin\") is origin's GUID: " .. json.encode(row))
    local whole2, row2 = log_untouched_by(t, eventd.field_guid("Origin"))
    t:assert(whole2, "uuid_v5(ns, \"Origin\") is not — the exact bytes, not a folded name: " .. json.encode(row2))
    local whole3, row3 = log_untouched_by(t, eventd.field_guid("origin "))
    t:assert(whole3, "nor is uuid_v5(ns, \"origin \"): " .. json.encode(row3))
end)

test("field GUIDs are computed: a field never seen before is governed by its GUID", {
    spec = "eventd *constant.field-guids-are-computed-never-hardcoded",
}, function(t)
    local field = "ptnew" .. eventd.marker()
    local function denied_by(name)
        local etype = event_type({ [field] = "secret", keep = "visible" })
        local r = user_sees(t, eventd.key_of("Events", etype), sd({
            deny_obj(0x1, USER, eventd.field_guid(name)), allow(0x1, SY), allow(0x1, USER) }),
            "EVENTS " .. etype .. " SINCE 10m ago")
        local row = r.rows[1]
        return not (row and row[field] == "secret"), row
    end
    local denied, row = denied_by(field)
    t:assert(denied, "a deny on the GUID computed from the brand-new name " .. field ..
        " takes effect: " .. json.encode(row))
    local denied2, row2 = denied_by(field .. "x")
    t:assert(not denied2, "one computed from another new name does not: " .. json.encode(row2))
end)

-- ---------------------------------------------------------------------------
-- Data type roots
-- ---------------------------------------------------------------------------

test("the events root is {a1b2c3d4-0001-4000-8000-000000000001}, the level-0 node", {
    spec = "eventd *constant.the-events-root-guid-is-a1b2c3d4-0001-4000-8000-000000000001"
        .. " eventd *constant.a-data-type-root-guid-is-the-level-0-object-type-list-node",
}, function(t)
    local etype = event_type({ n = 1 })
    local key = eventd.key_of("Events", etype)
    local q = "EVENTS " .. etype .. " SINCE 10m ago"
    local granted = user_sees(t, key, sd({ allow(0x1, SY), allow_obj(0x1, USER, guid_bytes(ROOT.events)) }), q)
    t:assert_eq(#granted.rows, 1, "an object ACE for the events root grants the event: " .. tostring(granted.stderr))
    -- Granting the root grants what is under it: every field.
    for _, f in ipairs({ "event.time", "event.cpu", "event.sequence", "emitter.class", "event.type",
                         "event.boot.guid", "n" }) do
        t:assert(granted.rows[1] and granted.rows[1][f] ~= nil, "the root grant covers " .. f)
    end
    local wrong = user_sees(t, key, sd({ allow(0x1, SY), allow_obj(0x1, USER, guid_bytes(ROOT.logs)) }), q)
    t:assert_eq(#wrong.rows, 0, "the logs root's GUID does not")
end)

test("the logs root is {a1b2c3d4-0001-4000-8000-000000000002}", {
    spec = "eventd *constant.the-logs-root-guid-is-a1b2c3d4-0001-4000-8000-000000000002",
}, function(t)
    local origin = log_origin()
    local key = eventd.key_of("Logs", origin)
    local q = "LOGS FROM " .. origin .. " SINCE 10m ago"
    t:assert_eq(#user_sees(t, key, sd({ allow(0x1, SY), allow_obj(0x1, USER, guid_bytes(ROOT.logs)) }), q).rows, 1,
        "an object ACE for the logs root grants the record")
    t:assert_eq(#user_sees(t, key, sd({ allow(0x1, SY), allow_obj(0x1, USER, guid_bytes(ROOT.metrics)) }), q).rows, 0,
        "the metrics root's GUID does not")
end)

test("the metrics root is {a1b2c3d4-0001-4000-8000-000000000003}", {
    spec = "eventd *constant.the-metrics-root-guid-is-a1b2c3d4-0001-4000-8000-000000000003",
}, function(t)
    local name = eventd.marker("mr")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 4 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local key = eventd.key_of("Metrics", name)
    local q = "METRIC " .. name .. " SINCE 10m ago"
    t:assert_eq(#user_sees(t, key, sd({ allow(0x9, SY), allow_obj(0x1, USER, guid_bytes(ROOT.metrics)) }), q).rows, 1,
        "an object ACE for the metrics root grants the sample")
    t:assert_eq(#user_sees(t, key, sd({ allow(0x9, SY), allow_obj(0x1, USER, guid_bytes(ROOT.events)) }), q).rows, 0,
        "the events root's GUID does not")
end)
