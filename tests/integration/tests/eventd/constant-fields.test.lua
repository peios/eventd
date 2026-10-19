-- eventd TRM Appendix B — Constants: the field names GUIDs are computed
-- from, and the origin classes.
--
-- One file-scope VM. The field-name tests read the names off what evctl
-- returns for a record with nothing else in it, and, where a name's GUID is
-- the claim, use the witness constant-access uses: a deny object ACE naming
-- uuid_v5(namespace, name) takes effect only if eventd put that GUID in the
-- object type list. The origin-class tests make one event of each class
-- happen: the agent emits a userspace one, an out-of-range KMES setting
-- makes KMES report itself, minting a token makes KACS report a logon
-- session, `reg backup` makes LCS report the backup, and NTFE reports the
-- policy it publishes at boot.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-const-fields" })

local A = access.ACE
local SY, USER = token.SID.LOCAL_SYSTEM, token.SID.TEST_USER

local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
local ENABLED = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local function reader()
    return {
        user_sid = USER, privs_present = NOTIFY, privs_enabled = NOTIFY,
        groups = {
            { sid = token.SID.EVERYONE, attributes = ENABLED },
            { sid = token.SID.AUTHENTICATED_USERS, attributes = ENABLED },
            { sid = token.SID.ADMINISTRATORS, attributes = ENABLED },
        },
    }
end

--- Whether TEST_USER still sees `field` of the one record `query` returns
--- once the descriptor at `key` grants the record and denies `guid`.
local function user_sees_field(t, key, query, field, guid)
    eventd.write_descriptor(vm, key, access.simple({
        access.ace(A.DENIED_OBJECT, 0x1, USER, 0, { object_type = guid }),
        access.ace(A.ALLOWED, 0x9, SY), access.ace(A.ALLOWED, 0x1, USER) })):assert_ok()
    vm:run("sleep 0.5")
    local row
    token.as_principal(t, vm, reader(), function(w)
        local r = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", query } })
        local line = r.stdout:match("[^\n]+")
        row = line and json.decode(line)
    end)
    return row ~= nil and row[field] ~= nil, row
end

local function keys_of(row)
    local out = {}
    for k in pairs(row) do out[#out + 1] = k end
    table.sort(out)
    return table.concat(out, ",")
end

local function one_event(payload)
    local etype = "pt.cf" .. eventd.marker()
    eventd.emit(vm, etype, payload)
    local rows = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    return etype, rows[1]
end

-- ---------------------------------------------------------------------------
-- Field names
-- ---------------------------------------------------------------------------

test("an event's header fields are the nine names", {
    spec = "eventd *constant.the-nine-event-header-field-names",
}, function(t)
    local _, row = one_event(eventd.map{})
    t:assert_eq(keys_of(row), "emitter.class,emitter.process.guid,emitter.token.guid,emitter.true-token.guid," ..
        "event.boot.guid,event.cpu,event.sequence,event.time,event.type",
        "an event with an empty payload has exactly these")
end)

test("a log record's fields are the six names", {
    spec = "eventd *constant.the-six-log-field-names",
}, function(t)
    -- With a job_id, which peinit attaches to service output; a record
    -- without one omits the field rather than carrying a seventh.
    local origin = eventd.marker("f")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "m",
        job_id = eventd.bin(string.rep("\x11", 16)) })
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(keys_of(rows[1]), "boot_id,is_error,job_id,message,origin,timestamp", "exactly these")
    for _, f in ipairs({ "timestamp", "origin", "is_error", "message", "job_id", "boot_id" }) do
        t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. origin .. " WHERE " .. f .. " IS NOT NULL SINCE 10m ago"), 1,
            "the query language knows " .. f)
    end
end)

test("a metric sample's fixed fields are the five names", {
    spec = "eventd *constant.the-five-fixed-metric-field-names",
}, function(t)
    local name = eventd.marker("f")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(keys_of(rows[1]), "boot_id,name,timestamp,type,value", "a sample with no labels has exactly these")
end)

test("a payload field's name is its flattened dot path", {
    spec = "eventd *constant.a-payload-field-name-is-its-flattened-dot-path",
}, function(t)
    local etype, row = one_event({ outer = { inner = 7 } })
    t:assert_eq(row["outer.inner"], 7, "the nested value is the field outer.inner: " .. json.encode(row))
    t:assert_eq(#eventd.rows(vm, "EVENTS " .. etype .. " WHERE outer.inner == 7 SINCE 10m ago"), 1,
        "and the query language names it so")
    local q = "EVENTS " .. etype .. " SINCE 10m ago"
    local key = eventd.SECURITY .. [[\Events\]] .. etype
    local seen = user_sees_field(t, key, q, "outer.inner", eventd.field_guid("outer.inner"))
    t:assert(not seen, "a deny on uuid_v5(ns, \"outer.inner\") governs it")
    local seen2 = user_sees_field(t, key, q, "outer.inner", eventd.field_guid("inner"))
    t:assert(seen2, "one on uuid_v5(ns, \"inner\") does not")
end)

test("a payload path that is suppressed, or collides with a header, is not a field", {
    spec = "eventd *constant.suppressed-and-header-colliding-payload-paths-have-no-field-guid",
}, function(t)
    -- `bad.key` contains a dot, which PSPU §3.22 makes unqueryable; the
    -- payload's map at `emitter.process.guid` collides with a header path
    -- and the header wins, its subtree included.
    local etype, row = one_event({ emitter = { process = { guid = { inner = 99 } } }, ["bad.key"] = 1, kept = "k" })
    t:assert(row["bad.key"] == nil, "the dotted key is not a field: " .. json.encode(row))
    t:assert(row["emitter.process.guid.inner"] == nil and type(row["emitter.process.guid"]) == "string",
        "emitter.process.guid is the header's, and the payload's subtree is gone: " .. json.encode(row))
    t:assert_eq(#eventd.rows(vm, "EVENTS " .. etype .. " WHERE emitter.process.guid.inner == 99 SINCE 10m ago"), 0,
        "and it cannot be queried")
    -- With no field there is no GUID: denies naming uuid_v5(ns, path) for
    -- those paths have nothing to act on, and the record is untouched.
    local q = "EVENTS " .. etype .. " SINCE 10m ago"
    local key = eventd.SECURITY .. [[\Events\]] .. etype
    t:assert(user_sees_field(t, key, q, "kept", eventd.field_guid("bad.key")),
        "a deny on uuid_v5(ns, \"bad.key\") leaves the record as it was")
    t:assert(user_sees_field(t, key, q, "kept", eventd.field_guid("emitter.process.guid.inner")),
        "and so does one on uuid_v5(ns, \"emitter.process.guid.inner\")")
end)

test("a metric label's field name is the label key itself", {
    spec = "eventd *constant.a-metric-label-field-name-is-the-label-key-itself",
}, function(t)
    local name = eventd.marker("lab")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = eventd.map{ core = "3" } })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(rows[1].core, "3", "the label is the field core: " .. json.encode(rows[1]))
    local key = eventd.SECURITY .. [[\Metrics\]] .. name
    local q = "METRIC " .. name .. " SINCE 10m ago"
    t:assert(not user_sees_field(t, key, q, "core", eventd.field_guid("core")), "uuid_v5(ns, \"core\") governs it")
    t:assert(user_sees_field(t, key, q, "core", eventd.field_guid("labels.core")), "uuid_v5(ns, \"labels.core\") does not")
end)

test("a metric whose label collides with a fixed field name is rejected", {
    spec = "eventd *constant.ingestion-rejects-labels-that-collide-with-fixed-metric-field-names",
}, function(t)
    local good = eventd.marker("ok")
    local bad = {}
    for _, label in ipairs({ "timestamp", "boot_id", "name", "type", "value" }) do
        local name = eventd.marker("col")
        bad[label] = name
        eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = eventd.map{ [label] = "x" } })
    end
    eventd.send_metric(vm, { name = good, type = "gauge", value = 1, labels = eventd.map{ zone = "x" } })
    eventd.wait_rows(vm, "METRIC " .. good .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    vm:run("sleep 1")
    for label, name in pairs(bad) do
        t:assert_eq(#eventd.rows(vm, "METRIC " .. name .. " SINCE 10m ago"), 0,
            "a sample labelled " .. label .. "= is not stored")
    end
end)

-- ---------------------------------------------------------------------------
-- Origin classes
-- ---------------------------------------------------------------------------

--- The newest event of `etype`, waited for.
local function newest(etype)
    local rows = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago TAKE 1",
        function(rs) return #rs >= 1 end, { timeout = 20 })
    return rows[1]
end

local function both_spellings(t, etype, number, name)
    local by_number = eventd.rows(vm, "EVENTS " .. etype .. " WHERE emitter.class == " .. number .. " SINCE 10m ago")
    local by_name = eventd.rows(vm, "EVENTS " .. etype .. " WHERE emitter.class == " .. name .. " SINCE 10m ago")
    t:assert(#by_number >= 1, etype .. " matches emitter.class == " .. number)
    t:assert_eq(#by_name, #by_number, "and emitter.class == " .. name .. " matches the same")
end

test("origin class 0 is userspace", {
    spec = "eventd *constant.origin-class-0-is-userspace",
}, function(t)
    local etype, row = one_event({ n = 1 })
    t:assert_eq(row["emitter.class"], 0, "an event a userspace process emitted through kmes_emit")
end)

test("origin class 1 is KMES", {
    spec = "eventd *constant.origin-class-1-is-kmes",
}, function(t)
    vm:run([[reg set -p 'Machine\System\KMES' MaxNestingDepth dword:1]]):assert_ok()
    vm:run([[reg del 'Machine\System\KMES' MaxNestingDepth]])
    local row = newest("kmes.config.value.rejected")
    t:assert_eq(row["emitter.class"], 1, "KMES's own report of a setting it rejected: " .. json.encode(row))
end)

test("origin class 2 is KACS", {
    spec = "eventd *constant.origin-class-2-is-kacs",
}, function(t)
    token.as_principal(t, vm, reader(), function() end)
    local row = newest("kacs.session.destroyed")
    t:assert_eq(row["emitter.class"], 2, "KACS's report of a logon session ending: " .. json.encode(row))
end)

test("origin class 3 is LCS", {
    spec = "eventd *constant.origin-class-3-is-lcs",
}, function(t)
    local key = [[Machine\System\PtBackup]] .. eventd.marker()
    vm:run("reg new '" .. key .. "'"):assert_ok()
    vm:run("reg backup '" .. key .. "' /tmp/pt-backup.bin"):assert_ok()
    local row = newest("lcs.audit.backup.started")
    t:assert_eq(row["emitter.class"], 3, "LCS's own report of a key backup: " .. json.encode(row))
end)

test("origin class 4 is NTFE", {
    spec = "eventd *constant.origin-class-4-is-ntfe",
}, function(t)
    -- NTFE publishes its policy at boot, so the record is already there.
    local row = newest("ntfe.policy.published")
    t:assert_eq(row["emitter.class"], 4, "NTFE's report of a policy it published: " .. json.encode(row))
end)

test("the query language takes the origin class names for the numbers", {
    spec = "eventd *constant.the-query-language-accepts-origin-class-names-as-aliases",
}, function(t)
    local etype = one_event({ n = 1 })
    both_spellings(t, etype, 0, "userspace")
    both_spellings(t, "kmes.config.value.rejected", 1, "kmes")
    both_spellings(t, "kacs.session.destroyed", 2, "kacs")
    both_spellings(t, "lcs.audit.backup.started", 3, "lcs")
end)
