-- eventd TRM §7.4 — enforcement: access control as the third query phase,
-- identifiers resolved one by one, records shaped by their identifier's
-- verdicts, which clauses count as reading a field, filtering as part of
-- the logical result, and the audit trail.
--
-- One file-scope eventd serves every test. Each test emits records under
-- marker-named event types, origins and metric names, and writes
-- descriptors only for those. A field is hidden with "deny the field's
-- GUID, then allow the record"; a whole identifier with a descriptor that
-- grants only a group nobody holds. The agent (SYSTEM, an Administrator)
-- is the caller throughout, through evctl.
--
-- Field GUIDs are UUID v5 of the field name in the §B namespace, computed
-- on the host by Python. KACS propagates a field's denial to the record's
-- root, so what is shown of a record with a hidden field is only ever its
-- granted fields. Most tests assert what a hidden field can never do
-- (match, group, sort, be shown), with a control a readable identifier
-- must pass.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
local sys = require("helpers.sys")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-enforce" })

local SY = token.SID.LOCAL_SYSTEM
local EVERYONE = token.SID.EVERYONE
local NOBODY = token.SID.TEST_GROUP_2
local READ, PUBLISH = 0x1, 0x8

local function allow(mask) return access.ace(access.ACE.ALLOWED, mask, SY) end
local function deny_field(f)
    return access.ace(access.ACE.DENIED_OBJECT, READ, SY, 0, { object_type = eventd.field_guid(f) })
end
local function allow_field(f)
    return access.ace(access.ACE.ALLOWED_OBJECT, READ, SY, 0, { object_type = eventd.field_guid(f) })
end

--- A readable record without `fields`.
local function hiding(fields, mask)
    local aces = {}
    for _, f in ipairs(fields) do aces[#aces + 1] = deny_field(f) end
    aces[#aces + 1] = allow(mask or READ)
    return access.simple(aces)
end
local DENY_ALL = access.simple({ access.ace(access.ACE.ALLOWED, READ | PUBLISH, NOBODY) })

local function emit(ty, payload)
    local r = eventd.emit(vm, ty, payload)
    assert(r.ret == 0, "kmes_emit " .. ty .. ": errno " .. tostring(r.errno))
end

local function rows(text)
    local r = eventd.query(vm, text)
    assert(r.ok, "query `" .. text .. "` failed: " .. tostring(r.stderr))
    return r.rows
end

--- Wait until `text` returns rows satisfying `pred`; returns them.
local function settle(text, pred, desc)
    local last
    wait_until(function()
        local r = eventd.query(vm, text)
        if not r.ok then return false end
        last = r.rows
        return pred(last)
    end, { timeout = 15, interval = 0.25, desc = desc or text })
    return last
end

local function count(n) return function(rs) return #rs == n end end

local function values(rs, f)
    local out = {}
    for _, r in ipairs(rs) do out[#out + 1] = tostring(r[f]) end
    table.sort(out)
    return table.concat(out, ",")
end

local function since(q) return q .. " SINCE 1h ago" end

-- ---------------------------------------------------------------------------
-- The phase and its steps
-- ---------------------------------------------------------------------------

--- Two types under one base, `a` hiding `secret` and `b` not, each with a
--- record carrying secret and num.
local function two_types(stem)
    local base = eventd.marker(stem)
    emit(base .. ".a", { secret = "sa", num = 1 })
    emit(base .. ".b", { secret = "sb", num = 2 })
    settle(since("EVENTS " .. base .. ".*"), count(2))
    eventd.put_descriptor(vm, "Events", base .. ".a", hiding({ "secret", "num" }))
    settle(since("EVENTS " .. base .. ".*"), function(rs)
        for _, r in ipairs(rs) do
            if r["event.type"] == base .. ".a" and r.secret ~= nil then return false end
        end
        return true
    end, "the field deny on " .. base .. ".a")
    return base
end

test("access is decided before predicates: a predicate on a field the caller may not read matches nothing there", {
    spec = "eventd *enforce.access-control-is-the-third-query-phase-before-predicates"
        .. " eventd *enforce.a-where-predicate-references-its-fields"
        .. " eventd *enforce.a-denied-referenced-field-excludes-that-identifiers-records"
        .. " eventd *enforce.a-denied-referenced-field-does-not-reject-the-query",
}, function(t)
    local base = two_types("ptphase")
    -- Both records carry a secret; if predicates ran first, `a`'s would
    -- match and then merely lose the field.
    local r = eventd.query(vm, since("EVENTS " .. base .. ".* WHERE secret STARTS_WITH \"s\""))
    t:assert(r.ok, "the query is answered, not rejected: " .. tostring(r.stderr))
    t:assert_eq(values(r.rows, "event.type"), base .. ".b",
        "only the type whose secret may be read contributes")
    r = eventd.query(vm, since("EVENTS " .. base .. ".* WHERE num > 0"))
    t:assert_eq(values(r.rows, "event.type"), base .. ".b", "for any field the predicate names")
end)

test("a broad selector is resolved into its concrete identifiers, each authorized on its own", {
    spec = "eventd *enforce.the-query-is-parsed-for-its-sources-and-filters"
        .. " eventd *enforce.the-concrete-identifiers-a-query-could-touch-are-discovered"
        .. " eventd *enforce.a-broad-selector-is-authorized-identifier-by-identifier"
        .. " eventd *enforce.each-discovered-identifier-is-resolved-checked-and-cached",
}, function(t)
    local base = eventd.marker("ptbroad")
    local tag = eventd.marker("tag")
    emit(base .. ".ok", { tag = tag }); emit(base .. ".no", { tag = tag })
    eventd.send_log(vm, { origin = base .. "ok", is_error = false, message = tag })
    eventd.send_log(vm, { origin = base .. "no", is_error = false, message = tag })
    eventd.send_metric(vm, { name = base .. ".ok", type = "gauge", value = 1 })
    eventd.send_metric(vm, { name = base .. ".no", type = "gauge", value = 2 })
    -- More than one series needs a window aggregation; summed over the
    -- hour, the two names make 3 and the readable one alone 1.
    local mq = since("METRIC " .. base .. ".*") .. " SUM_OVER 1h"
    local function total(rs) return rs[1] and rs[1].value end
    settle(since("EVENTS " .. base .. ".*"), count(2))
    settle(since('LOGS WHERE message == "' .. tag .. '"'), count(2))
    settle(mq, function(rs) return total(rs) == 3 end, "both metric names")

    eventd.put_descriptor(vm, "Events", base .. ".no", DENY_ALL)
    eventd.put_descriptor(vm, "Logs", base .. "no", DENY_ALL)
    eventd.put_descriptor(vm, "Metrics", base .. ".no", DENY_ALL)
    local ev = settle(since("EVENTS " .. base .. ".*"), count(1), "EVENTS " .. base .. ".*")
    t:assert_eq(ev[1]["event.type"], base .. ".ok", "EVENTS <prefix>.* yields only the readable type")
    ev = rows(since('EVENTS WHERE tag == "' .. tag .. '"'))
    t:assert_eq(values(ev, "event.type"), base .. ".ok", "and so does EVENTS with no selector at all")
    local lg = settle(since('LOGS WHERE message == "' .. tag .. '"'), count(1), "LOGS without FROM")
    t:assert_eq(lg[1].origin, base .. "ok", "LOGS without FROM yields only the readable origin")
    local m = settle(mq, function(rs) return total(rs) == 1 end, "METRIC <prefix>.*")
    t:assert_eq(total(m), 1, "METRIC <prefix>.* sums only the readable name's samples")
    local bad = eventd.query(vm, since("EVENTS " .. base .. ".* WHERE"))
    t:assert(not bad.ok, "and a query that does not parse is refused before anything is read")
end)

test("a denied identifier's records are gone before aggregation, ordering and pagination", {
    spec = "eventd *enforce.a-root-denied-identifiers-records-are-excluded-before-aggregation"
        .. " eventd *enforce.execution-runs-with-root-filtering-already-applied"
        .. " eventd *enforce.access-filtering-is-part-of-the-logical-result-not-presentation"
        .. " eventd *enforce.every-aggregate-reflects-only-what-the-caller-may-see",
}, function(t)
    local base = eventd.marker("ptlogical")
    for i = 1, 3 do emit(base .. ".seen", { i = i }) end
    eventd.put_descriptor(vm, "Events", base .. ".hidden", DENY_ALL)
    -- The hidden records are the newest, so a filter applied after
    -- pagination would hand back a short or empty page.
    for i = 1, 5 do emit(base .. ".hidden", { i = i }) end
    wait_until(function()
        local n = 0
        for _, shard in ipairs(eventd.shards(vm)) do
            n = n + eventd.sql(vm, shard, "SELECT count(*) FROM events WHERE event_type = '"
                .. base .. ".hidden'")[1][1]
        end
        return n == 5
    end, { timeout = 15, desc = "the hidden records to be stored" })
    local q = since("EVENTS " .. base .. ".*")
    settle(q, count(3))

    local page = rows(q .. " TAKE 1")
    t:assert_eq(#page, 1, "TAKE 1 is filled from the readable records")
    t:assert_eq(page[1]["event.type"], base .. ".seen", "and with one")
    t:assert_eq(#rows(q .. " SKIP 2 TAKE 5"), 1, "SKIP counts only readable records")
    local by = rows(q .. " COUNT BY event.type")
    t:assert_eq(#by, 1, "COUNT BY sees one type")
    t:assert_eq(by[1].count, 3, "and counts three")
    t:assert_eq(values(rows(q .. " TOP 5 BY event.type"), "event.type"), base .. ".seen",
        "TOP N BY ranks only the readable type")
    t:assert_eq(values(rows(q .. " DISTINCT event.type"), "event.type"), base .. ".seen",
        "DISTINCT lists only the readable type")
    t:assert_eq(rows(q .. " GROUP event.type COUNT")[1].count, 3, "GROUP … COUNT counts three")
    t:assert_eq(#rows(q .. " SORT i DESC"), 3, "SORT orders only the three")
end)

test("a field-only grant keeps the identifier visible, with records of exactly those fields", {
    spec = "eventd *enforce.the-identifier-check-adds-the-fields-the-descriptor-grants-by-name"
        .. " eventd *enforce.an-identifier-with-only-fields-granted-stays-visible"
        .. " eventd *enforce.a-field-only-grant-gives-records-holding-exactly-those-fields",
}, function(t)
    local ty = eventd.marker("ptonly")
    emit(ty, { n = 1, other = "x" })
    emit(ty, { other = "y" })
    settle(since("EVENTS " .. ty), count(2))
    eventd.put_descriptor(vm, "Events", ty, access.simple({ allow_field("n") }))
    local rs = settle(since("EVENTS " .. ty), count(1), "the record carrying n, shaped to n")
    local ks = {}
    for k in pairs(rs[1]) do ks[#ks + 1] = k end
    t:assert_eq(table.concat(ks, ","), "n", "the record holds exactly the granted field")
    t:assert_eq(rs[1].n, 1, "with its value")
end)

test("each record is shaped by the verdicts for its own identifier", {
    spec = "eventd *enforce.each-record-is-shaped-by-its-identifiers-cached-verdicts",
}, function(t)
    local base = eventd.marker("ptshape")
    for _, s in ipairs({ "a", "b", "c" }) do emit(base .. "." .. s, { secret = s, n = 1 }) end
    settle(since("EVENTS " .. base .. ".*"), count(3))
    eventd.put_descriptor(vm, "Events", base .. ".a", hiding({ "secret" }))
    eventd.put_descriptor(vm, "Events", base .. ".b", hiding({ "n" }))
    local rs = settle(since("EVENTS " .. base .. ".*"), function(r)
        local by = {}
        for _, x in ipairs(r) do by[x["event.type"]] = x end
        local a, b = by[base .. ".a"], by[base .. ".b"]
        return #r == 3 and a and a.secret == nil and b and b.n == nil
    end, "both descriptors to shape their records")
    local by = {}
    for _, r in ipairs(rs) do by[r["event.type"]] = r end
    t:assert(by[base .. ".a"].secret == nil and by[base .. ".a"].n == 1, ".a lacks secret, keeps n")
    t:assert(by[base .. ".b"].n == nil and by[base .. ".b"].secret == "b", ".b lacks n, keeps secret")
    t:assert(by[base .. ".c"].n == 1 and by[base .. ".c"].secret == "c",
        "and .c, whose root alone is granted, keeps every field")
end)

--- kacs.audit.access.checked records of eventd's checks against the
--- event-namespace pattern `pattern`, as eventd names it in the check's
--- audit context.
local function audits(pattern)
    return rows('EVENTS kacs.audit.access.checked WHERE object.event-namespace.pattern == "' .. pattern
        .. '" SINCE 1h ago TAKE 100000 SELECT event.sequence, object.kind, access.requested')
end

local AUDIT = access.acl({ access.ace(access.ACE.AUDIT, 0xf, EVERYONE,
    access.ACE_FLAG.SUCCESSFUL_ACCESS | access.ACE_FLAG.FAILED_ACCESS) })

test("each result identifier is re-checked for EVENTD_READ with an audit context naming the pattern", {
    spec = "eventd *enforce.each-result-identifier-is-rechecked-for-eventd-read-with-field-guids",
}, function(t)
    local base = eventd.marker("ptstep9")
    local ty = base .. ".x.y"
    emit(ty, { n = 1 })
    settle(since("EVENTS " .. ty), count(1))
    eventd.put_descriptor(vm, "Events", base, access.simple({ allow(READ) }, { sacl = AUDIT }))
    settle(since("EVENTS " .. ty), count(1))
    wait_until(function() return #audits(base) >= 1 end,
        { timeout = 15, desc = "the result check's audit record" })
    local by_pattern = audits(base)
    t:assert(#by_pattern >= 1, "the check is audited against the pattern " .. base)
    t:assert_eq(#audits(ty), 0, "and nothing against the identifier " .. ty)
    t:assert_eq(by_pattern[1]["object.kind"], "event-namespace", "as an event-namespace check")
    t:assert_eq(by_pattern[1]["access.requested"], READ, "for EVENTD_READ")
end)

-- ---------------------------------------------------------------------------
-- Which clauses read a field
-- ---------------------------------------------------------------------------

local function metric(name, value, labels, ty)
    local r = eventd.send_metric(vm, { name = name, type = ty or "gauge", value = value, labels = labels })
    assert(r.ret and r.ret > 0, "send_metric: errno " .. tostring(r.errno))
end

test("a label filter in the primary selector reads the label", {
    spec = "eventd *enforce.a-metric-label-filter-in-a-primary-selector-references-the-label",
}, function(t)
    local base = eventd.marker("ptlabel")
    local hidden, open = base .. ".h", eventd.marker("ptlabel")
    metric(hidden, 1, { core = "1" })
    metric(open, 1, { core = "1" })
    local filter = '[core="1"]'
    settle(since("METRIC " .. hidden .. filter), count(1))
    settle(since("METRIC " .. open .. filter), count(1))
    eventd.put_descriptor(vm, "Metrics", base, hiding({ "core" }, READ | PUBLISH))
    settle(since("METRIC " .. hidden .. filter), count(0), "the label deny")
    local r = eventd.query(vm, since("METRIC " .. hidden .. filter))
    t:assert(r.ok, "the filtered query is answered: " .. tostring(r.stderr))
    t:assert_eq(#r.rows, 0, "a filter on the hidden label matches nothing")
    t:assert_eq(#rows(since("METRIC " .. open .. filter)), 1,
        "while the same filter on a series whose label may be read still matches")
end)

test("GROUP, COUNT BY, TOP N BY, SORT and DISTINCT all read their fields", {
    spec = "eventd *enforce.grouping-counting-ranking-sorting-and-distinct-fields-are-referenced",
}, function(t)
    local base = two_types("ptclauses")
    local q = since("EVENTS " .. base .. ".*")
    for _, clause in ipairs({ " COUNT BY secret", " TOP 5 BY secret", " DISTINCT secret",
                              " GROUP secret COUNT", " SORT secret" }) do
        local r = eventd.query(vm, q .. clause)
        t:assert(r.ok, clause .. " is answered: " .. tostring(r.stderr))
        t:assert_eq(values(r.rows, "secret"), "sb", clause .. " sees only the readable secret")
    end
end)

test("event and log aggregation arguments are read", {
    spec = "eventd *enforce.event-and-log-aggregation-arguments-are-referenced",
}, function(t)
    local base = two_types("ptaggarg")
    local q = since("EVENTS " .. base .. ".*")
    for _, fn in ipairs({ "SUM", "AVG", "MIN", "MAX" }) do
        local r = rows(q .. " GROUP event.type " .. fn .. " num")
        t:assert_eq(values(r, "event.type"), base .. ".b", fn .. " num groups only the readable type")
    end
    -- Logs: MAX timestamp over two origins, one whose timestamp is hidden.
    local lb = eventd.marker("ptlogagg")
    eventd.send_log(vm, { origin = lb .. "a", is_error = false, message = "m" })
    eventd.send_log(vm, { origin = lb .. "b", is_error = false, message = "m" })
    local lq = since('LOGS WHERE origin STARTS_WITH "' .. lb .. '"')
    settle(lq, count(2))
    eventd.put_descriptor(vm, "Logs", lb .. "a", hiding({ "timestamp" }))
    local r = settle(lq .. " GROUP origin MAX timestamp", count(1), "the log timestamp deny")
    t:assert_eq(r[1].origin, lb .. "b", "MAX timestamp groups only the origin whose timestamp is readable")
end)

--- Whether any record in `rs` carries `field`.
local function any_carries(rs, field)
    for _, r in ipairs(rs) do if r[field] ~= nil then return true end end
    return false
end

test("a metric result's value is a source field a descriptor can withhold", {
    spec = "eventd *enforce.a-metric-results-value-is-a-source-field",
}, function(t)
    local name = eventd.marker("ptvalsrc")
    metric(name, 7)
    settle(since("METRIC " .. name), count(1))
    eventd.put_descriptor(vm, "Metrics", name, hiding({ "value" }, READ | PUBLISH))
    local raw = settle(since("METRIC " .. name), function(rs) return #rs == 1 and rs[1].value == nil end,
        "the value deny")
    t:assert_eq(raw[1].name, name, "a raw sample is still a record, without its value")
end)

test("metric transforms and terminal aggregations read the value field", {
    spec = "eventd *enforce.metric-transforms-and-terminal-aggregations-reference-value",
}, function(t)
    local hidden, open = eventd.marker("ptval"), eventd.marker("ptval")
    for i = 1, 3 do
        metric(hidden, i * 10, nil, "counter"); metric(open, i * 10, nil, "counter")
        vm:run("sleep 1")
    end
    settle(since("METRIC " .. hidden), count(3)); settle(since("METRIC " .. open), count(3))
    eventd.put_descriptor(vm, "Metrics", hidden, hiding({ "value" }, READ | PUBLISH))
    settle(since("METRIC " .. hidden), function(rs) return not any_carries(rs, "value") end,
        "the value deny")
    for _, clause in ipairs({ " RATE", " DELTA", " AVG", " SUM", " MAX", " MAX_OVER 1m", " SUM_OVER 1m" }) do
        local control = eventd.query(vm, since("METRIC " .. open) .. clause)
        t:assert(control.ok and #control.rows >= 1, clause .. " answers on a readable value: "
            .. tostring(control.stderr))
        local r = eventd.query(vm, since("METRIC " .. hidden) .. clause)
        t:assert_eq(#r.rows, 0, clause .. " yields nothing where value is hidden: " .. tostring(r.stderr))
    end
end)

test("a metric boot filter reads boot_id, and a type clause reads type", {
    spec = "eventd *enforce.a-metric-boot-filter-references-boot-id-and-type-clauses-reference-type",
}, function(t)
    local nb, nt, open = eventd.marker("ptboot"), eventd.marker("pttype"), eventd.marker("ptbtopen")
    metric(nb, 1); metric(nt, 1); metric(open, 1)
    local boot_q = function(n) return since("METRIC " .. n .. " WHERE boot_id IS NOT NULL") end
    local type_q = function(n) return since("METRIC " .. n .. ' WHERE type == "gauge"') end
    settle(boot_q(nb), count(1)); settle(type_q(nt), count(1))
    eventd.put_descriptor(vm, "Metrics", nb, hiding({ "boot_id" }, READ | PUBLISH))
    eventd.put_descriptor(vm, "Metrics", nt, hiding({ "type" }, READ | PUBLISH))
    settle(boot_q(nb), count(0), "a boot filter to match nothing where boot_id is hidden")
    settle(type_q(nt), count(0), "a type predicate to match nothing where type is hidden")
    t:assert_eq(#rows(boot_q(open)), 1, "where boot_id may be read, the boot filter matches")
    t:assert_eq(#rows(type_q(open)), 1, "and where type may be read, the type predicate does")
end)

test("SELECT reads nothing: an unreadable field it names is dropped, not the record", {
    spec = "eventd *enforce.select-does-not-count-as-referencing-a-field"
        .. " eventd *enforce.selecting-an-unreadable-field-removes-the-field-not-the-record",
}, function(t)
    local ty = eventd.marker("ptselect")
    emit(ty, { secret = "s", n = 1 })
    settle(since("EVENTS " .. ty), count(1))
    eventd.put_descriptor(vm, "Events", ty, hiding({ "secret" }))
    settle(since("EVENTS " .. ty), function(rs) return #rs == 1 and rs[1].secret == nil end)
    local rs = rows(since("EVENTS " .. ty) .. " SELECT secret, n")
    t:assert_eq(#rs, 1, "the record is returned")
    t:assert_eq(rs[1].n, 1, "with the readable field")
    t:assert_eq(rs[1].secret, nil, "and without the unreadable one")
end)

test("a field's authorization is from its written name, whether or not any record carries it", {
    spec = "eventd *enforce.field-authorization-is-resolved-from-the-written-name-regardless-of-presence",
}, function(t)
    local hidden, open = eventd.marker("ptghost"), eventd.marker("ptghost")
    emit(hidden, { n = 1 }); emit(open, { n = 1 })
    settle(since("EVENTS " .. hidden), count(1)); settle(since("EVENTS " .. open), count(1))
    -- No record anywhere carries `ghost`.
    eventd.put_descriptor(vm, "Events", hidden, hiding({ "ghost" }))
    settle(since("EVENTS " .. hidden .. " WHERE ghost IS NULL"), count(0), "the ghost deny")
    t:assert_eq(#rows(since("EVENTS " .. open .. " WHERE ghost IS NULL")), 1,
        "where ghost may be read, IS NULL matches the record that lacks it")
    t:assert_eq(#rows(since("EVENTS " .. hidden)), 1, "and the hidden type's record is itself readable")
end)

test("row and series identifiers and tiebreakers are not fields a descriptor can withhold", {
    spec = "eventd *enforce.internal-values-are-not-source-fields-unless-exposed-or-named",
}, function(t)
    local ty, name = eventd.marker("ptinternal"), eventd.marker("ptinternal")
    for i = 1, 2 do emit(ty, { n = i }) end
    metric(name, 1)
    settle(since("EVENTS " .. ty), count(2)); settle(since("METRIC " .. name), count(1))
    local internal = { "id", "rowid", "series_id", "tie", "shard" }
    eventd.put_descriptor(vm, "Events", ty, hiding(internal))
    eventd.put_descriptor(vm, "Metrics", name, hiding(internal, READ | PUBLISH))
    vm:run("sleep 1")
    t:assert_eq(#rows(since("EVENTS " .. ty)), 2, "default-order events, tiebroken by row id, are returned")
    t:assert_eq(#rows(since("EVENTS " .. ty) .. " SORT n TAKE 1"), 1, "and a sorted page")
    t:assert_eq(#rows(since("METRIC " .. name)), 1, "and the metric series")
end)

test("an error from an internal check does not carry a hidden field's value", {
    spec = "eventd *enforce.internal-check-errors-never-carry-a-denied-fields-value",
}, function(t)
    local base = eventd.marker("ptinterr")
    metric(base .. ".g", 1, nil, "gauge")
    metric(base .. ".c", 1, nil, "counter")
    settle(since("METRIC " .. base .. ".g"), count(1)); settle(since("METRIC " .. base .. ".c"), count(1))
    eventd.put_descriptor(vm, "Metrics", base, hiding({ "type" }, READ | PUBLISH))
    settle(since("METRIC " .. base .. ".g"), function(rs) return not any_carries(rs, "type") end)
    for _, q in ipairs({ "METRIC " .. base .. ".g RATE", "METRIC " .. base .. ".* SUM" }) do
        local r = eventd.query(vm, since(q))
        local text = (r.stderr or "") .. (r.stdout or "")
        -- The checks may say what they require; never what the hidden
        -- series' type is.
        t:assert(not text:find("gauge", 1, true),
            "`" .. q .. "` does not name the hidden type: " .. text)
    end
end)

test("an unreadable cross-type source, or a field its condition needs, means no matching data", {
    spec = "eventd *enforce.a-denied-cross-type-identifier-has-no-matching-data"
        .. " eventd *enforce.a-denied-cross-source-root-or-field-evaluates-as-no-matching-data",
}, function(t)
    local ty, origin, word = eventd.marker("ptcross"), eventd.marker("ptcross"), eventd.marker("w")
    emit(ty, { n = 1 })
    eventd.send_log(vm, { origin = origin, is_error = false, message = word })
    settle(since("EVENTS " .. ty), count(1)); settle(since("LOGS FROM " .. origin), count(1))
    local by_event = "LOGS FROM " .. origin .. " SINCE 1h ago WHERE EVENT " .. ty .. " EXISTS"
    local by_log = "EVENTS " .. ty .. " SINCE 1h ago WHERE LOG " .. origin .. " CONTAINING " .. word .. " EXISTS"
    t:assert_eq(#rows(by_event), 1, "with the event readable, the log meets the condition")
    t:assert_eq(#rows(by_log), 1, "with the log readable, the event meets the condition")

    eventd.put_descriptor(vm, "Logs", origin, hiding({ "message" }))
    settle(by_log, count(0), "the hidden message to fail CONTAINING")
    t:assert_eq(#rows("EVENTS " .. ty .. " SINCE 1h ago WHERE LOG " .. origin .. " EXISTS"), 1,
        "though the log still exists for a condition that needs only its timestamp")

    -- The log is still readable for EXISTS; deny the event type instead.
    eventd.drop_descriptor(vm, "Logs", origin)
    eventd.put_descriptor(vm, "Events", ty, DENY_ALL)
    settle(by_event, count(0), "the denied event type to count as absent")
end)

-- ---------------------------------------------------------------------------
-- The audit trail
-- ---------------------------------------------------------------------------

test("a check a SACL audits is recorded by KACS, against the data type and pattern, into the event store", {
    spec = "eventd *enforce.the-audit-context-names-the-data-type-and-pattern-accessed"
        .. " eventd *enforce.access-audit-events-are-stored-and-governed-like-any-other-event",
}, function(t)
    local base = eventd.marker("ptaudit")
    local ty, origin = base .. ".sub", base .. "log"
    emit(ty, { n = 1 })
    eventd.send_log(vm, { origin = origin, is_error = false, message = "m" })
    settle(since("EVENTS " .. ty), count(1)); settle(since("LOGS FROM " .. origin), count(1))
    eventd.put_descriptor(vm, "Events", base, access.simple({ allow(READ) }, { sacl = AUDIT }))
    eventd.put_descriptor(vm, "Logs", origin, access.simple({ allow(READ) }, { sacl = AUDIT }))
    vm:run("sleep 1")
    rows(since("EVENTS " .. ty)); rows(since("LOGS FROM " .. origin))
    local ev = settle('EVENTS kacs.audit.access.checked WHERE object.event-namespace.pattern == "' .. base
        .. '" SINCE 1h ago TAKE 1000', function(rs) return #rs >= 1 end, "the event check's audit")
    local lg = settle('EVENTS kacs.audit.access.checked WHERE object.log-namespace.pattern == "' .. origin
        .. '" SINCE 1h ago TAKE 1000', function(rs) return #rs >= 1 end, "the log check's audit")
    t:assert_eq(ev[1]["object.kind"], "event-namespace", "the event check names the event data type")
    t:assert_eq(lg[1]["object.kind"], "log-namespace", "and the log check the log data type")
    t:assert_eq(ev[1]["fields.attestation.userspace"], true, "each named by eventd, as an asserted value")
    t:assert_eq(ev[1]["access.requested"], READ, "each audited check asked for EVENTD_READ")
    t:assert_eq(lg[1]["emitter.process.executable"], "/usr/sbin/eventd", "and was eventd's own")

    -- The audit records are events like any other:
    -- Events\kacs.audit.access.checked governs them.
    local stored = 0
    for _, shard in ipairs(eventd.shards(vm)) do
        stored = stored + eventd.sql(vm, shard,
            "SELECT count(*) FROM events WHERE event_type = 'kacs.audit.access.checked'")[1][1]
    end
    t:assert(stored >= 2, "the audit records are in the event store: " .. stored)
    eventd.put_descriptor(vm, "Events", "kacs.audit.access.checked", DENY_ALL)
    settle("EVENTS kacs.audit.access.checked SINCE 1h ago TAKE 1", count(0),
        "Events\\kacs.audit.access.checked to govern them")
    eventd.drop_descriptor(vm, "Events", "kacs.audit.access.checked")
end)

--- A minted ordinary user: Everyone and Authenticated Users but not an
--- Administrator, so the default `Events\*` (SYSTEM and Administrators
--- only) denies it. SeChangeNotifyPrivilege for the walk to /run/eventd.
local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
local GROUP_ON = token.GROUP.MANDATORY | token.GROUP.ENABLED_BY_DEFAULT | token.GROUP.ENABLED
local function ordinary_user()
    return {
        user_sid = token.SID.TEST_USER,
        privs_present = NOTIFY, privs_enabled = NOTIFY,
        groups = {
            { sid = EVERYONE, attributes = GROUP_ON },
            { sid = token.SID.AUTHENTICATED_USERS, attributes = GROUP_ON },
        },
    }
end

--- kacs.audit.access.checked records under the default `Events\*` written
--- at or after `from`, narrowed by `where` (query text after the pattern).
local function default_audits(from, where)
    local out = {}
    for _, r in ipairs(rows('EVENTS kacs.audit.access.checked WHERE object.event-namespace.pattern == "*"'
        .. (where or "") .. " SINCE 1h ago TAKE 100000")) do
        if r["event.time"] >= from then out[#out + 1] = r end
    end
    return out
end

-- PEI-1279 item 5: every default descriptor carries (AU;FA;0xd;;;WD), so a
-- denial under one is a KACS record with no descriptor work by anyone, and
-- a grant, which no success ACE asks for, is not.
test("under a default descriptor every denied check is audited, and a granted one is not", {
    spec = "eventd *enforce.every-denied-check-under-a-default-descriptor-produces-a-kacs-audit-event",
}, function(t)
    local ty = eventd.marker("ptdefaudit") .. ".x"
    emit(ty, { n = 1 })
    settle(since("EVENTS " .. ty), count(1))
    local from = eventd.guest_ns(vm)
    -- Granted, as SYSTEM, and a fresh check: a field set the cache has not
    -- seen for this identifier (§7.5).
    t:assert_eq(#rows(since("EVENTS " .. ty .. " WHERE n == 1")), 1, "SYSTEM reads it under Events\\*")
    local seen
    token.as_principal(t, vm, ordinary_user(), function(w)
        local r = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", since("EVENTS " .. ty) } })
        seen = r.exit_code == 0 and r.stdout or ("exit " .. tostring(r.exit_code) .. ": " .. tostring(r.stderr))
    end)
    t:assert(not tostring(seen):find(ty, 1, true), "the ordinary user is shown nothing of it: " .. tostring(seen))
    local user = ' WHERE subject.token.sid == x"' .. eventd.hex(token.SID.TEST_USER) .. '"'
    local denied
    pcall(wait_until, function()
        denied = default_audits(from, user)[1]
        return denied ~= nil
    end, { timeout = 15, interval = 0.25, desc = "the denied check's audit record" })
    t:assert(denied, "the denial under Events\\* was audited, with no SACL written by anyone")
    t:assert_eq(denied and denied["outcome.success"], false, "as a failure")
    t:assert_eq(denied and denied["access.requested"], READ, "of EVENTD_READ")
    t:assert_eq(denied and denied["trigger.kind"], "sacl", "triggered by the descriptor's SACL")
    t:assert_eq(denied and denied["object.kind"], "event-namespace", "naming the event namespace")
    t:assert_eq(denied and denied["emitter.process.executable"], "/usr/sbin/eventd", "from eventd's check")
    -- The denial's record is there, so a record of the earlier grant would
    -- be too.
    local granted = default_audits(from, " WHERE outcome.success == true")
    t:assert_eq(#granted, 0, "and the granted check wrote nothing: " .. json.encode(granted[1]))
end)

-- A8: KACS writes SACL records from the AccessCheck syscall only for a
-- caller holding SeAuditPrivilege enabled. The denial above being recorded
-- at all is the consequence; this is the cause, read off eventd's token.
test("eventd's checks are audited because its token holds SeAuditPrivilege, enabled", {
    spec = "eventd *enforce.eventds-checks-are-audited-because-its-token-holds-seauditprivilege",
}, function(t)
    local pidfd = assert(token.pidfd_open(vm, eventd.pid(vm)))
    local fd, e = token.open_process(vm, pidfd, token.RIGHT.QUERY)
    sys.close(vm, pidfd)
    t:assert(fd, "eventd's token opens for query: errno " .. tostring(e))
    local p = fd and token.privileges(vm, fd)
    if fd then sys.close(vm, fd) end
    local audit = token.bit(token.PRIV.AUDIT)
    t:assert(p and p.present & audit ~= 0, "SeAuditPrivilege is on eventd's token")
    t:assert(p and p.enabled & audit ~= 0, "and enabled")
end)

-- Route closed: as for §7.1 (access-model.test.lua), KACS captures an
-- identity on every connect(), so nothing a client does makes eventd's
-- peer-token read fail; the branch is Authorizer::from_peer(...)? in
-- `handle` (eventd/src/query/mod.rs:269-271), taken before the request is
-- read or parsed.
test("the token is read before anything else, and a failure denies the query", {
    spec = "eventd *enforce.the-token-is-obtained-first-and-failure-denies-the-query",
    skip = true,
    covered_by = "cargo:eventd eventd query::security::tests::the_peer_token_is_read_before_the_request_and_a_failed_read_answers_nothing",
}, function() end)
