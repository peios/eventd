-- PEI-1336 item 3: canonical-label tiebreakers survive field filtering.
local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local access = require("helpers.access")
local token = require("helpers.token")
peinit.claim(1)
local vm = eventd.boot({ name = "ev-hidden-label-order" })

local function fixture(kind, pairs)
    local name = eventd.marker("hiddenorder")
    -- Keep both samples inside one completed minute for window tests.
    local ts = (math.floor(eventd.guest_ns(vm) / 60000000000) - 2) * 60000000000
    -- b is inserted first. Its ID would win if the hidden label were lost.
    for _, label in ipairs({ "b", "a" }) do
        local value = label == "a" and 20 or 10
        local r = eventd.send_metric(vm, { name = name, type = kind,
            labels = { core = label }, value = pairs and 0 or value, timestamp = ts })
        assert(r.ret > 0, "metric datagram sent")
        if pairs then
            r = eventd.send_metric(vm, { name = name, type = kind,
                labels = { core = label }, value = value, timestamp = ts + 1000000000 })
            assert(r.ret > 0, "second metric datagram sent")
        end
    end
    local query = "METRIC " .. name .. "[] SINCE 10m ago"
    eventd.wait_rows(vm, query, function(rs) return #rs == (pairs and 4 or 2) end)
    local sy = token.SID.LOCAL_SYSTEM
    eventd.put_descriptor(vm, "Metrics", name, access.simple({
        access.ace(access.ACE.DENIED_OBJECT, 1, sy, 0,
            { object_type = eventd.field_guid("core") }),
        access.ace(access.ACE.ALLOWED, 9, sy),
    }))
    eventd.wait_rows(vm, query, function(rs)
        if #rs ~= (pairs and 4 or 2) then return false end
        for _, row in ipairs(rs) do if row.core ~= nil then return false end end
        return true
    end)
    return name, query
end

local function ordered(t, query)
    local rows = eventd.rows(vm, query)
    t:assert_eq(#rows, 2, "both authorized records remain")
    t:assert_eq(rows[1].value, 20, "canonical core=a sorts before core=b")
    t:assert_eq(rows[2].value, 10, "sample ID does not replace the label tiebreak")
    for _, row in ipairs(rows) do
        t:assert(row.core == nil, "the denied label is absent")
        for key in pairs(row) do
            t:assert(key == "timestamp" or key == "boot_id" or key == "name"
                or key == "type" or key == "value", "no private ordering key: " .. key)
        end
    end
end

test("denied metric labels retain raw sample ordering and pagination", {
    spec = "eventd *order.hidden-metric-labels-remain-in-the-internal-tiebreaker",
}, function(t)
    local name, query = fixture("gauge", false)
    ordered(t, query)
    ordered(t, query .. " SORT timestamp")
    local first = eventd.rows(vm, query .. " TAKE 1")
    local second = eventd.rows(vm, query .. " SKIP 1 TAKE 1")
    t:assert_eq(first[1].value, 20, "first page")
    t:assert_eq(second[1].value, 10, "second page")
    for _, q in ipairs({ query .. " SORT core", query .. ' WHERE core == "a"',
        "METRIC " .. name .. '[core="a"] SINCE 10m ago' }) do
        t:assert_eq(#eventd.rows(vm, q), 0, "explicit denied-field reference filters records")
    end
end)

test("denied metric labels retain latest, scalar and window ordering", {
    spec = "eventd *order.hidden-metric-labels-remain-in-the-internal-tiebreaker",
}, function(t)
    local name, query = fixture("gauge", false)
    ordered(t, "METRIC " .. name .. "[]")
    ordered(t, query .. " SUM")
    ordered(t, query .. " SUM_OVER 1m")
end)

test("denied metric labels retain transformed sample and window ordering", {
    spec = "eventd *order.hidden-metric-labels-remain-in-the-internal-tiebreaker",
}, function(t)
    local _, query = fixture("counter", true)
    ordered(t, query .. " DELTA")
    ordered(t, query .. " RATE")
    ordered(t, query .. " DELTA SUM_OVER 1m")
end)
