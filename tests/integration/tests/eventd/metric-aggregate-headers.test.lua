-- PEI-1450 / PEI-1336 item 4: aggregate metadata obeys source-field grants.
local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local access = require("helpers.access")
local token = require("helpers.token")
peinit.claim(1)
local vm = eventd.boot({ name = "ev-aggregate-headers", noncritical = true })
local W = 60000000000
local HEADERS = { "timestamp", "name", "type" }
local SPEC = "eventd *fieldaccess.metric-aggregates-do-not-restore-denied-source-headers"

-- Read the wire map, including null-valued keys, to distinguish absent from null.
local function rows(query)
    local r = eventd.rq.ask(vm, query, { timeout = 20 })
    assert(r.status == "end", "query failed: " .. query .. ": " .. tostring(r.error))
    return r.records
end

local function seed(name, kind, core, sample_start)
    name = name or eventd.marker("aggregateheaders")
    local at = sample_start or (math.floor(eventd.guest_ns(vm) / W) - 2) * W
    local samples = {}
    for i = 0, 1 do
        samples[#samples + 1] = { name = name, type = kind or "gauge",
            labels = { core = core or "a" }, timestamp = at + i * 1000000000,
            value = kind == "counter" and i * 12 or 6 }
    end
    local sent = eventd.send_metric(vm, eventd.array(samples))
    assert(sent.ret > 0, "metric datagram accepted")
    eventd.wait_rows(vm, "METRIC " .. name .. "[] SINCE 10m ago", function(rs) return #rs == 2 end)
    return name, at
end

local function hide(name, fields)
    local aces = {}
    for _, field in ipairs(fields) do
        aces[#aces + 1] = access.ace(access.ACE.DENIED_OBJECT, 1, token.SID.LOCAL_SYSTEM, 0,
            { object_type = eventd.field_guid(field) })
    end
    aces[#aces + 1] = access.ace(access.ACE.ALLOWED, 9, token.SID.LOCAL_SYSTEM)
    eventd.put_descriptor(vm, "Metrics", name, access.simple(aces))
    wait_until(function()
        local rs = rows("METRIC " .. name .. "[] SINCE 10m ago")
        if #rs ~= 2 then return false end
        for _, row in ipairs(rs) do
            for _, field in ipairs(fields) do if row[field] ~= nil then return false end end
        end
        return true
    end, { timeout = 20, interval = 0.25, desc = "source headers denied for " .. name })
end

local function absent(t, row, fields)
    for _, field in ipairs(fields) do
        t:assert(row[field] == nil, field .. " remains absent, including no null-valued wire key")
    end
end

for _, denied in ipairs(HEADERS) do
    test("metric latest, scalar and window results do not restore denied " .. denied, { spec = SPEC }, function(t)
        local name = seed()
        local control = rows("METRIC " .. name .. "[] SINCE 10m ago SUM_OVER 1m")
        t:assert_eq(#control, 1, "one authorized window")
        for _, field in ipairs(HEADERS) do t:assert(control[1][field] ~= nil, field .. " is initially visible") end
        hide(name, { denied })
        for _, brackets in ipairs({ "", "[]" }) do
            local base = "METRIC " .. name .. brackets
            for _, case in ipairs({ { "", 6 }, { " SINCE 10m ago SUM", 12 },
                { " SINCE 10m ago AVG", 6 }, { " SINCE 10m ago MIN", 6 },
                { " SINCE 10m ago MAX", 6 }, { " SINCE 10m ago SUM_OVER 1m", 12 } }) do
                local rs = rows(base .. case[1])
                t:assert_eq(#rs, 1, "the authorized result is still returned: " .. base .. case[1])
                t:assert_eq(rs[1].value, case[2], "the numeric result is unchanged")
                absent(t, rs[1], { denied })
                for _, field in ipairs(HEADERS) do
                    if field ~= denied then t:assert(rs[1][field] ~= nil, field .. " stays visible") end
                end
            end
        end
        t:assert_eq(#rows("METRIC " .. name .. "[] SINCE 10m ago SORT " .. denied), 0,
            "explicit denied-field references still filter the sources")
    end)
end

test("combined metric headers require visibility in every contributor in either identifier order", { spec = SPEC }, function(t)
    for _, hidden_suffix in ipairs({ "a", "b" }) do
        local stem = eventd.marker("mixedheaders")
        local at = (math.floor(eventd.guest_ns(vm) / W) - 2) * W
        for _, suffix in ipairs({ "a", "b" }) do seed(stem .. "." .. suffix, "gauge", suffix, at) end
        hide(stem .. "." .. hidden_suffix, HEADERS)
        for _, case in ipairs({ { "", 6 }, { " SINCE 10m ago SUM_OVER 1m", 24 } }) do
            local rs = rows("METRIC " .. stem .. ".*" .. case[1])
            t:assert_eq(#rs, 1, "one combined result")
            t:assert_eq(rs[1].value, case[2], "both sources contribute")
            absent(t, rs[1], HEADERS)
        end
        local separate = rows("METRIC " .. stem .. ".*[] SINCE 10m ago SUM_OVER 1m")
        t:assert_eq(#separate, 2, "brackets keep the two series separate")
        for _, row in ipairs(separate) do
            t:assert_eq(row.value, 12, "one series per window")
            if row.core == hidden_suffix then absent(t, row, HEADERS)
            else for _, field in ipairs(HEADERS) do t:assert(row[field] ~= nil, "separate visible source retains " .. field) end end
        end
    end
end)

test("transformed metric samples and windows preserve hidden headers", { spec = SPEC }, function(t)
    local name = seed(nil, "counter")
    hide(name, HEADERS)
    for _, brackets in ipairs({ "", "[]" }) do
        for _, transform in ipairs({ "RATE", "DELTA" }) do
            for _, aggregate in ipairs({ "", " SUM_OVER 1m" }) do
                local rs = rows("METRIC " .. name .. brackets .. " SINCE 10m ago " .. transform .. aggregate)
                t:assert_eq(#rs, 1, "one pair or window")
                t:assert_eq(rs[1].value, 12, "private timestamps still determine the one-second pair")
                absent(t, rs[1], HEADERS)
            end
        end
    end
    -- Combined RATE/DELTA windows fold each series first, then combine
    -- those derived rows. A hidden header must survive neither stage.
    local stem = eventd.marker("mixedtransformedheaders")
    local at = (math.floor(eventd.guest_ns(vm) / W) - 2) * W
    for _, suffix in ipairs({ "a", "b" }) do seed(stem .. "." .. suffix, "counter", suffix, at) end
    hide(stem .. ".b", HEADERS)
    for _, transform in ipairs({ "RATE", "DELTA" }) do
        local rs = rows("METRIC " .. stem .. ".* SINCE 10m ago " .. transform .. " SUM_OVER 1m")
        t:assert_eq(#rs, 1, "one combined transformed window")
        t:assert_eq(rs[1].value, 24, "both transformed series contribute")
        absent(t, rs[1], HEADERS)
    end

end)

test("a cached metric window rechecks source-header visibility", { spec = SPEC }, function(t)
    local name, at = seed()
    -- A valid single-window cache fixture has a distinctive value so the
    -- test proves the cached path was used, rather than merely recomputing raw.
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.metrics,
        "INSERT INTO rollups (series_id,window_start,window_width,transform,function,value,overflow,source_max_sample_id,source_baseline_sample_id) " ..
        "SELECT id," .. at .. "," .. W .. ",0,0,42,0,(SELECT MAX(id) FROM samples WHERE series_id=series.id),NULL " ..
        "FROM series WHERE name='" .. name .. "'")
    eventd.start(vm)
    local span = " SINCE " .. os.date("!%Y-%m-%dT%H:%M:%S", at // 1000000000) ..
        " UNTIL " .. os.date("!%Y-%m-%dT%H:%M:%S", (at + W) // 1000000000) .. " AVG_OVER 1m"
    local query = "METRIC " .. name .. "[]" .. span
    local control = rows(query)
    t:assert_eq(#control, 1, "one cached window")
    t:assert_eq(control[1].value, 42, "the cache fixture, not the raw average 6")
    hide(name, HEADERS)
    local rs = rows(query)
    t:assert_eq(#rs, 1, "the authorized cached aggregate remains")
    t:assert_eq(rs[1].value, 42, "the cached path is still used")
    absent(t, rs[1], HEADERS)
end)
