-- PEI-1446: header literal types belong to a query namespace, not a spelling.
local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)
local vm = eventd.boot({ name = "ev-literal-namespace" })
local spec = "eventd *plan.header-literal-coercions-are-scoped-to-the-query-namespace"
local guid = "550E8400-E29B-41D4-A716-446655440000"

local function sent(r)
    assert(r.ret and r.ret > 0, "datagram send failed: " .. tostring(r.errno))
end
local function settled(q)
    return eventd.wait_rows(vm, q, function(rs) return #rs == 1 end)
end
local function one(t, q, why)
    t:assert_eq(#eventd.rows(vm, q), 1, why or q)
end

test("metric labels keep string literals in selectors and WHERE", { spec = spec }, function(t)
    local name = eventd.marker("literal")
    local labels = { ["emitter.class"] = "kacs", ["emitter.token.guid"] = guid,
                     ["event.boot.guid"] = guid, job_id = guid }
    sent(eventd.send_metric(vm, { name = name, type = "gauge", value = 9, labels = labels }))
    settled("METRIC " .. name .. " SINCE 10m ago")
    for field, value in pairs(labels) do
        local literal = '"' .. value .. '"'
        one(t, "METRIC " .. name .. "[" .. field .. "=" .. literal .. "] SINCE 10m ago")
        for _, predicate in ipairs({
            field .. " == " .. literal,
            field .. ' IN ("absent", ' .. literal .. ")",
            field .. ' NOT_IN ("absent") AND ' .. field .. " == " .. literal,
            field .. ' == "absent" OR ' .. field .. " == " .. literal,
        }) do
            one(t, "METRIC " .. name .. " WHERE " .. predicate .. " SINCE 10m ago")
        end
    end
    t:assert_eq(#eventd.rows(vm, "METRIC " .. name .. " WHERE emitter.class == 2 SINCE 10m ago"),
        0, "a string label is not an event class number")
    local row = eventd.rows(vm, "METRIC " .. name .. " SINCE 10m ago")[1]
    local boot = row.boot_id:gsub("[{}]", ""):upper()
    one(t, "METRIC " .. name .. ' WHERE boot_id == "' .. boot .. '" SINCE 10m ago',
        "the metric boot header still accepts an unbraced GUID")
end)

test("event aliases and GUID headers remain typed while payload names remain strings", { spec = spec }, function(t)
    local name = eventd.marker("literalEvent")
    t:assert_eq(eventd.emit(vm, name, { boot_id = guid, job_id = guid }).ret, 0)
    local row = settled("EVENTS " .. name .. " SINCE 10m ago")[1]
    one(t, "EVENTS " .. name .. " WHERE emitter.class == USERSPACE SINCE 10m ago",
        "SQL header pushdown keeps the alias")
    one(t, "EVENTS " .. name .. ' WHERE emitter.class IN ("userspace") SINCE 10m ago',
        "the generic evaluator keeps event aliases too")
    for _, field in ipairs({ "event.boot.guid", "emitter.token.guid", "emitter.process.guid" }) do
        local bare = row[field]:gsub("[{}]", ""):upper()
        one(t, "EVENTS " .. name .. " WHERE " .. field .. ' == "' .. bare .. '" SINCE 10m ago')
    end
    for _, field in ipairs({ "boot_id", "job_id" }) do
        one(t, "EVENTS " .. name .. " WHERE " .. field .. ' == "' .. guid .. '" SINCE 10m ago',
            "a payload field does not acquire the log header's type")
    end
    local origin = eventd.marker("literalLog")
    sent(eventd.send_log(vm, { origin = origin, is_error = false, message = "typed headers" }))
    local log = settled("LOGS FROM " .. origin .. " SINCE 10m ago")[1]
    one(t, "LOGS FROM " .. origin .. ' WHERE boot_id == "' .. log.boot_id:gsub("[{}]", ""):upper()
        .. '" SINCE 10m ago', "log boot GUID canonicalisation is retained")
end)

test("cross-metric selectors use metric label types in historical and watch queries", { spec = spec }, function(t)
    local name, origin = eventd.marker("literalCross"), eventd.marker("literalCrossLog")
    local base = eventd.guest_ns(vm) - 60 * 1000000000
    sent(eventd.send_metric(vm, { name = name, type = "gauge", value = 9,
        timestamp = base, labels = { ["emitter.class"] = "kacs" } }))
    settled("METRIC " .. name .. " SINCE 10m ago")
    sent(eventd.send_log(vm, { origin = origin, is_error = false, message = "historical",
        timestamp = base + 1000000000 }))
    settled("LOGS FROM " .. origin .. " SINCE 10m ago")
    local q = "LOGS FROM " .. origin .. " WHERE METRIC " .. name .. '[emitter.class="kacs"] > 5 SINCE 10m ago'
    one(t, q, "the historical cross-source selector resolves its string label")
    local worker = vm:spawn_worker()
    local rq, client = eventd.rq
    local ok, err = pcall(function()
        client = rq.open(worker)
        rq.timeout(client, 10)
        rq.send(client, q .. " STREAM")
        while true do
            local frame, why = rq.frame(client)
            t:assert(frame, "stream frame: " .. tostring(why))
            if frame.status == "watch" then break end
            t:assert_eq(frame.status, "ok", "historical stream phase")
        end
        sent(eventd.send_log(vm, { origin = origin, is_error = false, message = "watched",
            timestamp = base + 2000000000 }))
        local frame, why = rq.frame(client)
        t:assert(frame, "watch frame: " .. tostring(why))
        t:assert_eq(frame.status, "ok")
        t:assert_eq(frame.records[1].message, "watched",
            "the watch selector also uses metric label types")
    end)
    if client then rq.close(client) end
    worker:kill(); worker:join()
    if not ok then error(err, 0) end
end)
