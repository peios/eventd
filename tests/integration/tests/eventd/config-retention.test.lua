-- eventd TRM Appendix A — Configuration Keys: the three REG_QWORD retention
-- byte limits, and what their zero means.
--
-- A VM of its own, because the effect half of each test sets a limit of one
-- byte, and eventd then deletes everything it may from that store. The
-- default and range half works the way config-keys does: eventd records a
-- `eventd.config.changed` for every change it applies and nothing for one
-- it ignores, and an absence is only asserted behind a barrier change to
-- `CrossTypeMaxLookbackSeconds`, which eventd records last (config.rs:819).
--
-- Every applied change also requests a retention pass at once
-- (config.rs:1032, and retention.rs polls the request every 100 ms), so a
-- limit takes effect within moments rather than at the hourly check. The
-- tests run logs, then metrics, then events, because emptying the event
-- store also deletes the config_change records the other tests count.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-retention" })

local CC = eventd.T.config_change
local function q(s) return '"' .. s .. '"' end

local function changes(key)
    return eventd.rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) .. " SINCE 1h ago")
end

local barrier_seq = 0
local function barrier()
    barrier_seq = barrier_seq + 1
    local v = 300000 + barrier_seq
    eventd.set(vm, "CrossTypeMaxLookbackSeconds", "dword:" .. v):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. CC .. ' WHERE config.name == "CrossTypeMaxLookbackSeconds" AND config.value == ' ..
        v .. " SINCE 1h ago", function(rs) return #rs >= 1 end)
end

local function ignored(t, key, value, why)
    local before = #changes(key)
    eventd.set(vm, key, value):assert_ok()
    barrier()
    t:assert_eq(#changes(key), before, why .. " — " .. key .. "=" .. value .. " recorded no change")
end

local function applied(t, key, value, rendered, why)
    eventd.set(vm, key, value):assert_ok()
    -- `rendered` is the integer eventd records, compared as a number.
    local _, ok = eventd.wait_rows(vm, "EVENTS " .. CC .. " WHERE config.name == " .. q(key) ..
        " AND config.value == " .. rendered .. " SINCE 1h ago",
        function(rs) return #rs >= 1 end, { timeout = 10, desc = key .. "=" .. rendered })
    t:assert(ok, why .. " — " .. key .. "=" .. value .. " was recorded as applied")
end

--- Default, range and type for one REG_QWORD key.
local function sweep_qword(t, key, D)
    eventd.unset(vm, key)
    barrier()
    ignored(t, key, "qword:" .. D, "the documented default " .. D .. " is the value in use")
    -- A large value: a small one would start a deletion pass, and a pass
    -- runs on the configuration it started with (retention.rs:62-63) until
    -- the store is momentarily empty, eating records sent after it.
    applied(t, key, "qword:1099511627776", "1099511627776", "another value is applied")
    applied(t, key, "qword:18446744073709551615", "18446744073709551615",
        "2^64-1, the top of the range, is applied")
    if D ~= 0 then applied(t, key, "qword:0", "0", "0, the bottom of the range, is applied") end
    ignored(t, key, "dword:5", "a REG_DWORD is the wrong type and is ignored")
    applied(t, key, "qword:" .. D, tostring(D), "setting the default back is a change")
    local before = #changes(key)
    eventd.unset(vm, key)
    barrier()
    t:assert_eq(#changes(key), before, "deleting the key, back to its default " .. D .. ", changes nothing")
end

--- Wait until a deletion pass started under an earlier limit has finished:
--- a record sent now must still be there a few seconds later.
local function quiesce(kind)
    pcall(wait_until, function()
        local tag = eventd.marker("qz")
        local text
        if kind == "metric" then
            eventd.send_metric(vm, { name = tag, type = "gauge", value = 1 })
            text = "METRIC " .. tag .. " SINCE 1h ago"
        elseif kind == "event" then
            tag = "pt.qz" .. tag
            eventd.emit(vm, tag, { n = 1 })
            text = "EVENTS " .. tag .. " SINCE 1h ago"
        else
            eventd.send_log(vm, { origin = tag, is_error = false, message = "s" })
            text = "LOGS FROM " .. tag .. " SINCE 1h ago"
        end
        vm:run("sleep 3")
        return #eventd.rows(vm, text) == 1
    end, { timeout = 40, interval = 0.5, desc = "retention to finish its pass" })
end

--- Wait until `text` returns no rows (true) or give up (false).
local function emptied(text, seconds)
    return pcall(wait_until, function() return #eventd.rows(vm, text) == 0 end,
        { timeout = seconds or 30, interval = 0.5, desc = "retention to delete: " .. text })
end

local function logs(origin, n)
    for i = 1, n do
        local r = eventd.send_log(vm, { origin = origin, is_error = false, message = string.rep("r", 1000) .. i })
        assert(r.ret and r.ret > 0, "log " .. i .. " sent: errno " .. tostring(r.errno))
    end
    local ok = pcall(eventd.wait_rows, vm, "LOGS FROM " .. origin .. " SINCE 1h ago",
        function(rs) return #rs == n end)
    assert(ok, n .. " logs stored, saw " .. #eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 1h ago") ..
        "; logs.db holds " .. json.encode(eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs")))
end

local function live_size(db)
    local r = eventd.sql(vm, db, "SELECT (SELECT page_count FROM pragma_page_count()) - " ..
        "(SELECT freelist_count FROM pragma_freelist_count()), (SELECT page_size FROM pragma_page_size()), " ..
        "(SELECT page_count FROM pragma_page_count())")[1]
    return r[1] * r[2], r[3] * r[2]
end

test("LogRetentionMaxBytes is a REG_QWORD defaulting to 0, and 0 is no limit", {
    spec = "eventd *config.log-retention-max-bytes-defaults-to-0-meaning-no-limit",
}, function(t)
    sweep_qword(t, "LogRetentionMaxBytes", 0)
    -- A one-byte limit empties the log store: a limit is enforced.
    local first = eventd.marker("lr")
    logs(first, 20)
    eventd.set(vm, "LogRetentionMaxBytes", "qword:1"):assert_ok()
    t:assert(emptied("LOGS FROM " .. first .. " SINCE 1h ago"), "a 1-byte limit deletes the logs")
    -- 0, explicitly, is none: the same logs survive a retention pass.
    applied(t, "LogRetentionMaxBytes", "qword:0", "0", "an explicit 0")
    quiesce("log")
    local second = eventd.marker("lr")
    logs(second, 20)
    barrier() -- every applied change requests a retention pass
    vm:run("sleep 3")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. second .. " SINCE 1h ago"), 20,
        "with the limit at 0, a retention pass deletes nothing")
    eventd.unset(vm, "LogRetentionMaxBytes")
    barrier()
    vm:run("sleep 3")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. second .. " SINCE 1h ago"), 20,
        "nor at the default")
end)

test("retention measures the logical live size, (page_count - freelist_count) x page_size, not the file", {
    spec = "eventd *term.retention-is-enforced-against-logical-live-size-not-file-size",
}, function(t)
    -- Fill the log store well past a limit, then set the limit, deleting
    -- in the smallest batches: retention deletes until the live pages fit
    -- and stops. The freed pages stay in the file for reuse, so the file
    -- stays over the limit — measured by file size, retention would have
    -- gone on until no row was left.
    local origin = eventd.marker("ls")
    logs(origin, 600)
    applied(t, "RetentionDeleteBatchRows", "dword:100", "100", "small deletion batches")
    local live_before, file_before = live_size(eventd.DB.logs)
    local limit = math.floor(live_before / 3)
    eventd.set(vm, "LogRetentionMaxBytes", "qword:" .. limit):assert_ok()
    local live, file
    local ok = pcall(wait_until, function()
        live, file = live_size(eventd.DB.logs)
        return live <= limit
    end, { timeout = 30, interval = 0.5, desc = "the live size to fall under the limit" })
    vm:run("sleep 3")
    live, file = live_size(eventd.DB.logs)
    local left = #eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 1h ago")
    eventd.unset(vm, "LogRetentionMaxBytes")
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert(ok, "retention brought the live size under " .. limit .. ": " .. tostring(live) ..
        " (it was " .. live_before .. ")")
    t:assert(file > limit, "while the file itself is still " .. tostring(file) .. " bytes, over the limit " ..
        "(it was " .. file_before .. ")")
    t:assert(left > 0, "and retention stopped there, with rows left (" .. left .. " of 600): " ..
        "the measure it stopped on was the live size, not the file's")
end)

test("MetricRetentionMaxBytes is a REG_QWORD defaulting to 1 GiB, and 0 disables it", {
    spec = "eventd *config.metric-retention-max-bytes-defaults-to-1-gib-and-0-disables-it",
}, function(t)
    sweep_qword(t, "MetricRetentionMaxBytes", 1073741824)
    local first = eventd.marker("mr")
    eventd.send_metric(vm, { name = first, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. first .. " SINCE 1h ago", function(rs) return #rs == 1 end)
    eventd.set(vm, "MetricRetentionMaxBytes", "qword:1"):assert_ok()
    t:assert(emptied("METRIC " .. first .. " SINCE 1h ago"), "a 1-byte limit deletes the samples")
    applied(t, "MetricRetentionMaxBytes", "qword:0", "0", "an explicit 0")
    quiesce("metric")
    local second = eventd.marker("mr")
    eventd.send_metric(vm, { name = second, type = "gauge", value = 2 })
    eventd.wait_rows(vm, "METRIC " .. second .. " SINCE 1h ago", function(rs) return #rs == 1 end)
    barrier()
    vm:run("sleep 3")
    t:assert_eq(#eventd.rows(vm, "METRIC " .. second .. " SINCE 1h ago"), 1,
        "with the limit explicitly 0, a retention pass deletes nothing")
    eventd.unset(vm, "MetricRetentionMaxBytes")
end)

test("EventRetentionMaxBytes is a REG_QWORD defaulting to 0, and 0 is no limit", {
    spec = "eventd *config.event-retention-max-bytes-defaults-to-0-meaning-no-limit",
}, function(t)
    sweep_qword(t, "EventRetentionMaxBytes", 0)
    local first = "pt.er" .. eventd.marker()
    for i = 1, 20 do eventd.emit(vm, first, { i = i, pad = string.rep("e", 500) }) end
    eventd.wait_rows(vm, "EVENTS " .. first .. " SINCE 1h ago", function(rs) return #rs == 20 end)
    eventd.set(vm, "EventRetentionMaxBytes", "qword:1"):assert_ok()
    t:assert(emptied("EVENTS " .. first .. " SINCE 1h ago"), "a 1-byte limit deletes the events")
    -- The config_change for this may itself have been deleted; set 0 and
    -- give it a moment instead of waiting for its record.
    eventd.set(vm, "EventRetentionMaxBytes", "qword:0"):assert_ok()
    quiesce("event")
    local second = "pt.er" .. eventd.marker()
    for i = 1, 20 do eventd.emit(vm, second, { i = i }) end
    eventd.wait_rows(vm, "EVENTS " .. second .. " SINCE 1h ago", function(rs) return #rs == 20 end)
    barrier()
    vm:run("sleep 3")
    t:assert_eq(#eventd.rows(vm, "EVENTS " .. second .. " SINCE 1h ago"), 20,
        "with the limit at 0, a retention pass deletes nothing")
    eventd.unset(vm, "EventRetentionMaxBytes")
end)
