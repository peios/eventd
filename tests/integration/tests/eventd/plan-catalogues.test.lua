-- eventd TRM §6.1, "Planning" — identifier discovery from the compact
-- catalogues, and what a stale catalogue may and may not do.
--
-- A catalogue and the records table it summarises can only be made to
-- disagree from outside eventd, so this file has a VM of its own: it
-- stops eventd, edits a store on the host (a copy taken with its WAL,
-- edited with the host's sqlite, checkpointed and written back), and
-- starts eventd again on the edited store. eventd loads its catalogue
-- caches at open (log_store.rs:106-114) and never reconciles them with
-- the records, so the edit is what eventd sees.
--
-- The retention case lets eventd itself produce a stale name: records
-- old enough to be expired are sent with an explicit timestamp, the
-- retention interval is turned down to its one-minute minimum, and the
-- pass deletes the records while the catalogue keeps the name.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-catalogue" })

-- Stores are edited while eventd is stopped with eventd.edit_store, in
-- rollback-journal mode: copied out with the WAL, edited on the host, the
-- WAL folded into the main file, and the main file put back without one.

-- "Identifier discovery reads compact catalogues: the union of every
--  shard's event_types, the log store's log_origins, and the metric
--  store's series rows." / "Discovery therefore never runs a DISTINCT
--  scan over a hot records table and does not depend on an adaptive
--  event index being present."
test("a type or origin missing from its catalogue is not found, though its records are there", {
    spec = "eventd *plan.identifier-discovery-reads-the-event-types-log-origins-and-series-catalogues"
        .. " eventd *plan.discovery-never-scans-a-records-table-or-needs-an-adaptive-index",
}, function(t)
    local m = eventd.marker("cat")
    local gone_type, kept_type = "pt." .. m .. ".gone", "pt." .. m .. ".kept"
    local gone_origin, kept_origin = m .. "gone", m .. "kept"
    t:assert_eq(eventd.emit(vm, gone_type, { n = 1 }).ret, 0, "emit")
    t:assert_eq(eventd.emit(vm, kept_type, { n = 1 }).ret, 0, "emit")
    eventd.send_log(vm, { { origin = gone_origin, is_error = false, message = "one" },
                          { origin = gone_origin, is_error = false, message = "two" },
                          { origin = kept_origin, is_error = false, message = "kept" } })
    eventd.wait_rows(vm, "EVENTS pt." .. m .. ".*", function(rs) return #rs == 2 end)
    eventd.wait_rows(vm, "LOGS FROM " .. gone_origin .. ", " .. kept_origin, function(rs) return #rs == 3 end)

    eventd.stop(vm)
    local shard
    for _, path in ipairs(eventd.shards(vm)) do
        local n = eventd.sql(vm, path, "SELECT count(*) FROM events WHERE event_type = '" .. gone_type .. "'")
        if n[1][1] > 0 then shard = path end
    end
    t:assert(shard, "the gone type's event is in a shard")
    eventd.edit_store(vm, shard, "DELETE FROM event_types WHERE event_type = '" .. gone_type .. "';",
        { journal = "delete" })
    eventd.edit_store(vm, eventd.DB.logs, "DELETE FROM log_origins WHERE origin = '" .. gone_origin .. "';",
        { journal = "delete" })
    eventd.start(vm)

    -- The records are still there ...
    t:assert_eq(eventd.sql(vm, shard,
        "SELECT count(*) FROM events WHERE event_type = '" .. gone_type .. "'")[1][1], 1,
        "the event row survived the edit")
    t:assert_eq(eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM logs WHERE origin = '" .. gone_origin .. "'")[1][1], 2,
        "the two log rows survived the edit")
    -- ... but discovery, reading the catalogues, cannot find them.
    local ev = eventd.rows(vm, "EVENTS pt." .. m .. ".*")
    t:assert_eq(#ev, 1, "only the type still catalogued is found: " .. json.encode(ev))
    t:assert_eq(ev[1] and ev[1]["event.type"], kept_type, "the kept one")
    t:assert_eq(#eventd.rows(vm, "EVENTS " .. gone_type), 0, "naming the uncatalogued type finds nothing")
    t:assert_eq(#eventd.rows(vm, 'EVENTS WHERE event.type == "' .. gone_type .. '"'), 0,
        "nor does a predicate on it: no scan of the events table finds it")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. gone_origin), 0, "the uncatalogued origin finds nothing")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. kept_origin), 1, "the catalogued one is read")
end)

-- "Catalogues may conservatively contain a name whose final retained row
--  was deleted."
test("retention deletes an origin's last rows and its catalogue entry stays", {
    spec = "eventd *plan.a-catalogue-may-keep-a-name-whose-rows-were-all-deleted",
}, function(t)
    local origin = eventd.marker("old")
    local fresh = eventd.marker("new")
    local old = eventd.guest_ns(vm) - 3 * 86400 * 1000000000
    eventd.send_log(vm, {
        { origin = origin, is_error = false, message = "old one", timestamp = old },
        { origin = origin, is_error = false, message = "old two", timestamp = old + 1 },
        { origin = fresh, is_error = false, message = "fresh" },
    })
    eventd.wait_rows(vm, "LOGS FROM " .. origin, function(rs) return #rs == 2 end)

    eventd.set(vm, "LogRetentionDays", "dword:1"):assert_ok()
    eventd.set(vm, "RetentionCheckIntervalMinutes", "dword:1"):assert_ok()
    -- The interval in force is the one read when the wait began, so the
    -- retention thread is restarted with the new one.
    eventd.restart(vm)
    local ok = pcall(wait_until, function()
        return eventd.sql(vm, eventd.DB.logs,
            "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'")[1][1] == 0
    end, { timeout = 180, interval = 5, desc = "a retention pass to delete the old rows" })
    eventd.unset(vm, "LogRetentionDays")
    eventd.unset(vm, "RetentionCheckIntervalMinutes")
    t:assert(ok, "retention deleted every row of the expired origin")
    local cat = eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM log_origins WHERE origin = '" .. origin .. "'")
    t:assert_eq(cat[1][1], 1, "the origin is still in log_origins")
    t:assert_eq(eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM logs WHERE origin = '" .. fresh .. "'")[1][1], 1,
        "control: the unexpired origin's row was kept")
end)

-- "That safe superset can cause an extra access check but cannot expose
--  a row or omit a concrete identifier that committed successfully."
test("a catalogued name with no rows answers nothing, and a name committed afterwards is found", {
    spec = "eventd *plan.a-stale-catalogue-name-never-exposes-a-row-or-omits-a-committed-identifier",
}, function(t)
    local ghost = eventd.marker("ghost")
    local later = eventd.marker("later")
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.logs, "INSERT INTO log_origins(origin) VALUES ('" .. ghost .. "');",
        { journal = "delete" })
    eventd.start(vm)
    t:assert_eq(eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM log_origins WHERE origin = '" .. ghost .. "'")[1][1], 1,
        "the stale name is catalogued")
    local r = eventd.query(vm, "LOGS FROM " .. ghost)
    t:assert(r.ok, "a query for the stale name succeeds: " .. r.stderr)
    t:assert_eq(#r.rows, 0, "and exposes no row")
    local all = eventd.rows(vm, "LOGS SINCE 10m ago COUNT BY origin")
    for _, row in ipairs(all) do
        t:assert(row.origin ~= ghost, "the stale name never appears as a group")
    end
    -- A name committed now, with the stale entry still beside it, is found.
    eventd.send_log(vm, { origin = later, is_error = false, message = "committed" })
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. later, function(rs) return #rs == 1 end)
    t:assert_eq(#rows, 1, "the newly committed origin is discovered")
end)
