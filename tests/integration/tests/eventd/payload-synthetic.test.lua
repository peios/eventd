-- eventd TRM §3.2 "Synthetic Event Payloads"
-- (3--event-storage/2--synthetic-event-payloads.md): the MessagePack map
-- each of the five event types eventd writes about itself carries, field
-- by field, nested by catalogue path (PGSS §6.4).
--
-- One VM with two vCPUs: `store.resume.*` and `store.committed.*` hold one
-- entry per logical CPU in ascending order, and on one vCPU that order is
-- a list of one. Its eventd is restarted and stopped as the cases need —
-- each type is produced by the event that causes it:
--
--   * eventd.daemon.started: the boot's first start, then every restart;
--   * eventd.daemon.stopped: every stop;
--   * eventd.config.changed: a live `reg set` / `reg del`;
--   * eventd.events.lost: KMES events emitted while eventd is stopped,
--     enough to overrun the 4 MiB ring, so the next start finds sequences
--     gone;
--   * eventd.store.quarantined: a shard and the log store overwritten with
--     garbage while eventd is stopped, so the next start quarantines them.
--
-- Payloads are read as stored — the blob out of shard-0000.db (eventd
-- commits every daemon-wide record to shard 0) decoded here — because a
-- query does not show an absent key or a nested array's inner shape. The
-- cases run in file order; the quarantine case overwrites shard-0000 and
-- comes last but one, and the all-five-types case reads what the others
-- left.
--
-- Between a stop and a start the shard is copied to the host and edited
-- with the host's sqlite3 (`eventd.edit_store`), which is how a shutdown
-- payload is falsified and a gap record with mismatched CPU is planted.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(1, { cpus = 2 }) -- one VM, two vCPUs

-- Two vCPUs: the per-CPU arrays are the subject (see above).
local vm = eventd.boot({ name = "ev-payload", cpus = 2 })

local SHARD0 = eventd.STORE.events .. "/shard-0000.db"

-- The registry type numbers eventd.config.changed carries.
local REG_DWORD, REG_QWORD = 4, 11

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

--- Decode one MessagePack value (the whole of what eventd writes). Maps
--- come back as {map = {k = v}, keys = {k1, k2, ...}} so key order and
--- duplicates are visible; arrays as {array = {...}}; nil as eventd.NIL.
local function decode(b) return (eventd.decode(b, 1, { tagged = true, whole = true })) end

--- The newest stored record of one of the five types in any shard (a gap
--- goes to the shard its CPU writes to; the rest to shard 0): {payload =
--- decoded, cpu_id = column, id = rowid, timestamp = column, db = shard
--- path}.
local function newest(event_type)
    local best
    for _, db in ipairs(eventd.shards(vm)) do
        local rows = eventd.sql(vm, db, "SELECT id, hex(payload), cpu_id, timestamp FROM events WHERE event_type = '" ..
            event_type .. "' ORDER BY timestamp DESC, id DESC LIMIT 1")
        if rows[1] and (not best or rows[1][4] > best.timestamp) then
            best = { id = rows[1][1], payload = decode(eventd.unhex(rows[1][2])), cpu_id = rows[1][3],
                     timestamp = rows[1][4], db = db }
        end
    end
    assert(best, "a stored " .. event_type .. " record")
    return best
end

local function count_of(event_type)
    local n = 0
    for _, db in ipairs(eventd.shards(vm)) do
        n = n + eventd.sql(vm, db, "SELECT count(*) FROM events WHERE event_type = '" .. event_type .. "'")[1][1]
    end
    return n
end

local function sorted_keys(m)
    local ks = {}
    for _, k in ipairs(m.keys) do ks[#ks + 1] = k end
    table.sort(ks)
    return table.concat(ks, ",")
end

--- The tagged value at a dotted path of a decoded payload, or nil when any
--- segment is absent. `at(p, "store.resume.cpus")`.
local function at(tagged, path)
    local v = tagged
    for seg in path:gmatch("[^.]+") do
        if type(v) ~= "table" or not v.map then return nil end
        v = v.map[seg]
    end
    return v
end

--- The plain list an array field holds.
local function list(tagged) return tagged and tagged.array end

--- Every leaf of a decoded payload by dotted path: scalars as themselves,
--- arrays as plain lists. A nil anywhere is kept as eventd.NIL.
local function leaves(tagged, prefix, out)
    out = out or {}
    for k, v in pairs(tagged.map) do
        local path = prefix and (prefix .. "." .. k) or k
        if type(v) == "table" and v.map then
            leaves(v, path, out)
        elseif type(v) == "table" and v.array then
            out[path] = v.array
        else
            out[path] = v
        end
    end
    return out
end

--- Whether a decoded value holds a nil anywhere.
local function has_nil(v)
    if v == eventd.NIL then return true end
    if type(v) ~= "table" then return false end
    for _, x in pairs(v.map or v.array or {}) do
        if has_nil(x) then return true end
    end
    return false
end

local function boot_canonical()
    return eventd.boot_id(vm)
end

--- Highest sequence contiguously covered from 1 by this boot's receipts
--- for `cpu`, across every shard file present.
local function highest_contiguous(cpu)
    local ranges = {}
    for _, s in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, s, "SELECT first_sequence, last_sequence FROM receipt_ranges " ..
            "WHERE hex(boot_id) = '" .. eventd.boot_pcds_hex(boot_canonical()) .. "' AND cpu_id = " .. cpu)) do
            ranges[#ranges + 1] = r
        end
    end
    table.sort(ranges, function(a, b) return a[1] < b[1] end)
    local high = 0
    for _, r in ipairs(ranges) do
        if r[1] > high + 1 then break end
        if r[2] > high then high = r[2] end
    end
    return high
end

--- Wait for the config.changed record about value `name` committed after
--- `after_id`.
local function config_change_after(name, after_id)
    local rec
    wait_until(function()
        local rows = eventd.sql(vm, SHARD0, "SELECT id, hex(payload) FROM events WHERE event_type = '" ..
            eventd.T.config_change .. "' AND id > " .. after_id .. " ORDER BY id")
        for _, r in ipairs(rows) do
            local p = decode(eventd.unhex(r[2]))
            if at(p, "config.name") == name then rec = { id = r[1], payload = p } return true end
        end
        return false
    end, { timeout = 30, interval = 0.25, desc = "a config.changed record for " .. name })
    return rec
end

local function max_id()
    return eventd.sql(vm, SHARD0, "SELECT COALESCE(max(id), 0) FROM events")[1][1]
end

-- ---------------------------------------------------------------------------
-- eventd.daemon.started
-- ---------------------------------------------------------------------------

test("eventd.daemon.started carries store.restarted, store.shard-count and store.resume.{cpus,sequences}", {
    spec = "eventd *payload.the-eventd-daemon-started-payload-schema",
}, function(t)
    local s = newest(eventd.T.startup)
    t:assert(s.payload.map, "the payload is a map")
    t:assert_eq(sorted_keys(s.payload), "store", "one top-level key, store")
    local store = at(s.payload, "store")
    t:assert_eq(sorted_keys(store), "restarted,resume,shard-count", "store holds exactly the three")
    t:assert_eq(sorted_keys(at(s.payload, "store.resume")), "cpus,sequences", "store.resume holds the two arrays")
    t:assert_eq(type(at(s.payload, "store.restarted")), "boolean", "store.restarted is a bool")
    local n = at(s.payload, "store.shard-count")
    t:assert(math.type(n) == "integer" and n >= 0, "store.shard-count is an unsigned integer")
    t:assert(list(at(s.payload, "store.resume.cpus")), "store.resume.cpus is an array")
    t:assert(list(at(s.payload, "store.resume.sequences")), "store.resume.sequences is an array")
end)

test("eventd.daemon.started carries no boot ID; its boot is the record's event.boot.guid", {
    spec = "eventd *payload.daemon-started-carries-no-boot-id-the-boot-is-event-boot-guid",
}, function(t)
    local s = newest(eventd.T.startup)
    for path, v in pairs(leaves(s.payload)) do
        t:assert(not path:find("boot"), "no boot field in the payload: " .. path)
        t:assert(v ~= "{" .. boot_canonical():lower() .. "}", "no field holds the boot ID: " .. path)
    end
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago")
    t:assert(#rows >= 1, "a started record comes back through a query")
    local found = false
    for _, r in ipairs(rows) do
        if r["event.boot.guid"] == "{" .. boot_canonical():lower() .. "}" then found = true end
    end
    t:assert(found, "event.boot.guid is the current boot in PCDS canonical (braced, lowercase) form")
end)

test("startup store.resume: one entry per CPU in ascending order, the highest contiguously accounted sequence", {
    spec = "eventd *payload.startup-resume-cpus-and-sequences-give-each-cpus-highest-contiguous-sequence-in-cpu-order",
}, function(t)
    -- Bracket the value: at least what the receipts held while eventd was
    -- down, at most what they hold once it has started.
    eventd.stop(vm)
    local before = { [0] = highest_contiguous(0), [1] = highest_contiguous(1) }
    eventd.start(vm)
    local after = { [0] = highest_contiguous(0), [1] = highest_contiguous(1) }
    local p = newest(eventd.T.startup).payload
    local cpus, seqs = list(at(p, "store.resume.cpus")), list(at(p, "store.resume.sequences"))
    t:assert_eq(#cpus, 2, "one CPU per logical CPU")
    t:assert_eq(#seqs, #cpus, "the sequences are parallel to the CPUs")
    for i, c in ipairs(cpus) do
        t:assert_eq(c, i - 1, "CPUs in ascending order")
        t:assert(seqs[i] >= before[c] and seqs[i] <= after[c],
            "cpu " .. c .. ": " .. before[c] .. " <= " .. seqs[i] .. " <= " .. after[c])
    end
end)

test("startup store.restarted is false on the boot's first start and true once the boot has committed rows", {
    spec = "eventd *payload.startup-restart-is-true-when-the-boot-already-had-committed-rows-or-receipts",
}, function(t)
    local rows = eventd.sql(vm, SHARD0, "SELECT hex(payload) FROM events WHERE event_type = '" ..
        eventd.T.startup .. "' AND hex(boot_id) = '" .. eventd.boot_pcds_hex(boot_canonical()) .. "' ORDER BY id")
    t:assert(#rows >= 2, "the boot's first start and the restart above: " .. #rows)
    t:assert_eq(at(decode(eventd.unhex(rows[1][1])), "store.restarted"), false, "the first start of the boot: false")
    for i = 2, #rows do
        t:assert_eq(at(decode(eventd.unhex(rows[i][1])), "store.restarted"), true, "every later start: true")
    end
end)

test("startup store.shard-count is the active shard count after StorageShards is resolved", {
    spec = "eventd *payload.startup-shard-count-is-the-resolved-active-shard-count",
}, function(t)
    local function count() return at(newest(eventd.T.startup).payload, "store.shard-count") end
    t:assert_eq(count(), 2, "StorageShards 0 resolves to the two attached buffers")
    eventd.set(vm, "StorageShards", "dword:3"):assert_ok()
    eventd.restart(vm)
    local three = count()
    eventd.unset(vm, "StorageShards")
    eventd.restart(vm)
    t:assert_eq(three, 3, "StorageShards 3: three")
    t:assert_eq(count(), 2, "and back to two")
end)

-- ---------------------------------------------------------------------------
-- eventd.config.changed
-- ---------------------------------------------------------------------------

test("eventd.config.changed carries config.key.path, config.name and each side's type and value", {
    spec = "eventd *payload.the-eventd-config-changed-payload-schema"
        .. " eventd *payload.config-change-key-path-is-always-the-eventd-configuration-key"
        .. " eventd *payload.config-change-name-is-the-value-name-under-the-key-path"
        .. " eventd *payload.config-change-type-previous-is-the-reg-type-number-or-absent"
        .. " eventd *payload.config-change-value-previous-is-absent-when-no-value-was-stored"
        .. " eventd *payload.config-change-type-is-the-reg-type-number-or-absent"
        .. " eventd *payload.config-change-value-is-absent-when-the-change-removed-the-value",
}, function(t)
    local id0 = max_id()
    eventd.set(vm, "LogRetentionDays", "dword:13"):assert_ok()
    local add = config_change_after("LogRetentionDays", id0)
    eventd.set(vm, "LogRetentionDays", "dword:12"):assert_ok()
    local change = config_change_after("LogRetentionDays", add.id)
    eventd.unset(vm, "LogRetentionDays"):assert_ok()
    local del = config_change_after("LogRetentionDays", change.id)
    for _, r in ipairs({ add, change, del }) do
        t:assert_eq(sorted_keys(r.payload), "config", "one top-level key, config")
        t:assert_eq(sorted_keys(at(r.payload, "config.key")), "path", "config.key holds only path")
        t:assert_eq(at(r.payload, "config.key.path"), "Machine\\System\\eventd", "config.key.path is the eventd key")
        t:assert_eq(at(r.payload, "config.name"), "LogRetentionDays", "config.name is the value name under it")
    end
    local a = at(add.payload, "config")
    t:assert_eq(sorted_keys(a), "key,name,type,value", "added: no previous side, since nothing was stored")
    t:assert_eq(a.map.type, REG_DWORD, "and it is now a REG_DWORD (4)")
    t:assert_eq(a.map.value, 13, "of 13")
    local c = at(change.payload, "config")
    t:assert_eq(sorted_keys(c), "key,name,type,type-previous,value,value-previous", "changed: both sides")
    t:assert_eq(c.map["type-previous"], REG_DWORD, "it was a REG_DWORD")
    t:assert_eq(c.map["value-previous"], 13, "of 13")
    t:assert_eq(c.map.type, REG_DWORD, "and is a REG_DWORD")
    t:assert_eq(c.map.value, 12, "of 12")
    local d = at(del.payload, "config")
    t:assert_eq(sorted_keys(d), "key,name,type-previous,value-previous", "deleted: no current side")
    t:assert_eq(d.map["type-previous"], REG_DWORD, "it was a REG_DWORD")
    t:assert_eq(d.map["value-previous"], 12, "of 12")
end)

test("config values are integers, and a query compares them as numbers", {
    spec = "eventd *payload.config-values-are-integers",
}, function(t)
    -- Only DWORD and QWORD keys are reloadable at runtime (TRM §A), so
    -- those are the two types a change can show.
    local id0 = max_id()
    eventd.set(vm, "LogRetentionDays", "dword:7"):assert_ok()
    local d = config_change_after("LogRetentionDays", id0)
    eventd.set(vm, "EventRetentionMaxBytes", "qword:4294967306"):assert_ok()
    local q = config_change_after("EventRetentionMaxBytes", d.id)
    eventd.unset(vm, "LogRetentionDays")
    eventd.unset(vm, "EventRetentionMaxBytes")
    t:assert_eq(math.type(at(d.payload, "config.value")), "integer", "a DWORD is an integer")
    t:assert_eq(at(d.payload, "config.value"), 7, "of 7")
    t:assert_eq(at(q.payload, "config.type"), REG_QWORD, "a QWORD key is type 11")
    t:assert_eq(at(q.payload, "config.value"), 4294967306, "and an integer past 32 bits")
    local hit = eventd.rows(vm, "EVENTS " .. eventd.T.config_change ..
        ' WHERE config.name == "LogRetentionDays" WHERE config.value == 7 SINCE 10m ago')
    t:assert(#hit >= 1, "a numeric filter matches the value")
end)

test("a stored value of the wrong type is reported as the expected type, holding the integer in force", {
    spec = "eventd *payload.a-wrong-typed-value-reports-the-expected-type-and-the-integer-in-force",
}, function(t)
    -- A wrong-typed value set at runtime is ignored and the old one kept
    -- (TRM §8.3), so the stored record can only disagree with what is in
    -- force from a start: eventd starts on a REG_SZ EventRetentionMaxBytes
    -- and uses the compiled default, 0.
    eventd.set(vm, "EventRetentionMaxBytes", "sz:lots"):assert_ok()
    eventd.restart(vm)
    local id0 = max_id()
    eventd.set(vm, "EventRetentionMaxBytes", "qword:4096"):assert_ok()
    local r = config_change_after("EventRetentionMaxBytes", id0)
    eventd.unset(vm, "EventRetentionMaxBytes")
    local c = at(r.payload, "config")
    t:assert_eq(c.map["type-previous"], REG_QWORD, "the REG_SZ that was stored is reported as the QWORD expected")
    t:assert_eq(c.map["value-previous"], 0, "holding the default eventd was using, not the text")
    t:assert_eq(c.map.type, REG_QWORD, "the new side is the QWORD")
    t:assert_eq(c.map.value, 4096, "of 4096")
end)

-- ---------------------------------------------------------------------------
-- eventd.daemon.stopped
-- ---------------------------------------------------------------------------

test("eventd.daemon.stopped carries store.committed: each CPU's highest contiguous receipted sequence, in CPU order", {
    spec = "eventd *payload.the-eventd-daemon-stopped-payload-schema"
        .. " eventd *payload.shutdown-committed-cpus-and-sequences-give-each-cpus-highest-contiguous-receipted-sequence",
}, function(t)
    eventd.stop(vm)
    local s = newest(eventd.T.shutdown)
    local high = { [0] = highest_contiguous(0), [1] = highest_contiguous(1) }
    eventd.start(vm)
    t:assert_eq(sorted_keys(s.payload), "store", "one top-level key, store")
    t:assert_eq(sorted_keys(at(s.payload, "store")), "committed", "store holds committed")
    t:assert_eq(sorted_keys(at(s.payload, "store.committed")), "cpus,sequences", "the two arrays")
    local cpus = list(at(s.payload, "store.committed.cpus"))
    local seqs = list(at(s.payload, "store.committed.sequences"))
    t:assert_eq(#cpus, 2, "one entry per CPU")
    t:assert_eq(#seqs, #cpus, "parallel arrays")
    for i, c in ipairs(cpus) do
        t:assert_eq(c, i - 1, "in CPU order")
        t:assert_eq(seqs[i], high[c], "cpu " .. c .. ": the receipts' highest contiguous sequence at shutdown")
    end
end)

-- Making the receipts unreadable at the moment of a graceful stop, while
-- leaving a shard writable for the record itself, is not something this
-- harness can arrange; the payload eventd builds for it is unit-tested.
test("with unreadable coverage the shutdown payload leaves both arrays out (not observable)", {
    spec = "eventd *payload.shutdown-leaves-both-arrays-out-when-coverage-cannot-be-read",
    skip = true,
    covered_by = "cargo:eventd eventd synthetic::tests::shutdown_without_readable_coverage_is_an_empty_map",
}, function() end)

test("startup derives recovery coverage from receipts, never from the shutdown payload", {
    spec = "eventd *payload.startup-never-derives-recovery-coverage-from-the-shutdown-payload",
}, function(t)
    eventd.stop(vm)
    local s = newest(eventd.T.shutdown)
    local lie = eventd.msgpack({ store = { committed = {
        cpus = eventd.array({ 0, 1 }), sequences = eventd.array({ 900000000, 900000000 }) } } })
    eventd.edit_store(vm, s.db, "UPDATE events SET payload = X'" .. eventd.hex(lie, true) .. "' WHERE id = " .. s.id .. ";")
    eventd.start(vm)
    local after = { [0] = highest_contiguous(0), [1] = highest_contiguous(1) }
    local p = newest(eventd.T.startup).payload
    local cpus, seqs = list(at(p, "store.resume.cpus")), list(at(p, "store.resume.sequences"))
    for i, c in ipairs(cpus) do
        t:assert(seqs[i] <= after[c],
            "cpu " .. c .. " resumes from its receipts (" .. seqs[i] .. "), not the falsified 900000000")
    end
end)

-- ---------------------------------------------------------------------------
-- eventd.events.lost
-- ---------------------------------------------------------------------------

--- Stop eventd, overrun its ring with ~6 MB of events (a 4 MiB ring that
--- nothing is draining), start it, and return the gap record that start
--- wrote. Once per file; later calls return the same record.
local GAP
local function make_gap()
    if GAP then return GAP end
    local before = count_of(eventd.T.gap)
    eventd.stop(vm)
    local big = eventd.msgpack({ blob = eventd.bin(string.rep("g", 60000)) })
    local entries = {}
    for i = 1, 100 do entries[i] = { type = "pt.flood", payload = big } end
    local fr = kmes.emit_batch(vm, entries)
    assert(fr.emitted == 100, "the flood was emitted")
    eventd.start(vm)
    assert(count_of(eventd.T.gap) > before, "the restart recorded a gap")
    GAP = newest(eventd.T.gap)
    return GAP
end

--- Plant (once) a gap record whose column says CPU 1 and payload says 7.
local planted_gap = false
local function plant_gap()
    if planted_gap then return end
    eventd.stop(vm)
    local planted = eventd.msgpack({ buffer = { cpu = 7 },
        loss = { sequence = 987654321, ["sequence-last"] = 987654321, count = 1 } })
    eventd.edit_store(vm, SHARD0, "INSERT INTO events (boot_id, timestamp, cpu_id, event_type, payload) " ..
        "SELECT boot_id, timestamp, 1, '" .. eventd.T.gap .. "', X'" .. eventd.hex(planted, true) .. "' FROM events " ..
        "WHERE event_type = '" .. eventd.T.shutdown .. "' ORDER BY id DESC LIMIT 1;")
    eventd.start(vm)
    planted_gap = true
end

test("eventd.events.lost carries buffer.cpu and loss.{sequence,sequence-last,count,preceding-time}", {
    spec = "eventd *payload.the-eventd-events-lost-payload-schema"
        .. " eventd *payload.gap-buffer-cpu-is-the-ring-the-gap-was-found-on"
        .. " eventd *payload.gap-loss-sequence-is-the-first-missing-sequence"
        .. " eventd *payload.gap-loss-sequence-last-is-the-last-missing-sequence"
        .. " eventd *payload.gap-loss-count-is-how-many-sequences-are-missing"
        .. " eventd *payload.gap-loss-preceding-time-is-the-last-event-before-the-gap-or-absent"
        .. " eventd *payload.gap-event-time-is-the-revealing-events-timestamp"
        .. " eventd *payload.a-gap-carries-its-cpu-as-buffer-cpu-and-in-the-column",
}, function(t)
    make_gap()
    t:assert_eq(sorted_keys(GAP.payload), "buffer,loss", "two top-level keys")
    t:assert_eq(sorted_keys(at(GAP.payload, "buffer")), "cpu", "buffer holds cpu")
    local lk = sorted_keys(at(GAP.payload, "loss"))
    t:assert(lk == "count,preceding-time,sequence,sequence-last" or lk == "count,sequence,sequence-last",
        "loss holds the range and count, and preceding-time only when known: " .. lk)
    t:assert(not has_nil(GAP.payload), "no field is nil")
    local cpu = at(GAP.payload, "buffer.cpu")
    local first, last = at(GAP.payload, "loss.sequence"), at(GAP.payload, "loss.sequence-last")
    local preceding = at(GAP.payload, "loss.preceding-time")
    t:assert_eq(cpu, GAP.cpu_id, "buffer.cpu is the column's CPU")
    t:assert(cpu == 0 or cpu == 1, "and a CPU of this machine")
    t:assert_eq(at(GAP.payload, "loss.count"), last - first + 1, "count is the size of the inclusive range")
    local boot = eventd.boot_pcds_hex(boot_canonical())
    local inside = 0
    for _, s in ipairs(eventd.shards(vm)) do
        inside = inside + eventd.sql(vm, s, "SELECT count(*) FROM events WHERE hex(boot_id) = '" .. boot ..
            "' AND cpu_id = " .. cpu .. " AND sequence BETWEEN " .. first .. " AND " .. last)[1][1]
    end
    t:assert_eq(inside, 0, "no event in sequence..sequence-last was stored: they are the missing ones")
    local edge_before, edge_after
    for _, s in ipairs(eventd.shards(vm)) do
        local r = eventd.sql(vm, s, "SELECT sequence, timestamp FROM events WHERE hex(boot_id) = '" .. boot ..
            "' AND cpu_id = " .. cpu .. " AND sequence IN (" .. (first - 1) .. ", " .. (last + 1) .. ")")
        for _, row in ipairs(r) do
            if row[1] == first - 1 then edge_before = row[2] else edge_after = row[2] end
        end
    end
    t:assert(first == 1 or highest_contiguous(cpu) >= first - 1,
        "the sequence before loss.sequence was accounted for")
    t:assert(edge_after, "the event after loss.sequence-last is stored: it revealed the gap")
    t:assert_eq(GAP.timestamp, edge_after, "the record's own timestamp is the revealing event's")
    for path in pairs(leaves(GAP.payload)) do
        t:assert(not path:find("reveal"), "the reveal time is not a payload field: " .. path)
    end
    if preceding ~= nil then
        t:assert_eq(math.type(preceding), "integer", "loss.preceding-time is a timestamp")
        t:assert_eq(preceding, edge_before, "loss.preceding-time is the event before the gap")
    else
        -- "Known" is known to the drain in this run. This gap is found by
        -- restart reconciliation after the ring lapped, so the event before
        -- it is accounted only by a committed receipt, which eventd does
        -- not read back for a timestamp (§2.5): leaving it out is right
        -- even though that event is stored.
        t:assert(first == 1 or highest_contiguous(cpu) >= first - 1,
            "loss.preceding-time is left out only when the event before the gap is unseen in this run")
    end
end)

test("an event.cpu predicate matches a gap record's column, and a buffer.cpu predicate its payload field", {
    spec = "eventd *payload.an-event-cpu-predicate-matches-the-column-and-buffer-cpu-the-payload-field",
}, function(t)
    plant_gap()
    local q = "EVENTS " .. eventd.T.gap .. " WHERE loss.sequence == 987654321"
    t:assert_eq(#eventd.rows(vm, q .. " WHERE event.cpu == 1 SINCE 1h ago"), 1, "event.cpu == 1 (the column) matches")
    t:assert_eq(#eventd.rows(vm, q .. " WHERE event.cpu == 7 SINCE 1h ago"), 0, "event.cpu == 7 (the payload) does not")
    t:assert_eq(#eventd.rows(vm, q .. " WHERE buffer.cpu == 7 SINCE 1h ago"), 1, "buffer.cpu == 7 (the payload) matches")
    t:assert_eq(#eventd.rows(vm, q .. " WHERE buffer.cpu == 1 SINCE 1h ago"), 0, "buffer.cpu == 1 (the column) does not")
end)

test("every payload field is stored nested by path and queried by its dotted name", {
    spec = "eventd *payload.fields-are-nested-by-path-and-queried-by-their-dotted-names",
}, function(t)
    -- Each leaf of each stored payload reads back through a query under
    -- its dotted path, an array whole. The planted gap (column 1, payload
    -- 7) tells the event.cpu column and buffer.cpu apart.
    make_gap()
    plant_gap()
    local id0 = max_id()
    eventd.set(vm, "LogRetentionDays", "dword:11"):assert_ok()
    config_change_after("LogRetentionDays", id0)
    eventd.unset(vm, "LogRetentionDays")
    local function same(a, b)
        if type(a) == "table" and type(b) == "table" then
            if #a ~= #b then return false end
            for i = 1, #a do if a[i] ~= b[i] then return false end end
            return true
        end
        return a == b
    end
    --- Find `stored`'s record among a query's results by its leaves, then
    --- require every leaf to read back as itself, and no dotted key in the
    --- stored map.
    local function check(event_type, stored)
        for _, k in ipairs(stored.payload.keys) do
            t:assert(not k:find("%."), event_type .. ": no stored key contains a dot: " .. k)
        end
        local want = leaves(stored.payload)
        local rows = eventd.rows(vm, "EVENTS " .. event_type .. " SINCE 1h ago")
        local rec
        for _, r in ipairs(rows) do
            local all = true
            for path, v in pairs(want) do
                if not same(r[path], v) then all = false end
            end
            if all then rec = r break end
        end
        t:assert(rec, event_type .. ": its record comes back through a query with every field under its dotted path")
        if rec then
            for path, v in pairs(want) do
                t:assert(same(rec[path], v), event_type .. ": " .. path .. " is queryable as itself")
            end
        end
    end
    local planted = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " WHERE loss.sequence == 987654321 SINCE 1h ago")
    t:assert_eq(#planted, 1, "the planted gap record is found")
    t:assert_eq(planted[1]["event.cpu"], 1, "a query presents the gap's cpu_id column as event.cpu")
    t:assert_eq(planted[1]["buffer.cpu"], 7, "and its payload field as buffer.cpu")
    check(eventd.T.config_change, newest(eventd.T.config_change))
    check(eventd.T.shutdown, newest(eventd.T.shutdown))
    check(eventd.T.startup, newest(eventd.T.startup))
    check(eventd.T.gap, GAP)
end)

-- ---------------------------------------------------------------------------
-- eventd.store.quarantined, then all five
-- ---------------------------------------------------------------------------

test("eventd.store.quarantined carries store.kind, store.shard (event store only) and outcome.detail", {
    spec = "eventd *payload.the-eventd-store-quarantined-payload-schema"
        .. " eventd *payload.quarantine-store-kind-is-event-log-or-metric"
        .. " eventd *payload.quarantine-store-shard-is-present-only-for-the-event-store"
        .. " eventd *payload.quarantine-outcome-detail-is-a-human-readable-description",
}, function(t)
    eventd.stop(vm)
    local garbage = string.rep("this is not a database. ", 400)
    vm:write_file(SHARD0, garbage)
    vm:write_file(eventd.DB.logs, garbage)
    eventd.start(vm)
    local rows = eventd.sql(vm, SHARD0, "SELECT hex(payload) FROM events WHERE event_type = '" ..
        eventd.T.storage_error .. "' ORDER BY id")
    local by_store = {}
    for _, r in ipairs(rows) do
        local p = decode(eventd.unhex(r[1]))
        t:assert_eq(sorted_keys(p), "outcome,store", "two top-level keys")
        t:assert_eq(sorted_keys(at(p, "outcome")), "detail", "outcome holds only detail")
        t:assert(not has_nil(p), "no field is nil")
        by_store[at(p, "store.kind")] = p
    end
    local STORES = { event = true, log = true, metric = true }
    for s in pairs(by_store) do t:assert(STORES[s], "store.kind is one of the three names: " .. s) end
    t:assert(by_store.event, "the event shard's corruption was recorded: " .. json.encode(rows))
    t:assert(by_store.log, "and the log store's")
    if by_store.event then
        t:assert_eq(sorted_keys(at(by_store.event, "store")), "kind,shard", "an event-store record names its shard")
        t:assert_eq(at(by_store.event, "store.shard"), 0, "shard 0")
    end
    if by_store.log then
        t:assert_eq(sorted_keys(at(by_store.log, "store")), "kind", "a log-store record has no shard key at all")
    end
    for s, p in pairs(by_store) do
        local d = at(p, "outcome.detail")
        t:assert(type(d) == "string" and #d > 0 and d:find("%a"), s .. ": outcome.detail is text: " .. tostring(d))
    end
end)

test("every synthetic event carries a MessagePack map, and no field in any is nil", {
    spec = "eventd *payload.every-synthetic-event-carries-a-messagepack-map"
        .. " eventd *payload.no-field-is-ever-nil-an-absent-value-is-left-out",
}, function(t)
    -- shard-0000 was replaced above; restart to have a start, a stop and a
    -- config change in it alongside the quarantine records, and flood for
    -- a gap. The deletion leaves a config change with no current side.
    eventd.set(vm, "LogRetentionDays", "dword:9"):assert_ok()
    eventd.unset(vm, "LogRetentionDays")
    eventd.stop(vm)
    local big = eventd.msgpack({ blob = eventd.bin(string.rep("g", 60000)) })
    local entries = {}
    for i = 1, 100 do entries[i] = { type = "pt.flood", payload = big } end
    kmes.emit_batch(vm, entries)
    eventd.start(vm)
    for _, ty in ipairs({ eventd.T.startup, eventd.T.shutdown, eventd.T.gap, eventd.T.config_change,
                          eventd.T.storage_error }) do
        local rows = {}
        for _, db in ipairs(eventd.shards(vm)) do
            for _, r in ipairs(eventd.sql(vm, db, "SELECT hex(payload) FROM events WHERE event_type = '" .. ty .. "'")) do
                rows[#rows + 1] = r
            end
        end
        t:assert(#rows >= 1, ty .. " is present")
        for _, r in ipairs(rows) do
            local p = decode(eventd.unhex(r[1]))
            t:assert(p.map, ty .. ": the payload is one MessagePack map")
            t:assert(not has_nil(p), ty .. ": no field is nil; an absent value is left out")
        end
    end
end)
