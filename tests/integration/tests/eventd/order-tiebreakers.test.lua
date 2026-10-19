-- eventd TRM §6.2 — ordering and tiebreakers: the internal keys that make
-- every result order total, why stored order never stands in for time
-- order, the query language's own value ordering, and the canonical
-- representative of a group.
--
-- One file-scope VM with two vCPUs, because the event tiebreaker is about
-- shards: on two CPUs eventd keeps two active shards (StorageShards
-- defaults to one per KMES buffer), and an event emitted by a worker
-- pinned to one CPU lands in that CPU's shard. Which shard is whose is
-- read back from the shard files rather than assumed.
--
-- Logs and metric samples carry an explicit timestamp (PSPU §3.7, §3.11),
-- so for those two modes a test chooses timestamps freely and can make
-- insertion order (row id) and time order disagree on purpose.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local sys = require("helpers.sys")
peinit.claim(1, { cpus = 2 })

-- Two vCPUs: the event tiebreaker crosses shards, and one shard per CPU
-- is what gives this VM two.
local vm = eventd.boot({ name = "ev-order", cpus = 2 })

--- Emit from a worker pinned to `cpu`. A worker serves its syscalls on
--- one thread, so the affinity set by one call holds for the next.
local function pinned_emit(cpu, etype, payload)
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local r = w:syscall(sys.NR.sched_setaffinity, {
            args = { 0, 8, 0 }, bufs = { string.pack("<I8", 1 << cpu) }, ptrs = { 2 },
        })
        assert(r.ret == 0, "pin to cpu " .. cpu .. ": errno " .. tostring(r.errno))
        local e = eventd.emit(w, etype, payload)
        assert(e.ret == 0, "pinned emit: errno " .. tostring(e.errno))
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end

--- The shard number (from its file name) holding the event `etype` with
--- payload `tag`.
local function shard_of(etype, tag)
    for _, path in ipairs(eventd.shards(vm)) do
        local n = eventd.sql(vm, path, "SELECT count(*) FROM events WHERE event_type = '" .. etype
            .. "' AND instr(payload, '" .. tag .. "') > 0")
        if n[1][1] > 0 then return tonumber(path:match("shard%-(%d+)%.db$")) end
    end
end

local function logs(records)
    local r = eventd.send_log(vm, records)
    assert(r.ret and r.ret > 0, "log sendto: errno " .. tostring(r.errno))
end

local function metrics(records)
    local r = eventd.send_metric(vm, records)
    assert(r.ret and r.ret > 0, "metric sendto: errno " .. tostring(r.errno))
end

local function field(rows, name)
    local out = {}
    for i, r in ipairs(rows) do out[i] = r[name] end
    return out
end

-- ---------------------------------------------------------------------------
-- The tiebreakers
-- ---------------------------------------------------------------------------

-- "Where the query's explicit SORT keys — or the mode's default ordering
--  — do not uniquely order two records, eventd appends internal keys
--  until the order is total."
test("records equal in every visible key still come back in one order, and page without gaps or repeats", {
    spec = "eventd *order.internal-tiebreaker-keys-are-appended-until-the-order-is-total",
}, function(t)
    local o = eventd.marker("tie")
    local ts = eventd.guest_ns(vm) - 60 * 1000000000
    -- Six lines identical in timestamp, error flag and text: only their
    -- insertion order tells them apart.
    for i = 1, 6 do
        logs({ origin = o, is_error = false, message = "same", timestamp = ts, job_id = eventd.bin(string.rep(string.char(i), 16)) })
        os.execute("sleep 0.2")
    end
    eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 6 end)
    local order = field(eventd.rows(vm, "LOGS FROM " .. o), "job_id")
    for _ = 1, 3 do
        t:assert_eq(json.encode(field(eventd.rows(vm, "LOGS FROM " .. o), "job_id")), json.encode(order),
            "the default order is the same every time")
    end
    -- In the default order the last one in is first out (id descending).
    t:assert_eq(order[1], "{06060606-0606-0606-0606-060606060606}", "the newest insertion leads: " .. json.encode(order))
    local sorted = field(eventd.rows(vm, "LOGS FROM " .. o .. " SORT message"), "job_id")
    local paged = {}
    for i = 0, 5 do
        local page = eventd.rows(vm, "LOGS FROM " .. o .. " SORT message SKIP " .. i .. " TAKE 1")
        t:assert_eq(#page, 1, "page " .. i .. " has one record")
        paged[#paged + 1] = page[1].job_id
    end
    t:assert_eq(json.encode(paged), json.encode(sorted), "one-record pages reassemble the sorted result exactly")
    local seen = {}
    for _, id in ipairs(paged) do
        t:assert(not seen[id], "no record twice across pages")
        seen[id] = true
    end
end)

-- With an explicit SORT, records the SORT keys leave tied are still broken
-- by timestamp before the shard/row-id keys.
test("records tied on the SORT keys come newest first across shards, then by shard, then row id", {
    spec = "eventd *order.event-tiebreakers-are-timestamp-desc-then-shard-index-asc-then-id-desc",
}, function(t)
    local tag = eventd.marker("evtie")
    local first = tag .. "first"
    local second = tag .. "second"
    -- Find which CPU feeds the lower-numbered shard.
    pinned_emit(0, "pt.order.probe", { tag = tag .. "c0" })
    pinned_emit(1, "pt.order.probe", { tag = tag .. "c1" })
    eventd.wait_rows(vm, "EVENTS pt.order.probe SINCE 10m ago", function(rs) return #rs >= 2 end)
    local s0, s1 = shard_of("pt.order.probe", tag .. "c0"), shard_of("pt.order.probe", tag .. "c1")
    t:assert(s0 and s1 and s0 ~= s1, "the two CPUs feed different shards: " .. tostring(s0) .. ", " .. tostring(s1))
    local low_cpu, high_cpu = 0, 1
    if s1 < s0 then low_cpu, high_cpu = 1, 0 end
    -- The older event goes to the lower shard, the newer to the higher.
    pinned_emit(low_cpu, "pt.order.evtie", { tag = first, k = 1 })
    os.execute("sleep 0.5")
    pinned_emit(high_cpu, "pt.order.evtie", { tag = second, k = 1 })
    local rows = eventd.wait_rows(vm, 'EVENTS pt.order.evtie WHERE tag STARTS_WITH "' .. tag .. '"',
        function(rs) return #rs == 2 end)
    t:assert(rows[1]["event.time"] > rows[2]["event.time"], "control: in the default order the newer leads")
    t:assert_eq(rows[1].tag, second, "control: and it is the second one")
    local tied = eventd.rows(vm, 'EVENTS pt.order.evtie WHERE tag STARTS_WITH "' .. tag .. '" SORT k')
    t:assert_eq(json.encode(field(tied, "tag")), json.encode({ second, first }),
        "tied on k, the newer event comes first though it is in the higher shard")
end)

-- As above, for logs: after the SORT keys, timestamp before row id.
test("log records tied on the SORT keys come newest first, then by row id descending", {
    spec = "eventd *order.log-tiebreakers-are-timestamp-desc-then-id-desc",
}, function(t)
    local o = eventd.marker("logtie")
    local ts = eventd.guest_ns(vm) - 60 * 1000000000
    -- The newer line is inserted first, so row id and time disagree.
    logs({ origin = o, is_error = false, message = "newer", timestamp = ts + 2000000000 })
    eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 1 end)
    logs({ origin = o, is_error = false, message = "older", timestamp = ts })
    eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 2 end)
    t:assert_eq(json.encode(field(eventd.rows(vm, "LOGS FROM " .. o), "message")), '["newer","older"]',
        "control: the default order is newest first")
    local tied = eventd.rows(vm, "LOGS FROM " .. o .. " SORT is_error")
    t:assert_eq(json.encode(field(tied, "message")), '["newer","older"]',
        "tied on is_error, the newer line still comes first")
end)

test("samples tied on timestamp and name are ordered by canonical labels before sample id", {
    spec = "eventd *order.metric-tiebreakers-are-timestamp-name-labels-then-sample-id-all-ascending",
}, function(t)
    local name = "pt" .. eventd.marker("mtie")
    local ts = eventd.guest_ns(vm) - 60 * 1000000000
    -- k=b is written first, so its sample id is the lower one.
    metrics({ name = name, type = "gauge", value = 2, labels = { k = "b" }, timestamp = ts })
    eventd.wait_rows(vm, "METRIC " .. name .. "[] SINCE 1h ago", function(rs) return #rs == 1 end)
    metrics({ name = name, type = "gauge", value = 1, labels = { k = "a" }, timestamp = ts })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. "[] SINCE 1h ago", function(rs) return #rs == 2 end)
    t:assert_eq(rows[1].timestamp, rows[2].timestamp, "control: one timestamp")
    t:assert_eq(json.encode(field(rows, "k")), '["a","b"]', "labels ascending: k=a before k=b")
end)

-- "These are not query-language fields. They never appear in a result
--  record, cannot be named in a SORT or a SELECT, and have no
--  access-control identity."
test("no row id or shard index appears in a record, and naming one selects nothing", {
    spec = "eventd *order.tiebreaker-keys-never-appear-in-results-and-cannot-be-named",
}, function(t)
    local tag = eventd.marker("hidden")
    pinned_emit(0, "pt.order.hidden", { tag = tag })
    local o = tag .. "o"
    logs({ origin = o, is_error = false, message = "x" })
    local name = "pt" .. tag
    metrics({ name = name, type = "gauge", value = 1 })
    local ev = eventd.wait_rows(vm, 'EVENTS pt.order.hidden WHERE tag == "' .. tag .. '"',
        function(rs) return #rs == 1 end)
    local lg = eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 1 end)
    local mt = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 1h ago", function(rs) return #rs == 1 end)
    for _, rec in ipairs({ ev[1], lg[1], mt[1] }) do
        for _, k in ipairs({ "id", "rowid", "shard", "shard_index", "series_id" }) do
            t:assert(rec[k] == nil, "no " .. k .. " in " .. json.encode(rec))
        end
    end
    -- In event mode `id` is just an absent payload path ...
    local sel = eventd.rows(vm, 'EVENTS pt.order.hidden WHERE tag == "' .. tag .. '" SELECT id')
    t:assert_eq(#sel, 1, "the record is still there")
    t:assert(sel[1].id == nil, "but SELECT id yields no id: " .. json.encode(sel[1]))
    -- ... and in log mode, whose field set is closed, it is no field at all.
    local r = eventd.query(vm, "LOGS FROM " .. o .. " SORT id")
    t:assert_eq(r.exit_code, 1, "SORT id is refused in log mode")
    t:assert(r.stderr:find("unknown log field id", 1, true), r.stderr)
end)

-- "The shard index is the numeric identifier from the shard-NNNN.db
--  filename."
-- Route closed: the shard index decides only between events with equal
-- timestamps in different shards (with or without an explicit SORT), and
-- KMES stamps each event in nanoseconds on
-- the CPU that emitted it, so no test can produce two events on two CPUs
-- with one timestamp. The unit test merges three shards holding equal
-- timestamps and checks the lower shard leads; the index it uses is the
-- position in the query's store list, which pipeline.rs builds as active
-- shards 0..N by number (pipeline.rs:81) then historical shards sorted by
-- name (pipeline.rs:509-528, 348-353) — the filename order.
test("of events with equal timestamps the lower shard comes first", {
    spec = "eventd *order.the-shard-index-is-the-number-in-the-shard-filename",
    skip = true,
    covered_by = "cargo:eventd eventd query::executor::tests::the_newest_first_merge_is_the_default_result_order",
}, function() end)

-- ---------------------------------------------------------------------------
-- Why insertion order is never the answer
-- ---------------------------------------------------------------------------

-- "samples.id and events.id break ties within one database, but neither
--  is a substitute for the timestamp ordering they follow."
test("records written out of time order come back in time order", {
    spec = "eventd *order.row-ids-only-break-ties-and-never-replace-timestamp-order",
}, function(t)
    local o = eventd.marker("late")
    local ts = eventd.guest_ns(vm) - 60 * 1000000000
    for _, rec in ipairs({ { "third", 3 }, { "first", 1 }, { "second", 2 } }) do
        logs({ origin = o, is_error = false, message = rec[1], timestamp = ts + rec[2] * 1000000000 })
        os.execute("sleep 0.3")
    end
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 3 end)
    t:assert_eq(json.encode(field(rows, "message")), '["third","second","first"]',
        "logs: newest timestamp first, whatever the insertion order")

    local name = "pt" .. o
    for _, rec in ipairs({ { 30, 3 }, { 10, 1 }, { 20, 2 } }) do
        metrics({ name = name, type = "gauge", value = rec[1], timestamp = ts + rec[2] * 1000000000 })
        os.execute("sleep 0.3")
    end
    local samples = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 1h ago",
        function(rs) return #rs == 3 end)
    t:assert_eq(json.encode(field(samples, "value")), "[10,20,30]",
        "metrics: oldest timestamp first, whatever the insertion order")
end)

-- "Every metric computation — RATE's consecutive pairs, aggregate window
--  membership, cross-type interval construction — is defined over
--  (timestamp, id) ascending."
test("a late sample lands where its timestamp says in DELTA's pairs and in a window", {
    spec = "eventd *order.metric-computations-order-samples-by-timestamp-then-id-ascending",
}, function(t)
    local name = "pt" .. eventd.marker("calc")
    local ts = eventd.guest_ns(vm) - 120 * 1000000000
    -- Counter values 10, 20, 40 at t+1, t+2, t+3, written t+3 first.
    for _, rec in ipairs({ { 40, 3 }, { 10, 1 }, { 20, 2 } }) do
        metrics({ name = name, type = "counter", value = rec[1], timestamp = ts + rec[2] * 1000000000 })
        os.execute("sleep 0.3")
    end
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 1h ago", function(rs) return #rs == 3 end)
    local deltas = eventd.rows(vm, "METRIC " .. name .. " DELTA SINCE 1h ago")
    t:assert_eq(json.encode(field(deltas, "value")), "[10,20]",
        "consecutive pairs in time order: 10->20, 20->40 (insertion order would see a reset)")
    local window = eventd.rows(vm, "METRIC " .. name .. " SINCE 1h ago MAX_OVER 1h")
    local max = 0
    for _, r in ipairs(window) do if type(r.value) == "number" and r.value > max then max = r.value end end
    t:assert_eq(max, 40, "the window holds every sample: " .. json.encode(window))
end)

-- ---------------------------------------------------------------------------
-- Value ordering is not SQLite's
-- ---------------------------------------------------------------------------

-- "Sorting, grouping and equality all use the query language's
--  semantics, never the storage engine's dynamic-type rules."
test("sort folds case, numbers compare exactly, types order nulls first and arrays last", {
    spec = "eventd *order.sorting-grouping-and-equality-never-use-sqlite-type-rules",
}, function(t)
    local o = eventd.marker("fold")
    for _, msg in ipairs({ "b", "A", "a", "B" }) do
        logs({ origin = o, is_error = false, message = msg })
    end
    eventd.wait_rows(vm, "LOGS FROM " .. o, function(rs) return #rs == 4 end)
    t:assert_eq(json.encode(field(eventd.rows(vm, "LOGS FROM " .. o .. " SORT message"), "message")),
        '["A","a","B","b"]', "text folds ASCII case, original bytes break a fold-equal tie (SQLite: A,B,a,b)")
    local groups = eventd.rows(vm, "LOGS FROM " .. o .. " DISTINCT message")
    t:assert_eq(#groups, 2, "a and A are one group, b and B another: " .. json.encode(groups))

    local tag = eventd.marker("num")
    local big = 9007199254740993 -- 2^53 + 1: no binary64 holds it
    for i, v in ipairs({ { n = 10 }, { n = eventd.float(2.5) }, { n = 9 }, { n = big },
                         { n = eventd.float(1.0) }, { n = 1 } }) do
        v.tag = tag
        v.i = i
        pinned_emit(i % 2, "pt.order.num", v)
    end
    eventd.wait_rows(vm, 'EVENTS pt.order.num WHERE tag == "' .. tag .. '"', function(rs) return #rs == 6 end)
    local sorted = eventd.query(vm, 'EVENTS pt.order.num WHERE tag == "' .. tag .. '" SORT n, i SELECT i')
    t:assert_eq(json.encode(field(sorted.rows, "i")), "[5,6,2,3,1,4]",
        "numbers mathematically, int and float together: " .. sorted.stdout)
    local eq = eventd.rows(vm, 'EVENTS pt.order.num WHERE tag == "' .. tag .. '" WHERE n == 9007199254740992.0')
    t:assert_eq(#eq, 0, "2^53+1 is not equal to the float 2^53 (SQLite converts and calls them equal)")
    local one = eventd.rows(vm, 'EVENTS pt.order.num WHERE tag == "' .. tag .. '" GROUP n COUNT')
    local ones
    for _, g in ipairs(one) do if g.n == 1 then ones = g.count end end
    t:assert_eq(ones, 2, "1 and 1.0 are one group: " .. json.encode(one))

    local mix = eventd.marker("mix")
    local values = { { v = "x" }, { v = eventd.array({ 1 }) }, { v = true }, {}, { v = 5 }, { v = eventd.bin("\1") } }
    for i, p in ipairs(values) do
        p.tag = mix
        p.i = i
        pinned_emit(0, "pt.order.mix", p)
    end
    eventd.wait_rows(vm, 'EVENTS pt.order.mix WHERE tag == "' .. mix .. '"', function(rs) return #rs == 6 end)
    local typed = eventd.rows(vm, 'EVENTS pt.order.mix WHERE tag == "' .. mix .. '" SORT v SELECT i')
    t:assert_eq(json.encode(field(typed, "i")), "[4,3,5,1,6,2]",
        "null, boolean, number, string, binary, array")
end)

-- ---------------------------------------------------------------------------
-- Canonical representatives
-- ---------------------------------------------------------------------------

-- "A group whose members are equal under the language's rules but not
--  byte-identical emits the smallest member rather than the first."
test("a group of case variants is represented by its smallest member, whichever came first", {
    spec = "eventd *order.a-group-emits-its-smallest-member-not-its-first",
}, function(t)
    local tag = eventd.marker("rep")
    -- "loregd" first, on one shard; "Loregd" (the smaller bytes) later,
    -- on the other.
    pinned_emit(0, "pt.order.rep", { tag = tag, svc = "loregd" .. tag })
    os.execute("sleep 0.3")
    pinned_emit(1, "pt.order.rep", { tag = tag, svc = "Loregd" .. tag })
    os.execute("sleep 0.3")
    pinned_emit(0, "pt.order.rep", { tag = tag, svc = "LOREGD" .. tag })
    eventd.wait_rows(vm, 'EVENTS pt.order.rep WHERE tag == "' .. tag .. '"', function(rs) return #rs == 3 end)
    for _, text in ipairs({
        'EVENTS pt.order.rep WHERE tag == "' .. tag .. '" DISTINCT svc',
        'EVENTS pt.order.rep WHERE tag == "' .. tag .. '" COUNT BY svc',
        'EVENTS pt.order.rep WHERE tag == "' .. tag .. '" GROUP svc COUNT',
    }) do
        local rows = eventd.rows(vm, text)
        t:assert_eq(#rows, 1, "one group: " .. text)
        t:assert_eq(rows[1] and rows[1].svc, "LOREGD" .. tag, "represented by the smallest bytes: " .. text)
    end
end)
