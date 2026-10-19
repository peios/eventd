-- eventd TRM §6.4 — cross-shard fan-out: every database in the event
-- store answers every event query, results merge across them, aggregates
-- fold across them, and each database costs a read-only connection.
-- Also §6.6's statement that an event-store wake re-examines every shard.
--
-- One file-scope VM with two vCPUs, so the event store starts with two
-- active shards, one per KMES buffer. Events go to a chosen shard by
-- emitting from a worker pinned to that shard's CPU; which CPU feeds
-- which shard is read back from the shard files, not assumed. The memory
-- and timing side of fan-out (what a merge holds, when it stops, the
-- unbounded case) needs a large dataset and lives in account-memory.
--
-- Late in the file the shard count is lowered to one and eventd
-- restarted, which turns shard-0001 into a historical shard: the tests
-- after that point read across an active and a historical database. The
-- count is restored at the end.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local sys = require("helpers.sys")
peinit.claim(1, { cpus = 2 })

-- Two vCPUs: two active shards by default.
local vm = eventd.boot({ name = "ev-fanout", cpus = 2 })

-- The raw query channel (eventd.rq), for what evctl hides: the order
-- frames arrive in, and a reader that stops reading.
local rq = eventd.rq

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

--- Emit `entries` ({type, payload} with payload a Lua value) from a worker
--- pinned to `cpu`, in one batch.
local function pinned(cpu, etype, payloads)
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local r = w:syscall(sys.NR.sched_setaffinity, {
            args = { 0, 8, 0 }, bufs = { string.pack("<I8", 1 << cpu) }, ptrs = { 2 },
        })
        assert(r.ret == 0, "pin to cpu " .. cpu .. ": errno " .. tostring(r.errno))
        local entries = {}
        for i, p in ipairs(payloads) do entries[i] = { type = etype, payload = eventd.msgpack(p) } end
        local e = kmes.emit_batch(w, entries)
        assert(e.ret == 0 and e.emitted == #entries, "pinned batch: errno " .. tostring(e.errno))
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end

--- shard number -> CPU that feeds it, worked out once from two probes.
local SHARD_OF_CPU = {}
do
    local tag = eventd.marker("probe")
    pinned(0, "pt.fan.probe", { { tag = tag .. "c0" } })
    pinned(1, "pt.fan.probe", { { tag = tag .. "c1" } })
    eventd.wait_rows(vm, "EVENTS pt.fan.probe", function(rs) return #rs >= 2 end)
    for _, path in ipairs(eventd.shards(vm)) do
        local n = tonumber(path:match("shard%-(%d+)%.db$"))
        for cpu = 0, 1 do
            local c = eventd.sql(vm, path, "SELECT count(*) FROM events WHERE instr(payload, '"
                .. tag .. "c" .. cpu .. "') > 0")
            if c[1][1] > 0 then SHARD_OF_CPU[cpu] = n end
        end
    end
    assert(SHARD_OF_CPU[0] and SHARD_OF_CPU[1] and SHARD_OF_CPU[0] ~= SHARD_OF_CPU[1],
        "the two CPUs feed two shards: " .. json.encode(SHARD_OF_CPU))
end

local function field(rows, name)
    local out = {}
    for i, r in ipairs(rows) do out[i] = r[name] end
    return out
end

local SHARD0 = eventd.STORE.events .. "/shard-0000.db"
local SHARD1 = eventd.STORE.events .. "/shard-0001.db"
-- The writers' descriptors as first seen, for the lifetime test at the end.
local FIRST_FDS = eventd.fds(vm, nil, { by_fd = true })

-- ---------------------------------------------------------------------------
-- Merging
-- ---------------------------------------------------------------------------

-- "A non-aggregating query with no SORT reads every shard at once ... and
--  the coordinator takes whichever shard's next row comes first under the
--  tiebreakers of §6.2: an N-way merge of the sorted streams."
test("events alternating between two shards come back in one newest-first sequence", {
    spec = "eventd *fanout.non-aggregating-results-are-an-n-way-merge-of-sorted-shard-streams",
}, function(t)
    local tag = eventd.marker("merge")
    for i = 1, 6 do
        pinned((i - 1) % 2, "pt.fan.merge", { { tag = tag, i = i } })
        os.execute("sleep 0.1")
    end
    local rows = eventd.wait_rows(vm, 'EVENTS pt.fan.merge WHERE tag == "' .. tag .. '"',
        function(rs) return #rs == 6 end)
    t:assert_eq(json.encode(field(rows, "i")), "[6,5,4,3,2,1]", "newest first across both shards")
    local cpus = {}
    for _, r in ipairs(rows) do cpus[r["event.cpu"]] = true end
    t:assert(cpus[0] and cpus[1], "and the rows did come from both CPUs' shards")
    for k = 2, #rows do
        t:assert(rows[k - 1]["event.time"] >= rows[k]["event.time"], "timestamps never rise down the result")
    end
    local page = eventd.rows(vm, 'EVENTS pt.fan.merge WHERE tag == "' .. tag .. '" SKIP 2 TAKE 3')
    t:assert_eq(json.encode(field(page, "i")), "[4,3,2]", "and a page of the merge is a slice of it")
end)

-- "COUNT BY, TOP N BY, GROUP … COUNT: a count; at the end the groups are
--  sorted by count descending and TAKE applies."
test("counted groups come back by count descending, then TAKE", {
    spec = "eventd *fanout.grouped-counts-are-sorted-descending-then-taken",
}, function(t)
    local tag = eventd.marker("cnt")
    -- a once, b three times, c twice; a appears first, across both shards.
    pinned(0, "pt.fan.cnt", { { tag = tag, v = "a" }, { tag = tag, v = "b" } })
    pinned(1, "pt.fan.cnt", { { tag = tag, v = "b" }, { tag = tag, v = "c" } })
    pinned(0, "pt.fan.cnt", { { tag = tag, v = "c" }, { tag = tag, v = "b" } })
    local base = 'EVENTS pt.fan.cnt WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 6 end)
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " COUNT BY v"), "v")), '["b","c","a"]', "COUNT BY")
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " TOP 2 BY v"), "v")), '["b","c"]', "TOP 2 BY")
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " COUNT BY v TAKE 2"), "count")), "[3,2]",
        "COUNT BY with TAKE")
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " GROUP v COUNT"), "v")), '["b","c","a"]',
        "GROUP … COUNT, by count descending")
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " GROUP v COUNT TAKE 2"), "count")), "[3,2]",
        "GROUP … COUNT with TAKE keeps the two largest")
end)

-- PSPU §3.21: numeric groups have one order regardless of shard arrival.
test("numeric GROUP defaults to key order across shards before paging", {
    spec = "eventd *fanout.numeric-groups-are-sorted-by-keys-before-pagination", tags = { "pei1491" },
}, function(t)
    local tag = eventd.marker("group-order")
    -- Put z in shard 0 and a in shard 1 so encounter order cannot
    -- accidentally agree with key order, whichever CPU feeds each shard.
    local zcpu = SHARD_OF_CPU[0] == 0 and 0 or 1
    pinned(zcpu, "pt.fan.grouporder", {
        { tag = tag, g = "z", h = 2, n = 10 },
        { tag = tag, g = "z", h = 1, n = 20 },
    })
    pinned(1 - zcpu, "pt.fan.grouporder", {
        { tag = tag, g = "a", h = 2, n = 30 },
        { tag = tag, g = "a", h = 1, n = 40 },
    })
    local base = 'EVENTS pt.fan.grouporder WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rows) return #rows == 4 end)
    local function keys(rows)
        local result = {}
        for _, row in ipairs(rows) do result[#result + 1] = row.g .. tostring(row.h) end
        return table.concat(result, ",")
    end
    for _, function_name in ipairs({ "SUM", "AVG", "MIN", "MAX" }) do
        local query = base .. " GROUP g, h " .. function_name .. " n"
        t:assert_eq(keys(eventd.rows(vm, query)), "a1,a2,z1,z2", function_name .. " by keys")
        t:assert_eq(keys(eventd.rows(vm, query .. " SKIP 1 TAKE 2")), "a2,z1", "paging follows ordering")
        t:assert_eq(keys(eventd.rows(vm, query .. " SORT g DESC, h")), "z1,z2,a1,a2", "explicit SORT overrides")
        t:assert_eq(keys(eventd.rows(vm, query .. " SORT g SKIP 1 TAKE 2")), "a2,z1",
            "ties in explicit SORT use stable key order before paging")
    end
end)

-- "GROUP … SUM: the exact integer sum while every value is an integer and
--  it fits, and the binary64 sum."
test("a group sum across shards is exact for integers and binary64 once a float joins", {
    spec = "eventd *fanout.group-sum-keeps-the-exact-and-binary64-sums",
}, function(t)
    local tag = eventd.marker("sum")
    -- 2^53 + 1 and 2: binary64 cannot hold either sum exactly.
    pinned(0, "pt.fan.sum", { { tag = tag, g = "exact", n = 9007199254740993 },
                              { tag = tag, g = "wide", n = 9223372036854775807 },
                              { tag = tag, g = "mixed", n = 2 } })
    pinned(1, "pt.fan.sum", { { tag = tag, g = "exact", n = 2 },
                              { tag = tag, g = "wide", n = 1 },
                              { tag = tag, g = "mixed", n = eventd.float(0.5) } })
    local base = 'EVENTS pt.fan.sum WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 6 end)
    local r = eventd.query(vm, base .. " GROUP g SUM n SORT g")
    t:assert(r.ok, r.stderr)
    t:assert(r.stdout:find('"g":"exact","sum":9007199254740995', 1, true)
        or r.stdout:find('"sum":9007199254740995', 1, true), "the exact integer sum: " .. r.stdout)
    t:assert(r.stdout:find("9223372036854775808", 1, true), "past i64 it stays exact as an unsigned: " .. r.stdout)
    local mixed
    for _, row in ipairs(r.rows) do if row.g == "mixed" then mixed = row.sum end end
    t:assert_eq(mixed, 2.5, "with a float in the group the sum is binary64")
end)

-- "GROUP … AVG: the binary64 sum and count, divided once at the end."
test("an average across unevenly filled shards is the sum over the count", {
    spec = "eventd *fanout.group-avg-divides-the-whole-sum-by-the-whole-count",
}, function(t)
    local tag = eventd.marker("avg")
    pinned(0, "pt.fan.avg", { { tag = tag, n = 1 } })
    pinned(1, "pt.fan.avg", { { tag = tag, n = 2 }, { tag = tag, n = 3 } })
    local base = 'EVENTS pt.fan.avg WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 3 end)
    local rows = eventd.rows(vm, base .. " GROUP tag AVG n")
    -- Averaging the shards' averages would give (1 + 2.5) / 2 = 1.75.
    t:assert_eq(rows[1] and rows[1].avg, 2.0, "six over three")
end)

-- "GROUP … MIN / MAX: the extreme so far."
test("the extremes across shards are the extremes of the whole group", {
    spec = "eventd *fanout.group-min-and-max-keep-the-extreme-so-far",
}, function(t)
    local tag = eventd.marker("ext")
    pinned(0, "pt.fan.ext", { { tag = tag, n = 4 }, { tag = tag, n = eventd.float(7.5) }, { tag = tag, n = "text" } })
    pinned(1, "pt.fan.ext", { { tag = tag, n = -5 }, { tag = tag, n = 3 }, { tag = tag } })
    local base = 'EVENTS pt.fan.ext WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 6 end)
    t:assert_eq(eventd.rows(vm, base .. " GROUP tag MIN n")[1].min, -5, "the minimum, from the other shard")
    t:assert_eq(eventd.rows(vm, base .. " GROUP tag MAX n")[1].max, 7.5, "the maximum, a float")
end)

-- "DISTINCT: the value."
test("DISTINCT across shards gives each value once, in value order", {
    spec = "eventd *fanout.distinct-keeps-each-value",
}, function(t)
    local tag = eventd.marker("dis")
    pinned(0, "pt.fan.dis", { { tag = tag, v = "m" }, { tag = tag, v = "a" }, { tag = tag, v = "z" } })
    pinned(1, "pt.fan.dis", { { tag = tag, v = "a" }, { tag = tag, v = "m" }, { tag = tag, v = "b" } })
    local base = 'EVENTS pt.fan.dis WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 6 end)
    t:assert_eq(json.encode(field(eventd.rows(vm, base .. " DISTINCT v"), "v")), '["a","b","m","z"]',
        "four values, once each")
end)

-- "A group is found by hashing a key that every pair of language-equal
--  values shares — numbers by their nearest binary64, text with ASCII
--  case folded — and then by the language's own equality."
test("equal values share a group, and values that only share a binary64 do not", {
    spec = "eventd *fanout.groups-are-found-by-a-hash-that-equal-values-share",
}, function(t)
    local tag = eventd.marker("hash")
    pinned(0, "pt.fan.hash", { { tag = tag, v = 1 }, { tag = tag, v = "A" }, { tag = tag, v = 9007199254740992 } })
    pinned(1, "pt.fan.hash", { { tag = tag, v = eventd.float(1.0) }, { tag = tag, v = "a" },
                               { tag = tag, v = 9007199254740993 } })
    local base = 'EVENTS pt.fan.hash WHERE tag == "' .. tag .. '"'
    eventd.wait_rows(vm, base, function(rs) return #rs == 6 end)
    local r = eventd.query(vm, base .. " GROUP v COUNT")
    t:assert(r.ok, r.stderr)
    t:assert_eq(#r.rows, 4, "1 with 1.0, A with a, and the two big integers apart: " .. r.stdout)
    local counts = field(r.rows, "count")
    table.sort(counts)
    t:assert_eq(json.encode(counts), "[1,1,2,2]", "two pairs and two singles")
    t:assert(r.stdout:find("9007199254740992", 1, true) and r.stdout:find("9007199254740993", 1, true),
        "both big integers survive as their own groups: " .. r.stdout)
end)

-- §6.6: "a wake is 'something committed somewhere' and a handler
--  re-examines every shard it cares about."
test("one stream receives events committed to either shard", {
    spec = "eventd *stream.an-event-store-wake-makes-a-handler-re-examine-every-shard",
}, function(t)
    local tag = eventd.marker("wake")
    local etype = "pt.fan.wake" .. tag
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local c = rq.open(w)
        rq.timeout(c, 10)
        rq.send(c, "EVENTS " .. etype .. " STREAM")
        rq.frame(c)
        local watch = rq.frame(c)
        t:assert_eq(watch and watch.status, "watch", "the stream is established")
        pinned(0, etype, { { i = 1 } })
        pinned(1, etype, { { i = 2 } })
        pinned(0, etype, { { i = 3 } })
        local got = {}
        while #got < 3 do
            local f = rq.frame(c)
            if not f then break end
            for _, r in ipairs(f.records or {}) do got[#got + 1] = r end
        end
        rq.close(c)
        local cpus = {}
        for _, r in ipairs(got) do cpus[r["event.cpu"]] = true end
        t:assert_eq(#got, 3, "all three events streamed: " .. json.encode(got))
        t:assert(cpus[0] and cpus[1], "from both shards")
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- "Active shard writer connections stay open for the process lifetime."
test("each active shard's writer keeps the one read-write descriptor it started with", {
    spec = "eventd *fanout.active-shard-writer-connections-stay-open-for-the-process-lifetime",
}, function(t)
    for i = 1, 20 do eventd.rows(vm, "EVENTS pt.fan.merge TAKE " .. i .. " SELECT i") end
    eventd.rows(vm, "EVENTS COUNT BY event.type")
    pinned(0, "pt.fan.life", { { n = 1 } })
    pinned(1, "pt.fan.life", { { n = 2 } })
    eventd.wait_rows(vm, "EVENTS pt.fan.life", function(rs) return #rs >= 2 end)
    local now = eventd.fds(vm, nil, { by_fd = true })
    for _, path in ipairs({ SHARD0, SHARD1 }) do
        local first = eventd.fd_numbers(FIRST_FDS, path, 2)
        t:assert_eq(#first, 1, path .. ": one read-write descriptor at the start")
        t:assert_eq(json.encode(eventd.fd_numbers(now, path, 2)), json.encode(first),
            path .. ": the same descriptor after every test so far")
    end
end)

-- ---------------------------------------------------------------------------
-- Active and historical shards
-- ---------------------------------------------------------------------------

local HIST_TAG = eventd.marker("hist")
-- 600 events, 2 KB apiece, across both shards: enough results that a
-- client which stops reading leaves eventd blocked mid-answer.
pinned(SHARD_OF_CPU[0] == 1 and 0 or 1, "pt.fan.hist", (function()
    local p = {}
    for i = 1, 100 do p[i] = { tag = HIST_TAG, i = i, pad = string.rep("h", 2000) } end
    return p
end)())

-- "Event queries execute against every database in the event store
--  directory — active shards and historical ones alike." / "There is no
--  shard a query can skip on the basis of its contents, and a predicate
--  on event.cpu scans all of them."
test("after the shard count drops, the retired shard is still read, event.cpu predicates included", {
    spec = "eventd *fanout.an-event-query-runs-against-every-active-and-historical-shard"
        .. " eventd *fanout.no-shard-is-skipped-even-for-a-cpu-id-predicate",
}, function(t)
    -- The CPU feeding shard 1 now, whose events will outlive its shard.
    local cpu1 = SHARD_OF_CPU[0] == 1 and 0 or 1
    pinned(cpu1, "pt.fan.old", { { tag = HIST_TAG } })
    eventd.wait_rows(vm, 'EVENTS pt.fan.old WHERE tag == "' .. HIST_TAG .. '"', function(rs) return #rs == 1 end)
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    eventd.restart(vm)
    t:assert_eq(#eventd.shards(vm), 2, "shard-0001 is still in the directory")
    -- The same CPU now writes to the one active shard.
    pinned(cpu1, "pt.fan.old", { { tag = HIST_TAG, after = true } })
    local rows = eventd.wait_rows(vm, 'EVENTS pt.fan.old WHERE tag == "' .. HIST_TAG .. '"',
        function(rs) return #rs == 2 end)
    t:assert_eq(#rows, 2, "the event in the historical shard and the new one in the active shard")
    t:assert_eq(eventd.sql(vm, SHARD1,
        "SELECT count(*) FROM events WHERE event_type = 'pt.fan.old'")[1][1], 1,
        "control: one of them is in shard-0001, now historical")
    t:assert_eq(eventd.sql(vm, SHARD0,
        "SELECT count(*) FROM events WHERE event_type = 'pt.fan.old'")[1][1], 1,
        "and the other in shard-0000")
    local by_cpu = eventd.rows(vm, 'EVENTS pt.fan.old WHERE tag == "' .. HIST_TAG .. '" WHERE event.cpu == ' .. cpu1)
    t:assert_eq(#by_cpu, 2, "an event.cpu predicate finds that CPU's events in both shards")
    local hist = eventd.rows(vm, 'EVENTS pt.fan.hist WHERE tag == "' .. HIST_TAG .. '" COUNT BY event.type')
    t:assert_eq(hist[1] and hist[1].count, 100, "the bulk set in the historical shard is read")
end)

-- "An event query opens a read-only connection per database, and each
--  SQLite connection holds one or two descriptors for the database and
--  its write-ahead log."
test("a held event query reads each database, active and historical, through one read-only descriptor", {
    spec = "eventd *fanout.an-event-query-opens-a-read-only-connection-per-database",
}, function(t)
    -- Bulk in the active shard as well.
    pinned(0, "pt.fan.hist", (function()
        local p = {}
        for i = 101, 200 do p[#p + 1] = { tag = HIST_TAG, i = i, pad = string.rep("h", 2000) } end
        return p
    end)())
    eventd.wait_rows(vm, 'EVENTS pt.fan.hist WHERE tag == "' .. HIST_TAG .. '" COUNT BY event.type',
        function(rs) return rs[1] and rs[1].count == 200 end)
    local w = vm:spawn_worker()
    local c = rq.open(w)
    rq.send(c, "EVENTS pt.fan.hist")
    local first = rq.frame(c)
    local during = eventd.fds(vm, nil, { by_fd = true })
    rq.close(c)
    w:kill(); w:join()
    t:assert_eq(first and first.status, "ok", "the query was answering when held")
    -- Descriptors that SQLite parks after a connection closes are reused
    -- by the next connection to the same file (unix VFS), so what is
    -- counted is the read-only descriptors open at once: one per
    -- database the held query reads.
    for _, path in ipairs({ SHARD0, SHARD1 }) do
        t:assert_eq(#eventd.fd_numbers(during, path, 0), 1, path .. ": one read-only descriptor: " .. json.encode(during))
    end
    t:assert_eq(#eventd.fd_numbers(during, SHARD0, 1), 0, "nothing write-only")
    eventd.unset(vm, "StorageShards")
    eventd.restart(vm)
end)
