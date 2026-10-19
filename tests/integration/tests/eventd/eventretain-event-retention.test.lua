-- eventd TRM §3.6 — event retention: age and size, both enforced; size
-- measured as logical live size summed over the shards; whole old boots
-- before the current one; bounded writer-owned commands; no VACUUM.
--
-- One file-scope VM with two shards (StorageShards seeded at boot), so
-- "per shard" and "across all shards" have something to mean on one
-- vCPU: one CPU's events are striped over both shards 1024 sequences at
-- a time.
--
-- KMES stamps its own events, so old events and events of other boots
-- cannot be sent. They are inserted offline instead: eventd stopped
-- through the service manager, each shard edited on the host (a sqlite
-- copy with its WAL folded in, written back with -wal and -shm removed),
-- eventd started again. Payloads are small valid MessagePack maps padded
-- with a 1000-byte bin, so the rows have real weight and still decode.
--
-- Passes are triggered two ways: RetentionCheckIntervalMinutes at its
-- one-minute minimum (the age test waits for the timer on purpose), and
-- any applied configuration change, which requests a pass at once
-- (config.rs:1031).

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({
    name = "ev-eventretain",
    config = { { name = "StorageShards", type = "dword", data = 2 } },
})

local DAY = 86400 * 1000000000
-- {"p": bin(1000)}. `||` yields TEXT, and eventd reads payload as a blob.
local PAD = "CAST(X'81A170C503E8' || randomblob(1000) AS BLOB)"
local BOOT_A = "A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0A0"
local BOOT_B = "B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0B0"
local BOOT_OLD = "0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F0F"

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local SHARD = {
    eventd.STORE.events .. "/shard-0000.db",
    eventd.STORE.events .. "/shard-0001.db",
}

--- SQL inserting `n` rows into events: boot `boot` (hex), type `ty`,
--- timestamps `first + i * step`, cpu_id and sequence as given (`seq`
--- is a base the row number is added to, or nil).
local function rows_sql(boot, ty, n, first, step, cpu, seq)
    return string.format([[
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
INSERT INTO events (boot_id, timestamp, cpu_id, sequence, origin_class, event_type, payload)
SELECT X'%s', %d + i * %d, %s, %s, %s, '%s', %s FROM n;
INSERT OR IGNORE INTO event_types VALUES ('%s');
]], n, boot, first, step, cpu or "NULL", seq and (seq .. " + i") or "NULL", seq and "1" or "NULL",
        ty, PAD, ty)
end

local function count(shard, where)
    return eventd.sql(vm, shard, "SELECT count(*) FROM events WHERE " .. where)[1][1]
end

local function count_all(where)
    return count(SHARD[1], where) + count(SHARD[2], where)
end

--- Logical live bytes, file bytes and free pages of one shard.
local function shard_size(shard)
    local r = eventd.sql(vm, shard, [[
SELECT (SELECT page_count FROM pragma_page_count) - (SELECT freelist_count FROM pragma_freelist_count),
       (SELECT page_count FROM pragma_page_count),
       (SELECT freelist_count FROM pragma_freelist_count),
       (SELECT page_size FROM pragma_page_size)]])[1]
    return r[1] * r[4], r[2] * r[4], r[3]
end

local function total_live()
    return (shard_size(SHARD[1])) + (shard_size(SHARD[2]))
end

local function wal_state(db)
    local ok, bytes = pcall(vm.read_file, vm, db .. "-wal")
    if not ok or #bytes < 32 then return 0, nil end
    local pagesize, _, salt1, salt2 = string.unpack(">I4I4I4I4", bytes, 9)
    local n, pos = 0, 33
    while pos + 24 + pagesize - 1 <= #bytes do
        local _, commit, s1, s2 = string.unpack(">I4I4I4I4", bytes, pos)
        if s1 ~= salt1 or s2 ~= salt2 then break end
        if commit ~= 0 then n = n + 1 end
        pos = pos + 24 + pagesize
    end
    return n, salt1
end

local flip = 0
local function retention_pass()
    flip = flip + 1
    eventd.set(vm, "MetricMaxBatchSize", "dword:" .. (flip % 2 == 0 and 4000 or 4500)):assert_ok()
end

-- ---------------------------------------------------------------------------
-- Age
-- ---------------------------------------------------------------------------

test("events past EventRetentionDays go from every shard, KMES, synthetic and gap rows alike, on the timer", {
    spec = "eventd *eventretain.events-older-than-eventretentiondays-by-the-wall-clock-are-deleted"
        .. " eventd *eventretain.age-retention-runs-per-shard-and-covers-kmes-synthetic-and-gap-records-alike"
        .. " eventd *eventretain.the-coordinator-runs-every-retentioncheckintervalminutes"
        .. " eventd *eventretain.the-coordinator-submits-low-priority-commands-to-the-owning-writer"
        .. " eventd *eventretain.a-command-runs-at-a-transaction-boundary-deletes-at-most-retentiondeletebatchrows-and-yields",
}, function(t)
    t:assert_eq(#eventd.shards(vm), 2, "precondition: two shards")
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
    eventd.set(vm, "RetentionCheckIntervalMinutes", "dword:1"):assert_ok()
    vm:clock():sleep("3s")
    local now = eventd.guest_ns(vm)
    local old, young = now - 31 * DAY, now - 29 * DAY
    eventd.stop(vm)
    for idx, shard in ipairs(SHARD) do
        local script = ""
        for _, age in ipairs({ { "old", old }, { "young", young } }) do
            script = script
                .. rows_sql(BOOT_OLD, "pt.retain.kmes." .. age[1], 1, age[2], 1, 0, 1000 * idx)
                .. rows_sql(BOOT_OLD, "pt.retain.synthetic." .. age[1], 1, age[2], 1)
                .. rows_sql(BOOT_OLD, "pt.retain.gap." .. age[1], 1, age[2], 1, 0)
        end
        if idx == 2 then
            -- A thousand more expired rows in the shard no writer is
            -- writing to just after a start (stripes begin on shard 0),
            -- so its WAL holds only what retention does to it.
            script = script .. rows_sql(BOOT_OLD, "pt.retain.bulk", 1000, old, 1, 0, 5000)
        end
        eventd.edit_store(vm, shard, script)
    end
    eventd.start(vm)
    local c0, s0 = wal_state(SHARD[2])
    -- The timer was restarted with the process: a minute, not at once.
    vm:clock():sleep("15s")
    t:assert_eq(count_all("event_type LIKE 'pt.retain.%.old'"), 6,
        "fifteen seconds in, no pass has run yet")
    local gone = pcall(wait_until, function()
        return count_all("event_type LIKE 'pt.retain.%.old' OR event_type = 'pt.retain.bulk'") == 0
    end, { timeout = 90, interval = 0.5, desc = "the timed pass" })
    local c1, s1 = wal_state(SHARD[2])
    t:assert(gone, "the one-minute timer ran a pass that deleted every expired row")
    for _, shard in ipairs(SHARD) do
        for _, kind in ipairs({ "kmes", "synthetic", "gap" }) do
            t:assert_eq(count(shard, "event_type = 'pt.retain." .. kind .. ".old'"), 0,
                shard .. ": the 31-day " .. kind .. " row went")
            t:assert_eq(count(shard, "event_type = 'pt.retain." .. kind .. ".young'"), 1,
                shard .. ": the 29-day " .. kind .. " row stayed")
        end
    end
    local commits = (s0 == nil or s0 == s1) and (c1 - (s0 and c0 or 0)) or nil
    t:assert(commits and commits >= 10,
        "1,003 rows at 100 per command took at least ten of the shard writer's commits: " .. tostring(commits))
end)

-- ---------------------------------------------------------------------------
-- Size
-- ---------------------------------------------------------------------------

test("size retention takes whole old boots, oldest-ending first, across shards, then the current boot's oldest", {
    spec = "eventd *eventretain.an-event-is-deleted-when-it-exceeds-either-the-age-or-size-threshold"
        .. " eventd *eventretain.size-is-logical-live-size-measured-after-a-passive-checkpoint-attempt"
        .. " eventd *eventretain.the-event-store-size-is-the-sum-across-every-shard"
        .. " eventd *eventretain.pages-freed-by-retention-do-not-count-toward-size"
        .. " eventd *eventretain.size-retention-runs-only-when-eventretentionmaxbytes-is-non-zero-and-exceeded"
        .. " eventd *eventretain.size-retention-considers-every-non-current-boot-id-in-the-shards"
        .. " eventd *eventretain.non-current-boots-are-ordered-by-newest-event-timestamp-oldest-first"
        .. " eventd *eventretain.whole-non-current-boots-are-deleted-one-at-a-time-across-all-shards"
        .. " eventd *eventretain.only-then-are-the-current-boots-oldest-events-deleted"
        .. " eventd *eventretain.eventd-never-runs-vacuum-automatically"
        .. " eventd *eventretain.reclamation-is-an-explicit-administrative-operation",
}, function(t)
    -- Boot A's events span six to three days ago, boot B's five to two:
    -- by newest event A is older, by oldest event B is. 150 rows of each
    -- per shard, and 150 per shard of this boot's own, a day old.
    -- (The age half of "either threshold" is the test above.)
    local now = eventd.guest_ns(vm)
    local current = eventd.boot_pcds_hex(vm)
    eventd.stop(vm)
    for idx, shard in ipairs(SHARD) do
        eventd.edit_store(vm, shard,
            rows_sql(BOOT_A, "pt.size.a", 150, now - 5 * DAY, DAY // 75, 0, 10000 * idx)
            .. rows_sql(BOOT_B, "pt.size.b", 150, now - 6 * DAY, (4 * DAY) // 150, 0, 20000 * idx)
            .. rows_sql(current, "pt.size.c", 150, now - DAY, 1000000000, nil, 0))
    end
    eventd.start(vm)
    local function counts()
        return count_all("event_type = 'pt.size.a'"), count_all("event_type = 'pt.size.b'"),
            count_all("event_type = 'pt.size.c'")
    end
    local a, b, c = counts()
    t:assert(a == 300 and b == 300 and c == 300, "precondition: 300 rows of each")

    -- Not enabled (0, the default) and then not exceeded: nothing goes.
    retention_pass()
    vm:clock():sleep("5s")
    local t0 = total_live()
    eventd.set(vm, "EventRetentionMaxBytes", "qword:" .. (t0 + 50 * 1024 * 1024)):assert_ok()
    vm:clock():sleep("5s")
    a, b, c = counts()
    t:assert(a == 300 and b == 300 and c == 300, "with the limit 0 or not exceeded, size retention deletes nothing")

    -- About 300 KB per boot; ask for half a boot's worth back.
    local half = 150 * 1024
    local function step(desc, want)
        local before = total_live()
        local limit = before - half
        local each = math.max((shard_size(SHARD[1])), (shard_size(SHARD[2])))
        t:assert(each < limit, desc .. ": precondition: no single shard is over the limit, only their sum")
        eventd.set(vm, "EventRetentionMaxBytes", "qword:" .. limit):assert_ok()
        local ok = pcall(wait_until, function() return want(counts()) end,
            { timeout = 60, interval = 0.5, desc = desc })
        vm:clock():sleep("3s")
        return ok, limit
    end

    local f0 = { (select(2, shard_size(SHARD[1]))), (select(2, shard_size(SHARD[2]))) }
    local ok = step("boot A", function(x) return x == 0 end)
    a, b, c = counts()
    t:assert(ok and a == 0, "boot A, the one whose newest event is oldest, went first, from both shards: " .. a)
    t:assert_eq(b, 300, "boot B was untouched once the total was within the limit")
    t:assert_eq(c, 300, "and so was the current boot")
    for i, shard in ipairs(SHARD) do
        local _, file, free = shard_size(shard)
        t:assert(file >= f0[i] and free > 0,
            shard .. ": the file kept its size, the freed pages on the freelist — no VACUUM")
    end

    ok = step("boot B", function(_, x) return x == 0 end)
    a, b, c = counts()
    t:assert(ok and b == 0, "then boot B, whole: " .. b)
    t:assert_eq(c, 300, "the current boot still untouched")

    local real_before = count_all("hex(boot_id) = '" .. current .. "' AND event_type = '" .. eventd.T.startup .. "'")
    ok = step("the current boot", function(_, _, x) return x < 300 end)
    a, b, c = counts()
    t:assert(ok and c > 0 and c < 300, "with no old boot left, the current boot's oldest rows went: " .. c .. " left")
    for _, shard in ipairs(SHARD) do
        local r = eventd.sql(vm, shard, "SELECT count(*), min(sequence), max(sequence) FROM events WHERE event_type = 'pt.size.c'")[1]
        if r[1] > 0 then
            t:assert_eq(r[1], r[3] - r[2] + 1, shard .. ": what is left of them is a run of the newest")
            t:assert_eq(r[3], 150, shard .. ": ending at the newest")
        end
    end
    t:assert_eq(count_all("hex(boot_id) = '" .. current .. "' AND event_type = '" .. eventd.T.startup .. "'"),
        real_before, "and this boot's newer records are still there")
    eventd.unset(vm, "EventRetentionMaxBytes")
end)

-- ---------------------------------------------------------------------------
-- Who writes
-- ---------------------------------------------------------------------------

test("every store has its one read-write connection, and a pass adds none while it checkpoints", {
    spec = "eventd *eventretain.every-database-but-a-historical-shard-has-one-read-write-connection-owned-by-its-ingestion-writer"
        .. " eventd *eventretain.there-is-no-retention-writer-mutex-and-no-second-writer"
        .. " eventd *eventretain.the-pre-measurement-checkpoint-is-a-writer-owned-command"
        .. " eventd *eventretain.the-coordinator-requests-the-checkpoint-then-reads-page-counts-read-only"
        .. " eventd *eventretain.the-coordinator-never-checkpoints-through-or-promotes-its-read-only-connection",
}, function(t)
    -- A checkpoint needs a writer's lock; a read-only connection cannot
    -- take one. So: the shard's WAL is restarted across a pass (a
    -- checkpoint ran), and at no sample during it was there a second
    -- read-write descriptor — the checkpoint was the writer's.
    local pid = eventd.pid(vm)
    local dbs = { SHARD[1], SHARD[2], eventd.DB.logs, eventd.DB.metrics }
    for _, db in ipairs(dbs) do
        t:assert_eq((eventd.fds_on(vm, pid, db)), 1, db .. ": one read-write connection")
    end
    eventd.emit(vm, "pt.retain.tick", { n = 1 })
    vm:clock():sleep("1s")
    local _, s0 = wal_state(SHARD[1])
    retention_pass()
    local most = 0
    for _ = 1, 15 do
        for _, db in ipairs(dbs) do most = math.max(most, (eventd.fds_on(vm, pid, db))) end
    end
    vm:clock():sleep("2s")
    for i = 1, 5 do eventd.emit(vm, "pt.retain.tick", { n = i }) end
    vm:clock():sleep("2s")
    local _, s1 = wal_state(SHARD[1])
    t:assert_eq(most, 1, "no store ever had a second read-write connection during the pass")
    t:assert(s0 ~= nil and s1 ~= s0, "and the pass checkpointed the shard, so its WAL started over")
end)

-- A historical shard has no ingestion writer: the coordinator holds its one
-- read-write connection and deletes from it directly. An expired row
-- planted in shard-0001 before it becomes historical is gone after a pass.
test("the retention coordinator holds the one read-write connection to a historical shard and deletes through it", {
    spec = "eventd *eventretain.the-coordinator-measures-read-only-and-owns-read-write-connections-only-to-historical-shards",
}, function(t)
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    eventd.stop(vm)
    eventd.edit_store(vm, SHARD[2], rows_sql(BOOT_OLD, "pt.retain.hist.old", 1, eventd.guest_ns(vm) - 31 * DAY, 1))
    eventd.start(vm)
    local pid = eventd.pid(vm)
    t:assert_eq(#eventd.shards(vm), 2, "shard-0001 is still there, now historical")
    t:assert_eq((eventd.fds_on(vm, pid, SHARD[1])), 1, "the active shard has its writer's connection")
    local rw = eventd.fds_on(vm, pid, SHARD[2])
    t:assert_eq(count(SHARD[2], "event_type = 'pt.retain.hist.old'"), 1, "the expired row is planted")
    retention_pass()
    local gone = pcall(wait_until, function()
        return count(SHARD[2], "event_type = 'pt.retain.hist.old'") == 0
    end, { timeout = 60, interval = 0.5, desc = "the pass to delete from the historical shard" })
    eventd.unset(vm, "StorageShards")
    t:assert_eq(rw, 1, "the historical shard, which has no ingestion writer, has one read-write connection")
    t:assert(gone, "and the pass deleted the expired row through it")
end)

-- Permitted, not required, and not done: no code path joins a delete to
-- an open ingestion transaction (writer.rs takes maintenance only as its
-- own message), so there is nothing to observe.
test("under urgent size pressure one bounded delete may join an open ingestion transaction", {
    spec = "eventd *eventretain.under-urgent-size-pressure-one-bounded-delete-may-join-an-open-ingestion-transaction",
    skip = true,
}, function() end)

-- Route closed: a pass runs over the three stores within milliseconds
-- and nothing it leaves behind is timestamped, so the order is not
-- visible from outside. The unit test runs one pass against stub writers
-- that record the order commands reach them.
test("the coordinator processes events, then logs, then metrics", {
    spec = "eventd *eventretain.the-coordinator-processes-events-then-logs-then-metrics",
    skip = true,
    covered_by = "cargo:eventd eventd retention::tests::a_pass_processes_events_then_logs_then_metrics",
}, function() end)
