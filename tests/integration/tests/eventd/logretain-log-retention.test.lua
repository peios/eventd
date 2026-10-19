-- eventd TRM §4.4 — log retention: age and size, both enforced, oldest
-- first with no regard to boots, in bounded writer-owned transactions,
-- and never a VACUUM.
--
-- One file-scope VM. Log lines can be backdated at the source — a log
-- record's `timestamp` is the producer's when it gives one — so every
-- test here sends lines that are already as old as it needs. A retention
-- pass is triggered by any applied configuration change
-- (config.rs:1031, `retention_requested`), which is how each test makes
-- the pass happen now rather than at the sixty-minute default interval.
-- Rows from another boot, which no producer can send, are inserted
-- offline: eventd stopped through the service manager, logs.db edited on
-- the host and written back with its -wal and -shm removed, eventd
-- started again.
--
-- Sizes are measured the way the chapter defines them, on a host-side
-- copy of logs.db and its WAL: (page_count - freelist_count) * page_size.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-logretain" })

local DAY = 86400 * 1000000000

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function sql_quote(s) return "'" .. s:gsub("'", "''") .. "'" end

local function count(where)
    return eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE " .. where)[1][1]
end

--- Logical live size, file size, and free pages of logs.db.
local function sizes()
    local r = eventd.sql(vm, eventd.DB.logs, [[
SELECT (SELECT page_count FROM pragma_page_count) - (SELECT freelist_count FROM pragma_freelist_count),
       (SELECT page_count FROM pragma_page_count),
       (SELECT freelist_count FROM pragma_freelist_count),
       (SELECT page_size FROM pragma_page_size)]])[1]
    return r[1] * r[4], r[2] * r[4], r[3]
end

--- Make an applied configuration change (which also requests a retention
--- pass) to a key no test here depends on, alternating its value.
local flip = 0
local function retention_pass()
    flip = flip + 1
    eventd.set(vm, "MetricMaxBatchSize", "dword:" .. (flip % 2 == 0 and 4000 or 4500)):assert_ok()
end

--- Hold LogRetentionDays at 60, and wait until eventd has applied it.
--- Every applied change requests a retention pass, and a pass a previous
--- test's change requested may still be running: lines older than the
--- fourteen-day default must not be sent until no pass will delete them
--- by age. A pass reads its configuration again before every batch, so
--- none does once 60 is applied. Unsetting it again is itself a change
--- that starts a pass.
local function hold_age()
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "LogRetentionDays", "dword:60"):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change
        .. ' WHERE config.name == "LogRetentionDays" AND config.value == 60 SINCE 10m ago', function(rs)
            for _, r in ipairs(rs) do if r["event.time"] >= since then return true end end
            return false
        end)
end

--- Send `n` lines in datagrams of 500, as `make(i)` describes each.
local function send_many(n, make)
    local i = 1
    while i <= n do
        local batch = {}
        for j = i, math.min(n, i + 499) do batch[#batch + 1] = make(j) end
        eventd.send_log(vm, batch)
        i = i + 500
    end
end

local function wal_state()
    local ok, bytes = pcall(vm.read_file, vm, eventd.DB.logs .. "-wal")
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

-- ---------------------------------------------------------------------------
-- Age
-- ---------------------------------------------------------------------------

test("lines older than LogRetentionDays go, fourteen days by default", {
    spec = "eventd *logretain.logs-older-than-logretentiondays-are-deleted"
        .. " eventd *logretain.the-default-log-retention-is-fourteen-days",
}, function(t)
    local m = eventd.marker("age")
    local now = eventd.guest_ns(vm)
    local ages = { d1 = 1, d3 = 3, d13 = 13.9, d15 = 14.1, d40 = 40 }
    local recs = {}
    for name, days in pairs(ages) do
        recs[#recs + 1] = { origin = m, is_error = false, message = name,
            timestamp = math.tointeger(now - math.floor(days * DAY)) }
    end
    eventd.send_log(vm, recs)
    eventd.wait_rows(vm, "LOGS FROM " .. m, function(rs) return #rs == 5 end)
    retention_pass()
    wait_until(function() return count("origin = " .. sql_quote(m)) == 3 end,
        { timeout = 30, interval = 0.5, desc = "the default retention to apply" })
    local left = {}
    for _, r in ipairs(eventd.rows(vm, "LOGS FROM " .. m)) do left[r.message] = true end
    t:assert(left.d1 and left.d3 and left.d13, "lines up to fourteen days old stay by default")
    t:assert(not left.d15 and not left.d40, "lines past fourteen days are gone")
    eventd.set(vm, "LogRetentionDays", "dword:2"):assert_ok()
    wait_until(function() return count("origin = " .. sql_quote(m)) == 1 end,
        { timeout = 30, interval = 0.5, desc = "a two-day retention to apply" })
    eventd.unset(vm, "LogRetentionDays")
    t:assert_eq(eventd.rows(vm, "LOGS FROM " .. m)[1].message, "d1", "at two days, only the one-day line is left")
end)

-- ---------------------------------------------------------------------------
-- Size
-- ---------------------------------------------------------------------------

test("over LogRetentionMaxBytes the oldest lines go, by live size, across boots alike, catalogue included", {
    spec = "eventd *logretain.over-a-non-zero-logretentionmaxbytes-the-oldest-logs-by-timestamp-are-deleted"
        .. " eventd *logretain.log-size-is-logical-live-size-after-a-passive-checkpoint-attempt-excluding-freed-pages"
        .. " eventd *logretain.log-size-retention-has-no-boot-boundary-preference"
        .. " eventd *logretain.both-log-limits-are-enforced-and-the-more-aggressive-one-wins"
        .. " eventd *logretain.vacuum-is-never-run-automatically-on-the-log-store"
        .. " eventd *logs.catalogue-pages-count-toward-logical-live-size",
}, function(t)
    -- 3,000 lines of this boot, one hour apart from ten days ago, each
    -- under an origin of its own (a large catalogue), and 3,000 lines of
    -- another boot interleaved with them in time, inserted offline. A
    -- boot-preferring rule would take the other boot first; the oldest-
    -- first rule takes both from the old end. A few lines are past the age
    -- limit too.
    -- The aged lines are twenty days old: held while everything is
    -- staged, then released before the size limit is set.
    hold_age()
    local m = eventd.marker("sz")
    local now = eventd.guest_ns(vm)
    local base = now - 10 * DAY
    local step = 2 * 60 * 1000000000 -- two minutes
    local body = string.rep("b", 300)
    send_many(3000, function(i)
        return { origin = m .. "o" .. i .. string.rep("x", 40), is_error = false, message = body,
            timestamp = math.tointeger(base + (2 * i) * step) }
    end)
    send_many(5, function(i)
        return { origin = m .. "aged", is_error = false, message = "aged" .. i,
            timestamp = math.tointeger(now - 20 * DAY - i) }
    end)
    wait_until(function() return count("instr(origin, " .. sql_quote(m) .. ") = 1") == 3005 end,
        { timeout = 60, interval = 0.5, desc = "the lines to be stored" })
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.logs, string.format([[
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000)
INSERT INTO logs (boot_id, timestamp, origin, is_error, message, job_id)
SELECT X'00112233445566778899AABBCCDDEEFF', %d + (2 * i + 1) * %d, '%sother', 0, '%s', NULL FROM n;
INSERT OR IGNORE INTO log_origins VALUES ('%sother');
]], base, step, m, body, m))
    eventd.start(vm)
    local live0, file0 = sizes()
    local catalogue = eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) * 60 FROM log_origins WHERE instr(origin, " .. sql_quote(m) .. ") = 1")[1][1]
    -- Aim to remove about a third of the bulk.
    local limit = math.tointeger(live0 - (live0 // 3))
    t:assert(catalogue > 100000, "precondition: the catalogue alone is over 100 KB: " .. catalogue)
    eventd.unset(vm, "LogRetentionDays"):assert_ok()
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
    eventd.set(vm, "LogRetentionMaxBytes", "qword:" .. limit):assert_ok()
    local ok = pcall(wait_until, function()
        return (sizes()) <= limit
    end, { timeout = 180, interval = 1, desc = "size retention to bring the store under the limit" })
    eventd.unset(vm, "LogRetentionMaxBytes")
    eventd.unset(vm, "RetentionDeleteBatchRows")
    local live1, file1, free1 = sizes()
    t:assert(ok, "the live size, catalogue pages included, came under the limit: " .. live1 .. " <= " .. limit)
    t:assert_eq(count("origin = " .. sql_quote(m .. "aged")), 0, "the aged lines went too: both limits applied")
    local mine = count("instr(origin, " .. sql_quote(m) .. ") = 1 AND origin <> " .. sql_quote(m .. "other"))
    local other = count("origin = " .. sql_quote(m .. "other"))
    t:assert(mine > 0 and mine < 3000, "some of this boot's lines went, not all: " .. mine)
    t:assert(other > 0 and other < 3000, "and some of the other boot's, not all: " .. other)
    local oldest_left = eventd.sql(vm, eventd.DB.logs,
        "SELECT min(timestamp) FROM logs WHERE instr(origin, " .. sql_quote(m) .. ") = 1")[1][1]
    local deleted = (3000 - mine) + (3000 - other)
    -- Line k from the old end is at base + (k + 1) * step.
    t:assert_eq(oldest_left, math.tointeger(base + (deleted + 2) * step),
        "exactly the oldest " .. deleted .. " lines went, whichever boot they came from")
    t:assert(file1 >= file0 and free1 > 0,
        "the file did not shrink: the freed pages are on the freelist, not vacuumed away ("
        .. file0 .. " -> " .. file1 .. ", " .. free1 .. " free)")
    t:assert(file1 > limit, "so the file is still over the limit: only live pages were measured")
end)

-- ---------------------------------------------------------------------------
-- How it runs
-- ---------------------------------------------------------------------------

test("each retention transaction deletes at most RetentionDeleteBatchRows", {
    spec = "eventd *logretain.each-log-retention-transaction-deletes-at-most-retentiondeletebatchrows-then-ingestion-is-rechecked",
}, function(t)
    -- 1,000 expired lines and a batch of 100: at least ten commits in the
    -- WAL. The pass checkpoints afterwards, but a checkpointed WAL keeps
    -- its frames until the next write restarts it, so it is read as soon
    -- as the rows are gone; a measurement the restart got to first is
    -- taken again.
    --
    -- The lines are thirty days old, so no pass may run under the default
    -- fourteen days while they arrive: every applied change requests one.
    -- LogRetentionDays is raised past their age first, the passes that
    -- requested are left to finish, and only then is it lowered again —
    -- the one change that starts the pass being measured.
    local got
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
    for _ = 1, 3 do
        eventd.set(vm, "LogRetentionDays", "dword:60"):assert_ok()
        -- An idle log thread takes a maintenance command a second at most
        -- (log_ingest.rs:92), so a pass with nothing to delete is a few
        -- seconds of them.
        vm:clock():sleep("10s")
        local m = eventd.marker("bt")
        local old = eventd.guest_ns(vm) - 30 * DAY
        send_many(1000, function(i)
            return { origin = m, is_error = false, message = "x", timestamp = math.tointeger(old + i) }
        end)
        wait_until(function() return count("origin = " .. sql_quote(m)) == 1000 end,
            { timeout = 30, interval = 0.25, desc = "the lines to be stored" })
        vm:clock():sleep("1s")
        local c0, s0 = wal_state()
        eventd.unset(vm, "LogRetentionDays"):assert_ok()
        wait_until(function() return count("origin = " .. sql_quote(m)) == 0 end,
            { timeout = 60, interval = 0.1, desc = "the lines to be deleted" })
        local c1, s1 = wal_state()
        if s0 ~= nil and s1 == s0 then got = c1 - c0; break end
    end
    eventd.unset(vm, "LogRetentionDays")
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert(got, "a measurement completed without the WAL restarting")
    t:assert(got and got >= 10, "1,000 rows at 100 per transaction took at least ten commits: " .. tostring(got))
end)

test("retention plans read-only and deletes through the log writer, never a second writer", {
    spec = "eventd *logretain.log-retention-plans-read-only-and-submits-low-priority-commands-to-the-log-writer"
        .. " eventd *logretain.log-retention-takes-no-writer-mutex-and-opens-no-second-read-write-connection",
}, function(t)
    -- The lines are thirty days old: held while they are staged, and
    -- lowering the limit again is the change that starts the pass watched.
    hold_age()
    local m = eventd.marker("rw")
    local old = eventd.guest_ns(vm) - 30 * DAY
    send_many(5000, function(i)
        return { origin = m, is_error = false, message = "x", timestamp = math.tointeger(old + i) }
    end)
    wait_until(function() return count("origin = " .. sql_quote(m)) == 5000 end,
        { timeout = 30, interval = 0.25, desc = "the lines to be stored" })
    local pid = eventd.pid(vm)
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
    eventd.unset(vm, "LogRetentionDays"):assert_ok()
    -- Fifty delete commands of 100 rows, each a low-priority command the
    -- log writer takes between its own transactions.
    local most, samples = 0, 0
    local deadline = os.time() + 150
    repeat
        most = math.max(most, (eventd.fds_on(vm, pid, eventd.DB.logs)))
        samples = samples + 1
    until count("origin = " .. sql_quote(m)) == 0 or os.time() > deadline
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert_eq(count("origin = " .. sql_quote(m)), 0, "the pass deleted the lines")
    t:assert_eq(most, 1, "and logs.db never had more than its one read-write connection, over "
        .. samples .. " samples")
    local threads = select(2, eventd.thread_names(vm, pid))
    t:assert(threads:find("eventd-retentio", 1, true), "retention runs on a thread of its own")
end)

-- Permitted, not required, and not done: eventd has no urgent path
-- (retention.rs never touches an ingestion transaction; nothing in the
-- tree joins a delete to one), so there is nothing to observe.
test("under urgent size pressure one bounded delete may join an open transaction", {
    spec = "eventd *logretain.under-urgent-size-pressure-one-bounded-delete-may-join-an-open-transaction",
    skip = true,
}, function() end)

-- Route closed: a pass deletes from all three stores within milliseconds
-- and leaves nothing timestamped, so which store went first is not
-- visible from outside. The unit test runs one pass against stub writers
-- that record the order commands reach them.
test("log retention runs on the retention thread after events and before metrics", {
    spec = "eventd *logretain.log-retention-runs-on-the-retention-thread-after-events-and-before-metrics",
    skip = true,
    covered_by = "cargo:eventd eventd retention::tests::a_pass_processes_events_then_logs_then_metrics",
}, function() end)
