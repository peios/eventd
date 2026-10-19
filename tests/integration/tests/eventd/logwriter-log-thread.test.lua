-- eventd TRM §4.1 — the log writer: the one thread that reads the log
-- socket and writes logs.db, how it batches, what it adds to a record,
-- and what it does with a record whose origin is outside the grammar.
--
-- One file-scope VM. Three instruments do most of the work:
--
--   * Transaction boundaries are read from logs.db's write-ahead log on
--     the host. Every commit ends in a frame whose "database size" field
--     is non-zero, and a frame belongs to the current log while its salt
--     matches the header's, so counting such frames before and after a
--     send counts the commits the send caused. A checkpoint lets the next
--     write restart the WAL under a new salt; a measurement that sees the
--     salt change is simply taken again.
--   * A burst is staged by stopping eventd's process (SIGSTOP), queueing
--     datagrams on the log socket with MSG_DONTWAIT — one sending socket
--     each, because an AF_UNIX datagram is charged to its sender's send
--     buffer until it is read — and letting it go (SIGCONT). That is also
--     the stand-in for "eventd is not draining the socket", which no
--     outside observer can otherwise hold open for long enough.
--   * The rollback test points LogStorePath at a small tmpfs given the
--     store directory's required descriptor, fills it, and lets a commit
--     fail with SQLITE_FULL.
--
-- Order matters in three places: the stderr rate-limit test runs before
-- any other test sends a bad origin; the crash test and the diagnostic
-- dump (SIGQUIT, after which eventd exits cleanly and is started again by
-- hand) run last.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local unixsock = require("helpers.unixsock")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-logwriter" })

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local EAGAIN = 11

local function sql_quote(s) return "'" .. s:gsub("'", "''") .. "'" end

--- Commit frames in the current generation of logs.db's WAL, and its salt.
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

--- Commits logs.db took while `fn` ran. `fn` must be repeatable: it is
--- run again whenever a checkpoint restarted the WAL in between.
local function commits_during(fn)
    for _ = 1, 5 do
        local c0, s0 = wal_state()
        fn()
        local c1, s1 = wal_state()
        if s0 ~= nil and s0 == s1 then return c1 - c0 end
    end
    error("the logs WAL restarted under every measurement")
end

--- After a configuration change the retention pass it triggers
--- checkpoints logs.db, and the WAL restarts on the next write. Make that
--- write now, so a measurement does not straddle it.
local function settle()
    vm:clock():sleep("2s")
    local origin = eventd.marker("settle")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "settle" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
end

--- Send `records` (one datagram) and wait until all are stored.
local function send_and_wait(origin, records)
    eventd.send_log(vm, records)
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " TAKE 1000000", function(rs) return #rs == #records end,
        { timeout = 60 })
end

local function records(origin, n)
    local out = {}
    for i = 1, n do out[i] = { origin = origin, is_error = false, message = "" } end
    return out
end

--- Queue `n` datagrams of `per` records while eventd is stopped, then let
--- it drain them in one go. Returns the origin used.
local function burst(n, per)
    local origin = eventd.marker("burst")
    local data = eventd.msgpack(records(origin, per))
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    for _ = 1, n do
        local r = eventd.sendto(vm, nil, data, eventd.SOCKET.log, eventd.DONTWAIT)
        assert(r.ret == #data, "burst datagram queued (errno " .. tostring(r.errno) .. ")")
    end
    eventd.thaw(vm, pid)
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " TAKE 1000000",
        function(rs) return #rs == n * per end, { timeout = 60 })
    return origin
end

local function eventd_lines(since_ns)
    local out = {}
    for _, r in ipairs(eventd.rows(vm, "LOGS FROM eventd SINCE 1h ago TAKE 100000")) do
        if r.timestamp >= since_ns then out[#out + 1] = r end
    end
    table.sort(out, function(a, b) return a.timestamp < b.timestamp end)
    return out
end

-- ---------------------------------------------------------------------------
-- Rejected origins and the stderr report (first: nothing before it may
-- have sent a bad origin to this process)
-- ---------------------------------------------------------------------------

test("a discarded origin is reported on stderr at once, then at most once a minute, as a log under eventd", {
    spec = "eventd *logwriter.a-discard-is-reported-on-stderr-at-the-first-occurrence-then-at-most-once-a-minute"
        .. " eventd *logwriter.the-stderr-report-is-stored-as-a-log-record-under-the-origin-eventd",
}, function(t)
    local from = eventd.guest_ns(vm)
    local function reports(m)
        local n = 0
        for _, r in ipairs(eventd_lines(from)) do
            if r.message:find("discarding log records", 1, true) and r.message:find(m, 1, true) then n = n + 1 end
        end
        return n
    end
    local a, b, c = eventd.marker("ra") .. "/x/y", eventd.marker("rb") .. "/x/y", eventd.marker("rc") .. "/x/y"
    eventd.send_log(vm, { origin = a, is_error = false, message = "x" })
    local first = pcall(wait_until, function() return reports(a) == 1 end,
        { timeout = 20, interval = 0.5, desc = "the first report" })
    t:assert(first, "the first discard was reported, and the report is stored under origin eventd")
    eventd.send_log(vm, { origin = b, is_error = false, message = "x" })
    vm:clock():sleep("5s")
    t:assert_eq(reports(b), 0, "a second discard within the minute is not reported")
    vm:clock():sleep("57s")
    eventd.send_log(vm, { origin = c, is_error = false, message = "x" })
    local again = pcall(wait_until, function() return reports(c) == 1 end,
        { timeout = 20, interval = 0.5, desc = "the next report" })
    t:assert(again, "after a minute the next discard is reported again")
end)

test("a record with a bad origin is dropped where it is parsed and does not spoil its batch", {
    spec = "eventd *logwriter.a-record-with-an-invalid-origin-is-discarded-before-it-joins-a-batch",
}, function(t)
    local good = eventd.marker("ok")
    eventd.send_log(vm, {
        { origin = good, is_error = false, message = "one" },
        { origin = good .. "/a/b", is_error = false, message = "bad" },
        { origin = good, is_error = false, message = "two" },
    })
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. good .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    t:assert_eq(#rows, 2, "the two good records of the datagram were committed")
    local bad = eventd.sql(vm, eventd.DB.logs, "SELECT count(*) FROM logs WHERE origin = " .. sql_quote(good .. "/a/b"))
    t:assert_eq(bad[1][1], 0, "the bad one never reached the store")
end)

-- ---------------------------------------------------------------------------
-- One thread
-- ---------------------------------------------------------------------------

test("one log thread, separate from the event drain and writer threads", {
    spec = "eventd *logwriter.the-log-thread-is-independent-of-the-event-drain-and-writer-threads"
        .. " eventd *logwriter.one-thread-does-both-the-socket-reads-and-the-sqlite-writes",
}, function(t)
    local pid = eventd.pid(vm)
    local names = eventd.thread_names(vm, pid)
    local log, writers, drains = {}, 0, 0
    for _, n in ipairs(names) do
        if n:find("log", 1, true) then log[#log + 1] = n end
        if n:match("^eventd%-writer") then writers = writers + 1 end
        if n:match("^eventd%-drain") then drains = drains + 1 end
    end
    t:assert_eq(table.concat(log, ","), "eventd-log", "one thread for logs, and only one: " .. table.concat(names, ","))
    t:assert(writers >= 1 and drains >= 1, "beside the event writers and drains, which are threads of their own")
    -- And logs.db has exactly one read-write connection, which by the
    -- above can only be that thread's.
    local n = (eventd.fds_on(vm, pid, eventd.DB.logs))
    t:assert_eq(n, 1, "one read-write descriptor on logs.db")
end)

test("the log socket's receive buffer is four times the datagram ceiling", {
    spec = "eventd *logwriter.so-rcvbuf-is-sized-at-four-times-the-datagram-ceiling",
}, function(t)
    local line = vm:run("ss -xam").stdout:match("[^\n]*/run/eventd/log%.sock[^\n]*\n?[^\n]*")
    t:assert(line, "ss shows the log socket")
    local rb = tonumber(line and line:match("rb(%d+)"))
    -- MaxLogDatagramBytes is unset, so the ceiling is its 256 KiB default.
    t:assert_eq(rb, 4 * 256 * 1024, "SO_RCVBUF is 4 x 256 KiB")
end)

-- With eventd stopped the socket is not drained. A Unix datagram socket's
-- queue is counted in datagrams (net.unix.max_dgram_qlen; the kernel's
-- full test is "more than qlen queued", so qlen + 1 fit), and once it is
-- full a non-blocking send is refused with EAGAIN: nothing accepted is
-- discarded.
test("while the socket is not drained, its max_dgram_qlen queue fills and the kernel then refuses sends", {
    spec = "eventd *logwriter.the-socket-is-not-drained-during-a-batch-commit"
        .. " eventd *logwriter.a-full-receive-queue-of-max-dgram-qlen-datagrams-refuses-further-sends",
}, function(t)
    local qlen = tonumber(vm:read_file("/proc/sys/net/unix/max_dgram_qlen"):match("%d+"))
    local origin = eventd.marker("q")
    local fd = unixsock.socket(vm, unixsock.AF_UNIX, unixsock.SOCK.DGRAM)
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    local accepted, refused = 0, 0
    local body = string.rep("x", 1000)
    for i = 1, 300 do
        local data = eventd.msgpack({ origin = origin, is_error = false, message = body .. i })
        local r = eventd.sendto(vm, fd, data, eventd.SOCKET.log, eventd.DONTWAIT)
        if r.ret == #data then accepted = accepted + 1 elseif r.errno == EAGAIN then refused = refused + 1 end
    end
    eventd.thaw(vm, pid)
    vm:syscall(3, fd)
    local stored = #eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
        function(rs) return #rs >= accepted end, { timeout = 20 })
    t:assert(accepted >= 1 and accepted <= qlen + 1,
        "the queue holds at most max_dgram_qlen (" .. qlen .. ") + 1 datagrams, not a byte budget: it took "
        .. accepted .. " before refusing " .. refused)
    t:assert_eq(accepted + refused, 300, "every other send was refused to the sender with EAGAIN")
    t:assert_eq(stored, accepted, "and nothing the queue accepted was discarded: stored " .. stored
        .. " of " .. accepted)
end)

-- ---------------------------------------------------------------------------
-- Batching
-- ---------------------------------------------------------------------------

test("a batch commits as soon as no further datagram is waiting", {
    spec = "eventd *logwriter.a-batch-commits-when-no-further-datagram-is-immediately-available"
        .. " eventd *logwriter.a-transaction-opens-at-the-first-valid-record-and-commits-on-any-of-three-conditions",
}, function(t)
    -- The latency cap at its maximum, five seconds: a lone record that is
    -- visible well before then was committed because the queue was empty.
    eventd.set(vm, "LogMaxBatchLatencyMs", "dword:5000"):assert_ok()
    settle()
    local n = commits_during(function()
        local origin = eventd.marker("e")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "alone" })
        eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
            function(rs) return #rs == 1 end, { timeout = 4, interval = 0.05 })
    end)
    eventd.unset(vm, "LogMaxBatchLatencyMs")
    t:assert(n >= 1, "the record was committed inside the five-second latency cap")
end)

test("a batch commits at LogMaxBatchSize, and an overflowing datagram continues in a new transaction", {
    spec = "eventd *logwriter.a-batch-commits-when-it-holds-logmaxbatchsize-records"
        .. " eventd *logwriter.a-datagram-that-overflows-the-batch-continues-in-a-new-transaction"
        .. " eventd *logwriter.a-transaction-never-exceeds-the-size-cap-or-outlives-the-latency-cap",
}, function(t)
    eventd.set(vm, "LogMaxBatchSize", "dword:100"):assert_ok()
    settle()
    local n = commits_during(function()
        local origin = eventd.marker("s")
        send_and_wait(origin, records(origin, 250))
    end)
    eventd.unset(vm, "LogMaxBatchSize")
    t:assert(n >= 3, "250 records from one datagram at a cap of 100 took at least three commits: " .. n)
end)

-- The next two tests write tens of thousands of rows. Once logs.db's WAL
-- file has grown past WalCheckpointPages the writer checkpoints after
-- every commit (see logdb-lifecycle), which would restart the WAL under
-- each measurement; the threshold is raised to its maximum around them.

test("the batch size defaults to 5000 records", {
    spec = "eventd *logwriter.the-batch-defaults-are-5000-records-and-500-milliseconds",
}, function(t)
    -- The 500 ms latency default is not separable from outside: a burst
    -- that would outlast it cannot be queued (ten datagrams drain in less
    -- than that). The size default is: 4,999 records in one datagram are
    -- one commit, 5,001 are two. Background output can only add commits,
    -- so each is measured up to three times and the least taken.
    eventd.set(vm, "WalCheckpointPages", "dword:100000"):assert_ok()
    settle()
    local function least(n, want)
        local best
        for _ = 1, 3 do
            local c = commits_during(function()
                local origin = eventd.marker("d")
                send_and_wait(origin, records(origin, n))
            end)
            best = best and math.min(best, c) or c
            if best <= want then break end
        end
        return best
    end
    local one, two = least(4999, 1), least(5001, 2)
    eventd.unset(vm, "WalCheckpointPages")
    t:assert_eq(one, 1, "4,999 records: one transaction")
    t:assert_eq(two, 2, "5,001 records: two")
end)

test("a batch commits once LogMaxBatchLatencyMs has passed since its first record", {
    spec = "eventd *logwriter.a-batch-commits-when-logmaxbatchlatencyms-has-elapsed-since-its-first-record",
}, function(t)
    -- The size cap at its maximum, so only the queue emptying or the
    -- latency cap can end a batch. Ten queued datagrams of 4,000 records
    -- are drained without the queue ever being empty: under a long cap
    -- they go in one commit, under a 10 ms cap in more than one.
    eventd.set(vm, "WalCheckpointPages", "dword:100000"):assert_ok()
    eventd.set(vm, "LogMaxBatchSize", "dword:100000"):assert_ok()
    eventd.set(vm, "LogMaxBatchLatencyMs", "dword:5000"):assert_ok()
    settle()
    local long
    for _ = 1, 3 do
        long = commits_during(function() burst(10, 4000) end)
        if long == 1 then break end
    end
    eventd.set(vm, "LogMaxBatchLatencyMs", "dword:10"):assert_ok()
    settle()
    local short = commits_during(function() burst(10, 4000) end)
    eventd.unset(vm, "LogMaxBatchLatencyMs")
    eventd.unset(vm, "LogMaxBatchSize")
    eventd.unset(vm, "WalCheckpointPages")
    t:assert_eq(long, 1, "under a five-second cap the burst is one transaction")
    t:assert(short >= 2, "under a 10 ms cap the same burst is split: " .. short .. " commits")
end)

-- ---------------------------------------------------------------------------
-- The origin catalogue, from the writer's side
-- ---------------------------------------------------------------------------

test("a new origin is catalogued in the same commit as its first row", {
    spec = "eventd *logwriter.a-new-origin-is-inserted-into-log-origins-in-the-same-transaction-as-its-first-row",
}, function(t)
    for i = 1, 5 do
        local origin = eventd.marker("nw")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "x" })
        local snap
        wait_until(function()
            snap = eventd.sql(vm, eventd.DB.logs, "SELECT (SELECT count(*) FROM logs WHERE origin = "
                .. sql_quote(origin) .. "), (SELECT count(*) FROM log_origins WHERE origin = "
                .. sql_quote(origin) .. ")")
            return snap[1][1] > 0
        end, { timeout = 20, interval = 0.05, desc = "the first row to commit" })
        t:assert_eq(snap[1][2], 1, "no committed state holds the row without its catalogue entry (" .. i .. ")")
    end
end)

test("the writer works from its own set of known origins, loaded from the catalogue", {
    spec = "eventd *logwriter.the-writer-keeps-the-known-origin-names-in-memory",
}, function(t)
    -- Take an origin out of the catalogue while its rows stay. The writer
    -- that starts next loads its known set from log_origins, so the origin
    -- is new to it and its next line puts it back. A writer that decided
    -- "new" by looking at the logs table would not.
    local origin = eventd.marker("mem")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "1" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.logs, "DELETE FROM log_origins WHERE origin = " .. sql_quote(origin) .. ";")
    eventd.start(vm)
    eventd.send_log(vm, { origin = origin, is_error = false, message = "2" })
    local ok = pcall(wait_until, function()
        return #eventd.sql(vm, eventd.DB.logs, "SELECT 1 FROM log_origins WHERE origin = " .. sql_quote(origin)) == 1
    end, { timeout = 20, interval = 0.25, desc = "the origin to be catalogued again" })
    t:assert(ok, "the writer saw the origin as unknown and catalogued it again")
end)

-- No VM route: the catalogue insert is INSERT OR IGNORE, so running it once
-- or once per row of a batch leaves the same committed state. Only the
-- statement count differs, and nothing outside the process sees that.
test("the catalogue insert runs once per new origin per batch", {
    spec = "eventd *logwriter.the-origin-insert-runs-once-per-new-origin-per-batch",
    skip = true,
    covered_by = "cargo:eventd eventd-core log_store::tests::the_origin_insert_runs_once_per_new_origin_per_batch",
}, function() end)

-- ---------------------------------------------------------------------------
-- Adding to the record
-- ---------------------------------------------------------------------------

test("eventd adds the boot ID and, when absent, the receipt time; the rest is stored as given", {
    spec = "eventd *logwriter.eventd-supplies-the-boot-id-and-a-receipt-timestamp-when-none-was-given"
        .. " eventd *logwriter.everything-else-is-stored-as-given-with-message-byte-for-byte",
}, function(t)
    local origin = eventd.marker("as")
    local message = "  tabs\there, CRLF\r\n, quote \" back \\ snowman \u{2603} nul \0 end  "
    local job = "\1\2\3\4\5\6\7\8\9\10\11\12\13\14\15\16"
    local before = eventd.guest_ns(vm)
    eventd.send_log(vm, { origin = origin, is_error = true, message = message, job_id = eventd.bin(job) })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local after = eventd.guest_ns(vm)
    local r = eventd.sql(vm, eventd.DB.logs,
        "SELECT hex(boot_id), timestamp, origin, is_error, hex(CAST(message AS BLOB)), hex(job_id) FROM logs WHERE origin = "
        .. sql_quote(origin))[1]
    t:assert_eq(r[1], eventd.boot_pcds_hex(vm), "eventd supplied this boot's ID")
    t:assert(r[2] >= before and r[2] <= after, "and its own clock at receipt: " .. r[2])
    t:assert_eq(r[3], origin, "origin as given")
    t:assert_eq(r[4], 1, "is_error as given")
    t:assert_eq(r[5], eventd.hex(message, true), "message byte for byte")
    t:assert_eq(r[6], eventd.hex(job, true), "job_id as given")
end)

-- ---------------------------------------------------------------------------
-- Durability
-- ---------------------------------------------------------------------------

test("the log store is in WAL mode", {
    spec = "eventd *logwriter.the-log-store-runs-in-wal-mode-with-synchronous-normal",
}, function(t)
    -- synchronous=NORMAL is per connection and leaves no mark in the file;
    -- WAL mode is in the header (bytes 18 and 19 are 2), and the -wal file
    -- beside the database is the log itself.
    local header = vm:read_file(eventd.DB.logs)
    t:assert_eq(header:byte(19) .. "," .. header:byte(20), "2,2", "logs.db is in WAL mode")
    t:assert(vm:stat(eventd.DB.logs .. "-wal"), "with its write-ahead log beside it")
end)

test("an origin whose first row was rolled back is still catalogued by its next row", {
    spec = "eventd *logwriter.the-origin-cache-is-updated-only-after-commit-and-pending-origins-are-discarded-on-rollback",
}, function(t)
    -- A log store on a 512 KiB tmpfs with the store directory's required
    -- descriptor, filled so the first commit for a new origin fails with
    -- SQLITE_FULL. If the writer had added the origin to its known set
    -- before the commit, the line sent after space is freed would go in
    -- without a catalogue entry, and no query could find it.
    local dir = "/run/pt-logstore"
    local saved = vm:run("reg get '" .. eventd.KEY .. "' LogStorePath").stdout:gsub("%s+$", "")
    vm:run("mkdir -p " .. dir .. " && mount -t tmpfs -o size=512k tmpfs " .. dir):assert_ok()
    vm:run("sd set " .. dir .. " 'O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
        .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)'"):assert_ok()
    eventd.set(vm, "LogStorePath", "sz:" .. dir):assert_ok()
    eventd.restart(vm)
    local db = dir .. "/logs.db"
    local ok, err = pcall(function()
        t:assert(vm:stat(db), "eventd opened its log store on the small filesystem")
        vm:run("dd if=/dev/zero of=" .. dir .. "/filler bs=4096 2>/dev/null; sync")
        local origin = eventd.marker("full")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "lost" })
        vm:clock():sleep("3s")
        vm:run("rm -f " .. dir .. "/filler"):assert_ok()
        local lost = eventd.query(vm, "LOGS FROM " .. origin .. " SINCE 10m ago")
        t:assert(lost.ok and #lost.rows == 0, "precondition: the first line was lost with its failed batch")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "kept" })
        local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
            function(rs) return #rs == 1 end, { timeout = 30 })
        t:assert_eq(#rows, 1, "the next line committed with its origin catalogued, so a query finds it")
    end)
    eventd.set(vm, "LogStorePath", "sz:" .. saved):assert_ok()
    eventd.restart(vm)
    vm:run("umount " .. dir)
    if not ok then error(err, 0) end
end)

test("a committed log line survives eventd being killed", {
    spec = "eventd *logwriter.log-commits-survive-a-process-crash-but-not-necessarily-a-power-loss",
}, function(t)
    -- The crash half. Power loss is "not necessarily": nothing to assert.
    local origin = eventd.marker("crash")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "before the crash" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local pid = eventd.pid(vm)
    eventd.signal(vm, pid, "KILL")
    wait_until(function()
        local now = eventd.pid(vm)
        return now ~= nil and now ~= pid
    end, { timeout = 60, interval = 0.5, desc = "peinit to restart eventd" })
    eventd.ready(vm)
    local rows = eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago")
    t:assert_eq(#rows, 1, "the line is still there after the crash")
end)

-- ---------------------------------------------------------------------------
-- The diagnostic dump (last: SIGQUIT ends the process)
-- ---------------------------------------------------------------------------

test("discards are counted and the last bad origin is kept escaped and cut to 64 characters", {
    spec = "eventd *logwriter.discards-are-counted-and-the-last-rejected-origin-is-kept-escaped-and-truncated-to-64-characters",
}, function(t)
    eventd.restart(vm) -- fresh counters
    local last = "\"" .. string.rep("y", 100) .. "\n"
    eventd.send_log(vm, {
        { origin = eventd.marker("d1") .. "/a/b", is_error = false, message = "x" },
        { origin = eventd.marker("d2") .. " x", is_error = false, message = "x" },
        { origin = last, is_error = false, message = "x" },
    })
    vm:clock():sleep("2s")
    local d = eventd.quit_dump(vm, { timeout = 60 })
    local count, shown
    for _, m in ipairs(d.messages) do
        count = count or tonumber(m:match("log_ingress: rejected_origins=(%d+)"))
        shown = shown or m:match("last_rejected_log_origin: \"(.*)\"$")
    end
    t:assert_eq(count, 3, "the dump counts the three discards")
    local want = ("\\\"" .. string.rep("y", 100)):sub(1, 64) .. "…"
    t:assert_eq(shown, want, "and shows the last origin escaped, cut to 64 characters")
end)
