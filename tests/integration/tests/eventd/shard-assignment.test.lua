-- eventd TRM §2.3 — sharding: how many event shards there are, which CPU
-- writes to which, the writer threads, and what happens when the count
-- changes. Also the two §2.1 statements about shards and the §2.2 restart
-- statements that need more than one shard per CPU to see.
--
-- Two vCPUs: the assignment is a function of the attached-CPU count, and
-- with one CPU every shard count gives the same picture. Events are
-- emitted from a worker pinned to a chosen CPU.
--
-- One file-scope VM, booted with `StorageShards = 3` seeded, so the first
-- half of the file sees the "more shards than CPUs" case (CPU 0 owns shards
-- 0 and 2, CPU 1 owns shard 1). The rest of the file walks the count
-- through restarts, in order: a deleted middle stripe (restart recovery),
-- 1 (fewer shards than CPUs, and the earlier shards kept), the default
-- (one per CPU), then the range limit (257). 256 shards need eventd's
-- descriptor limit raised (§2.3), and are tried last, each on a one-vCPU
-- machine of its own seeded StorageShards=256 and made Normal /
-- RestartPolicy Never: once under the limit eventd inherits, once with
-- LimitNOFILE seeded into its service definition.
--
-- Which shard holds a CPU's events is read straight from the shard files
-- (the host-side sqlite copy); the routing itself has no other window.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local sys = require("helpers.sys")
peinit.claim(2, { cpus = 2 }) -- the file's machine, and one 256-shard machine at a time

local SCHED_OTHER, SCHED_FIFO, SCHED_IDLE = 0, 1, 5

local vm = eventd.boot({
    name = "ev-shard",
    cpus = 2,
    config = { { name = "StorageShards", type = "dword", data = 3 } },
})

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function pin(who, cpu)
    return who:syscall(sys.NR.sched_setaffinity, {
        args = { 0, 8, 0 }, bufs = { string.pack("<I8", 1 << cpu) }, ptrs = { 2 },
    }).ret == 0
end

--- Emit `n` events of `event_type` from a worker pinned to `cpu`.
local function emit_on(cpu, event_type, n)
    local worker = vm:spawn_worker()
    local ok, err = pcall(function()
        assert(pin(worker, cpu), "pin to cpu " .. cpu)
        local sent = 0
        while sent < n do
            local k = math.min(256, n - sent)
            local batch = {}
            for i = 1, k do batch[i] = { type = event_type, payload = kmes.PAYLOAD } end
            local r = kmes.emit_batch(worker, batch)
            assert(r.ret == 0 and r.emitted == k, "emit_batch on cpu " .. cpu)
            sent = sent + k
        end
    end)
    worker:kill(); worker:join()
    if not ok then error(err, 0) end
end

local function shard_name(path) return path:match("(shard%-%d+)%.db$") end

--- {shard name = count} of rows of one event type, and the total.
local function where(event_type)
    local out, total = {}, 0
    for _, shard in ipairs(eventd.shards(vm)) do
        local r = eventd.sql(vm, shard, string.format(
            "SELECT count(*), min(cpu_id), max(cpu_id) FROM events WHERE event_type = '%s'", event_type))
        if r[1][1] > 0 then
            out[shard_name(shard)] = r[1][1]
            total = total + r[1][1]
        end
    end
    return out, total
end

local function wait_where(event_type, n)
    local out, total
    wait_until(function()
        out, total = where(event_type)
        return total >= n
    end, { timeout = 60, interval = 0.5, desc = n .. " rows of " .. event_type })
    return out, total
end

local function keys(t)
    local out = {}
    for k in pairs(t) do out[#out + 1] = k end
    table.sort(out)
    return json.encode(out)
end

--- The shard holding each stored sequence of one CPU: {seq = shard name}.
local function cpu_sequences(cpu)
    local out = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard, string.format(
            "SELECT sequence FROM events WHERE cpu_id = %d AND sequence IS NOT NULL", cpu))) do
            out[r[1]] = out[r[1]] and (out[r[1]] .. "," .. shard_name(shard)) or shard_name(shard)
        end
    end
    return out
end

--- Writer threads in creation order, which is shard order (pipeline.rs
--- spawns them in index order; comm is truncated, so all read the same).
local function writer_tids()
    local out = {}
    for _, th in ipairs(eventd.threads(vm)) do
        if th.comm:find("^eventd%-writer") then out[#out + 1] = th.tid end
    end
    return out
end

--- Every start's "starting/restarting with N KMES buffer(s), M active
--- shard(s)" line, newest first. eventd's stderr reaches the log store
--- through peinit a little after the fact.
local function startup_lines()
    local out = {}
    for _, l in ipairs(eventd.rows(vm, "LOGS FROM eventd SINCE 1h ago TAKE 500")) do
        if l.message:find("KMES buffer") then out[#out + 1] = l.message end
    end
    return out
end

--- The line of the start `fn` causes: waits until one more start than
--- before has been logged, and returns the newest. Without `fn`, the
--- newest start already logged.
local function startup_line(fn)
    local before = fn and #startup_lines() or 0
    if fn then fn() end
    local lines
    wait_until(function()
        lines = startup_lines()
        return #lines > before
    end, { timeout = 30, interval = 0.5, desc = "the start's log line" })
    return lines[1]
end

local function restart_with(value)
    if value == nil then
        eventd.unset(vm, "StorageShards")
    else
        eventd.set(vm, "StorageShards", "dword:" .. value):assert_ok()
    end
    eventd.restart(vm)
end

-- ---------------------------------------------------------------------------
-- Three shards on two CPUs
-- ---------------------------------------------------------------------------

test("three shards on two CPUs: three databases, each with its own WAL and its own writer", {
    spec = "eventd *shard.neither-a-power-of-two-nor-a-multiple-of-the-buffer-count-is-enforced"
        .. " eventd *shard.shard-databases-are-created-on-first-use"
        .. " eventd *shard.each-shard-has-exactly-one-writer-thread-and-no-other-writer"
        .. " eventd *pipeline.each-shard-has-its-own-file-wal-and-writer-thread",
}, function(t)
    local line = startup_line()
    t:assert(line:find("2 KMES buffer(s), 3 active shard(s)", 1, true),
        "3 is used as given on 2 CPUs: " .. line)

    -- The store directory is supplied empty (the package's empty protected
    -- directories); these files are eventd's. eventd opens every active
    -- shard when it starts (pipeline.rs:82-93), which is its first use.
    local names = {}
    for _, s in ipairs(eventd.shards(vm)) do names[#names + 1] = shard_name(s) end
    t:assert_eq(json.encode(names), '["shard-0000","shard-0001","shard-0002"]',
        "eventd created one database per shard in the event store")

    local pid = eventd.pid(vm)
    local fds = eventd.fds(vm, pid)
    for i = 0, 2 do
        local db = string.format("/var/state/eventd/events/shard-%04d.db", i)
        local rw = #eventd.fd_numbers(fds, db, 2)
        t:assert_eq(rw, 1, "shard " .. i .. " has exactly one read-write connection")
        t:assert(#eventd.fd_numbers(fds, db .. "-wal") > 0, "and its own write-ahead log")
    end
    t:assert_eq(#writer_tids(), 3, "and there is one writer thread per shard")
end)

test("CPU 0 owns shards 0 and 2 in fixed-length stripes, CPU 1 owns shard 1", {
    spec = "eventd *shard.ordinal-c-is-assigned-every-shard-j-where-j-mod-attached-count-equals-c"
        .. " eventd *shard.more-shards-than-cpus-means-a-cpu-owns-several"
        .. " eventd *shard.a-multi-shard-drain-thread-sends-fixed-length-contiguous-stripes-to-its-shards-in-turn"
        .. " eventd *shard.uneven-counts-leave-an-imbalance-of-at-most-one-shard-or-cpu"
        .. " eventd *shard.the-logical-cpu-id-not-the-ordinal-is-stored"
        .. " eventd *shard.assignment-is-computed-once-at-startup-and-fixed-for-the-process-lifetime"
        .. " eventd *shard.drain-threads-never-write-to-sqlite",
}, function(t)
    local e0 = "pt.shard." .. eventd.marker("c0")
    local e1 = "pt.shard." .. eventd.marker("c1")
    emit_on(0, e0, 2500)
    emit_on(1, e1, 300)
    local at0 = wait_where(e0, 2500)
    local at1 = wait_where(e1, 300)

    t:assert_eq(keys(at1), '["shard-0001"]', "CPU 1 (ordinal 1) wrote only to shard 1")
    t:assert_eq(keys(at0), '["shard-0000","shard-0002"]',
        "CPU 0 (ordinal 0) wrote to shards 0 and 2, every j with j % 2 == 0")
    t:assert_eq(json.encode(eventd.sql(vm, eventd.shards(vm)[2],
        string.format("SELECT DISTINCT cpu_id FROM events WHERE event_type = '%s'", e1))), "[[1]]",
        "rows carry the logical CPU id they were emitted on")

    -- CPU 0's whole sequence space, in runs of one shard at a time.
    local seqs = cpu_sequences(0)
    local max = 0
    for s in pairs(seqs) do if s > max then max = s end end
    local runs, current, len = {}, nil, 0
    for s = 1, max do
        local sh = seqs[s]
        t:assert(sh and not sh:find(","), "CPU 0 sequence " .. s .. " is in exactly one shard: " .. tostring(sh))
        if sh ~= current then
            if current then runs[#runs + 1] = { shard = current, len = len } end
            current, len = sh, 0
        end
        len = len + 1
    end
    runs[#runs + 1] = { shard = current, len = len }
    t:assert(#runs >= 3, "CPU 0's events span at least three stripes: " .. json.encode(runs))
    local stripe = runs[1].len
    for i, r in ipairs(runs) do
        local want = (i % 2 == 1) and "shard-0000" or "shard-0002"
        t:assert_eq(r.shard, want, "stripe " .. i .. " goes to the next of CPU 0's shards in turn")
        if i < #runs then
            t:assert_eq(r.len, stripe, "every complete stripe has the same length: " .. json.encode(runs))
        else
            t:assert(r.len <= stripe, "and the current one is not longer")
        end
    end

    -- Two shards for one CPU, one for the other: one shard's difference.
    local n0, n1 = 0, 0
    for _ in pairs(at0) do n0 = n0 + 1 end
    for _ in pairs(at1) do n1 = n1 + 1 end
    t:assert_eq(n0 - n1, 1, "CPU 0 carries one shard more than CPU 1, no more")

    -- Drain threads hand off and never write: no write syscalls at all,
    -- while every writer has made some.
    local ths, pid = eventd.threads(vm)
    for _, th in ipairs(ths) do
        if th.comm:find("^eventd%-drain") or th.comm:find("^eventd%-writer") then
            local io = vm:read_file("/proc/" .. pid .. "/task/" .. th.tid .. "/io")
            local syscw = tonumber(io:match("syscw: (%d+)"))
            if th.comm:find("drain") then
                t:assert_eq(syscw, 0, th.comm .. " made no write syscall")
            else
                t:assert(syscw > 0, th.comm .. " " .. th.tid .. " wrote its shard")
            end
        end
    end
end)

test("a stalled writer holds up only its own shards: there is no cross-shard coordination", {
    spec = "eventd *pipeline.shards-share-no-write-path-state-or-coordination-point",
}, function(t)
    -- Starve both of CPU 0's writers (shards 0 and 2) with the drain
    -- threads at SCHED_FIFO and two busy loops; CPU 1 writes to shard 1.
    local writers = writer_tids()
    local drains = {}
    for _, th in ipairs(eventd.threads(vm)) do
        if th.comm:find("^eventd%-drain") then drains[#drains + 1] = th.tid end
    end
    local stalled = "pt.shard." .. eventd.marker("stall0")
    local flowing = "pt.shard." .. eventd.marker("flow1")
    local hogs = {}
    local during
    local ok, err = pcall(function()
        eventd.set_policy(vm, writers[1], SCHED_IDLE)
        eventd.set_policy(vm, writers[3], SCHED_IDLE)
        for _, d in ipairs(drains) do eventd.set_policy(vm, d, SCHED_FIFO, 1) end
        for _ = 1, 4 do
            local r = vm:run("sh -c 'while :; do :; done' >/dev/null 2>&1 & echo $!")
            hogs[#hogs + 1] = r.stdout:match("(%d+)")
        end
        vm:run("sleep 1")
        -- A backlog for CPU 0's starved writers first, then a handful for
        -- CPU 1. The ring holds all of it; nothing is lost.
        emit_on(0, stalled, 10000)
        emit_on(1, flowing, 20)
        local _, total = wait_where(flowing, 20)
        during = select(2, where(stalled))
        t:assert_eq(total, 20, "shard 1 committed CPU 1's events")
    end)
    for _, h in ipairs(hogs) do vm:run("kill -9 " .. h) end
    for _, w in ipairs(writers) do eventd.set_policy(vm, w, SCHED_OTHER) end
    for _, d in ipairs(drains) do eventd.set_policy(vm, d, SCHED_OTHER) end
    if not ok then error(err, 0) end
    t:assert(during < 10000,
        "while CPU 0's earlier backlog was still being written by its own starved writers " ..
        "(" .. during .. " of 10000 stored): shard 1 did not wait behind it")
    wait_where(stalled, 10000)
end)

-- ---------------------------------------------------------------------------
-- Restart recovery across stripes
-- ---------------------------------------------------------------------------

test("a stripe missing from the middle is re-ingested: receipts, not a high-water mark, decide", {
    spec = "eventd *kmes.restart-resumes-from-the-union-of-committed-receipt-ranges-not-one-number"
        .. " eventd *kmes.sequence-checkpoints-and-the-shutdown-event-take-no-part-in-recovery",
}, function(t)
    -- CPU 0's second stripe is in shard 2 and its third in shard 0, so the
    -- committed maximum is beyond everything shard 2 holds.
    local seqs = cpu_sequences(0)
    local lo, hi, max = nil, nil, 0
    for s, sh in pairs(seqs) do
        if sh == "shard-0002" then
            lo = (lo and lo < s) and lo or s
            hi = (hi and hi > s) and hi or s
        end
        if s > max then max = s end
    end
    t:assert(lo and hi and max > hi, "shard 2 holds a stripe below CPU 0's highest sequence " ..
        tostring(lo) .. ".." .. tostring(hi) .. " < " .. max)

    eventd.stop(vm)
    -- The stop wrote eventd.daemon.stopped (shard 0) and the metadata
    -- checkpoints, both of which put CPU 0 at or beyond `max`.
    local shutdown = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.shutdown .. "'")[1][1]
    t:assert(shutdown >= 1, "a shutdown record is in shard 0")
    vm:run("rm -f /var/state/eventd/events/shard-0002.db /var/state/eventd/events/shard-0002.db-wal " ..
        "/var/state/eventd/events/shard-0002.db-shm"):assert_ok()
    eventd.start(vm)

    local after
    wait_until(function()
        after = cpu_sequences(0)
        return after[hi] ~= nil
    end, { timeout = 60, interval = 0.5, desc = "the deleted stripe to be re-ingested" })
    for s = lo, hi do
        t:assert(after[s] and not after[s]:find(","),
            "sequence " .. s .. " of the lost stripe was ingested again, once: " .. tostring(after[s]))
    end
    for s = 1, max do
        t:assert(after[s] and not after[s]:find(","), "CPU 0 sequence " .. s .. " is stored exactly once")
    end
    local gaps = eventd.rows(vm, string.format("EVENTS %s WHERE event.cpu == 0 SINCE 1h ago", eventd.T.gap))
    t:assert_eq(#gaps, 0, "and no gap was recorded: every lost row was still in the ring")
end)

-- ---------------------------------------------------------------------------
-- Changing the count
-- ---------------------------------------------------------------------------

test("a StorageShards change waits for a restart, then both CPUs share one shard and old shards stay readable", {
    spec = "eventd *shard.a-storage-shards-change-is-deferred-to-the-next-restart"
        .. " eventd *shard.fewer-shards-than-cpus-means-cpus-share-shards"
        .. " eventd *shard.an-ordinal-with-no-shard-is-assigned-c-mod-shard-count"
        .. " eventd *shard.every-drain-thread-has-at-least-one-shard"
        .. " eventd *shard.the-handoff-channel-is-multi-producer-single-consumer"
        .. " eventd *shard.shards-left-by-a-previous-configuration-are-kept-and-still-read"
        .. " eventd *shard.assignment-is-not-persisted"
        .. " eventd *shard.the-stripe-length-is-chosen-at-startup-and-not-persisted"
        .. " eventd *shard.the-query-path-assumes-no-relationship-between-a-shard-and-a-cpu",
}, function(t)
    local old1 = "pt.shard." .. eventd.marker("old1")
    emit_on(1, old1, 5)
    t:assert_eq(keys((wait_where(old1, 5))), '["shard-0001"]', "before: CPU 1 writes shard 1")

    local pid = eventd.pid(vm)
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    eventd.wait_rows(vm, "LOGS FROM eventd SINCE 10m ago TAKE 200", function(rs)
        for _, r in ipairs(rs) do
            if r.message:find("StorageShards is deferred until restart", 1, true) then return true end
        end
        return false
    end, { desc = "eventd to say the change is deferred" })
    local still = "pt.shard." .. eventd.marker("still1")
    emit_on(1, still, 5)
    t:assert_eq(keys((wait_where(still, 5))), '["shard-0001"]',
        "after the change, until a restart, CPU 1 still writes shard 1")
    t:assert_eq(eventd.pid(vm), pid, "and eventd did not restart itself")

    local line = startup_line(function() eventd.restart(vm) end)
    t:assert(line:find("2 KMES buffer(s), 1 active shard(s)", 1, true),
        "after the restart there is one active shard: " .. line)
    local names = {}
    for _, s in ipairs(eventd.shards(vm)) do names[#names + 1] = shard_name(s) end
    t:assert_eq(json.encode(names), '["shard-0000","shard-0001","shard-0002"]',
        "the two shards the old configuration used are still in place")

    local new0 = "pt.shard." .. eventd.marker("new0")
    local new1 = "pt.shard." .. eventd.marker("new1")
    emit_on(0, new0, 300)
    emit_on(1, new1, 300)
    t:assert_eq(keys((wait_where(new0, 300))), '["shard-0000"]', "CPU 0 writes shard 0")
    t:assert_eq(keys((wait_where(new1, 300))), '["shard-0000"]',
        "and CPU 1, with no j % 2 == 1 below 1, writes shard 1 % 1 == 0: both feed one writer")

    -- CPU 1's events are now in two shards; a query by CPU reads both.
    local function by_cpu(event_type)
        return #eventd.rows(vm, "EVENTS " .. event_type .. " WHERE event.cpu == 1 SINCE 1h ago TAKE 1000")
    end
    t:assert_eq(by_cpu(old1), 5, "a query by event.cpu found CPU 1's events in the shard it used to own")
    t:assert_eq(by_cpu(new1), 300, "and in the shard it writes now")

    -- Nothing about the routing was written down.
    for _, s in ipairs(eventd.shards(vm)) do
        local meta = eventd.sql(vm, s, "SELECT key FROM metadata ORDER BY key")
        t:assert_eq(json.encode(meta), '[["created_at"],["schema_version"]]',
            shard_name(s) .. " records no assignment or stripe")
    end
end)

test("the default, zero, is one shard per attached ring, and then each CPU has its own", {
    spec = "eventd *shard.storage-shards-zero-is-the-default-and-means-one-shard-per-attached-buffer"
        .. " eventd *shard.equal-counts-give-one-shard-per-cpu",
}, function(t)
    local line = startup_line(function() restart_with(nil) end)
    t:assert(line:find("2 KMES buffer(s), 2 active shard(s)", 1, true),
        "with StorageShards unset, two rings give two shards: " .. line)
    local e0 = "pt.shard." .. eventd.marker("eq0")
    local e1 = "pt.shard." .. eventd.marker("eq1")
    emit_on(0, e0, 300)
    emit_on(1, e1, 300)
    t:assert_eq(keys((wait_where(e0, 300))), '["shard-0000"]', "CPU 0 writes only shard 0")
    t:assert_eq(keys((wait_where(e1, 300))), '["shard-0001"]', "CPU 1 writes only shard 1")
end)

--- The soft and hard "Max open files" of process `pid` on `v`, from
--- /proc/<pid>/limits.
local function nofile(v, pid)
    local ok, text = pcall(v.read_file, v, "/proc/" .. pid .. "/limits")
    if not ok then text = v:run("cat /proc/" .. pid .. "/limits").stdout end
    local soft, hard = text:match("Max open files%s+(%S+)%s+(%S+)")
    return soft, hard
end

-- §2.3: "eventd neither raises its own descriptor limit nor caps the shard
-- count to fit it." Here, with no LimitNOFILE in its service definition,
-- eventd runs under the limit it inherited from peinit, and with three
-- shards configured it uses three.
test("eventd runs under the descriptor limit it inherited, raising none", {
    spec = "eventd *shard.eventd-neither-raises-its-descriptor-limit-nor-caps-the-shard-count",
}, function(t)
    t:assert(vm:run("reg get '" .. eventd.SERVICE .. "' LimitNOFILE").exit_code ~= 0,
        "precondition: eventd's service definition sets no LimitNOFILE")
    local soft, hard = nofile(vm, eventd.pid(vm))
    local init_soft, init_hard = nofile(vm, 1)
    t:assert(soft and init_soft, "both limits were read: eventd " .. tostring(soft) .. ", peinit " .. tostring(init_soft))
    t:assert_eq(soft, init_soft, "eventd's soft NOFILE limit is peinit's, inherited, not raised")
    t:assert_eq(hard, init_hard, "and so is its hard limit")
end)

-- Last in the file. The range, then 256 itself, which needs the descriptor
-- limit raised (§2.3): on machines of their own, since a Critical service
-- that cannot start would take this VM down.
test("the shard count tops out at 256: 257 is not used", {
    spec = "eventd *shard.there-are-between-1-and-256-event-shards",
}, function(t)
    local line = startup_line(function() restart_with(257) end)
    t:assert(line:find("2 KMES buffer(s), 2 active shard(s)", 1, true),
        "257 is not a shard count eventd will use: " .. line)
end)

--- Boot a one-vCPU machine whose eventd is seeded StorageShards=256 and
--- made Normal / RestartPolicy Never, with `values` further service
--- values. Returns the VM and eventd's settled status.
local function boot_256(name, values)
    local v = eventd.boot({
        name = name,
        wait = false,
        config = { { name = "StorageShards", type = "dword", data = 256 } },
        noncritical = { values = values },
    })
    local status
    wait_until(function()
        local r = v:run("svctl --json status eventd")
        if r.exit_code ~= 0 then return false end
        status = json.decode(r.stdout)
        return (status.state == "active" and eventd.query(v, "EVENTS TAKE 1").ok)
            or status.state == "failed" or status.cause == "process_crash"
    end, { timeout = 120, interval = 0.5, desc = "eventd to start with 256 shards, or fail" })
    return v, status
end

-- §2.3: "Under a 1024-descriptor soft limit, 256 shards do not start".
-- eventd does not cap the count to fit the limit it inherited; it fails.
test("256 shards do not start under the inherited descriptor limit", {
    spec = "eventd *shard.a-high-storage-shards-needs-a-raised-descriptor-limit"
        .. " eventd *shard.eventd-neither-raises-its-descriptor-limit-nor-caps-the-shard-count",
}, function(t)
    local v, status = boot_256("ev-shard256-low", nil)
    t:assert(status.state ~= "active", "without LimitNOFILE eventd does not run 256 shards: " .. json.encode(status))
    t:assert_eq(status.cause, "process_crash", "it fails during startup: " .. json.encode(status))
    t:assert(not eventd.query(v, "EVENTS TAKE 1").ok, "and nothing answers")
end)

-- §2.3: "a machine configured with a high StorageShards raises eventd's
-- limit to match, with LimitNOFILE in its service definition (peinit TRM
-- §3.2)". Seeded at 4096, 256 shards start and are used as given.
test("with LimitNOFILE raised, eventd runs 256 shards", {
    spec = "eventd *shard.there-are-between-1-and-256-event-shards"
        .. " eventd *shard.a-high-storage-shards-needs-a-raised-descriptor-limit",
}, function(t)
    local v, status = boot_256("ev-shard256", { { name = "LimitNOFILE", type = "dword", data = 4096 } })
    t:assert_eq(status.state, "active", "eventd runs with 256 shards: " .. json.encode(status))
    local soft, hard = nofile(v, eventd.pid(v))
    t:assert_eq(soft .. "/" .. tostring(hard), "4096/4096", "under the LimitNOFILE the definition sets")
    local started
    wait_until(function()
        for _, l in ipairs(eventd.rows(v, "LOGS FROM eventd SINCE 1h ago TAKE 500")) do
            if l.message:find("KMES buffer") then started = l.message end
        end
        return started ~= nil
    end, { timeout = 30, interval = 0.5, desc = "the start's log line" })
    t:assert(started:find("1 KMES buffer(s), 256 active shard(s)", 1, true), "256 is used as given: " .. started)
    local shards = eventd.shards(v)
    t:assert_eq(#shards, 256, "with 256 shard databases")
    t:assert_eq(shard_name(shards[256]), "shard-0255", "numbered 0000 to 0255")
end)

-- Documented skip: this says where the channel bounds live (compiled
-- constants, config.rs:19-20), not a behaviour. The configuration surface
-- is §A's key table; eventd reads no value naming the channel, and its
-- runtime consequence — a bound that does not move with MaxBatchSize — is
-- tested in pipeline-backpressure.test.lua.
test("the handoff channel's bounds are not registry settings", {
    spec = "eventd *shard.the-channel-bounds-are-not-registry-settings",
    skip = true,
}, function() end)
