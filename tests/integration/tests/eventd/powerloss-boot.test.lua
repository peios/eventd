-- eventd TRM §9.5 — power loss: what each store's durability setting
-- keeps across a sudden loss of power, and what the next boot's start
-- does about the boundary.
--
-- Two VMs.
--
-- The first ("ev-powerloss") is about how eventd treats a new boot, and a
-- new boot is a new kernel boot ID. A valid UUID that is not this boot's
-- is bound over /proc/sys/kernel/random/boot_id and eventd restarted, so
-- it starts as the first start of a boot it has no receipts for. Unlike a
-- real power cycle the KMES ring survives (its sequences are this boot's,
-- not a fresh 1..n), which is what makes "never reconciled against the
-- previous boot's receipts" observable: a reconciling eventd would skip
-- the survivors the old boot's receipts cover; one that keeps the boots
-- apart reads them all again under the new ID.
--
-- The second ("ev-pl-disk") has its three stores on provium-mediated
-- disks (`eventd.boot{store_disk = {}}`): provium holds every write the
-- guest has not flushed, so `power_cut()` discards exactly what was never
-- made durable, and `vm:reset()` is the next boot finding what is left.
-- The power is cut three times on that one VM:
--
--   1. The checkpoint boundary. Log records and metric samples are sent,
--      then a configuration change makes the retention pass checkpoint
--      every store (and sets WalCheckpointPages to its ceiling, so nothing
--      checkpoints again). The main database files are read on their own,
--      without their WALs, to watch the "before" records arrive there.
--      More log records, metric samples and events follow — committed, in
--      the WALs only — and the power goes. The events survive; the logs
--      and metrics after the checkpoint do not; those before it do.
--   2. The in-flight batch. A few events are committed, then the event
--      store's filesystem is frozen (FIFREEZE), so the next batch the
--      writer takes blocks inside its commit and nothing of it reaches the
--      disk. Its events are emitted, the writer is seen blocked (D) in
--      /proc, and the power goes. The committed events survive, the batch
--      does not, and no eventd.events.lost record appears for it.
--   3. A store's creation lost: fresh stores, power cut a few seconds
--      after creation, and the next start must still come up. Last,
--      because it throws the earlier stores away.
--
-- ext4 flushes on its own: the journal commits every 5 seconds, and one
-- FLUSH makes the whole of provium's held overlay durable, including WAL
-- frames SQLite never synced. For cut 1 the three store mounts are
-- remounted with commit=600, so what survives is what eventd itself made
-- durable rather than what the filesystem happened to flush meanwhile.
-- (Cut 2 needs no remount: the frozen event store takes no writes at all.)
-- No cut is preceded by `vm:pause()`: QEMU's stop path flushes every
-- block device, which would commit the very writes the cut is to drop.
--
-- The disk VM is seeded ErrorControl=Normal / RestartPolicy=Never (as in
-- bootstrap-failure.test.lua) so a start that fails after a cut is one
-- failed start rather than peinit's Critical policy rebooting the machine
-- for ever.
-- Nothing else here reads either value.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local sys = require("helpers.sys")
peinit.claim(2)

local vm = eventd.boot({ name = "ev-powerloss" })

local SERVICE = [[Machine\System\Services\eventd]]

local dvm = eventd.boot({
    name = "ev-pl-disk",
    store_disk = {},
    files = peinit.seed("zz-pt-eventd-svc", {
        { path = [[Machine\System]] },
        { path = [[Machine\System\Services]] },
        { path = SERVICE, values = {
            { name = "ErrorControl", type = "dword", data = 0 },
            { name = "RestartPolicy", type = "dword", data = 0 },
        } },
    }),
})

local FAKE = "5eed0b00-7a11-4c0d-8e55-0123456789ab"

local function now_ns()
    return tonumber(vm:run("date +%s%N").stdout:match("%d+"))
end

local new = {}

test("a new boot's coverage for each CPU starts before sequence 1", {
    spec = "eventd *powerloss.a-new-boots-per-cpu-coverage-starts-before-sequence-1",
}, function(t)
    new.tag = eventd.marker("old")
    eventd.emit(vm, "pt.oldboot", { tag = new.tag })
    eventd.wait_rows(vm, 'EVENTS pt.oldboot WHERE tag == "' .. new.tag .. '" SINCE 10m ago',
        function(r) return #r == 1 end)
    vm:run("mkdir -p /run/pt-pl-bid"):assert_ok()
    vm:run("mount -t tmpfs -o size=64k,policy=synth-ephemeral --synth-sddl "
        .. "'O:SYG:SYD:(A;OICI;GA;;;SY)(A;OICI;GR;;;WD)' none /run/pt-pl-bid"):assert_ok()
    vm:write_file("/run/pt-pl-bid/id", FAKE .. "\n")
    vm:run("mount --bind /run/pt-pl-bid/id /proc/sys/kernel/random/boot_id"):assert_ok()
    new.since = now_ns()
    eventd.restart(vm)
    local s
    for _, r in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 10m ago")) do
        if r["event.time"] >= new.since then s = r end
    end
    t:assert(s, "eventd started under the new boot ID")
    t:assert_eq(s and s["event.boot.guid"], "{" .. FAKE .. "}", "the new boot")
    t:assert_eq(s and s["store.restarted"], false, "as that boot's first start")
    -- Coverage that begins before sequence 1 has nothing covered and
    -- nothing missing below the ring's oldest survivor: that survivor
    -- (sequence 1, the ring has not wrapped) is the first event of the
    -- new boot, and no gap precedes it. (The startup record's
    -- store.resume.* are where coverage stands once the start's recovery
    -- has committed, not where it began.)
    local found
    pcall(wait_until, function()
        for _, r in ipairs(eventd.rows(vm, "EVENTS WHERE event.cpu == 0 AND event.sequence == 1 SINCE 1h ago")) do
            if r["event.boot.guid"] == "{" .. FAKE .. "}" then found = r end
        end
        return found ~= nil
    end, { timeout = 15, interval = 0.5 })
    t:assert(found, "sequence 1 on CPU 0 was ingested for the new boot")
    local gaps = 0
    for _, g in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")) do
        if g["event.time"] >= new.since then gaps = gaps + 1 end
    end
    t:assert_eq(gaps, 0, "with no gap before it")
end)

test("a new boot's ring is never reconciled against the previous boot's receipts", {
    spec = "eventd *powerloss.a-new-boots-ring-is-never-reconciled-against-the-previous-boots-receipts",
}, function(t)
    t:assert(new.tag, "the restart under a new boot ID from the previous test")
    -- The event committed under the real boot is in the old boot's
    -- receipts and still in the ring. Read again under the new boot, it
    -- was not skipped on the old receipts' account.
    local rows = eventd.wait_rows(vm, 'EVENTS pt.oldboot WHERE tag == "' .. new.tag .. '" SINCE 10m ago',
        function(r) return #r >= 2 end)
    local boots = {}
    for _, r in ipairs(rows) do boots[r["event.boot.guid"]] = true end
    vm:run("umount /proc/sys/kernel/random/boot_id")
    eventd.restart(vm)
    t:assert(boots["{" .. FAKE .. "}"], "the survivor was ingested for the new boot: " .. json.encode(rows))
    t:assert_eq(#rows, 2, "once for each boot, the old receipts having played no part")
end)

-- ---------------------------------------------------------------------------
-- The disk VM: the stores on mediated disks, and the power cut
-- ---------------------------------------------------------------------------

local STORE_DIRS = { eventd.STORE.events, eventd.STORE.logs, eventd.STORE.metrics }

--- FIFREEZE, _IOWR('X', 119, int): sync the filesystem, then block every
--- further write to it until thawed (or, here, until the machine is gone).
local FIFREEZE = 0xC0045877

--- Cut the power to all three store disks and boot the machine again.
---
--- The three cuts and the reset are back to back: the guest keeps running
--- until the reset lands, and a write it flushed in between would be one
--- the cut never saw. `vm:reset()` returns once the agent serves (Phase
--- 1.5); eventd is a Phase 2 service, so the second boot's Phase 2 mark is
--- waited for (`expect` consumed the first boot's). Readiness is left to
--- the caller, which judges whether eventd came up.
local function power_cut(v)
    for _, store in ipairs(eventd.STORE_DISK_ORDER) do
        v:disk(eventd.STORE_DISK[store]):power_cut()
    end
    v:reset()
    v:console():expect(peinit.marks.phase2, peinit.STAGE_TIMEOUT)
end

--- Remount the three store filesystems with a 600-second journal commit
--- interval (see the header): from here on only an explicit flush — an
--- fsync of eventd's — makes anything on them durable. Each boot mounts
--- them afresh with the default, so this is redone after every cut.
local function long_commit(t, v)
    for _, dir in ipairs(STORE_DIRS) do
        local r = v:run("mount -o remount,commit=600 " .. dir)
        t:assert_eq(r.exit_code, 0, "remount " .. dir .. ": " .. r.stdout .. r.stderr)
    end
    local mounts = v:read_file("/proc/mounts")
    for _, dir in ipairs(STORE_DIRS) do
        local line = mounts:match("[^\n]* " .. (dir:gsub("%p", "%%%0")) .. " [^\n]*")
        t:assert(line and line:find("commit=600", 1, true),
            dir .. " is mounted with commit=600: " .. tostring(line))
    end
end

--- The running kernel's boot ID, in eventd's braced form.
local function boot_id(v)
    return "{" .. v:read_file("/proc/sys/kernel/random/boot_id"):match("[%x%-]+") .. "}"
end

local DB_ONLY_PY = [[
import json, sqlite3, sys
d = sys.argv[1]
c = sqlite3.connect("file:" + d + "/db?immutable=1", uri=True)
print(json.dumps(c.execute(open(d + "/q.sql").read()).fetchall()))
]]

--- Run SQL against a copy of a guest database's MAIN FILE ALONE, on the
--- host. `eventd.sql` copies the -wal too, so it sees every committed
--- transaction; this one sees only what a checkpoint has copied into the
--- database file (opened immutable, so SQLite neither looks for nor builds
--- a WAL). A torn copy is retried, as `eventd.sql` does.
local function db_only(v, db, query)
    local err
    for _ = 1, 4 do
        local ok, rows = pcall(function()
            local p = assert(io.popen("mktemp -d", "r"))
            local dir = p:read("l")
            p:close()
            for name, bytes in pairs({ db = v:read_file(db), ["q.sql"] = query, ["run.py"] = DB_ONLY_PY }) do
                local f = assert(io.open(dir .. "/" .. name, "wb"))
                f:write(bytes)
                f:close()
            end
            local q = assert(io.popen("python3 " .. dir .. "/run.py " .. dir .. " 2>&1", "r"))
            local out = q:read("a")
            local okc = q:close()
            os.execute("rm -rf '" .. dir .. "'")
            assert(okc, "sqlite on host failed: " .. out)
            return json.decode(out)
        end)
        if ok then return rows end
        err = rows
        v:clock():sleep("250ms")
    end
    error(err, 2)
end

--- Committed log records from `origin` (main file and WAL), or with
--- `main_only` the ones in logs.db's main file.
local function log_count(v, origin, main_only)
    local q = "SELECT count(*) FROM logs WHERE origin = '" .. origin .. "'"
    return (main_only and db_only or eventd.sql)(v, eventd.DB.logs, q)[1][1]
end

--- As `log_count`, for the samples of the metric series `name`.
local function metric_count(v, name, main_only)
    local q = "SELECT count(*) FROM samples s JOIN series e ON e.id = s.series_id " ..
        "WHERE e.name = '" .. name .. "'"
    return (main_only and db_only or eventd.sql)(v, eventd.DB.metrics, q)[1][1]
end

local function send_logs(v, origin, n)
    for i = 1, n do
        local r = eventd.send_log(v, { origin = origin, is_error = false, message = origin .. "-" .. i })
        assert(r.ret and r.ret > 0, "log datagram " .. i .. " sent: errno " .. tostring(r.errno))
    end
end

local function send_metrics(v, name, n)
    local now = tonumber(v:run("date +%s%N").stdout:match("%d+"))
    local recs = {}
    for i = 1, n do recs[i] = { name = name, type = "gauge", value = i, timestamp = now + i } end
    local r = eventd.send_metric(v, eventd.array(recs))
    assert(r.ret and r.ret > 0, "metric datagram sent: errno " .. tostring(r.errno))
end

local function emit_n(v, event_type, tag, n)
    for i = 1, n do
        local r = eventd.emit(v, event_type, { tag = tag, i = i })
        assert(r.ret == 0, event_type .. " " .. i .. " emitted: errno " .. tostring(r.errno))
    end
end

local function event_rows(v, event_type, tag)
    return eventd.rows(v, "EVENTS " .. event_type .. ' WHERE tag == "' .. tag .. '" SINCE 1h ago')
end

--- The thread ids of eventd's event writers (`eventd-writer-NNNN`, which
--- the kernel's 15-byte comm cuts to `eventd-writer-0`).
local function writer_tids(v, pid)
    local out = {}
    for _, e in ipairs(v:listdir("/proc/" .. pid .. "/task")) do
        local tid = type(e) == "table" and e.name or e
        local ok, comm = pcall(v.read_file, v, "/proc/" .. pid .. "/task/" .. tid .. "/comm")
        if ok and comm:match("^eventd%-writer") then out[#out + 1] = tid end
    end
    return out
end

--- A thread's scheduler state, the letter after the parenthesised comm.
local function thread_state(v, pid, tid)
    local ok, stat = pcall(v.read_file, v, "/proc/" .. pid .. "/task/" .. tid .. "/stat")
    return ok and stat:match("%) (%a)") or nil
end

--- The eventd.events.lost rows of boot `boot` that reach past sequence
--- `after` (all of that boot's, when `after` is nil).
local function gaps_past(v, boot, after)
    local out = {}
    for _, g in ipairs(eventd.rows(v, "EVENTS " .. eventd.T.gap .. " SINCE 1h ago")) do
        if g["event.boot.guid"] == boot and (after == nil or (g["loss.sequence-last"] or 0) > after) then
            out[#out + 1] = g
        end
    end
    return out
end

--- Make the stores' creation durable, once, before the first cut.
---
--- The stores were created on this machine's first boot, and a log or
--- metric store's creating transaction sits in its WAL until a checkpoint:
--- cut the power before one and the next start creates the store afresh
--- (the test at the end), losing what the cuts here are to measure. A
--- clean restart checkpoints every store as its last connection closes;
--- the main files alone then hold the schema, and no later cut can take it.
local settled = false
local function settle()
    if settled then return end
    settled = true
    eventd.restart(dvm)
    for _, db in ipairs({ eventd.DB.logs, eventd.DB.metrics }) do
        local r = db_only(dvm, db, "SELECT value FROM metadata WHERE key = 'schema_version'")
        assert(r[1], db .. "'s schema is in its main file after the clean restart")
    end
end

-- Cut 1: the checkpoint boundary, shared by the first three tests below
-- and run by whichever of them comes first.
local ck

local function checkpoint_cut(t)
    if ck then return ck end
    ck = {}
    settle()
    long_commit(t, dvm)

    ck.log_before, ck.log_after = eventd.marker("plbl"), eventd.marker("plal")
    ck.metric_before, ck.metric_after = eventd.marker("plbm"), eventd.marker("plam")
    ck.tag = eventd.marker("plev")

    -- Before the checkpoint: committed, and so far in the WALs only.
    send_logs(dvm, ck.log_before, 5)
    send_metrics(dvm, ck.metric_before, 5)
    wait_until(function()
        return log_count(dvm, ck.log_before) == 5 and metric_count(dvm, ck.metric_before) == 5
    end, { timeout = 30, interval = 0.5, desc = "the 'before' logs and samples committed" })

    -- The checkpoint. Any applied configuration change requests a
    -- retention pass, and the pass checkpoints all three stores
    -- (config.rs apply_reload; retention.rs pass). This change also puts
    -- WalCheckpointPages at its ceiling, so the writers' own threshold
    -- cannot checkpoint again before the cut.
    eventd.set(dvm, "WalCheckpointPages", "dword:100000"):assert_ok()
    wait_until(function()
        return log_count(dvm, ck.log_before, true) == 5
            and metric_count(dvm, ck.metric_before, true) == 5
    end, { timeout = 30, interval = 0.5,
           desc = "the retention pass to checkpoint the 'before' records into the main files" })

    -- After the checkpoint: logs, samples and events, all committed.
    send_logs(dvm, ck.log_after, 5)
    send_metrics(dvm, ck.metric_after, 5)
    emit_n(dvm, "pt.plcommit", ck.tag, 5)
    wait_until(function()
        return log_count(dvm, ck.log_after) == 5 and metric_count(dvm, ck.metric_after) == 5
            and #event_rows(dvm, "pt.plcommit", ck.tag) == 5
    end, { timeout = 30, interval = 0.5, desc = "the 'after' logs, samples and events committed" })
    ck.events = event_rows(dvm, "pt.plcommit", ck.tag)
    ck.old_boot = boot_id(dvm)

    -- Where each lives at the moment of the cut: nothing after the
    -- checkpoint has reached a main file.
    ck.log_after_main = log_count(dvm, ck.log_after, true)
    ck.metric_after_main = metric_count(dvm, ck.metric_after, true)
    -- (pt.plcommit is emitted on this boot only, so its sequences name
    -- these five.)
    local seqs = {}
    for _, r in ipairs(ck.events) do seqs[#seqs + 1] = tostring(math.tointeger(r["event.sequence"])) end
    ck.events_main = 0
    local shards = eventd.shards(dvm)
    assert(#shards > 0, "the event store's shard files were found")
    for _, shard in ipairs(shards) do
        ck.events_main = ck.events_main + db_only(dvm, shard,
            "SELECT count(*) FROM events WHERE event_type = 'pt.plcommit' AND sequence IN ("
            .. table.concat(seqs, ",") .. ")")[1][1]
    end

    power_cut(dvm)
    eventd.ready(dvm)
    ck.new_boot = boot_id(dvm)
    eventd.unset(dvm, "WalCheckpointPages")

    ck.events_now = event_rows(dvm, "pt.plcommit", ck.tag)
    ck.log_before_now = log_count(dvm, ck.log_before)
    ck.log_after_now = log_count(dvm, ck.log_after)
    ck.metric_before_now = metric_count(dvm, ck.metric_before)
    ck.metric_after_now = metric_count(dvm, ck.metric_after)
    ck.done = true
    return ck
end

test("every committed event transaction survives power loss", {
    spec = "eventd *powerloss.every-committed-event-transaction-survives-power-loss",
}, function(t)
    local c = checkpoint_cut(t)
    t:assert(c.done, "the checkpoint cut ran to the end")
    t:assert_eq(#(c.events or {}), 5, "five events were committed before the cut")
    t:assert_eq(c.events_main, 0,
        "none of them had been checkpointed into a shard's main file: they were committed " ..
        "transactions in the WAL, which synchronous=FULL synced at each commit")
    t:assert(c.new_boot ~= c.old_boot, "the machine came back as a new boot")
    local survived = {}
    for _, r in ipairs(c.events_now or {}) do
        if r["event.boot.guid"] == c.old_boot then survived[r["event.sequence"]] = true end
    end
    for _, r in ipairs(c.events or {}) do
        t:assert(survived[r["event.sequence"]],
            "the committed event at sequence " .. tostring(r["event.sequence"]) .. " survived the power cut")
    end
    t:assert_eq(#(c.events_now or {}), 5, "every one, once each")
end)

test("log transactions survive only up to the last checkpoint", {
    spec = "eventd *powerloss.log-transactions-survive-only-up-to-the-last-checkpoint",
}, function(t)
    local c = checkpoint_cut(t)
    t:assert(c.done, "the checkpoint cut ran to the end")
    t:assert_eq(c.log_after_main, 0,
        "at the cut the records sent after the checkpoint were committed but in the WAL only")
    t:assert_eq(c.log_before_now, 5, "the records committed before the last checkpoint survived")
    t:assert_eq(c.log_after_now, 0,
        "the records committed after the last checkpoint did not: under synchronous=NORMAL " ..
        "the checkpoint, not the commit, is the durability boundary")
end)

test("metric transactions survive only up to the last checkpoint", {
    spec = "eventd *powerloss.metric-transactions-survive-only-up-to-the-last-checkpoint",
}, function(t)
    local c = checkpoint_cut(t)
    t:assert(c.done, "the checkpoint cut ran to the end")
    t:assert_eq(c.metric_after_main, 0,
        "at the cut the samples sent after the checkpoint were committed but in the WAL only")
    t:assert_eq(c.metric_before_now, 5, "the samples committed before the last checkpoint survived")
    t:assert_eq(c.metric_after_now, 0,
        "the samples committed after the last checkpoint did not")
end)

-- Cut 2: an event batch caught in flight, shared by the next two tests.
local inf

local function inflight_cut(t)
    if inf then return inf end
    inf = {}
    settle()
    inf.kept_tag, inf.lost_tag = eventd.marker("plkeep"), eventd.marker("pllost")

    emit_n(dvm, "pt.plkept", inf.kept_tag, 3)
    local kept = eventd.wait_rows(dvm, "EVENTS pt.plkept WHERE tag == \"" .. inf.kept_tag .. "\" SINCE 1h ago",
        function(r) return #r == 3 end)
    inf.old_boot = boot_id(dvm)
    inf.last_committed = 0
    for _, r in ipairs(kept) do
        if r["event.sequence"] > inf.last_committed then inf.last_committed = r["event.sequence"] end
    end
    inf.gaps_before = #gaps_past(dvm, inf.old_boot, inf.last_committed)
    inf.old_boot_gaps_before = #gaps_past(dvm, inf.old_boot)

    -- Hold the writer: with the event store's filesystem frozen, the next
    -- batch it takes blocks inside its commit (the WAL write cannot
    -- proceed), and none of it reaches the disk. A signal cannot do this:
    -- SIGSTOP stops a whole thread group, and a long MaxBatchLatencyMs
    -- does not hold a lone event (writer.rs commits as soon as its queue
    -- is empty). The freeze syncs the filesystem first, which is why the
    -- committed-event claim is shown by cut 1 and not here.
    local pid = assert(eventd.pid(dvm), "eventd is running")
    local writers = writer_tids(dvm, pid)
    assert(#writers > 0, "eventd's event writer threads were found")
    local fd, errno = sys.open(dvm, eventd.DB.meta, sys.O.RDONLY)
    assert(fd, "open a file on the event store: " .. tostring(errno))
    -- FIFREEZE takes no argument, so the plain three-register call rather
    -- than `sys.ioctl_word` (whose pointer argument came back EBADF here
    -- although the freeze had taken effect).
    local fr = dvm:syscall(sys.NR.ioctl, { args = { fd, FIFREEZE, 0 } })
    assert(fr.ret == 0, "FIFREEZE the event store: errno " .. tostring(fr.errno))

    emit_n(dvm, "pt.pllost", inf.lost_tag, 3)
    -- No query from here to the cut: a reader would block on the frozen
    -- filesystem too.
    local states = {}
    inf.writer_blocked = pcall(wait_until, function()
        for _, tid in ipairs(writers) do
            states[tid] = thread_state(dvm, pid, tid)
            if states[tid] == "D" then return true end
        end
        return false
    end, { timeout = 15, interval = 0.25, desc = "an event writer blocked in its commit" })
    inf.writer_states = json.encode(states)

    power_cut(dvm)
    eventd.ready(dvm)
    inf.new_boot = boot_id(dvm)
    inf.kept_now = event_rows(dvm, "pt.plkept", inf.kept_tag)
    inf.lost_now = event_rows(dvm, "pt.pllost", inf.lost_tag)
    inf.gaps_after = gaps_past(dvm, inf.old_boot, inf.last_committed)
    inf.old_boot_gaps_after = #gaps_past(dvm, inf.old_boot)
    inf.done = true
    return inf
end

test("the event store loses only the in-flight batch", {
    spec = "eventd *powerloss.the-event-store-loses-only-the-in-flight-batch",
}, function(t)
    local c = inflight_cut(t)
    t:assert(c.done, "the in-flight cut ran to the end")
    t:assert(c.writer_blocked,
        "the batch was in flight at the cut: an event writer was blocked in its commit " ..
        tostring(c.writer_states))
    t:assert(c.new_boot ~= c.old_boot, "the machine came back as a new boot")
    local kept = 0
    for _, r in ipairs(c.kept_now or {}) do
        if r["event.boot.guid"] == c.old_boot then kept = kept + 1 end
    end
    t:assert_eq(kept, 3, "every event committed before the batch survived")
    t:assert_eq(#(c.lost_now or {}), 0,
        "the in-flight batch's events were lost: " .. json.encode(c.lost_now or {}))
end)

test("no gap record is written for a batch lost to power loss", {
    spec = "eventd *powerloss.no-gap-record-is-written-for-a-batch-lost-to-power-loss",
}, function(t)
    local c = inflight_cut(t)
    t:assert(c.done, "the in-flight cut ran to the end")
    t:assert_eq(#(c.lost_now or {}), 0, "the batch was lost")
    t:assert_eq(c.gaps_before, 0, "no gap reached past the last committed sequence before the cut")
    t:assert_eq(#(c.gaps_after or {}), 0,
        "and none after it: nothing records the lost batch's sequences (old boot, past " ..
        tostring(c.last_committed) .. ") as a gap: " .. json.encode(c.gaps_after or {}))
    -- The same, without reading a payload field: the old boot has exactly
    -- the gap records it had before the cut.
    t:assert_eq(c.old_boot_gaps_after, c.old_boot_gaps_before,
        "no gap record of the old boot was written after the cut")
end)

-- A log or metric store whose creating transaction was never checkpointed
-- is left, after a power cut, as a database file with no schema (the file
-- and its directory entry durable, the WAL holding the creation not). The
-- next start creates such a store afresh rather than failing on it; the
-- book's restart is "Nothing manual … finds its databases consistent". No
-- spec: the cut can as well land after the creation is durable, so this
-- does not prove the no-schema anchors, which the lifecycle files prove on
-- crafted schemaless files; the power-loss anchors are homed by the
-- passing tests above.
test("eventd starts after power is lost before a fresh store was checkpointed", {}, function(t)
    -- Fresh stores on the same disks: stop eventd, empty the three store
    -- directories, and make that emptiness durable (sync) so the cut
    -- cannot bring the old stores back. The default journal commit
    -- applies again here (this boot remounted nothing), as on a real
    -- machine's first boot.
    local stop = dvm:run("svctl stop eventd")
    local st
    local stopped = pcall(wait_until, function()
        st = eventd.status(dvm)
        return st.state ~= "active" and st.current_operation == nil
    end, { timeout = 30, interval = 0.25, desc = "eventd to stop" })
    t:assert(stopped, "eventd stopped (" .. stop.stdout .. stop.stderr .. "): " .. json.encode(st))
    for _, dir in ipairs(STORE_DIRS) do
        for _, e in ipairs(dvm:listdir(dir)) do
            local name = type(e) == "table" and e.name or e
            if name ~= "lost+found" and name ~= "." and name ~= ".." then
                dvm:run("rm -rf '" .. dir .. "/" .. name .. "'"):assert_ok()
            end
        end
    end
    dvm:run("sync"):assert_ok()
    dvm:run("svctl start eventd")
    eventd.ready(dvm)
    t:assert(dvm:run("test -s " .. eventd.DB.logs).exit_code == 0, "eventd created a fresh logs.db")

    -- A few seconds of a running machine: long enough for ext4's 5-second
    -- journal commit to make the new files' directory entries durable,
    -- well short of the ~30-second writeback that would carry the WALs.
    dvm:clock():sleep("10s")
    power_cut(dvm)

    local ok = pcall(eventd.ready, dvm, 60)
    local status = dvm:run("svctl --json status eventd").stdout
    local lines = {}
    for _, l in ipairs(peinit.lines(dvm:console():read_log())) do
        if l:find("eventd", 1, true) and (l:find("error", 1, true) or l:find("fail", 1, true)) then
            lines[#lines + 1] = l
        end
    end
    local tail = table.concat(lines, "\n", math.max(1, #lines - 5))
    -- What the cut left of each single-file store: the file, and whether
    -- its main file and WAL together hold any schema at all.
    local stores = {}
    for _, db in ipairs({ eventd.DB.logs, eventd.DB.metrics }) do
        local size = dvm:run("stat -c %s " .. db .. " " .. db .. "-wal 2>&1").stdout:gsub("\n", " ")
        local okq, tables = pcall(eventd.sql, dvm, db, "SELECT name FROM sqlite_master WHERE type = 'table'")
        stores[#stores + 1] = db .. ": sizes " .. size .. "tables " ..
            (okq and json.encode(tables) or tostring(tables))
    end
    local kmsg = dvm:run("dmesg 2>&1 | grep -i 'eventd' | tail -5").stdout
    t:assert(ok, "eventd starts on the stores the power cut left and answers on its query " ..
        "socket; status: " .. status .. "\nconsole: " .. tail ..
        "\nstores: " .. table.concat(stores, "; ") .. "\nkmsg: " .. kmsg)
end)
