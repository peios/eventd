-- eventd TRM §2.2 — KMES consumption: attaching to every CPU's ring, one
-- drain thread per ring, copying events out, following a ring
-- replacement (generation change), and resuming after a restart from
-- committed receipts.
--
-- Two vCPUs throughout: what is under test is per-CPU structure — one
-- ring, one drain thread and one sequence stream per CPU — and on one vCPU
-- there is exactly one of each, so "every CPU" and "independently" could
-- not be told apart from "the only one". Events are emitted from a worker
-- pinned to a chosen CPU (a worker serves all its syscalls on one thread,
-- so the affinity holds; see kmes/wire.test.lua).
--
-- One file-scope VM, default configuration (two shards, one per CPU) and
-- the default 4 MiB ring. The image has no `Machine\System\KMES` key; the
-- generation-change tests create it and change BufferCapacity live, which
-- makes KMES replace every ring. Those run last, in order, because they
-- reshape the rings for everything after them.
--
-- A second VM is booted for the one destructive case: eventd's service
-- token without SeSecurityPrivilege, which eventd cannot start with (the
-- Critical policy then takes that machine to recovery).
--
-- KMES behaviour that no guest can produce — a hole in the slot space, a
-- machine with no ring, a sequence that goes backwards — is homed on unit
-- tests, most of which do not exist yet (TODO stubs).

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local sys = require("helpers.sys")
peinit.claim(2, { cpus = 2 }) -- the file VM, plus the recovery boot

local KMES_KEY = [[Machine\System\KMES]]

local vm = eventd.boot({ name = "ev-kmes", cpus = 2 })

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function pin(who, cpu, tid)
    return who:syscall(sys.NR.sched_setaffinity, {
        args = { tid or 0, 8, 0 },
        bufs = { string.pack("<I8", 1 << cpu) },
        ptrs = { 2 },
    }).ret == 0
end

--- Run `fn(worker)` on a worker pinned to `cpu`.
local function on_cpu(cpu, fn)
    local worker = vm:spawn_worker()
    local ok, err = pcall(function()
        assert(pin(worker, cpu), "pin the worker to cpu " .. cpu)
        fn(worker)
    end)
    worker:kill(); worker:join()
    if not ok then error(err, 0) end
end

--- Emit `entries` (a list of {type=, payload=}) from CPU `cpu`, `per` to
--- a batch. A batch must fit in the ring and holds at most 255 distinct
--- buffers, so callers with distinct payloads pass a small `per`.
local function emit_on(cpu, entries, per)
    per = per or 256
    on_cpu(cpu, function(worker)
        local i = 1
        while i <= #entries do
            local batch = {}
            for j = i, math.min(i + per - 1, #entries) do batch[#batch + 1] = entries[j] end
            local r = kmes.emit_batch(worker, batch)
            assert(r.ret == 0 and r.emitted == #batch,
                "emit_batch on cpu " .. cpu .. ": ret " .. tostring(r.ret) .. " errno " .. tostring(r.errno))
            i = i + #batch
        end
    end)
end

local function same(event_type, n)
    local out = {}
    for i = 1, n do out[i] = { type = event_type, payload = kmes.PAYLOAD } end
    return out
end

--- Rows of one event type straight from every shard: {sequence, cpu_id,
--- payload hex, shard}.
local function stored(event_type)
    local out = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard, string.format(
            "SELECT sequence, cpu_id, hex(payload), timestamp FROM events " ..
            "WHERE event_type = '%s' ORDER BY sequence", event_type))) do
            out[#out + 1] = { sequence = r[1], cpu_id = r[2], payload = r[3],
                              timestamp = r[4], shard = shard }
        end
    end
    table.sort(out, function(a, b) return a.sequence < b.sequence end)
    return out
end

local function wait_count(event_type, n, timeout)
    local rows
    wait_until(function()
        rows = stored(event_type)
        return #rows >= n
    end, { timeout = timeout or 60, interval = 0.5,
           desc = n .. " rows of " .. event_type })
    return rows
end

--- Gap records on one CPU, through evctl.
local function gaps_on(cpu)
    return eventd.rows(vm, string.format(
        "EVENTS %s WHERE event.cpu == %d SINCE 1h ago TAKE 10000", eventd.T.gap, cpu))
end

--- Set the KMES ring capacity and wait until a fresh attach sees it.
local function set_capacity(bytes)
    vm:run("reg new '" .. KMES_KEY .. "'"):assert_ok()
    vm:run(string.format("reg set '%s' BufferCapacity qword:%d", KMES_KEY, bytes)):assert_ok()
    wait_until(function()
        local ring = kmes.attach(vm, 0)
        if not ring then return false end
        local cap = ring.capacity
        kmes.detach(ring)
        return cap == bytes
    end, { timeout = 30, interval = 0.5, desc = "KMES rings at " .. bytes .. " bytes" })
end

local function kmes_fds()
    local n = 0
    for _ in eventd.fd_listing(vm):gmatch("anon_inode:kmes%-cpu") do n = n + 1 end
    return n
end

-- ---------------------------------------------------------------------------
-- Attachment and drain threads
-- ---------------------------------------------------------------------------

test("eventd attaches every CPU's ring at startup and drains each with its own thread", {
    spec = "eventd *kmes.startup-queries-the-slot-count-and-tries-every-slot-below-it"
        .. " eventd *kmes.every-attachable-cpu-is-attached-with-no-way-to-choose-a-subset"
        .. " eventd *kmes.one-drain-thread-per-cpu-reads-exactly-one-ring-buffer",
}, function(t)
    -- eventd's stderr reaches the log store through peinit, a little later.
    local line
    eventd.wait_rows(vm, "LOGS FROM eventd SINCE 1h ago TAKE 50", function(logs)
        for _, l in ipairs(logs) do
            if l.message:find("with %d+ KMES buffer") then line = l.message end
        end
        return line ~= nil
    end, { desc = "eventd's startup line" })
    t:assert(line and line:find("with 2 KMES buffer(s)", 1, true),
        "eventd reports attaching both CPUs' rings: " .. tostring(line))

    local startup = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1")[1]
    local cpus = {}
    for _, cpu in ipairs(startup["store.resume.cpus"]) do cpus[#cpus + 1] = cpu end
    table.sort(cpus)
    t:assert_eq(json.encode(cpus), "[0,1]", "the startup record names both CPUs")

    local drains = {}
    for _, th in ipairs(eventd.threads(vm)) do
        if th.comm:find("^eventd%-drain%-") then drains[#drains + 1] = th.comm end
    end
    table.sort(drains)
    t:assert_eq(json.encode(drains), '["eventd-drain-0","eventd-drain-1"]',
        "one drain thread per CPU, named for it")

    -- Each thread's ring holds only its CPU's events, and both reach the store.
    local tags = {}
    for cpu = 0, 1 do
        tags[cpu] = "pt.kmes." .. eventd.marker("cpu" .. cpu)
        emit_on(cpu, same(tags[cpu], 3))
    end
    for cpu = 0, 1 do
        local rows = wait_count(tags[cpu], 3)
        for _, r in ipairs(rows) do
            t:assert_eq(r.cpu_id, cpu, tags[cpu] .. " was read from CPU " .. cpu .. "'s ring")
        end
    end
end)

test("the first attachment reads from the oldest surviving event, not the newest", {
    spec = "eventd *kmes.first-attachment-starts-reading-at-the-oldest-surviving-event",
}, function(t)
    local startup = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1")[1]
    for cpu = 0, 1 do
        local first
        for _, shard in ipairs(eventd.shards(vm)) do
            local r = eventd.sql(vm, shard, string.format(
                "SELECT timestamp FROM events WHERE cpu_id = %d AND sequence = 1", cpu))
            if r[1] then first = r[1][1] end
        end
        t:assert(first, "CPU " .. cpu .. "'s sequence 1 is stored")
        t:assert(first < startup["event.time"],
            "and it was emitted before eventd started: the boot's earliest events were read")
    end
end)

test("events of every size are read whole, in order, each exactly its own length", {
    spec = "eventd *kmes.the-drain-loop-checks-lapping-validates-each-event-and-advances-by-event-size"
        .. " eventd *kmes.the-copy-is-bounded-by-the-events-own-event-size",
}, function(t)
    local event_type = "pt.kmes." .. eventd.marker("size")
    local sizes = { 0, 1, 2, 31, 32, 255, 256, 1000, 4095, 4096, 20000, 60000 }
    local entries, payloads = {}, {}
    for i, n in ipairs(sizes) do
        payloads[i] = eventd.msgpack({ i = i, b = eventd.bin(string.rep(string.char(64 + i), n)) })
        entries[i] = { type = event_type, payload = payloads[i] }
    end
    -- One event per syscall: the largest is close to MaxEventSize.
    emit_on(0, entries, 1)
    local rows = wait_count(event_type, #sizes)
    t:assert_eq(#rows, #sizes, "every event was stored once")
    for i, r in ipairs(rows) do
        t:assert_eq(r.payload, eventd.hex(payloads[i], true),
            "event " .. i .. " (" .. sizes[i] .. "-byte blob) came back byte-exact, " ..
            "no shorter and no longer")
        if i > 1 then
            t:assert_eq(r.sequence, rows[i - 1].sequence + 1,
                "and directly after its predecessor: each advance was exactly one event")
        end
    end
end)

test("an idle drain thread and an idle writer sleep rather than spin", {
    spec = "eventd *kmes.an-empty-buffer-is-waited-on-through-the-futex-not-by-spinning"
        .. " eventd *batch.an-idle-writer-sleeps-until-a-producer-wakes-it",
}, function(t)
    local ths, pid = eventd.threads(vm)
    local function ticks()
        local out = {}
        for _, th in ipairs(ths) do
            if th.comm:find("^eventd%-drain") or th.comm:find("^eventd%-writer") then
                local stat = vm:read_file("/proc/" .. pid .. "/task/" .. th.tid .. "/stat")
                local after = stat:match("%) (.*)$")
                local f = {}
                for x in after:gmatch("%S+") do f[#f + 1] = x end
                -- fields 14 and 15 overall; 12 and 13 after "pid (comm)"
                out[th.tid] = { comm = th.comm, state = f[1],
                                ticks = tonumber(f[12]) + tonumber(f[13]) }
            end
        end
        return out
    end
    local a = ticks()
    vm:run("sleep 3")
    local b = ticks()
    local n = 0
    for tid, x in pairs(a) do
        n = n + 1
        t:assert(b[tid].ticks - x.ticks <= 5,
            x.comm .. " used " .. (b[tid].ticks - x.ticks) .. " ticks in 3 idle seconds")
        local wchan = vm:read_file("/proc/" .. pid .. "/task/" .. tid .. "/wchan")
        t:assert(wchan:find("futex", 1, true),
            x.comm .. " is asleep on a futex: " .. wchan)
    end
    t:assert(n >= 4, "both drain threads and both writers were sampled: " .. n)

    -- And a producer wakes them: the sleeping pair stores a new event.
    local event_type = "pt.kmes." .. eventd.marker("wake")
    emit_on(1, same(event_type, 1))
    t:assert_eq(#wait_count(event_type, 1, 5), 1, "an emitted event wakes them and is stored")
end)

-- ---------------------------------------------------------------------------
-- Restart
-- ---------------------------------------------------------------------------

test("after a restart, receipted survivors are skipped and the rest are ingested", {
    spec = "eventd *kmes.restart-skips-covered-survivors-and-re-ingests-uncovered-ones",
}, function(t)
    local committed = "pt.kmes." .. eventd.marker("covered")
    local missed = "pt.kmes." .. eventd.marker("uncovered")
    local gaps_before = { #gaps_on(0), #gaps_on(1) }
    emit_on(0, same(committed, 50))
    emit_on(1, same(committed, 50))
    wait_count(committed, 100)

    eventd.stop(vm)
    -- Emitted with nothing reading: in the ring, behind the receipts.
    emit_on(0, same(missed, 50))
    emit_on(1, same(missed, 50))
    eventd.start(vm)

    local rows = wait_count(missed, 100)
    t:assert_eq(#rows, 100, "every event emitted while eventd was down was ingested")
    t:assert_eq(#stored(committed), 100,
        "and the receipted ones, still in the ring too, were not ingested again")
    for cpu = 0, 1 do
        t:assert_eq(#gaps_on(cpu), gaps_before[cpu + 1],
            "no gap was recorded for CPU " .. cpu .. ": everything was in the ring or receipted")
    end
end)

-- ---------------------------------------------------------------------------
-- Generation changes (these reshape the rings: keep them last, in order)
-- ---------------------------------------------------------------------------

test("a ring replacement is followed: the old ring is drained to its end, the new one from the next sequence", {
    spec = "eventd *kmes.a-drain-thread-checks-the-generation-after-each-cycle-and-follows-a-change"
        .. " eventd *kmes.generation-change-records-the-last-handed-off-sequence"
        .. " eventd *kmes.generation-change-drains-the-old-buffer-to-its-frozen-write-pos"
        .. " eventd *kmes.generation-change-reattaches-and-maps-the-replacement-while-the-old-mapping-is-valid"
        .. " eventd *kmes.generation-change-resumes-at-the-first-sequence-after-the-last-drained"
        .. " eventd *kmes.generation-change-unmaps-the-old-buffer-only-after-the-scan-succeeds"
        .. " eventd *kmes.generation-change-resumes-draining-from-the-new-buffer"
        .. " eventd *kmes.each-drain-thread-handles-a-generation-change-independently",
}, function(t)
    local pid = eventd.pid(vm)
    local before = { "pt.kmes." .. eventd.marker("old0"), "pt.kmes." .. eventd.marker("old1") }
    local after = { "pt.kmes." .. eventd.marker("new0"), "pt.kmes." .. eventd.marker("new1") }
    local gaps_before = { #gaps_on(0), #gaps_on(1) }

    eventd.freeze(vm)
    local ok, err = pcall(function()
        -- 1500 small events per CPU: ~130 KB, all of it in the 4 MiB ring,
        -- twice what the 64 KiB replacement can carry over.
        emit_on(0, same(before[1], 1500))
        emit_on(1, same(before[2], 1500))
        set_capacity(65536)
        emit_on(0, same(after[1], 20))
        emit_on(1, same(after[2], 20))
    end)
    eventd.thaw(vm)
    if not ok then error(err, 0) end

    for cpu = 0, 1 do
        local old = wait_count(before[cpu + 1], 1500)
        local new = wait_count(after[cpu + 1], 20)
        local distinct = {}
        for _, r in ipairs(old) do distinct[r.sequence] = true end
        local n = 0
        for _ in pairs(distinct) do n = n + 1 end
        t:assert_eq(#old, 1500,
            "CPU " .. cpu .. ": all 1500 pre-swap events stored once, though the replacement " ..
            "kept only the newest of them and also holds copies of those")
        t:assert_eq(n, 1500, "CPU " .. cpu .. ": with no duplicated sequence")
        t:assert_eq(#new, 20, "CPU " .. cpu .. ": the post-swap events were read from the new ring")
        t:assert_eq(#gaps_on(cpu), gaps_before[cpu + 1], "CPU " .. cpu .. ": and nothing was lost")
    end
    t:assert_eq(eventd.pid(vm), pid, "the same eventd process followed the change")
    t:assert_eq(kmes_fds(), 2, "holding exactly one ring descriptor per CPU: the old ones were closed")
end)

test("a replacement ring of a different size is read correctly across its wrap point", {
    spec = "eventd *kmes.each-descriptor-is-mapped-at-the-size-derived-from-its-capacity",
}, function(t)
    -- The rings are now 64 KiB. ~600-byte distinct events, 400 of them,
    -- wrap the ring about four times; any mapping-size error shows as a
    -- garbled payload at the wrap.
    local event_type = "pt.kmes." .. eventd.marker("wrap")
    local entries, payloads = {}, {}
    for i = 1, 400 do
        payloads[i] = eventd.msgpack({ i = i, b = eventd.bin(string.rep(string.char(33 + i % 90), 600)) })
        entries[i] = { type = event_type, payload = payloads[i] }
    end
    emit_on(0, entries, 32)
    local rows = wait_count(event_type, 1)
    vm:run("sleep 2")
    rows = stored(event_type)
    t:assert(#rows >= 100, "the events were stored: " .. #rows)
    local first = rows[1].sequence
    for _, r in ipairs(rows) do
        local i = r.sequence - first + 1
        t:assert_eq(r.payload, eventd.hex(payloads[i], true), "sequence " .. r.sequence .. " is byte-exact")
    end
end)

test("each CPU keeps its own last sequence: a loss on one CPU is a gap on that CPU, in its shard", {
    spec = "eventd *kmes.each-drain-thread-holds-the-last-sequence-seen-for-its-cpu"
        .. " eventd *synthetic.a-gap-record-goes-to-a-shard-of-the-cpu-that-generated-it",
}, function(t)
    local marker = "pt.kmes." .. eventd.marker("last1")
    emit_on(1, same(marker, 1))
    local last = wait_count(marker, 1)[1]
    local gaps0 = #gaps_on(0)

    eventd.freeze(vm)
    local ok, err = pcall(function()
        emit_on(1, same("pt.kmes." .. eventd.marker("lap1"), 1500))
    end)
    eventd.thaw(vm)
    if not ok then error(err, 0) end

    local g
    wait_until(function()
        for _, x in ipairs(gaps_on(1)) do
            if x["loss.sequence"] > last.sequence then g = x end
        end
        return g ~= nil
    end, { timeout = 30, interval = 0.5, desc = "a gap on CPU 1" })
    t:assert_eq(g["buffer.cpu"], 1, "the record names CPU 1's ring as buffer.cpu")
    -- The last sequence CPU 1's thread saw is CPU 1's, whatever CPU 0 did.
    local seen = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard, string.format(
            "SELECT max(sequence) FROM events WHERE cpu_id = 1 AND sequence < %d", g["loss.sequence"]))) do
            if r[1] then seen[#seen + 1] = r[1] end
        end
    end
    table.sort(seen)
    t:assert_eq(g["loss.sequence"], seen[#seen] + 1,
        "the gap starts right after the last CPU-1 sequence eventd stored")
    t:assert_eq(#gaps_on(0), gaps0, "and CPU 0 recorded no gap")

    local in_shard
    for _, shard in ipairs(eventd.shards(vm)) do
        local r = eventd.sql(vm, shard, string.format(
            "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.gap .. "' AND cpu_id = 1 " ..
            "AND timestamp = %d", g["event.time"]))
        if r[1][1] > 0 then in_shard = shard end
    end
    local cpu1_shard = stored(marker)[1].shard
    t:assert_eq(in_shard, cpu1_shard, "the gap is in the shard CPU 1's events go to")
end)

test("events torn by a concurrent overwrite are never stored garbled", {
    spec = "eventd *kmes.tail-pos-is-re-read-after-each-event-to-detect-a-torn-read",
}, function(t)
    -- The rings are 64 KiB. The emitter is pinned to CPU 0 and CPU 0's
    -- drain thread to CPU 1, so the producer overwrites the ring while the
    -- consumer is copying out of it, on another CPU, with no pause between.
    local ths = eventd.threads(vm)
    local drain
    for _, th in ipairs(ths) do
        if th.comm == "eventd-drain-0" then drain = th.tid end
    end
    t:assert(pin(vm, 1, drain), "CPU 0's drain thread is moved to CPU 1")
    local event_type = "pt.kmes." .. eventd.marker("torn")
    local n = 3200
    local entries, payloads = {}, {}
    for i = 1, n do
        payloads[i] = eventd.msgpack({ i = i, b = eventd.bin(string.rep(string.char(33 + i % 90), 900)) })
        entries[i] = { type = event_type, payload = payloads[i] }
    end
    local ok, err = pcall(emit_on, 0, entries, 32)
    -- Free the drain thread to run on either CPU again.
    vm:syscall(sys.NR.sched_setaffinity, { args = { drain, 8, 0 },
        bufs = { string.pack("<I8", 3) }, ptrs = { 2 } })
    if not ok then error(err, 0) end
    vm:run("sleep 3")
    local rows = stored(event_type)
    t:assert(#rows > 0, "some events were stored: " .. #rows)
    local emitted = {}
    for i = 1, n do emitted[eventd.hex(payloads[i], true)] = true end
    local bad = 0
    for _, r in ipairs(rows) do
        if not emitted[r.payload] then bad = bad + 1 end
    end
    t:assert_eq(bad, 0, "every stored payload is exactly one emitted event's, never a mix of " ..
        "two (" .. #rows .. " stored of " .. n .. ")")
end)

test("events a shrink discards before eventd reads them are recorded as an ordinary gap", {
    spec = "eventd *kmes.survivors-discarded-by-a-shrink-appear-as-an-ordinary-gap",
}, function(t)
    set_capacity(4194304)
    local marker = "pt.kmes." .. eventd.marker("pre")
    emit_on(0, same(marker, 1))
    local last = wait_count(marker, 1)[1]

    eventd.stop(vm)
    local flood = "pt.kmes." .. eventd.marker("shrunk")
    -- All 1500 fit in the 4 MiB ring; the 64 KiB replacement keeps the
    -- newest few hundred.
    emit_on(0, same(flood, 1500))
    set_capacity(65536)
    eventd.start(vm)

    local g
    wait_until(function()
        for _, x in ipairs(gaps_on(0)) do
            if x["loss.sequence"] > last.sequence then g = x end
        end
        return g ~= nil
    end, { timeout = 30, interval = 0.5, desc = "a gap after the shrink" })
    local rows = stored(flood)
    local first, last_lost = g["loss.sequence"], g["loss.sequence-last"]
    t:assert(#rows > 0 and #rows < 1500, "only the newest survivors were stored: " .. #rows)
    t:assert_eq(last_lost + 1, rows[1].sequence,
        "the gap ends at the oldest event the shrink kept")
    t:assert(first <= rows[1].sequence - (1500 - #rows),
        "and covers every flood event the shrink discarded")
    t:assert_eq(g["loss.count"], last_lost - first + 1, "as an ordinary gap record")
end)

-- ---------------------------------------------------------------------------
-- The privilege
-- ---------------------------------------------------------------------------

test("without SeSecurityPrivilege in its token eventd cannot attach and fails to start", {
    spec = "eventd *kmes.attachment-requires-sesecurityprivilege",
}, function(t)
    -- The service definition's RequiredPrivileges, overridden by a later
    -- seed, without SeSecurityPrivilege. Everything else about eventd is
    -- unchanged, so the attach is what fails; eventd's ErrorControl is
    -- Critical, so its failure to start sends the boot to recovery.
    local seed = peinit.seed("zz-pt-eventd-nosec", {
        { path = [[Machine\System]] },
        { path = [[Machine\System\Services]] },
        { path = [[Machine\System\Services\eventd]], values = {
            { name = "RequiredPrivileges", type = "multi",
              data = { "SeChangeNotifyPrivilege", "SeAuditPrivilege" } },
        } },
    })
    -- No stage wait: the agent is up before Phase 2, and the window before
    -- recovery is what the assertions need.
    local nosec = peinit.boot({ name = "ev-kmes-nosec", cpus = 2,
        files = peinit.merge(seed, eventd.override_files()), stage = false })
    -- Assert quickly: once eventd's restarts are exhausted, the Critical
    -- policy takes the machine (and this agent) to recovery.
    local status
    wait_until(function()
        local r = nosec:run("svctl --json status eventd")
        if r.exit_code ~= 0 then return false end
        status = json.decode(r.stdout)
        return status.cause == "process_crash"
    end, { timeout = 90, interval = 0.5, desc = "eventd to fail its start" })
    t:assert(status.state ~= "active", "eventd never became active: " .. tostring(status.state))
    -- It got no further than attaching: the metadata database and the
    -- shards, opened immediately after (pipeline.rs:53-84), were never made,
    -- while the store directories, checked just before, were there.
    local ls = nosec:run("ls /var/state/eventd/events")
    t:assert_eq(ls.exit_code, 0, "the event store directory exists: " .. ls.stderr)
    t:assert(not ls.stdout:find("eventd-meta.db", 1, true) and not ls.stdout:find("shard-", 1, true),
        "nothing after the attach was reached: " .. ls.stdout)
end)

-- ---------------------------------------------------------------------------
-- Unreachable from a guest
-- ---------------------------------------------------------------------------

-- Route closed: a hole needs a sparse possible-CPU mask, and a VM's
-- possible CPUs are always dense 0..n-1 (KMES §2.4: holes occur only when
-- nr_cpu_ids and the possible mask disagree); offline-but-possible CPUs
-- still have attachable rings. `kmes::attach_all` walks the slots through
-- `attach_slots`, which the unit test drives with a stand-in slot source.
test("an EINVAL slot is skipped and enumeration continues past it", {
    spec = "eventd *kmes.an-einval-slot-is-a-hole-and-enumeration-continues",
    skip = true,
    covered_by = "cargo:eventd eventd kmes::tests::an_einval_slot_is_a_hole_and_enumeration_continues_past_it",
}, function() end)

-- Route closed: as above, a VM's CPU IDs are dense, so ordinal and logical
-- ID coincide and the distinction cannot be seen. pipeline.rs:277 numbers
-- attachments by position and keeps `attachment.cpu_id` for every external
-- use.
test("sparse logical CPU IDs get dense ordinals for routing but are stored as themselves", {
    spec = "eventd *kmes.sparse-cpus-get-dense-ordinals-internally-but-keep-their-logical-cpu-id-externally",
    skip = true,
    covered_by = "cargo:eventd eventd pipeline::tests::sparse_cpus_route_by_dense_ordinal_but_store_their_logical_id",
}, function() end)

-- Route closed: every Peios kernel has at least one CPU and KMES gives
-- each possible CPU a ring at init, so no guest sees zero attachable rings.
test("finding no attachable ring fails startup", {
    spec = "eventd *kmes.discovering-zero-cpus-is-a-startup-failure",
    skip = true,
    covered_by = "cargo:eventd eventd kmes::tests::discovering_no_attachable_buffer_fails_startup",
}, function() end)

-- Route closed: KMES never re-issues a sequence within a boot, so a guest
-- cannot put a lower, unreceipted sequence in front of the drain thread.
-- The check is `Reconciler::observe` (eventd-core reconcile.rs), surfaced
-- by the drain as a fatal KmesError::Sequence (kmes.rs `drain`); the unit
-- test covers the regression branch of `observe`.
test("a sequence regression under the same boot ID stops eventd", {
    spec = "eventd *kmes.a-sequence-regression-under-the-same-boot-id-stops-eventd",
    skip = true,
    covered_by = "cargo:eventd eventd-core reconcile::tests::an_uncovered_sequence_below_the_next_expected_one_is_a_regression",
}, function() end)
