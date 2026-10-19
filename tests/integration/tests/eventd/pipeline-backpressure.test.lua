-- eventd TRM §2.1 (the pipeline) and the backpressure half of §2.3 (the
-- handoff channel) and §2.2 (copying): drain → detect → hand off → write,
-- with the KMES ring as the only buffer, so a writer that falls behind
-- stops the drain thread and the loss shows up as a gap rather than as
-- memory.
--
-- One file-scope VM on one vCPU with the KMES ring seeded to its 64 KiB
-- minimum, so the ring overruns after a few hundred events.
--
-- The lever is a starved writer with a live drain thread. The shard's
-- writer thread is moved to SCHED_IDLE and two busy-loop shells are
-- started at normal priority: on one vCPU the writer then gets almost no
-- CPU while the drain thread (also normal priority) keeps running. A
-- flood from the agent then fills the handoff channel and the ring. If
-- the drain thread could buffer without bound nothing would be lost; that
-- something is lost, and that every lost sequence is inside a gap record,
-- is the behaviour under test. Each starvation is undone in a guarded
-- cleanup (hogs killed, the writer returned to SCHED_OTHER) so later tests
-- see a normal eventd.
--
-- The channel's actual slot and byte bounds are internal and the book
-- gives no number for them, so nothing here asserts one: a bound is shown
-- by comparing two floods that differ only in the property under test.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(1)

local RING = 65536
local SCHED_OTHER, SCHED_FIFO, SCHED_IDLE = 0, 1, 5

local vm = eventd.boot({
    name = "ev-pipe",
    config = {},
    config_keys = { {
        path = [[Machine\System\KMES]],
        values = { { name = "BufferCapacity", type = "qword", data = RING } },
    } },
})

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function thread_named(prefix)
    for _, th in ipairs(eventd.threads(vm)) do
        if th.comm:sub(1, #prefix) == prefix then return th.tid end
    end
end

--- Run `fn` with the shard writer starved of CPU and the drain thread not.
---
--- The drain thread is also raised to SCHED_FIFO, so it runs the moment a
--- KMES emission wakes it: the only thing that can then stop it reading
--- is the channel, never a lack of CPU. Without that, an emitter on the
--- same vCPU could overrun the ring before the drain thread was scheduled
--- at all, and a loss would say nothing about the channel.
local function starved(fn)
    local writer = assert(thread_named("eventd-writer"), "eventd has a writer thread")
    local drain = assert(thread_named("eventd-drain-0"), "eventd has CPU 0's drain thread")
    eventd.set_policy(vm, writer, SCHED_IDLE)
    eventd.set_policy(vm, drain, SCHED_FIFO, 1)
    local hogs = {}
    local ok, err = pcall(function()
        for _ = 1, 2 do
            local r = vm:run("sh -c 'while :; do :; done' >/dev/null 2>&1 & echo $!")
            hogs[#hogs + 1] = assert(r.stdout:match("(%d+)"), "a busy loop started: " .. r.stdout)
        end
        fn()
    end)
    for _, pid in ipairs(hogs) do vm:run("kill -9 " .. pid) end
    eventd.set_policy(vm, writer, SCHED_OTHER)
    eventd.set_policy(vm, drain, SCHED_OTHER)
    if not ok then error(err, 0) end
end

--- Emit `n` events of `event_type` carrying `payload` (raw MessagePack),
--- `per` to a batch (default 256). One batch must fit in the ring, or the
--- syscall overwrites its own events before any reader can run.
local function flood(event_type, n, payload, per)
    payload = payload or kmes.PAYLOAD
    per = per or 256
    local sent = 0
    while sent < n do
        local k = math.min(per, n - sent)
        local batch = {}
        for i = 1, k do batch[i] = { type = event_type, payload = payload } end
        local r = kmes.emit_batch(vm, batch)
        assert(r.ret == 0 and r.emitted == k,
            "emit_batch: ret " .. tostring(r.ret) .. " emitted " .. tostring(r.emitted) ..
            " errno " .. tostring(r.errno))
        sent = sent + k
    end
end

--- The oldest and newest event in CPU 0's ring, read through the agent's
--- own attachment.
local function ring_span()
    local ring = assert(kmes.attach(vm, 0))
    ring.cursor = 0
    local events = kmes.drain(ring)
    kmes.detach(ring)
    assert(#events > 0, "the ring holds events")
    return events[1], events[#events]
end

--- Stored sequences of one event type: a sorted list.
local function stored_sequences(event_type)
    local out = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard, string.format(
            "SELECT sequence FROM events WHERE event_type = '%s'", event_type))) do
            out[#out + 1] = r[1]
        end
    end
    table.sort(out)
    return out
end

--- Every CPU-0 sequence in [lo, hi] is either a stored row or inside a
--- gap record. Returns the number of sequences inside gaps, and the first
--- sequence that is neither (nil when all are accounted for).
local function account(lo, hi)
    local seen = {}
    local gaps = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard, string.format(
            "SELECT sequence FROM events WHERE cpu_id = 0 AND sequence BETWEEN %d AND %d", lo, hi))) do
            seen[r[1]] = true
        end
    end
    for _, q in ipairs(eventd.rows(vm, string.format(
        "EVENTS %s WHERE event.cpu == 0 SINCE 1h ago TAKE 10000", eventd.T.gap))) do
        gaps[#gaps + 1] = { q["loss.sequence"], q["loss.sequence-last"] }
    end
    local lost, hole, first_lost = 0, nil, nil
    for s = lo, hi do
        local in_gap = false
        for _, g in ipairs(gaps) do
            if s >= g[1] and s <= g[2] then in_gap = true; break end
        end
        if in_gap then
            lost = lost + 1
            first_lost = first_lost or s
        elseif not seen[s] then hole = hole or s end
    end
    return lost, hole, first_lost
end

--- Wait until the newest event of a flood is stored.
local function wait_stored(event_type, sequence)
    wait_until(function()
        local seqs = stored_sequences(event_type)
        return #seqs > 0 and seqs[#seqs] >= sequence
    end, { timeout = 60, interval = 0.5,
           desc = event_type .. " through sequence " .. sequence .. " to be stored" })
end

--- One starved flood: returns {first, last, stored, lost, hole}.
local function starved_flood(event_type, n, payload, during, per)
    local newest
    starved(function()
        flood(event_type, n, payload, per)
        if during then during() end
        newest = select(2, ring_span())
    end)
    wait_stored(event_type, newest.sequence)
    local seqs = stored_sequences(event_type)
    local lost, hole, first_lost = account(seqs[1], newest.sequence)
    return { first = seqs[1], last = newest.sequence, stored = #seqs, lost = lost, hole = hole,
             before_loss = first_lost and (first_lost - seqs[1]) or #seqs }
end

local function wait_config_change(key, new_value)
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago TAKE 1000",
        function(rs)
            for _, r in ipairs(rs) do
                if r["config.name"] == key and r["config.value"] == tonumber(new_value) then return true end
            end
            return false
        end, { desc = "a config change of " .. key .. " to " .. tostring(new_value) })
end

-- ---------------------------------------------------------------------------
-- The pipeline
-- ---------------------------------------------------------------------------

test("a KMES event reaches a committed shard row through a drain thread and a writer thread", {
    spec = "eventd *pipeline.events-travel-from-the-ring-buffers-to-a-committed-row-in-four-stages",
}, function(t)
    local names = {}
    for _, th in ipairs(eventd.threads(vm)) do names[th.comm] = true end
    t:assert(names["eventd-drain-0"], "a drain thread reads CPU 0's ring")
    t:assert(thread_named("eventd-writer"), "and a writer thread owns the shard")

    local event_type = "pt.pipe." .. eventd.marker("flow")
    local r = eventd.emit(vm, event_type, { n = 1 })
    t:assert_eq(r.ret, 0, "the event is emitted into KMES")
    local rows = eventd.wait_rows(vm, "EVENTS " .. event_type .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    local committed = eventd.sql(vm, eventd.shards(vm)[1], string.format(
        "SELECT sequence, cpu_id FROM events WHERE event_type = '%s'", event_type))
    t:assert_eq(#committed, 1, "it is a committed row in the shard database")
    t:assert_eq(committed[1][1], rows[1]["event.sequence"], "carrying the KMES sequence it was read with")
end)

-- ---------------------------------------------------------------------------
-- Backpressure
-- ---------------------------------------------------------------------------

test("a starved writer stops the drain thread, and the overrun is recorded rather than buffered", {
    spec = "eventd *pipeline.the-ring-buffers-are-the-only-buffer"
        .. " eventd *pipeline.backpressure-propagates-from-the-writer-through-the-channel-to-the-ring-buffer"
        .. " eventd *pipeline.a-full-ring-buffer-loses-events-visibly-rather-than-buffering-without-bound"
        .. " eventd *shard.each-writer-thread-has-one-bounded-handoff-channel"
        .. " eventd *shard.channel-pressure-never-drops-events-or-grows-the-channel"
        .. " eventd *shard.the-drain-thread-resumes-as-soon-as-the-channel-has-room"
        .. " eventd *kmes.without-both-reservations-the-event-stays-in-kmes-and-the-drain-waits",
}, function(t)
    local event_type = "pt.pipe." .. eventd.marker("slot")
    local emitted = 10240
    local oldest, newest
    starved(function()
        flood(event_type, emitted)
        -- What is still in KMES while the writer is starved.
        oldest, newest = ring_span()
    end)
    t:assert_eq(newest.type, event_type, "the flood's newest event is in the ring")

    -- Released: the drain thread picks up where it stopped.
    wait_stored(event_type, newest.sequence)
    local seqs = stored_sequences(event_type)
    local lost, hole = account(seqs[1], newest.sequence)

    t:assert(lost > 0,
        "events were lost although the drain thread was never starved: it stopped " ..
        "reading when the channel filled, and the ring overran (" .. #seqs .. " of " ..
        emitted .. " stored)")
    t:assert(hole == nil,
        "every sequence of the flood is either stored or inside a gap record; first " ..
        "unaccounted: " .. tostring(hole))
    t:assert_eq(#seqs + lost, emitted,
        "stored plus recorded-lost is exactly what was emitted: nothing dropped silently")
    local survivors = 0
    for _, s in ipairs(seqs) do
        if s >= oldest.sequence then survivors = survivors + 1 end
    end
    t:assert_eq(survivors, newest.sequence - oldest.sequence + 1,
        "every event that was waiting in the ring when the writer recovered was stored")
end)

test("the byte bound stops the drain thread too: large events overrun where small ones do not", {
    spec = "eventd *shard.a-drain-thread-stops-reading-when-either-channel-bound-would-be-exceeded"
        .. " eventd *kmes.a-slot-and-the-events-bytes-are-reserved-before-copying-and-advancing",
}, function(t)
    -- A 64 KiB ring holds two ~30 KB events, too few for the drain thread
    -- to be sure of keeping up with the emitter; for this test the ring is
    -- 4 MiB (about 135 of them), and back to 64 KiB afterwards.
    local function set_ring(bytes)
        vm:run(string.format([[reg set 'Machine\System\KMES' BufferCapacity qword:%d]], bytes)):assert_ok()
        wait_until(function()
            local ring = kmes.attach(vm, 0)
            if not ring then return false end
            local cap = ring.capacity
            kmes.detach(ring)
            return cap == bytes
        end, { timeout = 30, interval = 0.5, desc = "the ring at " .. bytes .. " bytes" })
    end
    local n = 1000
    set_ring(4194304)
    local ok, err = pcall(function()
        -- Control: the same count of small events, which the slot bound absorbs.
        local small = starved_flood("pt.pipe." .. eventd.marker("small"), n)
        t:assert_eq(small.lost, 0, "a starved writer absorbs " .. n .. " small events without loss")
        t:assert_eq(small.stored, n, "all of them are stored")

        -- ~30 KB each: the byte reservation fails long before the slot
        -- reservation would.
        local big_payload = eventd.msgpack(eventd.bin(string.rep("x", 30000)))
        local big = starved_flood("pt.pipe." .. eventd.marker("big"), n, big_payload, nil, 16)
        t:assert(big.lost > 0,
            "the same count of ~30 KB events overruns: the channel's byte bound stopped " ..
            "the drain thread (" .. big.stored .. " stored)")
        t:assert(big.before_loss > 200,
            "the loss began only after " .. big.before_loss .. " of them had been taken, more " ..
            "than the ring holds: the channel absorbed them until its byte bound refused more")
        t:assert(big.hole == nil, "and every lost sequence is inside a gap: " .. tostring(big.hole))
        t:assert_eq(big.stored + big.lost, n, "stored plus recorded-lost is what was emitted")
    end)
    set_ring(65536)
    if not ok then error(err, 0) end
end)

test("events waiting in the channel are copies: overwriting the ring under them changes nothing", {
    spec = "eventd *kmes.nothing-derived-from-the-mapped-region-reaches-a-writer-thread",
}, function(t)
    -- 2000 distinct ~200-byte events with the writer starved: the first
    -- ones wait in the channel while the ring wraps many times over the
    -- slots they were read from. If the channel carried pointers into the
    -- ring, they would come out as later events' bytes.
    local event_type = "pt.pipe." .. eventd.marker("copy")
    local payloads = {}
    for i = 1, 2000 do
        payloads[i] = eventd.msgpack({ i = i, b = eventd.bin(string.rep(string.char(65 + i % 26), 200)) })
    end
    local newest
    starved(function()
        local i = 1
        while i <= #payloads do
            local batch = {}
            for j = i, math.min(i + 63, #payloads) do
                batch[#batch + 1] = { type = event_type, payload = payloads[j] }
            end
            local r = kmes.emit_batch(vm, batch)
            assert(r.ret == 0 and r.emitted == #batch, "emit_batch")
            i = i + #batch
        end
        newest = select(2, ring_span())
    end)
    wait_stored(event_type, newest.sequence)
    local want = {}
    for i, p in ipairs(payloads) do
        want[(p:gsub(".", function(c) return string.format("%02X", c:byte()) end))] = i
    end
    local rows = eventd.sql(vm, eventd.shards(vm)[1], string.format(
        "SELECT sequence, hex(payload) FROM events WHERE event_type = '%s' ORDER BY sequence", event_type))
    t:assert(#rows > 64, "events were stored: " .. #rows)
    local first_index = want[rows[1][2]]
    t:assert(first_index, "the first stored payload is one that was emitted")
    for _, r in ipairs(rows) do
        local i = want[r[2]]
        t:assert(i, "sequence " .. r[1] .. " carries an emitted payload, byte for byte")
        t:assert_eq(i - first_index, r[1] - rows[1][1],
            "and the one emitted at its own position in the sequence")
    end
    t:assert_eq(#rows, #payloads, "every event was held in the channel and stored")
    t:assert(#payloads * #payloads[1] > 4 * RING,
        "while the ring they were read from was rewritten several times over")
end)

test("the channel is not sized from MaxBatchSize, and changing it live does not touch the channel", {
    spec = "eventd *shard.channel-capacity-is-fixed-at-startup-in-slots-and-bytes-independent-of-max-batch-size"
        .. " eventd *shard.changing-max-batch-size-never-resizes-or-replaces-a-live-channel",
}, function(t)
    local ok, err = pcall(function()
        eventd.set(vm, "MaxBatchSize", "dword:100"):assert_ok()
        wait_config_change("MaxBatchSize", "100")

        -- With the smallest batch, a starved writer still absorbs far more
        -- than a batch; and halfway through, with the channel full, the
        -- setting changes to the largest.
        local a = starved_flood("pt.pipe." .. eventd.marker("b100"), 10240, nil, function()
            eventd.set(vm, "MaxBatchSize", "dword:100000"):assert_ok()
            vm:run("sleep 1")
        end)
        wait_config_change("MaxBatchSize", "100000")
        t:assert(a.stored > 1000,
            "with MaxBatchSize 100 the channel still absorbed " .. a.stored .. " events")
        t:assert(a.hole == nil,
            "and the live change dropped nothing in flight: every sequence is stored or " ..
            "in a gap; first unaccounted " .. tostring(a.hole))

        -- With the largest batch, a flood a tenth of it still overruns.
        local b = starved_flood("pt.pipe." .. eventd.marker("b100k"), 10240)
        t:assert(b.lost > 0,
            "with MaxBatchSize 100000 a 10240-event flood still overruns: the channel " ..
            "did not grow to the batch size (" .. b.stored .. " stored)")
        t:assert(b.hole == nil, "and the loss is recorded: " .. tostring(b.hole))
    end)
    eventd.unset(vm, "MaxBatchSize")
    if not ok then error(err, 0) end
end)
