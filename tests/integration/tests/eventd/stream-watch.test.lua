-- eventd TRM §6.6 — the streaming machinery: commit generations and
-- wakes, what a restart does to a stream, the DISTINCT seen set,
-- cross-type conditions in the watch phase, and backpressure.
--
-- Streams are driven with the raw query client below rather than evctl:
-- a test needs to see each frame as it arrives, to stop reading on
-- purpose, and to see how a stream ends. One file-scope VM, one vCPU;
-- the cross-shard wake (§6.6, "re-examines every shard") needs two
-- shards and is in fanout-shards.test.lua.
--
-- Watch-phase cross-type conditions are judged per commit batch, so the
-- batch is made deliberately: one log datagram carrying several records
-- with explicit timestamps arrives as one commit, and the records'
-- timestamps are chosen against metric samples and events whose
-- timestamps are known.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-stream" })

-- The raw query channel (eventd.rq), for what evctl hides: each frame as
-- it arrives, a reader that stops reading, and how a stream ends.
local rq = eventd.rq

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

--- Open a stream for `text` and read through its initial result set.
--- Returns the stream (with a worker of its own) and the initial records.
local function stream(text, seconds)
    local w = vm:spawn_worker()
    local c = rq.open(w)
    rq.timeout(c, seconds or 10)
    rq.send(c, text)
    local initial = {}
    while true do
        local f, why = rq.frame(c)
        assert(f, "the stream ended before watch: " .. tostring(why))
        if f.status == "watch" then break end
        assert(f.status == "ok", "the stream failed before watch: " .. json.encode(f))
        for _, r in ipairs(f.records) do initial[#initial + 1] = r end
    end
    return c, initial
end

local function finish(c)
    rq.close(c)
    c.w:kill(); c.w:join()
end

--- Records from the stream until `n` have arrived or a read times out.
--- Returns the records and the frame or reason that ended the reading.
local function take(c, n)
    local got = {}
    while #got < n do
        local f, why = rq.frame(c)
        if not f then return got, why end
        if f.status ~= "ok" then return got, f end
        for _, r in ipairs(f.records) do got[#got + 1] = r end
    end
    return got
end

local function emit(etype, payload)
    local r = eventd.emit(vm, etype, payload)
    assert(r.ret == 0, "kmes_emit: errno " .. tostring(r.errno))
end

local function logs(records)
    local r = eventd.send_log(vm, records)
    assert(r.ret and r.ret > 0, "log sendto: errno " .. tostring(r.errno))
end

local function field(rows, name)
    local out = {}
    for i, r in ipairs(rows) do out[i] = r[name] end
    return out
end

-- ---------------------------------------------------------------------------
-- Commit generations
-- ---------------------------------------------------------------------------

test("DISTINCT watch values follow first occurrence in commit order", {
    spec = "eventd *stream.watch-records-follow-store-order",
    tags = { "pei1237" },
}, function(t)
    local m = eventd.marker("distinctorder")
    local c = stream("LOGS FROM " .. m .. " DISTINCT message STREAM")
    logs({
        { origin = m, is_error = false, message = "Zulu" },
        { origin = m, is_error = false, message = "alpha" },
        { origin = m, is_error = false, message = "ZULU" },
        { origin = m, is_error = false, message = "middle" },
    })
    local got = take(c, 3)
    finish(c)
    t:assert_eq(table.concat(field(got, "message"), ","), "Zulu,alpha,middle",
        "first value representation and insertion order survive grouping")
end)

test("watch records retain commit order despite timestamps and initial sorting/pagination", {
    spec = "eventd *stream.watch-records-follow-store-order",
    tags = { "pei1237" },
}, function(t)
    local m = eventd.marker("order")
    local base = eventd.guest_ns(vm) - 60 * 1000000000
    logs({
        { origin = m, is_error = false, message = "a", timestamp = base },
        { origin = m, is_error = false, message = "b", timestamp = base + 1 },
        { origin = m, is_error = false, message = "c", timestamp = base + 2 },
    })
    eventd.wait_rows(vm, "LOGS FROM " .. m .. " SINCE 1h ago", function(rs) return #rs == 3 end)
    local c, initial = stream("LOGS FROM " .. m .. " SINCE 1h ago SORT message DESC SKIP 1 TAKE 1 STREAM")
    t:assert_eq(#initial, 1, "initial set is paginated")
    t:assert_eq(initial[1].message, "b", "initial set is sorted before pagination")
    logs({
        { origin = m, is_error = false, message = "first", timestamp = base + 20 },
        { origin = m, is_error = false, message = "second", timestamp = base + 10 },
        { origin = m, is_error = false, message = "third", timestamp = base + 30 },
    })
    local got = take(c, 3)
    finish(c)
    t:assert_eq(#got, 3, "watch ignores initial SKIP and TAKE")
    t:assert_eq(table.concat(field(got, "message"), ","), "first,second,third",
        "watch follows stored order, not timestamp or message sort")
end)

-- "eventd keeps a monotonic u64 commit generation counter for each
--  streamable store: one for the event store as a whole, and one for the
--  log store." / "Metric queries do not stream, so the metric store has
--  none."
test("an event stream and a log stream are each woken by their own store's commits; metrics do not stream", {
    spec = "eventd *stream.there-is-one-commit-generation-counter-for-the-event-store-and-one-for-the-log-store"
        .. " eventd *stream.the-metric-store-has-no-commit-generation-counter",
}, function(t)
    local m = eventd.marker("gen")
    local ev = stream("EVENTS pt.st.gen" .. m .. " STREAM")
    local lg = stream("LOGS FROM " .. m .. " STREAM")
    emit("pt.st.gen" .. m, { n = 1 })
    local got = take(ev, 1)
    t:assert_eq(#got, 1, "the event stream woke for the event commit")
    logs({ origin = m, is_error = false, message = "wake" })
    local lines = take(lg, 1)
    t:assert_eq(#lines, 1, "the log stream woke for the log commit")
    t:assert_eq(lines[1] and lines[1].message, "wake", "with the line")
    finish(ev); finish(lg)
    local r = eventd.query(vm, "METRIC pt" .. m .. " STREAM")
    t:assert_eq(r.exit_code, 1, "a metric stream is refused")
    t:assert(r.stderr:find("METRIC queries cannot stream", 1, true), r.stderr)
end)

-- "After a writer commits a batch it increments the counter for its store
--  and wakes the streaming handlers waiting on it."
test("each commit wakes the stream promptly", {
    spec = "eventd *stream.a-writer-increments-its-stores-generation-and-wakes-handlers-after-each-commit",
}, function(t)
    local etype = "pt.st.wake" .. eventd.marker()
    local c = stream("EVENTS " .. etype .. " STREAM")
    for i = 1, 3 do
        local t0 = os.clock()
        local w0 = os.time()
        emit(etype, { i = i })
        local got = take(c, 1)
        t:assert_eq(#got, 1, "commit " .. i .. " was delivered")
        t:assert(os.time() - w0 <= 2, "within a couple of seconds of the emit")
        local _ = t0
        os.execute("sleep 1")
    end
    finish(c)
end)

-- "A handler records the last generation it processed and waits until
--  the counter exceeds it."
test("across many commits every record is streamed exactly once", {
    spec = "eventd *stream.a-handler-waits-until-the-generation-exceeds-its-last-processed-one",
}, function(t)
    local etype = "pt.st.once" .. eventd.marker()
    local c = stream("EVENTS " .. etype .. " STREAM")
    for i = 1, 10 do
        emit(etype, { i = i })
        os.execute("sleep 0.15")
    end
    local got = take(c, 10)
    -- Anything more within two seconds would be a repeat.
    rq.timeout(c, 2)
    local extra = take(c, 1)
    finish(c)
    table.sort(got, function(a, b) return a.i < b.i end)
    t:assert_eq(json.encode(field(got, "i")), "[1,2,3,4,5,6,7,8,9,10]", "each once")
    t:assert_eq(#extra, 0, "and nothing twice")
end)

-- "Delivery latency is bounded below by the commit interval of the store
--  concerned, because a record is not streamable until it is committed."
test("a streamed record is already readable by an ordinary query", {
    spec = "eventd *stream.a-record-is-not-streamed-until-it-is-committed",
}, function(t)
    local etype = "pt.st.committed" .. eventd.marker()
    local c = stream("EVENTS " .. etype .. " STREAM")
    for i = 1, 3 do
        emit(etype, { i = i })
        local got = take(c, 1)
        t:assert_eq(#got, 1, "streamed")
        local q = eventd.query(vm, "EVENTS " .. etype .. " WHERE i == " .. i)
        t:assert(q.ok and #q.rows == 1, "the moment it streamed, a fresh query sees it committed")
    end
    finish(c)
end)

-- "The counter is process-local and never persisted; it has no meaning
--  across a restart, and a streaming query does not survive one."
test("a stream ends when eventd restarts, and its connection carries nothing after", {
    spec = "eventd *stream.the-generation-is-never-persisted-and-a-stream-does-not-survive-a-restart",
}, function(t)
    local etype = "pt.st.restart" .. eventd.marker()
    local c = stream("EVENTS " .. etype .. " STREAM", 60)
    eventd.restart(vm)
    emit(etype, { after = true })
    local got, ended = take(c, 1)
    finish(c)
    t:assert_eq(#got, 0, "the old stream delivers nothing from the new process")
    local how = type(ended) == "table" and (ended.status .. ": " .. tostring(ended.error)) or tostring(ended)
    t:assert(how == "eof" or how:find("^error"), "the stream ended (" .. how .. ")")
    -- A new stream works.
    local fresh, initial = stream("EVENTS " .. etype .. " STREAM")
    t:assert_eq(#initial, 1, "a new stream's initial set has the event")
    finish(fresh)
end)

-- "On wraparound the next increment is treated as a wake for every
--  handler and operation continues."
-- Route closed: the counter is u64 and starts at zero with each process
-- (commit_signal.rs:15-21); 2^64 commits are not reachable. The unit test
-- starts a signal at u64::MAX and wraps it under a waiting handler.
test("a commit that wraps the generation still wakes waiting handlers", {
    spec = "eventd *stream.generation-wraparound-is-treated-as-a-wake-for-every-handler",
    skip = true,
    covered_by = "cargo:eventd eventd commit_signal::tests::a_commit_that_wraps_the_generation_wakes_a_waiting_handler",
}, function() end)

-- ---------------------------------------------------------------------------
-- Latency
-- ---------------------------------------------------------------------------

-- "Events: MaxBatchLatencyMs, default 100 ms" as the approximate floor.
-- Documented skip: an approximate floor is not an assertable bound. The
-- adaptive batcher commits as soon as its input drains (§2.4), so under
-- the load a test can produce delivery comes in well under the cap, and
-- "converges on the cap under sustained load" names no measurable
-- threshold. What is assertable — nothing streams before it commits — is
-- tested above.
test("the event stream's latency floor is MaxBatchLatencyMs", {
    spec = "eventd *stream.the-event-stream-latency-floor-is-max-batch-latency-ms",
    skip = true,
}, function() end)

-- "Logs: LogMaxBatchLatencyMs, default 500 ms." Documented skip, for the
-- reason above.
test("the log stream's latency floor is LogMaxBatchLatencyMs", {
    spec = "eventd *stream.the-log-stream-latency-floor-is-log-max-batch-latency-ms",
    skip = true,
}, function() end)

-- ---------------------------------------------------------------------------
-- The DISTINCT seen set
-- ---------------------------------------------------------------------------

-- "A DISTINCT stream holds a per-query set of the values it has already
--  emitted, initialised from the initial result set and added to as new
--  values appear."
test("a DISTINCT stream emits only values neither in its initial set nor already streamed", {
    spec = "eventd *stream.a-distinct-streams-seen-set-starts-from-the-initial-result-set",
}, function(t)
    local etype = "pt.st.dis" .. eventd.marker()
    emit(etype, { v = "a" })
    emit(etype, { v = "b" })
    eventd.wait_rows(vm, "EVENTS " .. etype, function(rs) return #rs == 2 end)
    local c, initial = stream("EVENTS " .. etype .. " DISTINCT v STREAM")
    t:assert_eq(json.encode(field(initial, "v")), '["a","b"]', "the initial set")
    for _, v in ipairs({ "a", "c", "C", "b", "c", "d" }) do
        emit(etype, { v = v })
        os.execute("sleep 0.2")
    end
    local got = take(c, 2)
    rq.timeout(c, 2)
    local extra = take(c, 1)
    finish(c)
    t:assert_eq(json.encode(field(got, "v")), '["c","d"]', "only the new values, once each")
    t:assert_eq(#extra, 0, "nothing more: a, b (initial), C (= c) and c again were all seen")
end)

-- "It is bounded by MaxDistinctStreamValues, and exceeding the bound
--  terminates the query with an error rather than evicting."
test("past MaxDistinctStreamValues a DISTINCT stream ends with an error, having never re-emitted a value", {
    spec = "eventd *stream.exceeding-max-distinct-stream-values-terminates-the-query-without-eviction",
}, function(t)
    local etype = "pt.st.cap" .. eventd.marker()
    local function batch(from, to)
        local entries = {}
        for i = from, to do entries[#entries + 1] = { type = etype, payload = eventd.msgpack({ v = i }) } end
        local r = kmes.emit_batch(vm, entries)
        assert(r.ret == 0, "emit_batch: errno " .. tostring(r.errno))
    end
    eventd.set(vm, "MaxDistinctStreamValues", "dword:1000"):assert_ok()
    os.execute("sleep 1")
    local ok, err = pcall(function()
        local c = stream("EVENTS " .. etype .. " DISTINCT v STREAM", 20)
        -- 1000 values: exactly the bound.
        for k = 0, 3 do batch(k * 250 + 1, k * 250 + 250) end
        local got = take(c, 1000)
        t:assert_eq(#got, 1000, "a thousand distinct values streamed")
        -- An old value again is not news, and is not re-emitted.
        batch(1, 1)
        rq.timeout(c, 2)
        local again = take(c, 1)
        t:assert_eq(#again, 0, "a value already seen is not emitted again")
        -- The 1001st value overflows the set.
        rq.timeout(c, 10)
        batch(1001, 1001)
        local more, ended = take(c, 1)
        finish(c)
        t:assert_eq(#more, 0, "the overflowing value is not emitted")
        t:assert(type(ended) == "table" and ended.status == "error", "the stream ended with an error: "
            .. json.encode(ended))
        t:assert(tostring(ended and ended.error):find("seen-value limit", 1, true), json.encode(ended))
        -- Initialising past the bound fails the same way.
        local w = vm:spawn_worker()
        local c2 = rq.open(w)
        rq.timeout(c2, 20)
        rq.send(c2, "EVENTS " .. etype .. " DISTINCT v STREAM")
        local last
        repeat last = rq.frame(c2) until not last or last.status ~= "ok"
        rq.close(c2); w:kill(); w:join()
        t:assert_eq(last and last.status, "error", "an initial set of 1001 values is refused")
    end)
    eventd.unset(vm, "MaxDistinctStreamValues")
    if not ok then error(err, 0) end
end)

-- ---------------------------------------------------------------------------
-- Cross-type re-evaluation
-- ---------------------------------------------------------------------------

-- "Pre-computed cross-type ranges describe the past and are discarded
--  when the watch phase begins."
test("an existence condition false for the whole initial range still passes records in the watch", {
    spec = "eventd *stream.precomputed-cross-type-ranges-are-discarded-when-the-watch-begins",
}, function(t)
    local m = eventd.marker("xt")
    local etype = "pt.st.xt" .. m
    -- No such event exists, so the initial ranges are empty.
    local c, initial = stream("LOGS FROM " .. m .. " WHERE EVENT " .. etype .. " EXISTS SINCE 10m ago STREAM")
    t:assert_eq(#initial, 0, "nothing in the initial set")
    emit(etype, { n = 1 })
    eventd.wait_rows(vm, "EVENTS " .. etype, function(rs) return #rs == 1 end)
    logs({ origin = m, is_error = false, message = "beside the event" })
    local got = take(c, 1)
    finish(c)
    t:assert_eq(#got, 1, "the condition was evaluated afresh for the watch record")
end)

--- A gauge with value `a` at `base` and `b` at `base + 5 s`.
local function gauge(name, base, a, b)
    local r = eventd.send_metric(vm, {
        { name = name, type = "gauge", value = a, timestamp = base },
        { name = name, type = "gauge", value = b, timestamp = base + 5000000000 },
    })
    assert(r.ret > 0)
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 1h ago", function(rs) return #rs == 2 end)
end

-- "A metric condition costs one index seek per batch ... finding the
--  active sample at the batch's latest candidate timestamp."
test("a batch is judged by the metric's value at its latest record: rising past the threshold passes it all", {
    spec = "eventd *stream.a-metric-condition-is-evaluated-once-per-batch-at-its-latest-candidate-timestamp",
}, function(t)
    local m = eventd.marker("rise")
    local name = "pt" .. m
    local base = eventd.guest_ns(vm) - 60 * 1000000000
    gauge(name, base, 1, 10) -- below 5 until base+5s, above after
    local c = stream("LOGS FROM " .. m .. " WHERE METRIC " .. name .. " > 5 SINCE 10m ago STREAM")
    -- One datagram, one commit: a line while the gauge was 1, one while 10.
    logs({ { origin = m, is_error = false, message = "while low", timestamp = base + 1000000000 },
           { origin = m, is_error = false, message = "while high", timestamp = base + 6000000000 } })
    local got = take(c, 2)
    finish(c)
    table.sort(got, function(a, b) return a.timestamp < b.timestamp end)
    t:assert_eq(json.encode(field(got, "message")), '["while low","while high"]',
        "both lines pass: the batch was judged at its latest timestamp, where the gauge is 10")
    -- Control: outside the watch, each line is judged at its own time.
    local hist = eventd.rows(vm, "LOGS FROM " .. m .. " WHERE METRIC " .. name .. " > 5 SINCE 10m ago")
    t:assert_eq(json.encode(field(hist, "message")), '["while high"]', "control: per record, only the late line")
end)

-- "Records near a threshold crossing are included or excluded as a
--  group."
test("a batch whose latest record falls after the metric drops is excluded whole", {
    spec = "eventd *stream.records-in-one-batch-pass-or-fail-a-metric-condition-together",
}, function(t)
    local m = eventd.marker("fall")
    local name = "pt" .. m
    local base = eventd.guest_ns(vm) - 60 * 1000000000
    gauge(name, base, 10, 1) -- above 5 until base+5s, below after
    local c = stream("LOGS FROM " .. m .. " WHERE METRIC " .. name .. " > 5 SINCE 10m ago STREAM")
    logs({ { origin = m, is_error = false, message = "while high", timestamp = base + 1000000000 },
           { origin = m, is_error = false, message = "while low", timestamp = base + 6000000000 } })
    rq.timeout(c, 4)
    local got = take(c, 1)
    finish(c)
    t:assert_eq(#got, 0, "neither line passes, though one was written while the gauge was 10: "
        .. json.encode(got))
    local hist = eventd.rows(vm, "LOGS FROM " .. m .. " WHERE METRIC " .. name .. " > 5 SINCE 10m ago")
    t:assert_eq(json.encode(field(hist, "message")), '["while high"]', "control: per record, the early line passes")
end)

-- "An existence condition is evaluated per candidate record rather than
--  per batch."
test("in one batch, the line near the event passes and the line far from it does not", {
    spec = "eventd *stream.an-existence-condition-is-evaluated-per-candidate-record",
}, function(t)
    local m = eventd.marker("near")
    local etype = "pt.st.near" .. m
    eventd.set(vm, "CrossTypeWindowMs", "dword:1000"):assert_ok()
    os.execute("sleep 1")
    local ok, err = pcall(function()
        emit(etype, { n = 1 })
        local ev = eventd.wait_rows(vm, "EVENTS " .. etype, function(rs) return #rs == 1 end)
        local at = ev[1]["event.time"]
        local c = stream("LOGS FROM " .. m .. " WHERE EVENT " .. etype .. " EXISTS SINCE 10m ago STREAM")
        logs({ { origin = m, is_error = false, message = "near", timestamp = at + 100000000 },
               { origin = m, is_error = false, message = "far", timestamp = at - 30000000000 } })
        local got = take(c, 1)
        rq.timeout(c, 2)
        local extra = take(c, 1)
        finish(c)
        t:assert_eq(json.encode(field(got, "message")), '["near"]', "the line within the window passes")
        t:assert_eq(#extra, 0, "the line thirty seconds away, in the same batch, does not")
    end)
    eventd.unset(vm, "CrossTypeWindowMs")
    if not ok then error(err, 0) end
end)

-- ---------------------------------------------------------------------------
-- Backpressure
-- ---------------------------------------------------------------------------

-- "Backpressure is detected on the socket send buffer. When a result
--  message cannot be sent because the buffer is full, the query is
--  terminated immediately; eventd never blocks on the send."
test("a stream whose reader stops is dropped once the socket fills, while ingestion and queries carry on", {
    spec = "eventd *stream.backpressure-is-detected-on-the-socket-send-buffer"
        .. " eventd *stream.a-full-send-buffer-terminates-the-query-and-eventd-never-blocks",
}, function(t)
    local etype = "pt.st.bp" .. eventd.marker()
    local c = stream("EVENTS " .. etype .. " STREAM", 5)
    -- The reader now stops. 1.6 MB of matching events follow, several
    -- times a socket buffer.
    local pad = string.rep("p", 4000)
    for b = 0, 7 do
        local entries = {}
        for i = 1, 50 do entries[i] = { type = etype, payload = eventd.msgpack({ i = b * 50 + i, pad = pad }) } end
        local r = kmes.emit_batch(vm, entries)
        t:assert_eq(r.ret, 0, "emit batch " .. b)
        os.execute("sleep 0.2")
    end
    -- eventd did not stop for the stalled reader: everything was stored,
    -- and other queries are answered.
    local stored = eventd.wait_rows(vm, "EVENTS " .. etype .. " COUNT BY event.type",
        function(rs) return rs[1] and rs[1].count == 400 end)
    t:assert_eq(stored[1] and stored[1].count, 400, "all 400 events were committed")
    -- Now drain what eventd managed to send before giving up.
    local got, ended = take(c, 400)
    finish(c)
    t:assert(#got < 400, "the stream was cut short: " .. #got .. " of 400 delivered")
    t:assert(ended == "eof" or (type(ended) == "table" and ended.status == "error"),
        "and ended rather than waiting: " .. json.encode(ended))
end)
