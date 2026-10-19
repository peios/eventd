-- eventd TRM §6.5 — accounting and limits, the cheap half: what every
-- event query records for the adaptive indexer, the concurrency limits
-- (global, streaming, per user), request and response sizes, and the
-- streaming exemption from the query timeout. The memory budget and the
-- timeouts that need a slow query live in account-memory.test.lua, with
-- the dataset that makes them reachable.
--
-- Counters: the policy interval is at least sixty minutes, too long to
-- wait. An `EVENTS INDEX` query sends the policy thread a Recompute
-- (mod.rs:313-314), which indexing.rs:191 handles in the same match arm
-- as the interval's timeout, running the same recompute and the same
-- write of every counter to eventd-meta.db. Each counter test therefore
-- runs its queries, reads the metadata database, forces one policy pass
-- with an INDEX on a throwaway field, and reads it again.
--
-- Limits: each is lowered live (the query server reads them at every
-- accept, mod.rs:173-175), held with raw connections that send nothing
-- or open a stream, and restored at the end of the test. A held slot
-- also blocks the harness's own evctl queries, so nothing here waits on
-- eventd.wait_rows while slots are held. Ordinary users come from minted
-- tokens in Administrators (the query socket's directory admits no one
-- else but SYSTEM and eventd's service SID).
--
-- One file-scope VM, one vCPU.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-account" })

-- The raw query channel (eventd.rq), for what evctl hides: where one
-- response frame ends and the next begins, a caller other than SYSTEM
-- (`rq.user` mints one; each call is a fresh logon session, so two tokens
-- for one rid are one user in two sessions), and a reader that stops
-- reading.
local rq = eventd.rq

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function emit(etype, payload)
    local r = eventd.emit(vm, etype, payload)
    assert(r.ret == 0, "kmes_emit: errno " .. tostring(r.errno))
end

--- Every counter in the metadata database: field path -> count.
local function stored_counters()
    local out = {}
    for _, row in ipairs(eventd.sql(vm, eventd.DB.meta, "SELECT field_path, query_count FROM index_counters")) do
        out[row[1]] = row[2]
    end
    return out
end

--- Force one policy pass and wait until its write has landed: the INDEX
--- command recomputes immediately and gives `throwaway` a counter of its
--- own, whose appearance in the database marks the pass.
local function policy_pass(t)
    local throwaway = "flush" .. eventd.marker()
    local r = eventd.query(vm, "EVENTS INDEX " .. throwaway)
    t:assert(r.ok, "EVENTS INDEX: " .. r.stderr)
    local ok = pcall(wait_until, function() return stored_counters()[throwaway] ~= nil end,
        { timeout = 30, interval = 0.5, desc = "the policy pass to write the counters" })
    t:assert(ok, "a policy pass wrote the counters")
    return stored_counters()
end

--- Lower a query limit for the length of `fn`.
local function with_limit(name, value, fn)
    eventd.set(vm, name, "dword:" .. value):assert_ok()
    -- A registry change is applied asynchronously; wait until the new
    -- config is visible to eventd by the config-change event it records.
    os.execute("sleep 1")
    local ok, err = pcall(fn)
    eventd.unset(vm, name)
    os.execute("sleep 1")
    if not ok then error(err, 0) end
end

-- ---------------------------------------------------------------------------
-- Recording what was asked
-- ---------------------------------------------------------------------------

-- "Counters are in-memory and are flushed to the metadata database at
--  each policy interval. Query handlers never write to that database
--  directly."
test("a query's counts reach the metadata database at the policy pass, not from the query", {
    spec = "eventd *account.counters-are-flushed-to-the-metadata-database-each-policy-interval"
        .. " eventd *account.query-handlers-never-write-the-metadata-database-directly",
}, function(t)
    local f = "pfl" .. eventd.marker()
    for _ = 1, 3 do eventd.rows(vm, "EVENTS pt.acct WHERE " .. f .. " == 1") end
    os.execute("sleep 2")
    t:assert_eq(stored_counters()[f], nil, "after three queries the metadata database has no counter for the path")
    local after = policy_pass(t)
    t:assert_eq(after[f], 3, "the policy pass wrote it, with all three")
end)

-- "Every event query is recorded by the adaptive indexing system." / "A
--  payload field reference increments that path's counter."
test("each event query adds one to the counter of every payload path its predicates name", {
    spec = "eventd *account.every-event-query-is-recorded-by-the-adaptive-indexer"
        .. " eventd *account.a-payload-field-predicate-increments-that-paths-counter",
}, function(t)
    local m = eventd.marker()
    local a, b = "pa" .. m .. ".x", "pb" .. m
    eventd.rows(vm, "EVENTS pt.acct WHERE " .. a .. " == 1")
    eventd.rows(vm, "EVENTS pt.acct WHERE " .. a .. ' == 2 OR ' .. b .. ' == "y"')
    eventd.rows(vm, "EVENTS pt.acct WHERE " .. a .. " IS NULL WHERE " .. a .. " > 0")
    eventd.rows(vm, "EVENTS pt.acct SINCE 1h ago WHERE " .. b .. " IN (1, 2)")
    local c = policy_pass(t)
    t:assert_eq(c[a], 3, "the nested path counted once per query that names it: " .. tostring(c[a]))
    t:assert_eq(c[b], 2, "the other path once per query that names it: " .. tostring(c[b]))
end)

-- "A header column reference increments that column's frequency counter."
test("a predicate on a header column adds one to that column's counter", {
    spec = "eventd *account.a-header-column-predicate-increments-that-columns-counter",
}, function(t)
    local before = policy_pass(t)
    eventd.rows(vm, "EVENTS pt.acct WHERE emitter.process.guid IS NOT NULL")
    eventd.rows(vm, 'EVENTS pt.acct WHERE emitter.true-token.guid == "00000000-0000-0000-0000-000000000000"')
    eventd.rows(vm, "EVENTS pt.acct WHERE emitter.process.guid IS NULL")
    local after = policy_pass(t)
    local process, true_token = "emitter.process.guid", "emitter.true-token.guid"
    t:assert_eq((after[process] or 0) - (before[process] or 0), 2, "emitter.process.guid counted twice")
    t:assert_eq((after[true_token] or 0) - (before[true_token] or 0), 1, "emitter.true-token.guid once")
end)

-- "Cross-type WHERE predicates are counted like any other."
test("a cross-type condition's fields are counted like a plain predicate's", {
    spec = "eventd *account.cross-type-where-predicates-are-counted-like-any-other",
}, function(t)
    local m = eventd.marker()
    local label = "lab" .. m
    local before = policy_pass(t)
    eventd.rows(vm, "EVENTS pt.acct SINCE 10m ago WHERE METRIC pt" .. m .. "[" .. label .. '="x"] > 1')
    eventd.rows(vm, "EVENTS pt.acct SINCE 10m ago WHERE LOG pt-" .. m .. ' CONTAINING "x" EXISTS')
    local after = policy_pass(t)
    t:assert_eq(after[label], 1, "the metric condition's label was counted")
    t:assert_eq((after.value or 0) - (before.value or 0), 1, "and its value")
    t:assert_eq((after.message or 0) - (before.message or 0), 1, "the log condition's CONTAINING counts message")
end)

-- "This applies to event queries only ... their queries increment
--  nothing."
test("log and metric queries leave no counter behind", {
    spec = "eventd *account.log-and-metric-queries-increment-no-counters",
}, function(t)
    local m = eventd.marker()
    local before = policy_pass(t)
    eventd.rows(vm, "LOGS SINCE 10m ago WHERE job_id IS NULL WHERE is_error == true")
    eventd.rows(vm, "LOGS SINCE 10m ago WHERE EVENT pt.acct EXISTS")
    eventd.rows(vm, "METRIC pt" .. m .. "[mlab" .. m .. '="x"] SINCE 10m ago WHERE mw' .. m .. " == 1")
    local after = policy_pass(t)
    t:assert_eq(after.job_id, nil, "no job_id counter")
    t:assert_eq(after.is_error, nil, "no is_error counter")
    t:assert_eq(after["mlab" .. m], nil, "no counter for the metric label")
    t:assert_eq(after["mw" .. m], nil, "no counter for the metric predicate")
    t:assert_eq(after["event.type"] or 0, before["event.type"] or 0,
        "the log query's EVENT condition counted nothing either")
end)

-- ---------------------------------------------------------------------------
-- Concurrency
-- ---------------------------------------------------------------------------

-- "eventd bounds concurrent queries — streaming and non-streaming
--  together — at MaxConcurrentQueries. Beyond it a query is rejected
--  with an error rather than queued." / "Queries and ingestion are
--  separate channels, so exhausting the query side cannot exhaust
--  ingestion."
test("a stream and an idle connection fill a limit of two; a third is refused at once, and ingestion goes on", {
    spec = "eventd *account.max-concurrent-queries-bounds-streaming-and-non-streaming-queries-together"
        .. " eventd *account.a-query-over-the-concurrency-limit-is-rejected-not-queued"
        .. " eventd *account.exhausting-the-query-slots-cannot-exhaust-ingestion",
}, function(t)
    local m = eventd.marker("full")
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "MaxConcurrentQueries", 2, function()
        local stream = rq.open(w)
        rq.send(stream, "EVENTS pt.acct.none" .. m .. " STREAM")
        local o = rq.frame(stream)
        local watch = rq.frame(stream)
        t:assert_eq(watch and watch.status, "watch", "the stream is established: " .. json.encode(o))
        local idle = rq.open(w)
        -- Both slots are held, one streaming and one not.
        local third = rq.open(w)
        rq.timeout(third, 5)
        local refused = rq.frame(third)
        t:assert_eq(refused and refused.status, "error", "the third connection is refused")
        t:assert(tostring(refused and refused.error):find("too many concurrent queries", 1, true),
            "over the limit: " .. json.encode(refused))
        rq.close(third)
        local t0 = os.time()
        local r = eventd.query(vm, "EVENTS pt.acct TAKE 1")
        t:assert_eq(r.exit_code, 1, "evctl is refused too")
        t:assert(r.stderr:find("too many concurrent queries", 1, true), r.stderr)
        t:assert(os.time() - t0 < 10, "refused at once rather than queued")
        -- Ingestion is a different channel.
        t:assert_eq(eventd.emit(vm, "pt.acct.full", { tag = m }).ret, 0, "emit while full")
        local s = eventd.send_log(vm, { origin = m, is_error = false, message = "while full" })
        t:assert(s.ret > 0, "log datagram taken while full")
        local mt = eventd.send_metric(vm, { name = "pt" .. m, type = "gauge", value = 1 })
        t:assert(mt.ret > 0, "metric datagram taken while full")
        os.execute("sleep 2")
        rq.close(stream)
        rq.close(idle)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
    t:assert_eq(#eventd.wait_rows(vm, 'EVENTS pt.acct.full WHERE tag == "' .. m .. '"',
        function(rs) return #rs == 1 end), 1, "the event sent while full was stored")
    t:assert_eq(#eventd.wait_rows(vm, "LOGS FROM " .. m, function(rs) return #rs == 1 end), 1,
        "and the log line")
    t:assert_eq(#eventd.wait_rows(vm, "METRIC pt" .. m .. " SINCE 10m ago", function(rs) return #rs == 1 end), 1,
        "and the sample")
end)

-- "MaxStreamingQueries is enforced separately and is lower."
test("with the defaults the sixty-fifth stream is refused while ordinary queries still run", {
    spec = "eventd *account.max-streaming-queries-is-a-separate-lower-limit",
}, function(t)
    local w = vm:spawn_worker()
    local streams = {}
    local ok, err = pcall(function()
        for i = 1, 64 do
            local c = rq.open(w)
            rq.send(c, "EVENTS pt.acct.idle STREAM")
            local first = rq.frame(c)
            local second = rq.frame(c)
            t:assert_eq(second and second.status, "watch",
                "stream " .. i .. " established: " .. json.encode(first) .. json.encode(second))
            streams[#streams + 1] = c
        end
        local extra = rq.open(w)
        rq.send(extra, "EVENTS pt.acct.idle STREAM")
        local refused = rq.frame(extra)
        t:assert_eq(refused and refused.status, "error", "the 65th stream is refused")
        t:assert(tostring(refused and refused.error):find("streaming", 1, true),
            "by the streaming limit: " .. json.encode(refused))
        rq.close(extra)
        -- 64 streams are 64 running queries, well inside the default 128.
        local plain = eventd.query(vm, "EVENTS pt.acct TAKE 1")
        t:assert(plain.ok, "an ordinary query still runs beside 64 streams: " .. plain.stderr)
    end)
    for _, c in ipairs(streams) do rq.close(c) end
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- "eventd also bounds the queries one caller may have running at once,
--  at MaxQueriesPerUser." / "The slot is taken as soon as the token is
--  read, before the request, so connections held open without a query
--  count too." / "Over the limit, the query is rejected with an error."
test("two idle connections use up a user's two slots, and the third gets an error", {
    spec = "eventd *account.max-queries-per-user-bounds-one-callers-running-queries"
        .. " eventd *account.the-per-user-slot-is-taken-before-the-request-is-read"
        .. " eventd *account.a-query-over-the-per-user-limit-is-rejected-with-an-error",
}, function(t)
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "MaxQueriesPerUser", 2, function()
        local u = rq.user(w, 3001)
        local one, two = rq.open(w, u), rq.open(w, u)
        -- Neither has sent anything.
        os.execute("sleep 0.5")
        local three = rq.open(w, u)
        rq.timeout(three, 5)
        local refused = rq.frame(three)
        t:assert_eq(refused and refused.status, "error", "the third connection is answered with an error")
        t:assert(tostring(refused and refused.error):find("from this user", 1, true),
            "the per-user limit: " .. json.encode(refused))
        rq.close(three)
        -- Freeing one slot lets the user in again.
        rq.close(one)
        os.execute("sleep 0.5")
        local again = rq.open(w, u)
        rq.send(again, "LOGS TAKE 1")
        local res = rq.collect(again)
        t:assert_eq(res.status, "end", "with a slot free the user's query runs: " .. tostring(res.error))
        rq.close(again)
        rq.close(two)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- "The caller is the user SID of the token read at connect." / "SYSTEM's
--  queries are not counted."
test("the per-user budget follows the user SID across sessions, and SYSTEM has none", {
    spec = "eventd *account.the-per-user-limit-counts-by-the-user-sid-of-the-peer-token"
        .. " eventd *account.system-queries-are-not-counted-per-user",
}, function(t)
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "MaxQueriesPerUser", 1, function()
        local held = rq.open(w, rq.user(w, 4001))
        os.execute("sleep 0.5")
        -- The same user from a second logon session shares the budget.
        local same = rq.open(w, rq.user(w, 4001))
        rq.timeout(same, 5)
        local refused = rq.frame(same)
        t:assert_eq(refused and refused.status, "error", "a second session of the same user is refused")
        rq.close(same)
        -- Another user has a budget of their own.
        local other = rq.open(w, rq.user(w, 4002))
        rq.send(other, "LOGS TAKE 1")
        local res = rq.collect(other)
        t:assert_eq(res.status, "end", "another user is admitted: " .. tostring(res.error))
        rq.close(other)
        -- SYSTEM holds two idle connections against a limit of one and
        -- still runs a third query.
        local s1, s2 = rq.open(w), rq.open(w)
        os.execute("sleep 0.5")
        local r = eventd.query(vm, "EVENTS TAKE 1")
        t:assert(r.ok, "SYSTEM is not counted: " .. r.stderr)
        rq.close(s1); rq.close(s2)
        rq.close(held)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- ---------------------------------------------------------------------------
-- Request and response sizes
-- ---------------------------------------------------------------------------

-- "eventd rejects an inbound frame above MaxQueryRequestBytes before
--  allocating or reading its payload."
test("a length prefix over the request ceiling is refused without the payload ever being sent", {
    spec = "eventd *account.a-request-frame-over-max-query-request-bytes-is-rejected-before-reading",
}, function(t)
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "MaxQueryRequestBytes", 1024, function()
        local c = rq.open(w)
        rq.timeout(c, 5)
        -- Four bytes announcing 4 MiB, and nothing after them.
        local s = rq.send_raw(c, string.pack("<I4", 4 * 1024 * 1024))
        t:assert_eq(s.ret, 4, "the prefix was sent")
        local answer, why = rq.frame(c)
        t:assert(answer, "eventd answered without waiting for the payload: " .. tostring(why))
        t:assert_eq(answer and answer.status, "error", "with an error")
        t:assert(tostring(answer and answer.error):find("exceeds 1024", 1, true), json.encode(answer))
        rq.close(c)
        -- A request inside the ceiling is served.
        local fine = rq.open(w)
        rq.send(fine, "EVENTS TAKE 1 WHERE pad == \"" .. string.rep("x", 900) .. "\"")
        local res = rq.collect(fine)
        t:assert_eq(res.status, "end", "a 0.9 KiB request is read and run: " .. tostring(res.error))
        rq.close(fine)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- "Outbound result records are grouped at record boundaries toward
--  QueryResponseTargetBytes. The target is not a limit: a complete record
--  that exceeds it is sent alone ... Only the protocol's u32 frame length
--  is a hard outbound bound."
test("records are packed whole into frames near the target, and one larger than the target travels alone", {
    spec = "eventd *account.result-records-are-grouped-at-record-boundaries-toward-the-response-target"
        .. " eventd *account.a-record-over-the-response-target-is-sent-alone-and-whole"
        .. " eventd *account.the-u32-frame-length-is-the-only-hard-outbound-bound",
}, function(t)
    local o = eventd.marker("frames")
    local batch = {}
    for i = 1, 60 do
        batch[i] = { origin = o, is_error = false, message = string.format("%03d", i) .. string.rep("m", 120) }
    end
    -- One record far over the target, and one far over a socket buffer.
    batch[#batch + 1] = { origin = o, is_error = false, message = "big" .. string.rep("B", 5000) }
    eventd.send_log(vm, batch)
    eventd.send_log(vm, { origin = o, is_error = false, message = "huge" .. string.rep("H", 150000) })
    eventd.wait_rows(vm, "LOGS FROM " .. o .. " COUNT BY origin",
        function(rs) return rs[1] and rs[1].count == 62 end)
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "QueryResponseTargetBytes", 1024, function()
        local c = rq.open(w)
        rq.send(c, "LOGS FROM " .. o .. " SORT message")
        local res = rq.collect(c)
        rq.close(c)
        t:assert_eq(res.status, "end", "the query completed: " .. tostring(res.error))
        t:assert_eq(#res.records, 62, "every record arrived")
        local okframes, multi = 0, 0
        for i, f in ipairs(res.frames) do
            if f.status == "ok" then
                okframes = okframes + 1
                local n = #f.records
                if n > 1 then
                    multi = multi + 1
                    t:assert(f.size <= 1024, "frame " .. i .. " of " .. n .. " records is within the target: " .. f.size)
                end
                for _, r in ipairs(f.records) do
                    if r.message:sub(1, 3) == "big" then
                        t:assert_eq(n, 1, "the record over the target is alone in its frame")
                        t:assert_eq(#r.message, 5003, "and whole")
                    elseif r.message:sub(1, 4) == "huge" then
                        t:assert_eq(n, 1, "the 150 KB record is alone in its frame")
                        t:assert_eq(#r.message, 150004, "and whole, in a frame of " .. f.size .. " bytes")
                    end
                end
            end
        end
        t:assert(multi >= 5, "small records share frames rather than travelling one per frame: "
            .. multi .. " of " .. okframes)
        -- Grouped toward the target: a frame is closed only when the next
        -- record would not fit.
        for i = 1, #res.frames - 2 do
            local f, nxt = res.frames[i], res.frames[i + 1]
            if f.status == "ok" and nxt.status == "ok" and #f.records > 1 and #nxt.records > 0 then
                local first = nxt.records[1]
                t:assert(f.size + #first.message + 120 > 1024 - 64,
                    "frame " .. i .. " (" .. f.size .. " bytes) was closed only when the next record would not fit")
            end
        end
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)

-- ---------------------------------------------------------------------------
-- Timeouts: the watch phase
-- ---------------------------------------------------------------------------

-- "It bounds the initial result set only. A non-streaming query sends end
--  before it expires; a streaming query sends watch." / "Past watch the
--  stream is not time-limited."
test("a stream established under a one-second timeout still delivers several seconds later", {
    spec = "eventd *account.the-timeout-bounds-only-the-initial-result-set"
        .. " eventd *account.a-stream-past-watch-is-not-time-limited",
}, function(t)
    local m = eventd.marker("late")
    local etype = "pt.acct.late" .. m
    local w = vm:spawn_worker()
    local ok, err = pcall(with_limit, "QueryTimeoutMs", 1000, function()
        local c = rq.open(w)
        rq.timeout(c, 20)
        rq.send(c, "EVENTS " .. etype .. " STREAM")
        local first = rq.frame(c)
        local watch = rq.frame(c)
        t:assert_eq(first and first.status, "ok", "the initial result set")
        t:assert_eq(watch and watch.status, "watch", "then watch, within the timeout")
        os.execute("sleep 4")
        emit(etype, { n = 1 })
        local later = rq.frame(c)
        t:assert_eq(later and later.status, "ok", "four seconds past watch the stream still delivers: "
            .. json.encode(later))
        t:assert_eq(later and later.records and #later.records, 1, "the new event")
        rq.close(c)
    end)
    w:kill(); w:join()
    if not ok then error(err, 0) end
end)
