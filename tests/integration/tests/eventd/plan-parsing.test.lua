-- eventd TRM §6.1 — parsing and planning: the four phases between a query
-- string and an answer, what can only fail once data is involved, and the
-- read-only connections a query executes on.
--
-- One file-scope VM (one vCPU, one event shard) carries every case. The
-- phase order is observed by building queries that would fail in more
-- than one phase at once and seeing which failure comes back: a parse
-- error against a store that cannot be opened, a planning failure
-- against an identifier whose descriptor is broken, and so on. Two
-- conditions are staged for that and always undone in the same test:
-- the log store's database file moved aside (so opening it fails), and a
-- descriptor key holding a REG_SZ (so authorizing that one event type
-- fails with an error rather than a denial).
--
-- The connections a query holds are read from /proc/<eventd>/fd and
-- fdinfo while the query is held open by a client that has stopped
-- reading: eventd blocks writing the initial result set, so whatever it
-- opened for the query is still open, and the access mode of each new
-- descriptor says whether the connection is read-only.
--
-- The catalogue statements (identifier discovery, stale names) need a
-- stopped eventd and a store edited from the host, and live in
-- plan-catalogues.test.lua.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-plan" })

-- The raw query channel (eventd.rq), for what evctl hides: where one
-- response frame ends and the next begins, and a reader that stops reading.
local rq = eventd.rq

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function q(text) return eventd.query(vm, text) end

--- Emit `n` events of `etype` and wait until all are queryable.
local function emit_wait(etype, payload, n)
    n = n or 1
    for _ = 1, n do
        local r = eventd.emit(vm, etype, payload or {})
        assert(r.ret == 0, "kmes_emit " .. etype .. ": errno " .. tostring(r.errno))
    end
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago",
        function(rs) return #rs >= n end)
end

local function send_logs_wait(origin, records)
    local r = eventd.send_log(vm, records)
    assert(r.ret and r.ret > 0, "log sendto: errno " .. tostring(r.errno))
    local n = records[1] and #records or 1
    eventd.wait_rows(vm, "LOGS FROM " .. origin, function(rs) return #rs >= n end)
end

local function send_metric_wait(name, records)
    local r = eventd.send_metric(vm, records)
    assert(r.ret and r.ret > 0, "metric sendto: errno " .. tostring(r.errno))
    local n = records[1] and #records or 1
    eventd.wait_rows(vm, "METRIC " .. name .. "[] SINCE 1h ago",
        function(rs) return #rs >= n end)
end

--- A descriptor key for one event type whose default value is REG_SZ:
--- authorizing that type then fails with an error, not a denial
--- (security.rs load_descriptor), which makes the access check visible.
local function break_descriptor(etype)
    local key = eventd.SECURITY .. [[\Events\]] .. etype
    vm:run("reg set -p '" .. key .. "' @ 'sz:pt-not-a-descriptor'"):assert_ok()
    return function() vm:run("reg del '" .. key .. "'") end
end

local LOGS_DB = eventd.DB.logs
local LOGS_ASIDE = eventd.DB.logs .. ".pt-aside"

--- Move the log store's database file aside for the duration of `fn`.
--- eventd's own writer keeps its open descriptor, so ingestion is
--- untouched; only a fresh open by path fails.
local function without_log_store(fn)
    vm:rename(LOGS_DB, LOGS_ASIDE)
    local ok, err = pcall(fn)
    vm:rename(LOGS_ASIDE, LOGS_DB)
    if not ok then error(err, 0) end
end

-- eventd's descriptors are read with eventd.fds (by fd number, each with
-- its O_ACCMODE `mode`: 0 read-only, 1 write-only, 2 read-write), and
-- counted on a path and mode with eventd.fd_numbers.
--
-- Counting connections by descriptor needs care: SQLite's unix VFS does
-- not close a connection's descriptor while the process holds POSIX
-- locks on that file, but parks it and hands it to the next connection
-- that opens the same file with the same flags. So a read-only
-- descriptor on a store outlives the query that opened it, and the next
-- query reuses it. What does hold is that connections open at the same
-- moment each need their own descriptor: while a query is held open,
-- the read-only descriptors on a file bound how many connections it has
-- there.

-- ---------------------------------------------------------------------------
-- Parsing
-- ---------------------------------------------------------------------------

-- "Identifies the mode from the first token — EVENTS, LOGS or METRIC."
test("the first token selects events, logs or metric mode, and nothing else is a mode", {
    spec = "eventd *plan.the-mode-is-identified-from-the-first-token",
}, function(t)
    local tag = eventd.marker("mode")
    emit_wait("pt.plan.mode", { tag = tag })
    send_logs_wait(tag, { origin = tag, is_error = false, message = "mode" })
    send_metric_wait(tag, { name = tag, type = "gauge", value = 3 })

    local ev = eventd.rows(vm, 'EVENTS pt.plan.mode WHERE tag == "' .. tag .. '"')
    t:assert_eq(#ev, 1, "EVENTS reads the event store")
    t:assert_eq(ev[1]["event.type"], "pt.plan.mode", "and the record is an event")
    local lg = eventd.rows(vm, "LOGS FROM " .. tag)
    t:assert_eq(#lg, 1, "LOGS reads the log store")
    t:assert_eq(lg[1].message, "mode", "and the record is a log line")
    local mt = eventd.rows(vm, "METRIC " .. tag)
    t:assert_eq(#mt, 1, "METRIC reads the metric store")
    t:assert_eq(mt[1].value, 3, "and the record is a sample")

    for _, text in ipairs({
        "SINCE 10m ago EVENTS pt.plan.mode",
        "WHERE tag == 1",
        "EVENT pt.plan.mode",
        "pt.plan.mode",
    }) do
        local r = q(text)
        t:assert_eq(r.exit_code, 1, "no mode in the first token is refused: " .. text)
        t:assert(r.stderr:find("must begin with EVENTS, LOGS or METRIC", 1, true),
            "with the mode error: " .. text .. " -> " .. r.stderr)
    end
end)

-- "Extracts the primary selector: a type pattern, a FROM list, or a
--  metric name with an optional label selector."
test("the primary selector picks types by pattern, origins by FROM, series by name and labels", {
    spec = "eventd *plan.the-parser-extracts-the-primary-selector",
}, function(t)
    local m = eventd.marker("sel")
    emit_wait("pt." .. m .. ".cat1")
    emit_wait("pt." .. m .. ".cat2")
    emit_wait("pt." .. m .. ".dog")
    local types = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS pt." .. m .. ".cat*")) do types[r["event.type"]] = true end
    t:assert(types["pt." .. m .. ".cat1"] and types["pt." .. m .. ".cat2"], "the pattern selects both cats")
    t:assert(not types["pt." .. m .. ".dog"], "and not the dog: " .. json.encode(types))

    local a, b, c = m .. "a", m .. "b", m .. "c"
    for _, o in ipairs({ a, b, c }) do
        send_logs_wait(o, { origin = o, is_error = false, message = o })
    end
    local origins = {}
    for _, r in ipairs(eventd.rows(vm, "LOGS FROM " .. a .. ", " .. b)) do origins[r.origin] = true end
    t:assert(origins[a] and origins[b] and not origins[c], "FROM selects its origins: " .. json.encode(origins))

    local name = "pt" .. m .. "series"
    send_metric_wait(name, {
        { name = name, type = "gauge", value = 1, labels = { k = "a" } },
        { name = name, type = "gauge", value = 2, labels = { k = "b" } },
    })
    local rows = eventd.rows(vm, "METRIC " .. name .. '[k="b"] SINCE 1h ago')
    t:assert_eq(#rows, 1, "the label selector narrows to one series")
    t:assert_eq(rows[1].value, 2, "the k=b one")
end)

-- "Collects every clause, in whatever order they appear."
test("the same clauses in a different order give the same answer", {
    spec = "eventd *plan.clauses-are-collected-in-whatever-order-they-appear",
}, function(t)
    local m = eventd.marker("ord")
    for i = 1, 5 do emit_wait("pt.plan.clauses", { tag = m, n = i }) end
    local forms = {
        'EVENTS pt.plan.clauses SINCE 10m ago WHERE tag == "' .. m .. '" WHERE n >= 2 SORT n DESC SKIP 1 TAKE 2 SELECT n',
        'EVENTS pt.plan.clauses SELECT n TAKE 2 SKIP 1 SORT n DESC WHERE n >= 2 WHERE tag == "' .. m .. '" SINCE 10m ago',
        'EVENTS pt.plan.clauses TAKE 2 WHERE tag == "' .. m .. '" SELECT n SINCE 10m ago SKIP 1 WHERE n >= 2 SORT n DESC',
    }
    local first
    for _, text in ipairs(forms) do
        local rows = eventd.rows(vm, text)
        local got = {}
        for _, r in ipairs(rows) do got[#got + 1] = r.n end
        if not first then
            first = got
            t:assert_eq(json.encode(got), "[4,3]", "the clauses all applied: " .. text)
        else
            t:assert_eq(json.encode(got), json.encode(first), "same answer for: " .. text)
        end
    end
end)

-- "Validates that the clauses suit the mode — CONTAINING only in log
--  mode, RATE only in metric mode, SELECT only where a result schema is
--  not fixed."
test("a clause in the wrong mode is a parse error, whatever the stores hold", {
    spec = "eventd *plan.clause-and-mode-compatibility-is-checked-at-parse-time",
}, function(t)
    -- Each is refused although every name in it is real and readable; the
    -- same text with the mode-appropriate clause is accepted.
    local cases = {
        { "EVENTS pt.plan.mode CONTAINING \"x\"", "CONTAINING is valid only in LOGS mode",
          'LOGS CONTAINING "x"' },
        { "LOGS RATE", "metric transform", nil },
        { "EVENTS pt.plan.mode RATE", "metric transform", nil },
        { "METRIC eventd.queries.active SELECT value", "not valid in METRIC mode",
          "EVENTS pt.plan.mode SELECT event.type" },
        { "LOGS SELECT message COUNT BY origin", "SELECT cannot be combined", "LOGS SELECT message TAKE 1" },
        { "EVENTS pt.plan.mode ERROR ONLY", "ERROR ONLY is valid only in LOGS mode", "LOGS ERROR ONLY TAKE 1" },
        { "METRIC eventd.queries.active STREAM", "METRIC queries cannot stream", nil },
    }
    for _, c in ipairs(cases) do
        local r = q(c[1])
        t:assert_eq(r.exit_code, 1, "refused: " .. c[1])
        t:assert(r.stderr:find(c[2], 1, true), c[1] .. " -> " .. r.stderr)
        if c[3] then
            local ok = q(c[3])
            t:assert(ok.ok, "the clause in its own mode is accepted: " .. c[3] .. " " .. ok.stderr)
        end
    end
end)

-- "Parse errors are returned immediately, before anything is opened, read
--  or authorized."
test("a parse error comes back from a query whose store cannot be opened and whose type cannot be authorized", {
    spec = "eventd *plan.parse-errors-return-before-anything-is-opened-read-or-authorized",
}, function(t)
    local etype = "pt." .. eventd.marker("perr") .. ".bad"
    emit_wait(etype)
    local restore = break_descriptor(etype)
    local ok, err = pcall(function()
        -- Authorizing this type now fails with an error ...
        local well = q("EVENTS " .. etype .. " TAKE 1")
        t:assert_eq(well.exit_code, 1, "the well-formed query reaches authorization and fails there")
        t:assert(well.stderr:find("not REG_BINARY", 1, true), "with the descriptor error: " .. well.stderr)
        -- ... but a malformed query naming it never gets that far.
        local bad = q("EVENTS " .. etype .. " TAKE")
        t:assert_eq(bad.exit_code, 1, "the malformed query fails")
        t:assert(not bad.stderr:find("REG_BINARY", 1, true), "not at authorization: " .. bad.stderr)
        t:assert(bad.stderr:find("expected", 1, true), "but with the parse error: " .. bad.stderr)
    end)
    restore()
    if not ok then error(err, 0) end

    without_log_store(function()
        local well = q("LOGS TAKE 1")
        t:assert_eq(well.exit_code, 1, "a well-formed log query cannot open the moved store")
        t:assert(well.stderr:find("storage", 1, true), "and says so: " .. well.stderr)
        local bad = q("LOGS TAKE 1 TAKE 2")
        t:assert_eq(bad.exit_code, 1, "the malformed one fails too")
        t:assert(bad.stderr:find("TAKE appears more than once", 1, true),
            "but with its parse error, before any open: " .. bad.stderr)
    end)
end)

-- "Turning it into an answer has four phases before any data is read:
--  parse, plan, authorize, execute."
test("planning fails before authorization, and authorization fails before any record is sent", {
    spec = "eventd *plan.a-query-runs-parse-then-plan-then-authorize-then-execute",
}, function(t)
    local etype = "pt." .. eventd.marker("phase") .. ".bad"
    emit_wait(etype)
    local restore = break_descriptor(etype)
    local ok, err = pcall(function()
        -- Plan before authorize: the effective range is past the
        -- cross-type lookback limit (default 7 days), which planning
        -- decides; the broken descriptor is never reached.
        local r = q("EVENTS " .. etype .. " SINCE 30d ago WHERE LOG pt-anything EXISTS")
        t:assert_eq(r.exit_code, 1, "the over-long cross-type range fails")
        t:assert(r.stderr:find("range is too large", 1, true), "at planning: " .. r.stderr)

        -- Authorize before execute: the authorization failure is the first
        -- thing the client hears; no "ok" carrying records precedes it.
        local w = vm:spawn_worker()
        local c = rq.open(w)
        rq.send(c, "EVENTS " .. etype)
        local first = rq.frame(c)
        rq.close(c)
        w:kill(); w:join()
        t:assert(first, "eventd answered")
        t:assert_eq(first.status, "error", "the first frame is the error: " .. json.encode(first))
        t:assert(tostring(first.error):find("REG_BINARY", 1, true), "the authorization error")
    end)
    restore()
    if not ok then error(err, 0) end

    -- And with nothing broken, the same shape of query parses, plans,
    -- authorizes and executes.
    local fine = q("EVENTS " .. etype .. " TAKE 1")
    t:assert(fine.ok and #fine.rows == 1, "with the descriptor removed it runs: " .. fine.stderr)
end)

-- ---------------------------------------------------------------------------
-- What can only fail later
-- ---------------------------------------------------------------------------

-- "Some failures need data ... they surface at planning or execution time."
test("one query string succeeds or fails depending on what the store holds", {
    spec = "eventd *plan.store-dependent-failures-surface-at-planning-or-execution-not-parse",
}, function(t)
    local name = "pt" .. eventd.marker("late")
    local text = "METRIC " .. name .. " RATE SINCE 1h ago"
    local before = q(text)
    t:assert(before.ok, "with no such series the query parses and runs: " .. before.stderr)
    t:assert_eq(#before.rows, 0, "and answers nothing")
    send_metric_wait(name, { name = name, type = "gauge", value = 5 })
    local after = q(text)
    t:assert_eq(after.exit_code, 1, "once the name is a gauge the same text fails")
    t:assert(after.stderr:find("RATE and DELTA require a counter", 1, true), after.stderr)
end)

-- ---------------------------------------------------------------------------
-- Planning
-- ---------------------------------------------------------------------------

-- "Which concrete identifiers the data could carry — event types, log
--  origins, metric names — because access control resolves per identifier
--  and a broad selector authorizes nothing by itself."
test("a pattern is resolved to the concrete types it matches, each authorized on its own", {
    spec = "eventd *plan.planning-resolves-the-concrete-identifiers-the-data-could-carry",
}, function(t)
    local m = eventd.marker("conc")
    local good, bad = "pt." .. m .. ".good", "pt." .. m .. ".bad"
    emit_wait(good)
    emit_wait(bad)
    local restore = break_descriptor(bad)
    local ok, err = pcall(function()
        -- The pattern matches both concrete types; the broken one's own
        -- descriptor is consulted, so the pattern query fails ...
        local wide = q("EVENTS pt." .. m .. ".*")
        t:assert_eq(wide.exit_code, 1, "the pattern reaches the broken type's own descriptor")
        t:assert(wide.stderr:find(bad, 1, true), "naming that type's key: " .. wide.stderr)
        -- ... while naming the other concrete type does not touch it.
        local one = q("EVENTS " .. good)
        t:assert(one.ok and #one.rows == 1, "the good type alone is authorized and read: " .. one.stderr)
    end)
    restore()
    if not ok then error(err, 0) end
end)

-- "The primary selector filters that set before access checks."
test("an identifier the selector excludes is never access-checked", {
    spec = "eventd *plan.the-primary-selector-filters-discovered-identifiers-before-access-checks",
}, function(t)
    local m = eventd.marker("filt")
    local keep, drop = "pt." .. m .. ".keep", "pt." .. m .. ".drop"
    emit_wait(keep)
    emit_wait(drop)
    local restore = break_descriptor(drop)
    local ok, err = pcall(function()
        -- Both types are in the catalogue; only the selector keeps the
        -- broken one out of the access checks.
        local r = q("EVENTS pt." .. m .. ".k*")
        t:assert(r.ok, "the broken type, discovered but filtered out, is not checked: " .. r.stderr)
        t:assert_eq(#r.rows, 1, "and the kept type is read")
        local all = q("EVENTS pt." .. m .. ".*")
        t:assert_eq(all.exit_code, 1, "control: with the selector admitting it, it is checked and fails")
    end)
    restore()
    if not ok then error(err, 0) end
end)

-- "Which series a metric selector matches, and whether they are
--  type-homogeneous."
test("a selector matching a counter and a gauge is refused; one matching only counters is not", {
    spec = "eventd *plan.planning-resolves-matched-series-and-checks-they-are-type-homogeneous",
}, function(t)
    local m = "pt" .. eventd.marker("homo")
    send_metric_wait(m .. ".c1", { name = m .. ".c1", type = "counter", value = 1 })
    send_metric_wait(m .. ".c2", { name = m .. ".c2", type = "counter", value = 2 })
    send_metric_wait(m .. ".g", { name = m .. ".g", type = "gauge", value = 3 })
    local mixed = q("METRIC " .. m .. ".*")
    t:assert_eq(mixed.exit_code, 1, "counter and gauge together are refused")
    t:assert(mixed.stderr:find("more than one type", 1, true), mixed.stderr)
    local same = q("METRIC " .. m .. ".c*[]")
    t:assert(same.ok, "two counters are one type: " .. same.stderr)
    t:assert_eq(#same.rows, 2, "both series answer")
end)

-- "Which stores are involved, including any cross-type source."
test("an event query with a log condition reads the log store as well", {
    spec = "eventd *plan.planning-identifies-every-store-involved-including-cross-type-sources",
}, function(t)
    local m = eventd.marker("xs")
    local etype = "pt." .. m .. ".ev"
    emit_wait(etype)
    send_logs_wait(m, { origin = m, is_error = false, message = "near" })
    local near = eventd.rows(vm, "EVENTS " .. etype .. " SINCE 10m ago WHERE LOG " .. m .. " EXISTS")
    t:assert_eq(#near, 1, "the log store answered the condition: the event has a log near it")
    local none = eventd.rows(vm, "EVENTS " .. etype .. " SINCE 10m ago WHERE LOG " .. m .. "nothing EXISTS")
    t:assert_eq(#none, 0, "and with no such log nothing passes")
    without_log_store(function()
        local plain = q("EVENTS " .. etype .. " SINCE 10m ago")
        t:assert(plain.ok, "the event store alone still answers: " .. plain.stderr)
        local cross = q("EVENTS " .. etype .. " SINCE 10m ago WHERE LOG " .. m .. " EXISTS")
        t:assert_eq(cross.exit_code, 1, "the cross-type query needs the log store too")
        t:assert(cross.stderr:find("storage", 1, true), cross.stderr)
    end)
end)

-- "Event payloads are stored as opaque MessagePack and are never decoded
--  on the write path. Decoding happens here, on the read path, and only
--  where a query needs it."
-- Route closed: KMES admits only well-formed MessagePack payloads, and a
-- payload that failed to decode is silently flattened to nothing
-- (value.rs flatten_event_payload), so no query can tell whether a row it
-- skipped was decoded. The unit test plants an undecodable payload under
-- a type the caller may not read and proves the scan skips it undecoded.
test("rows of types the caller may not read are skipped before their payloads are decoded", {
    spec = "eventd *plan.payloads-are-decoded-only-where-a-query-needs-them",
    skip = true,
    covered_by = "cargo:eventd eventd query::executor::tests::denied_event_types_are_skipped_before_payload_decoding",
}, function() end)

-- ---------------------------------------------------------------------------
-- Read connections
-- ---------------------------------------------------------------------------

--- Start `text` on a raw connection, take its first frame, and stop
--- reading. Returns the connection and the worker. The result must be
--- larger than a socket buffer so that eventd blocks writing it.
local function stall(text)
    local w = vm:spawn_worker()
    local c = rq.open(w)
    rq.send(c, text)
    local first = rq.frame(c)
    assert(first and first.status == "ok", "the query started: " .. json.encode(first))
    return c, w
end

local function unstall(c, w)
    rq.close(c)
    w:kill(); w:join()
end

-- "Execution uses read-only SQLite connections." / "A log or metric query
--  opens one."
test("a log query opens one read-only connection to the log store, and a metric query one to the metric store", {
    spec = "eventd *plan.queries-execute-on-read-only-sqlite-connections"
        .. " eventd *plan.a-log-or-metric-query-opens-a-single-connection",
}, function(t)
    local m = eventd.marker("ro")
    -- About 700 KiB of results, far past a socket buffer.
    local batch = {}
    for i = 1, 500 do batch[i] = { origin = m, is_error = false, message = string.rep("r", 200) .. i } end
    for _ = 1, 6 do
        local r = eventd.send_log(vm, batch)
        assert(r.ret and r.ret > 0, "log sendto: errno " .. tostring(r.errno))
    end
    eventd.wait_rows(vm, "LOGS FROM " .. m .. " COUNT BY origin",
        function(rs) return rs[1] and rs[1].count == 3000 end)
    local name = "pt" .. m
    local now = eventd.guest_ns(vm)
    for b = 0, 4 do
        local samples = {}
        for i = 1, 1000 do
            samples[i] = { name = name, type = "gauge", value = i, labels = { pad = string.rep("p", 40) },
                           timestamp = now - (6000 - (b * 1000 + i)) * 1000000 }
        end
        eventd.send_metric(vm, samples)
    end
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 1h ago SKIP 4999",
        function(rs) return #rs == 1 end)

    -- While each query is held open: the store's writer has its one
    -- read-write descriptor, as it always does, and everything else open
    -- on that file is read-only, and is one descriptor.
    local c, w = stall("LOGS FROM " .. m)
    local during = eventd.fds(vm, nil, { by_fd = true })
    unstall(c, w)
    t:assert_eq(#eventd.fd_numbers(during, LOGS_DB, 2), 1, "logs.db: only the writer's descriptor is read-write")
    t:assert_eq(#eventd.fd_numbers(during, LOGS_DB, 1), 0, "logs.db: nothing is write-only")
    t:assert_eq(#eventd.fd_numbers(during, LOGS_DB, 0), 1,
        "logs.db: the held log query reads through one read-only descriptor: " .. json.encode(during))

    c, w = stall("METRIC " .. name .. " SINCE 1h ago")
    during = eventd.fds(vm, nil, { by_fd = true })
    unstall(c, w)
    t:assert_eq(#eventd.fd_numbers(during, eventd.DB.metrics, 2), 1, "metrics.db: only the writer's is read-write")
    t:assert_eq(#eventd.fd_numbers(during, eventd.DB.metrics, 0), 1,
        "metrics.db: the held metric query reads through one read-only descriptor: " .. json.encode(during))
end)

-- "Where it cannot allocate what an admitted query needs, it fails that
--  query rather than blocking a writer or exceeding the limit."
test("a query that cannot get a descriptor fails, and ingestion carries on", {
    spec = "eventd *plan.an-admitted-query-that-cannot-get-resources-fails-rather-than-blocking-a-writer",
}, function(t)
    local pid = eventd.pid(vm)
    local fds = eventd.fds(vm, nil, { by_fd = true })
    -- RLIMIT_NOFILE bounds descriptor numbers. Leave room for exactly the
    -- accepted connection and the caller's peer token, so the query is
    -- admitted and its first database open is the one that fails. The
    -- listener's accept() must still succeed: eventd treats a failing
    -- accept as fatal to the query server (mod.rs:218). New descriptors
    -- take the lowest free numbers, so the limit is the first one below
    -- which exactly two numbers are free.
    local limit, free = 0, 0
    while free < 2 do
        if not fds[limit] then free = free + 1 end
        limit = limit + 1
    end
    local old = vm:syscall(302, { args = { pid, 7, 0, 0 },
        bufs = { string.rep("\0", 16) }, ptrs = { 3 } })
    t:assert_eq(old.ret, 0, "prlimit64 read eventd's limit: errno " .. tostring(old.errno))
    local soft, hard = string.unpack("<I8I8", old.out_bufs[1])
    local function set(limit)
        return vm:syscall(302, { args = { pid, 7, 0, 0 },
            bufs = { string.pack("<I8I8", limit, hard) }, ptrs = { 2 } })
    end
    local m = eventd.marker("rl")
    local r = set(limit)
    t:assert_eq(r.ret, 0, "prlimit64 lowered eventd's limit: errno " .. tostring(r.errno))
    local ok, err = pcall(function()
        local starved = q("EVENTS pt.plan.mode TAKE 1")
        t:assert_eq(starved.exit_code, 1, "the admitted query fails")
        t:assert(starved.stderr:find("storage", 1, true) or starved.stderr:find("open", 1, true),
            "for want of a descriptor: " .. starved.stderr)
        -- Ingestion, meanwhile, is not held up.
        t:assert_eq(eventd.emit(vm, "pt.plan.rlimit", { tag = m }).ret, 0, "emitted while starved")
        eventd.send_log(vm, { origin = m, is_error = false, message = "while starved" })
        os.execute("sleep 2")
    end)
    set(soft)
    if not ok then error(err, 0) end
    t:assert(eventd.pid(vm) == pid, "eventd survived")
    local ev = eventd.wait_rows(vm, 'EVENTS pt.plan.rlimit WHERE tag == "' .. m .. '"',
        function(rs) return #rs == 1 end)
    t:assert_eq(#ev, 1, "the event emitted while queries were starved was committed")
    local lg = eventd.wait_rows(vm, "LOGS FROM " .. m, function(rs) return #rs == 1 end)
    t:assert_eq(#lg, 1, "and so was the log line")
end)
