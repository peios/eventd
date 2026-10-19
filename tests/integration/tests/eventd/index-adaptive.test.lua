-- eventd TRM §3.4 "Adaptive Indexing" (3--event-storage/4--adaptive-indexing.md):
-- query-frequency counters, the index policy and its desired set, shard
-- convergence, graduated and emergency shedding, the candidate fields, and
-- index naming.
--
-- Two VMs, one vCPU each:
--
--   * `vm`, seeded with StorageShards 2 (so "every shard" means two) and
--     the lowest thresholds a12 allows (create 10, drop 1). Its cases run
--     WHERE queries on fresh field names, then make the policy run and
--     read the result out of eventd-meta.db (`index_counters`,
--     `desired_indexes`) and out of each shard's sqlite_master.
--   * `press`, one shard, seeded MaxBatchSize 100 and a 10-second shedding
--     window, for shedding. Write pressure is made by freezing eventd
--     (SIGSTOP), emitting a burst into its KMES ring, and thawing it: the
--     drain then hands the writer a backlog, so batches are full.
--
-- The policy interval cannot be shortened below 60 minutes (a1: 60–1440),
-- so no case waits for it. eventd also recomputes on any applied
-- configuration change (config.rs:1033 sends PolicyMessage::Recompute),
-- and that is the trigger used here: a LogRetentionDays set/unset. The
-- "every interval" half of the policy anchor is therefore not observed;
-- the "only writer of the desired set" half is.
--
-- Index builds are cancelled when the writer's queue is non-empty, which
-- on a guest-sized shard happens in well under the time a test can
-- observe; cancellation, retry and the progress handler are unit-test
-- stubs. Shedding is read from sqlite_master, which names exactly which
-- indexes went (the eventd.events.index.sheds health counter only counts
-- them, and is sampled on its own interval).

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
peinit.claim(2) -- vm and press, both for the whole file

local vm = eventd.boot({ name = "ev-index", config = {
    { name = "StorageShards", type = "dword", data = 2 },
    { name = "AdaptiveIndexCreateThreshold", type = "dword", data = 10 },
    { name = "AdaptiveIndexDropThreshold", type = "dword", data = 1 },
} })
local press = eventd.boot({ name = "ev-index-press", config = {
    { name = "AdaptiveIndexCreateThreshold", type = "dword", data = 10 },
    { name = "AdaptiveIndexDropThreshold", type = "dword", data = 1 },
    { name = "MaxBatchSize", type = "dword", data = 100 },
    { name = "SheddingWindowSeconds", type = "dword", data = 10 },
    { name = "SheddingBatchPercent", type = "dword", data = 50 },
    { name = "EmergencySheddingBufferPercent", type = "dword", data = 95 },
} })

local STORE = eventd.STORE.events
local SHARD0 = STORE .. "/shard-0000.db"
local SHARD1 = STORE .. "/shard-0001.db"
-- The adaptively indexed header fields, by path, and the column each is
-- stored in (and its index named for).
local HEADERS = { "event.type", "emitter.class", "event.cpu", "emitter.token.guid", "emitter.true-token.guid",
                  "emitter.process.guid", "event.boot.guid" }
local COLUMN = {
    ["event.type"] = "event_type", ["emitter.class"] = "origin_class", ["event.cpu"] = "cpu_id",
    ["emitter.token.guid"] = "effective_token_guid", ["emitter.true-token.guid"] = "true_token_guid",
    ["emitter.process.guid"] = "process_guid", ["event.boot.guid"] = "boot_id",
}

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

--- Run `n` queries whose one WHERE predicate is `pred`.
local function query_n(v, n, pred, ty)
    for _ = 1, n do
        local r = eventd.query(v, "EVENTS " .. (ty or "pt.ix.q") .. " WHERE " .. pred .. " SINCE 1h ago")
        assert(r.ok, "query WHERE " .. pred .. " failed: " .. tostring(r.stderr))
    end
end

--- Make the policy run: any applied configuration change sends it a
--- Recompute.
local function run_policy(v)
    eventd.set(v, "LogRetentionDays", "dword:13"):assert_ok()
    eventd.unset(v, "LogRetentionDays"):assert_ok()
end

local function counters(v)
    local out = {}
    for _, r in ipairs(eventd.sql(v, eventd.DB.meta, "SELECT field_path, query_count FROM index_counters")) do
        out[r[1]] = r[2]
    end
    return out
end

local function desired(v)
    local out, order = {}, {}
    for _, r in ipairs(eventd.sql(v, eventd.DB.meta,
        "SELECT field_path, priority, is_expression FROM desired_indexes ORDER BY priority")) do
        out[r[1]] = { priority = r[2], expression = r[3] }
        order[#order + 1] = r[1]
    end
    return out, order
end

--- `eventd.sql`, retried: the host-side copy of a database and its WAL is
--- not a snapshot, and a shard taking a flood can be caught mid-commit.
local function sql(v, db, q)
    local last
    for _ = 1, 10 do
        local ok, rows = pcall(eventd.sql, v, db, q)
        if ok then return rows end
        last = rows
        v:clock():sleep("250ms")
    end
    error(last, 0)
end

--- Explicit indexes on events in `db`: {name = sql}.
local function indexes(v, db)
    local out = {}
    for _, r in ipairs(sql(v, db,
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'events' AND sql IS NOT NULL")) do
        out[r[1]] = r[2]
    end
    return out
end

--- idx_events_payload_ + the 32 lowercase hex digits of the field GUID,
--- uuid_v5(EVENTD_FIELD_NAMESPACE, path), computed on the host.
local function payload_index_name(path)
    local p = assert(io.popen("python3 -c \"import uuid,sys; print(uuid.uuid5(uuid.UUID(" ..
        "'e7d3a1b0-5c2f-4e8a-9b1d-0a6f3c8e2d4b'), sys.argv[1]).hex)\" '" .. path .. "'", "r"))
    local h = p:read("l")
    p:close()
    return "idx_events_payload_" .. h
end

local function header_index_name(path) return "idx_events_" .. COLUMN[path] end

local function wait_for(desc, fn, timeout)
    wait_until(fn, { timeout = timeout or 30, interval = 0.25, desc = desc })
end

--- Emit `n` events of `ty` with `payload`, 256 to a syscall.
local function burst(v, ty, payload, n)
    local sent = 0
    while sent < n do
        local entries = {}
        for i = 1, math.min(256, n - sent) do entries[i] = { type = ty, payload = payload } end
        local r = kmes.emit_batch(v, entries)
        assert(r.emitted and r.emitted > 0, "kmes_emit_batch emitted nothing: errno " .. tostring(r.errno))
        sent = sent + r.emitted
    end
    return sent
end

--- `eventd.stored_count`, retried as `sql` is: a shard taking a flood can
--- be caught mid-commit more often than `eventd.sql`'s own retries cover.
local function stored(v, ty)
    local last
    for _ = 1, 3 do
        local ok, n = pcall(eventd.stored_count, v, ty)
        if ok then return n end
        last = n
    end
    error(last, 0)
end

-- ---------------------------------------------------------------------------
-- Counters, policy and convergence, on vm
-- ---------------------------------------------------------------------------

test("a WHERE field's counter counts its queries and is flushed to eventd-meta.db, never to a shard", {
    spec = "eventd *index.a-field-in-a-where-predicate-increments-its-counter-once-per-query-per-predicate"
        .. " eventd *index.counters-are-flushed-periodically-to-the-metadata-database-never-a-shard",
}, function(t)
    local f = eventd.marker("ixc")
    query_n(vm, 3, f .. " == 1")
    t:assert_eq(counters(vm)[f], nil, "the counter is not written per query")
    run_policy(vm)
    wait_for("the counter to be flushed", function() return counters(vm)[f] ~= nil end)
    t:assert_eq(counters(vm)[f], 3, "three queries, three counts")
    for _, s in ipairs(eventd.shards(vm)) do
        local tables = eventd.sql(vm, s, "SELECT name FROM sqlite_master WHERE type = 'table'")
        for _, r in ipairs(tables) do
            t:assert(not r[1]:find("counter", 1, true) and not r[1]:find("desired", 1, true),
                s .. " holds no index-policy state: " .. r[1])
        end
    end
end)

local PB, PA, PC -- payload fields queried 15, 12 and 5 times

test("the policy alone writes the desired set: the fields over threshold, most queried first", {
    spec = "eventd *index.the-policy-computes-the-desired-set-as-fields-in-priority-order"
        .. " eventd *index.the-policy-runs-every-policy-interval-and-is-the-only-desired-set-writer"
        .. " eventd *index.payload-indexes-are-ordinary-desired-set-members",
}, function(t)
    PB, PA, PC = eventd.marker("ixb"), eventd.marker("ixa"), eventd.marker("ixl")
    query_n(vm, 15, PB .. " == 1")
    query_n(vm, 12, PA .. " == 1")
    query_n(vm, 5, PC .. " == 1")
    local d = desired(vm)
    t:assert(not d[PB] and not d[PA], "past the threshold, but not desired until the policy runs")
    run_policy(vm)
    wait_for("the policy to admit both fields", function()
        local now = desired(vm)
        return now[PB] ~= nil and now[PA] ~= nil
    end)
    d = desired(vm)
    t:assert(d[PB].priority < d[PA].priority, "15 queries rank above 12: " .. d[PB].priority .. " < " .. d[PA].priority)
    t:assert_eq(d[PC], nil, "5 queries are under the threshold of 10")
    t:assert_eq(d[PB].expression, 1, "a payload field is an expression member of the same ordered set")
    local _, order = desired(vm)
    local seen = {}
    for i, f in ipairs(order) do seen[i] = d[f].priority end
    for i = 2, #seen do t:assert(seen[i] > seen[i - 1], "priorities are a strict order") end
end)

test("writers move every shard's indexes to the one global desired set, on their own connections", {
    spec = "eventd *index.writer-threads-move-their-material-indexes-toward-the-desired-set"
        .. " eventd *index.the-desired-set-is-one-global-list-for-every-shard"
        .. " eventd *index.writer-threads-never-read-counters-or-write-the-desired-set"
        .. " eventd *index.index-creation-and-removal-run-on-the-shards-writer-thread",
}, function(t)
    t:assert(PB and PA, "the policy case above ran")
    local want = { payload_index_name(PB), payload_index_name(PA) }
    for _, s in ipairs({ SHARD0, SHARD1 }) do
        wait_for(s .. " to converge", function()
            local ix = indexes(vm, s)
            return ix[want[1]] ~= nil and ix[want[2]] ~= nil
        end)
        t:assert_eq(indexes(vm, s)[payload_index_name(PC)], nil, s .. ": nothing for the under-threshold field")
    end
    local a, b = indexes(vm, SHARD0), indexes(vm, SHARD1)
    for name in pairs(a) do t:assert(b[name], "shard-0001 has " .. name .. " too") end
    for name in pairs(b) do t:assert(a[name], "shard-0000 has " .. name .. " too") end
    -- A writer that read counters would have built an index the moment a
    -- field crossed the threshold; none exists before the policy runs.
    local early = eventd.marker("ixe")
    query_n(vm, 11, early .. " == 1")
    t:assert_eq(indexes(vm, SHARD0)[payload_index_name(early)], nil, "a field over threshold waits for the policy")
    -- The only read-write descriptor on each shard is its writer's, so the
    -- indexes were written through the writer's connection.
    local pid = eventd.pid(vm)
    for _, s in ipairs({ SHARD0, SHARD1 }) do
        local rw = (eventd.fds_on(vm, pid, s))
        t:assert_eq(rw, 1, s .. " has exactly one read-write connection")
    end
end)

test("a payload index is named from the field GUID and keys rows on a deterministic extraction", {
    spec = "eventd *index.payload-indexes-are-named-by-the-32-lowercase-hex-digit-field-guid"
        .. " eventd *index.a-payload-index-keys-each-row-on-a-deterministic-extraction-of-the-field",
}, function(t)
    t:assert(PB, "the policy case above ran")
    local name = payload_index_name(PB)
    t:assert(name:match("^idx_events_payload_%x+$") and #name == #"idx_events_payload_" + 32,
        "32 lowercase hex digits: " .. name)
    local sql = indexes(vm, SHARD0)[name]
    t:assert(sql, "shard-0000 has " .. name)
    t:assert(sql:find("eventd_payload_key(payload, '" .. PB .. "')", 1, true),
        "keyed on an extraction of the field from the payload: " .. sql)
    -- SQLite accepts an expression index only on a deterministic function.
    t:assert(not sql:find("random", 1, true), "and nothing else")
end)

test("every header field but event.time is a candidate, indexed as idx_events_<column>; raw payload never is", {
    spec = "eventd *index.the-candidate-header-columns"
        .. " eventd *index.header-column-indexes-are-named-idx-events-column"
        .. " eventd *index.the-timestamp-column-is-not-adaptively-managed"
        .. " eventd *index.the-raw-payload-column-never-receives-a-column-index"
        .. " eventd *index.any-field-usable-in-a-where-predicate-is-a-candidate",
}, function(t)
    local lit = {
        ["event.type"] = '"pt.ix.q"', ["emitter.class"] = "0", ["event.cpu"] = "0",
        ["emitter.token.guid"] = '"{00000000-0000-0000-0000-000000000000}"',
        ["emitter.true-token.guid"] = '"{00000000-0000-0000-0000-000000000000}"',
        ["emitter.process.guid"] = '"{00000000-0000-0000-0000-000000000000}"',
        ["event.boot.guid"] = '"{00000000-0000-0000-0000-000000000000}"',
    }
    for _, c in ipairs(HEADERS) do query_n(vm, 10, c .. " == " .. lit[c]) end
    query_n(vm, 10, "event.time > 0")
    query_n(vm, 10, 'payload == "x"')
    run_policy(vm)
    for _, s in ipairs({ SHARD0, SHARD1 }) do
        wait_for(s .. " to index the header columns", function()
            local ix = indexes(vm, s)
            for _, c in ipairs(HEADERS) do if not ix[header_index_name(c)] then return false end end
            return true
        end)
        local ix = indexes(vm, s)
        for _, c in ipairs(HEADERS) do
            t:assert(ix[header_index_name(c)]:find("ON events%(" .. COLUMN[c]),
                s .. ": " .. header_index_name(c) .. " is on " .. COLUMN[c] .. ", " .. c .. "'s column")
        end
        for name, sql in pairs(ix) do
            t:assert(name == "idx_events_timestamp" or not sql:find("ON events%(timestamp"),
                s .. ": no adaptive index on timestamp (" .. name .. ")")
            t:assert(not sql:find("ON events%(payload%)"), s .. ": no column index on payload (" .. name .. ")")
        end
    end
    local c = counters(vm)
    t:assert_eq(c["event.time"], nil, "event.time is not counted")
    for _, h in ipairs(HEADERS) do
        t:assert(c[h], h .. " is counted by its path")
        t:assert_eq(c[COLUMN[h]], nil, "not by its column, " .. COLUMN[h])
    end
    t:assert_eq(c.payload, nil, "nor is the raw payload")
end)

test("a flattened payload path gets an expression index; a path suppressed by flattening never does", {
    spec = "eventd *index.a-queryable-flattened-payload-path-is-an-expression-index-candidate"
        .. " eventd *index.a-suppressed-payload-path-never-receives-an-index",
}, function(t)
    local nested = eventd.marker("ixn") .. ".inner"
    -- A map at a header path is suppressed, subtree and all (PSPU §3.22),
    -- so emitter.process.guid.<x> is never a payload field; a field beside
    -- the header path, under emitter.process, is an ordinary one.
    local suppressed = "emitter.process.guid." .. eventd.marker("ixs")
    local beside = "emitter.process." .. eventd.marker("ixb")
    query_n(vm, 10, nested .. " == 1")
    query_n(vm, 10, suppressed .. " == 1")
    query_n(vm, 10, beside .. " == 1")
    run_policy(vm)
    wait_for("the nested path's index", function()
        return indexes(vm, SHARD0)[payload_index_name(nested)] ~= nil
    end)
    t:assert(indexes(vm, SHARD1)[payload_index_name(nested)], "on both shards")
    wait_for("the index of the path beside a header path", function()
        return indexes(vm, SHARD0)[payload_index_name(beside)] ~= nil
    end)
    t:assert(indexes(vm, SHARD1)[payload_index_name(beside)], "on both shards: " .. beside .. " is a payload field")
    -- Give the suppressed one the same chance: both were admitted together.
    for _, s in ipairs({ SHARD0, SHARD1 }) do
        t:assert_eq(indexes(vm, s)[payload_index_name(suppressed)], nil, s .. ": no index for " .. suppressed)
        for _, sql in pairs(indexes(vm, s)) do
            t:assert(not sql:find(suppressed, 1, true), s .. ": nothing extracts " .. suppressed)
        end
    end
end)

test("with or without a payload index a predicate gives the query language's answer", {
    spec = "eventd *index.payload-indexes-resolve-fields-as-the-query-language-does-and-otherwise-only-narrow-candidates"
        .. " eventd *index.sqlite-comparison-never-substitutes-for-query-language-comparison"
        .. " eventd *index.rows-lacking-a-queryable-field-index-as-null-or-are-excluded-preserving-semantics",
}, function(t)
    local f = eventd.marker("ixv")
    local ty = "pt.ix.sem"
    local values = {
        { "upper", "Alpha" }, { "lower", "alpha" }, { "caps", "ALPHA" }, { "other", "beta" },
        { "int", 1 }, { "float", eventd.float(1.0) }, { "str1", "1" }, { "bool", true },
        { "bin", eventd.bin("alpha") }, { "map", eventd.map({ x = 1 }) }, { "nil", eventd.NIL },
        { "missing", nil },
    }
    local mark = eventd.marker("sem")
    for _, v in ipairs(values) do
        local p = { m = mark, label = v[1] }
        if v[2] ~= nil then p[f] = v[2] end
        t:assert_eq(eventd.emit(vm, ty, p).ret, 0, "emitted " .. v[1])
    end
    eventd.wait_rows(vm, "EVENTS " .. ty .. ' WHERE m == "' .. mark .. '" SINCE 10m ago',
        function(rs) return #rs == #values end)
    local preds = { f .. ' == "alpha"', f .. " == 1", f .. ' == "1"', f .. " == true", f .. " IS NULL",
                    f .. " != 1", f .. ' > "a"' }
    local function answers()
        local out = {}
        for _, p in ipairs(preds) do
            local labels = {}
            for _, r in ipairs(eventd.rows(vm, "EVENTS " .. ty .. ' WHERE m == "' .. mark .. '" WHERE ' .. p ..
                " SINCE 10m ago")) do
                labels[#labels + 1] = r.label
            end
            table.sort(labels)
            out[p] = table.concat(labels, ",")
        end
        return out
    end
    local before = answers()
    t:assert_eq(indexes(vm, SHARD0)[payload_index_name(f)], nil, "no index on the field yet")
    query_n(vm, 10, f .. ' == "alpha"', ty)
    run_policy(vm)
    wait_for("the field's index", function()
        local name = payload_index_name(f)
        return indexes(vm, SHARD0)[name] ~= nil and indexes(vm, SHARD1)[name] ~= nil
    end)
    local after = answers()
    for _, p in ipairs(preds) do
        t:assert_eq(after[p], before[p], "WHERE " .. p .. ": the same rows with the index as without")
    end
    t:assert(before[preds[1]]:find("upper", 1, true) and before[preds[1]]:find("caps", 1, true),
        "string equality folds case: " .. before[preds[1]])
end)

-- Route closed: the writer cancels a build when its queue is non-empty
-- (writer.rs handle_control, the converge_indexes cancel closure), and on
-- a shard a guest can build the CREATE INDEX finishes in milliseconds —
-- there is no window in which to land an event and then see the build
-- abandoned, rather than finished, from outside.
test("rising write pressure cancels an index build and the writer returns to batches", {
    spec = "eventd *index.rising-write-pressure-cancels-an-index-build-and-the-writer-returns-to-batches",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::a_cancelled_index_build_leaves_no_index_and_the_writer_takes_the_next_batch",
}, function() end)

-- Route closed: as above; the retry is the writer re-queueing its
-- IndexPolicy message after a cancel (writer.rs, `retry`).
test("a cancelled build is retried at the next quiet period", {
    spec = "eventd *index.a-cancelled-index-build-is-retried-at-the-next-quiet-period",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::a_cancelled_index_build_is_created_when_retried",
}, function() end)

-- Route closed: the progress-handler period is internal to the
-- connection (shard.rs converge_indexes registers it at 1,000).
test("a progress handler checks for cancellation every thousand opcodes", {
    spec = "eventd *index.a-progress-handler-checks-for-cancellation-every-thousand-opcodes",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::an_index_build_checks_for_cancellation_every_thousand_opcodes",
}, function() end)

-- Route closed: sqlite_master records a CREATE INDEX without its IF NOT
-- EXISTS, and the writer checks its material set before creating or
-- dropping (shard.rs converge_indexes), so neither guard is ever the one
-- that decides.
test("indexes are created IF NOT EXISTS and dropped IF EXISTS", {
    spec = "eventd *index.indexes-are-created-if-not-exists-and-dropped-if-exists",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::index_convergence_tolerates_an_index_created_or_dropped_since_its_material_read",
}, function() end)

-- Not observable: "may lag indefinitely" permits an outcome and
-- obliges none; no run can show a shard was allowed to lag.
test("a shard under sustained pressure may lag the desired set indefinitely", {
    spec = "eventd *index.a-shard-under-sustained-pressure-may-lag-the-desired-set-indefinitely",
    skip = true,
}, function() end)

-- ---------------------------------------------------------------------------
-- Shedding, on press
-- ---------------------------------------------------------------------------

-- Nine fields, queried 30, 28, …, 14 times so the policy ranks them in
-- this order: seven header columns and two payload paths.
local PRESS_FIELDS
local function press_index_names()
    local out = {}
    for i, f in ipairs(PRESS_FIELDS) do
        out[i] = (i <= #HEADERS) and header_index_name(f) or payload_index_name(f)
    end
    return out
end

local function material(v)
    local ix = indexes(v, SHARD0)
    local have = {}
    for i, name in ipairs(press_index_names()) do have[i] = ix[name] ~= nil end
    return have, ix
end

local function all_built(v)
    local have = material(v)
    for _, h in ipairs(have) do if not h then return false end end
    return true
end

--- Once per file: query nine fields 30, 28, …, 14 times, run the policy,
--- and wait for all nine indexes on press's shard.
local function press_fields()
    if PRESS_FIELDS then return end
    PRESS_FIELDS = {}
    for _, c in ipairs(HEADERS) do PRESS_FIELDS[#PRESS_FIELDS + 1] = c end
    PRESS_FIELDS[#PRESS_FIELDS + 1] = eventd.marker("ixp")
    PRESS_FIELDS[#PRESS_FIELDS + 1] = eventd.marker("ixq")
    local lit = { ["event.type"] = '"pt.x"', ["emitter.class"] = "0", ["event.cpu"] = "0" }
    for i, f in ipairs(PRESS_FIELDS) do
        query_n(press, 32 - 2 * i, f .. " == " .. (lit[f] or '"{00000000-0000-0000-0000-000000000000}"'))
    end
    run_policy(press)
    wait_for("all nine indexes", function() return all_built(press) end, 60)
end

--- Once per file: with all nine built and the shedding window quiet,
--- freeze eventd, emit 300 events, thaw it, and record what is left:
--- {have = material after the burst, ix = its indexes, large = how many
--- of the burst's batches exceeded 75% of MaxBatchSize}.
local SHED
local function shed_burst()
    if SHED then return SHED end
    press_fields()
    -- Let the shedding window empty of anything but quiet batches.
    press:clock():sleep("11s")
    local boot_seq = sql(press, SHARD0, "SELECT COALESCE(max(last_sequence), 0) FROM receipt_ranges")[1][1]
    local pid = eventd.pid(press)
    eventd.freeze(press, pid)
    local n = burst(press, "pt.ix.burst", eventd.msgpack({ k = 1 }), 300)
    eventd.thaw(press, pid)
    wait_for("the burst to be stored", function() return stored(press, "pt.ix.burst") >= n end)
    -- The last large batch has committed by now: the shedding window runs
    -- from about here.
    local stored_at = eventd.guest_ns(press)
    -- The graduated check runs after each batch's commit (writer.rs
    -- commit_batch), so the last batch's shed lands after its events are
    -- already visible, and a loaded host can stretch that gap. Read the
    -- material set once the shedding has shown and stopped moving, not at
    -- the moment the count is reached; on timeout the last read is what
    -- the case judges.
    local have, ix, prev
    pcall(wait_until, function()
        local h, x = material(press)
        local shed, key = 0, {}
        for i, b in ipairs(h) do
            if not b then shed = shed + 1 end
            key[i] = b and "1" or "0"
        end
        key = table.concat(key)
        local settled = key == prev and shed >= 2
        have, ix, prev = h, x, key
        return settled
    end, { timeout = 60, interval = 1, desc = "the burst's shedding to show and settle" })
    if not have then have, ix = material(press) end
    local settled_at = eventd.guest_ns(press)
    -- Batches of this burst: one receipt range per committed batch.
    local large, sizes = 0, {}
    for _, r in ipairs(sql(press, SHARD0, "SELECT first_sequence, last_sequence FROM receipt_ranges " ..
        "WHERE last_sequence > " .. boot_seq .. " ORDER BY first_sequence")) do
        sizes[#sizes + 1] = r[2] - r[1] + 1
        if (r[2] - r[1] + 1) * 4 > 300 then large = large + 1 end
    end
    SHED = { have = have, ix = ix, large = large, sizes = sizes, stored_at = stored_at, settled_at = settled_at }
    return SHED
end

test("too many large batches in the window shed the lowest-priority index, then the next, one per commit", {
    spec = "eventd *index.too-many-large-batches-in-the-window-sheds-the-lowest-priority-index"
        .. " eventd *index.persistent-pressure-sheds-the-next-lowest-index-in-turn"
        .. " eventd *index.the-graduated-shedding-check-runs-once-per-batch-commit"
        .. " eventd *index.the-timestamp-index-is-never-shed",
}, function(t)
    press_fields()
    local _, order = desired(press)
    t:assert_eq(table.concat(order, ","), table.concat(PRESS_FIELDS, ","), "the policy ranked them as queried")
    local s = shed_burst()
    local have, ix, large = s.have, s.ix, s.large
    local shed, kept_after_shed = 0, false
    for i = #have, 1, -1 do
        if not have[i] then
            shed = shed + 1
            t:assert(not kept_after_shed, "shed in priority order: " .. PRESS_FIELDS[i] ..
                " went while a lower one stayed")
        else
            kept_after_shed = true
        end
    end
    t:assert(shed >= 2, "pressure shed more than one index: " .. shed .. " (large batches " .. large .. ")")
    t:assert(shed <= large, "no more than one per large batch commit: " .. shed .. " of " .. large
        .. " (the burst's batches: " .. json.encode(s.sizes) .. ")")
    t:assert(ix.idx_events_timestamp, "idx_events_timestamp is still there")
end)

-- A shard is quiet with no pending events and no batch over 75% of
-- MaxBatchSize within SheddingWindowSeconds (10 on press), and a quiet
-- shard converges without waiting for the next policy run (an hour away).
-- So nothing shed comes back while the burst's large batches are inside
-- the window, and everything does soon after it.
test("once pressure subsides a quiet shard rebuilds what it shed, highest priority first", {
    spec = "eventd *index.shed-indexes-are-rebuilt-highest-priority-first-once-pressure-subsides"
        .. " eventd *index.an-idle-writer-takes-one-convergence-action-then-rechecks-pressure"
        .. " eventd *index.a-shard-is-quiet-with-no-pending-events-and-no-large-batch-in-the-shedding-window",
}, function(t)
    local s = shed_burst()
    local shed = 0
    for _, h in ipairs(s.have) do if not h then shed = shed + 1 end end
    t:assert(shed > 0, "the burst left indexes shed")
    t:assert(s.settled_at - s.stored_at < 8e9, "precondition: the shedding was read inside the window, "
        .. string.format("%.1f", (s.settled_at - s.stored_at) / 1e9) .. "s after the burst was stored")
    local first_back
    local ok = pcall(wait_for, "the shed indexes to be rebuilt", function()
        local have = material(press)
        local back, all = false, true
        for i, h in ipairs(have) do
            if h and not s.have[i] then back = true end
            if not h then all = false end
        end
        if back and not first_back then first_back = eventd.guest_ns(press) end
        return all
    end, 30)
    t:assert(ok, "30 quiet seconds later every shed index is back")
    local after = first_back and (first_back - s.stored_at) / 1e9 or -1
    t:assert(after >= 8, "and none came back while a large batch was inside the 10-second shedding window: "
        .. "the first, " .. string.format("%.1f", after) .. "s after the burst was stored")
end)

test("a full batch with the ring past EmergencySheddingBufferPercent drops every secondary index at once", {
    spec = "eventd *index.emergency-shedding-drops-every-secondary-index-at-once"
        .. " eventd *index.ring-fill-above-emergency-shedding-buffer-percent-raises-the-pressure-signal"
        .. " eventd *index.emergency-shedding-triggers-whether-or-not-a-build-is-in-progress",
}, function(t)
    press_fields()
    -- Graduated shedding off (it needs more than 100% of batches large),
    -- emergency at its floor; the change also runs the policy, which
    -- rebuilds what was shed.
    eventd.set(press, "SheddingBatchPercent", "dword:100"):assert_ok()
    eventd.set(press, "EmergencySheddingBufferPercent", "dword:50"):assert_ok()
    wait_for("all nine indexes again", function() return all_built(press) end, 60)
    -- About 13,000 events of ~280 bytes: ~87% of the 4 MiB ring. The
    -- writer's queue takes 4,096 of them; the rest stay in the ring, well
    -- over half of it, while the writer commits full batches of 100.
    local pid = eventd.pid(press)
    eventd.freeze(press, pid)
    local n = burst(press, "pt.ix.flood", eventd.msgpack({ s = string.rep("e", 190) }), 13000)
    eventd.thaw(press, pid)
    -- Wait for ingestion to settle (a ring this full may also lose a few
    -- to overwrite, so wait for the count to stop moving, not for n).
    local last, steady = -1, 0
    wait_for("the flood to be stored", function()
        local now = stored(press, "pt.ix.flood")
        if now == last then steady = steady + 1 else steady = 0 end
        last = now
        return now > 0 and steady >= 8
    end, 120)
    t:assert(last >= n * 0.9, "the flood was ingested: " .. last .. " of " .. n)
    local have, ix = material(press)
    eventd.unset(press, "SheddingBatchPercent")
    eventd.unset(press, "EmergencySheddingBufferPercent")
    for i, h in ipairs(have) do
        t:assert(not h, PRESS_FIELDS[i] .. "'s index was dropped")
    end
    t:assert(ix.idx_events_timestamp, "idx_events_timestamp was not")
end)
