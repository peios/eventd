-- eventd TRM §5.6 — adaptive rollups: a disposable query cache seeded by
-- repeated wide window queries and proven fresh at read time.
--
-- The cache is global and pruned by oldest window start, so rows one test
-- leaves behind (written ones reach an hour into the future) would outlive a
-- later test's own. `set_cap` therefore empties the cache, with a zero cap,
-- before it sets the one a test wants. Then:
--
--   * the seeding side (thresholds, which windows, recorded proofs, the batch
--     cap, the global row cap) is observed on the rows eventd itself writes;
--   * the read side (all-or-raw, the validity proof, baselines, non-finite
--     values) is observed with rollup rows written into the stopped store on
--     the host — valid proofs, but values offset so a cache-served answer is
--     distinguishable from a raw one.
--
-- One VM with eventd made ErrorControl=Normal / RestartPolicy=Never (so it can
-- be stopped for a store edit without peinit restarting it). One-minute
-- windows over "SINCE 1h ago"; 300 samples spread over the last ten complete
-- minutes. AdaptiveRollupMinSamples is lowered to 100 throughout.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local W = 60 * 1000000000
local Q = " SINCE 1h ago "

local vm = eventd.boot({
    name = "ev-rollup",
    noncritical = true,
})
eventd.set(vm, "AdaptiveRollupMinSamples", "dword:100")

local function minute_now() return math.floor(os.time() / 60) end

-- 300 samples over the ten complete minutes before the current one. Gauge
-- values cycle 1..50; `counter` makes them strictly increasing.
local function seed_samples(name, counter)
    local base = (minute_now() - 10) * W
    local recs = {}
    for i = 0, 299 do
        recs[#recs + 1] = { name = name, type = counter and "counter" or "gauge",
            value = counter and (i + 1) * 3 or (i % 50) + 1, timestamp = base + i * 2000000000 }
    end
    eventd.send_metric(vm, eventd.array(recs))
    wait_until(function()
        return eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '"
            .. name .. "'")[1][1] >= 300
    end, { timeout = 60, interval = 0.4, desc = "300 samples for " .. name })
    return base
end

local function rollups(name)
    return eventd.sql(vm, eventd.DB.metrics,
        "SELECT r.window_start, r.value, r.source_max_sample_id, r.transform, r.function, "
        .. "r.source_baseline_sample_id, r.window_width FROM rollups r JOIN series e ON e.id = r.series_id "
        .. "WHERE e.name = '" .. name .. "' ORDER BY r.window_start")
end

local function total_rollups()
    return eventd.sql(vm, eventd.DB.metrics, "SELECT COUNT(*) FROM rollups")[1][1]
end

-- Window values of a query result, keyed by window start.
local function by_window(q)
    local out = {}
    for _, row in ipairs(q.rows) do out[row.timestamp] = row.value end
    return out
end

-- True per-window averages straight from the samples.
local function raw_avgs(name)
    local out = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics,
        "SELECT (timestamp / " .. W .. ") * " .. W .. ", AVG(value) FROM samples s JOIN series e "
        .. "ON e.id = s.series_id WHERE e.name = '" .. name .. "' GROUP BY 1")) do
        out[r[1]] = r[2]
    end
    return out
end

local function close(a, b) return a ~= nil and b ~= nil and math.abs(a - b) < 1e-6 end

-- Set the global cap on a clean slate: a zero cap first empties the cache
-- (§5.6), then the cap the test wants, waiting until eventd has applied it.
local cap_now -- the cap this file last applied (nil: the default)
local function apply_cap(n)
    if cap_now == n then return end
    local since = eventd.guest_ns(vm)
    eventd.set(vm, "AdaptiveRollupMaxRows", "dword:" .. n):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. ' WHERE config.name == "AdaptiveRollupMaxRows"'
        .. ' AND config.value == ' .. n .. ' SINCE 10m ago', function(rs)
            for _, r in ipairs(rs) do if r["event.time"] >= since then return true end end
            return false
        end)
    cap_now = n
end

local function set_cap(n)
    apply_cap(0)
    wait_until(function() return total_rollups() == 0 end,
        { timeout = 15, interval = 0.3, desc = "a zero cap to empty the cache" })
    apply_cap(n)
end

-- Stop eventd, apply `sql` to the store on the host, put it back, restart.
local function edit_store(sql)
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.metrics, sql)
    eventd.start(vm)
end

-- SQL writing a complete, valid set of rollup rows for `name` over every
-- minute window from two hours ago to an hour ahead: each window's proof is
-- its true greatest sample id (0 when empty) and, for pair transforms, its
-- true immediately preceding sample. `value_sql` is the value written for a
-- non-empty window (an expression over the window's true AVG as `a`).
local function cache_sql(name, transform, func, value_sql)
    local first, last = (minute_now() - 120) * W, (minute_now() + 60) * W
    local baseline = (transform == 1 or transform == 2)
        and "(SELECT id FROM samples WHERE series_id = s.id AND timestamp < w.ws "
            .. "ORDER BY timestamp DESC, id DESC LIMIT 1)"
        or "NULL"
    return "WITH RECURSIVE w(ws) AS (SELECT " .. first .. " UNION ALL SELECT ws + " .. W
        .. " FROM w WHERE ws + " .. W .. " < " .. last .. ") "
        .. "INSERT OR REPLACE INTO rollups (series_id, window_start, window_width, transform, "
        .. "function, value, overflow, source_max_sample_id, source_baseline_sample_id) "
        .. "SELECT s.id, w.ws, " .. W .. ", " .. transform .. ", " .. func .. ", "
        .. "(SELECT CASE WHEN n = 0 THEN NULL ELSE " .. value_sql .. " END FROM "
        .. "(SELECT AVG(value) AS a, COUNT(*) AS n FROM samples WHERE series_id = s.id "
        .. "AND timestamp >= w.ws AND timestamp < w.ws + " .. W .. ")), 0, "
        .. "COALESCE((SELECT MAX(id) FROM samples WHERE series_id = s.id AND timestamp >= w.ws "
        .. "AND timestamp < w.ws + " .. W .. "), 0), " .. baseline .. " "
        .. "FROM w, series s WHERE s.name = '" .. name .. "';"
end

-- §5.6 Schema and identity.
test("the rollups table has the version-2 schema, CHECKs and pruning index", {
    spec = "eventd *rollup.schema-version-2-defines-the-rollups-table"
        .. " eventd *rollup.transform-distinguishes-none-rate-delta-p50-p95-and-p99"
        .. " eventd *rollup.function-distinguishes-the-four-window-functions"
        .. " eventd *rollup.value-is-null-only-for-an-empty-window-or-a-percentile-overflow",
}, function(t)
    local schema = eventd.schema(vm, eventd.DB.metrics)
    local r = schema.rollups
    t:assert(r, "there is a rollups table")
    t:assert(r:find("transform INTEGER NOT NULL CHECK (transform IN (0, 1, 2, 50, 95, 99))", 1, true),
        "transform distinguishes none/RATE/DELTA/P50/P95/P99")
    t:assert(r:find("function INTEGER NOT NULL CHECK (function BETWEEN 0 AND 3)", 1, true),
        "function distinguishes the four window functions")
    t:assert(r:find("CHECK (overflow = 0 OR value IS NULL)", 1, true),
        "value is null for an overflow")
    t:assert(r:find("PRIMARY KEY", 1, true) and r:find("WITHOUT ROWID", 1, true),
        "the cache identity is the five-column primary key, WITHOUT ROWID")
    t:assert(schema.idx_rollups_window and schema.idx_rollups_window:find("rollups(window_start)", 1, true),
        "the pruning index is on window_start")
end)

-- §5.6: "Metric ingestion does not inspect, update or coordinate with
-- rollups" and there is no background scanner. Even with the cap at a level
-- where rows would survive, ingestion alone writes none.
test("ingestion never writes rollups and nothing seeds them without a query", {
    spec = "eventd *rollup.metric-ingestion-never-touches-rollups"
        .. " eventd *rollup.there-is-no-background-rollup-scanner",
}, function(t)
    set_cap(5)
    local name = eventd.marker("ruping")
    seed_samples(name)
    vm:run("sleep 5")
    t:assert_eq(#rollups(name), 0, "no rollups exist for a series that was ingested but never queried")
end)

-- §5.6 says the writer "upserts a candidate … and prunes the oldest window
-- starts until no more than AdaptiveRollupMaxRows remain", so under the cap
-- nothing is pruned.
test("under the cap, a seeding query's windows stay cached", {
    spec = "eventd *rollup.the-writer-upserts-then-prunes-oldest-windows-down-to-adaptiverollupmaxrows",
}, function(t)
    set_cap(100000)
    local name = eventd.marker("rkeep")
    seed_samples(name)
    eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
    pcall(wait_until, function() return #rollups(name) > 0 end,
        { timeout = 15, interval = 0.5, desc = "rollups to be committed" })
    t:assert(#rollups(name) > 0,
        "with 100000 rows allowed, the query's windows remain in the cache: " .. #rollups(name))
end)

-- §5.6 Seeding and which windows are cached.
test("seeding needs enough inputs and caches complete, aligned windows with their greatest sample id", {
    spec = "eventd *rollup.seeding-requires-at-least-adaptiverollupminsamples-raw-inputs"
        .. " eventd *rollup.only-complete-epoch-aligned-windows-are-cached"
        .. " eventd *rollup.partial-edge-windows-are-always-read-from-raw-samples"
        .. " eventd *rollup.a-row-records-the-greatest-sample-id-in-its-window",
}, function(t)
    set_cap(12)
    local name = eventd.marker("rseed")
    seed_samples(name)
    eventd.set(vm, "AdaptiveRollupMinSamples", "dword:1000"):assert_ok()
    eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
    vm:run("sleep 4")
    t:assert_eq(#rollups(name), 0, "300 inputs do not seed when the threshold is 1000")
    eventd.set(vm, "AdaptiveRollupMinSamples", "dword:100"):assert_ok()
    local rows
    wait_until(function()
        eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
        rows = rollups(name)
        return #rows > 0
    end, { timeout = 30, interval = 1, desc = "rollups once the threshold is met" })
    local now = os.time() * 1000000000
    for _, r in ipairs(rows) do
        t:assert_eq(r[1] % W, 0, "window_start is a multiple of the width: " .. r[1])
        t:assert_eq(r[7], W, "window_width is the query's width")
        t:assert(r[1] + W <= now, "every cached window is complete (none overlaps now): " .. r[1])
        local maxid = eventd.sql(vm, eventd.DB.metrics,
            "SELECT COALESCE(MAX(s.id), 0) FROM samples s JOIN series e ON e.id = s.series_id "
            .. "WHERE e.name = '" .. name .. "' AND s.timestamp >= " .. r[1]
            .. " AND s.timestamp < " .. (r[1] + W))[1][1]
        t:assert_eq(r[3], maxid, "source_max_sample_id is the greatest sample id in the window")
        if maxid == 0 then t:assert(r[2] == nil, "an empty window is cached with no value and id 0") end
    end
    -- The query's partial edge windows are computed from raw samples: a
    -- sample in the current, incomplete minute is in the answer though it can
    -- never be cached.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1000 })
    local edge = minute_now() * W
    wait_until(function()
        return by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))[edge] == 1000
    end, { timeout = 20, interval = 0.5, desc = "the partial edge window from raw samples" })
    for _, r in ipairs(rollups(name)) do
        t:assert(r[1] ~= edge, "the partial edge window is never cached")
    end
end)

-- §5.6: "It enqueues no more than AdaptiveRollupBatchRows missing windows."
-- With the cap equal to the batch size a single submission survives intact,
-- and it is the OLDEST missing windows: had more been submitted, pruning
-- would have kept the newest instead.
test("one submission carries at most AdaptiveRollupBatchRows windows", {
    spec = "eventd *rollup.one-submission-carries-at-most-adaptiverollupbatchrows-windows-via-try-send",
}, function(t)
    eventd.set(vm, "AdaptiveRollupBatchRows", "dword:16"):assert_ok()
    set_cap(16)
    local name = eventd.marker("rbatch")
    seed_samples(name)
    local q = eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
    t:assert(q.ok, "the query answers")
    local rows
    wait_until(function() rows = rollups(name); return #rows > 0 end,
        { timeout = 20, interval = 0.5, desc = "one submission to commit" })
    t:assert_eq(#rows, 16, "the submission carried 16 windows")
    for i = 2, #rows do
        t:assert_eq(rows[i][1] - rows[i - 1][1], W, "consecutive windows")
    end
    local oldest_possible = (math.floor((os.time() - 3600) / 60) + 1) * W
    t:assert(math.abs(rows[1][1] - oldest_possible) <= W,
        "starting at the query's first complete window (the oldest missing ones)")
    eventd.unset(vm, "AdaptiveRollupBatchRows")
end)

-- §5.6: the cap is global, and pruning never touches series or samples.
test("the row cap is global and pruning leaves series and raw samples alone", {
    spec = "eventd *rollup.the-row-cap-is-global-to-the-metric-store"
        .. " eventd *rollup.pruning-never-deletes-series-or-raw-samples"
        .. " eventd *rollup.rollups-leave-raw-sample-retention-unchanged",
}, function(t)
    set_cap(3)
    local a, b = eventd.marker("rcapa"), eventd.marker("rcapb")
    seed_samples(a)
    seed_samples(b)
    for _ = 1, 3 do
        eventd.query(vm, "METRIC " .. a .. Q .. "AVG_OVER 1m")
        eventd.query(vm, "METRIC " .. b .. Q .. "AVG_OVER 1m")
        vm:run("sleep 2")
    end
    t:assert(total_rollups() <= 3, "the store-wide cache is held at 3 rows: " .. total_rollups())
    t:assert(total_rollups() > 0, "and the cap, not an empty cache, is what holds it there")
    for _, name in ipairs({ a, b }) do
        t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '"
            .. name .. "'")[1][1], 300, "pruning left " .. name .. "'s 300 raw samples intact")
        t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM series WHERE name = '" .. name .. "'")[1][1], 1,
            "and its series row")
    end
end)

-- §5.6 Eligibility. Seeding is observed with a surviving cap.
test("only SINCE + an explicit window function, per series, without WHERE, seeds", {
    spec = "eventd *rollup.eligibility-requires-since-and-an-explicit-window-function"
        .. " eventd *rollup.eligibility-requires-per-series-output"
        .. " eventd *rollup.eligibility-excludes-where-predicates-and-cross-type-filters",
}, function(t)
    set_cap(12)
    local function seeds(name, query)
        seed_samples(name)
        local q = eventd.query(vm, query)
        t:assert(q.ok, "the query itself runs: " .. query .. " " .. tostring(q.stderr))
        vm:run("sleep 4")
        return #rollups(name) > 0
    end
    local n1 = eventd.marker("relig1")
    t:assert(not seeds(n1, "METRIC " .. n1 .. "[]" .. Q .. "AVG"), "a scalar AVG does not seed")
    local n2 = eventd.marker("relig2")
    t:assert(not seeds(n2, "METRIC " .. n2 .. Q .. "AVG_OVER 1m WHERE boot_id IS NOT NULL"),
        "a window query with a WHERE predicate does not seed")
    local n3 = eventd.marker("relig3")
    t:assert(not seeds(n3, "METRIC " .. n3 .. Q .. "AVG_OVER 1m WHERE LOG eventd EXISTS"),
        "a window query with a cross-type filter does not seed")
    -- Two series under one unbracketed name: a combined result, not per series.
    local stem = eventd.marker("relig4")
    seed_samples(stem .. ".a")
    seed_samples(stem .. ".b")
    eventd.query(vm, "METRIC " .. stem .. ".*" .. Q .. "AVG_OVER 1m")
    vm:run("sleep 4")
    t:assert_eq(#rollups(stem .. ".a") + #rollups(stem .. ".b"), 0,
        "an unbracketed multi-series window query does not seed")
    -- The bracketed form of the same selection is per series, and seeds.
    eventd.query(vm, "METRIC " .. stem .. ".*[]" .. Q .. "AVG_OVER 1m")
    wait_until(function() return #rollups(stem .. ".a") + #rollups(stem .. ".b") > 0 end,
        { timeout = 20, interval = 0.5, desc = "the bracketed form to seed" })
end)

-- §5.6: a zero cap disables the cache immediately and live; answers are the
-- raw answers throughout; the first (miss) query is answered from raw.
test("raw stays authoritative; a zero cap disables rollups live without changing answers", {
    spec = "eventd *rollup.raw-samples-are-always-authoritative"
        .. " eventd *rollup.deleting-every-rollup-never-changes-a-query-answer"
        .. " eventd *rollup.a-miss-or-stale-row-is-answered-from-raw-samples-without-waiting-for-repair"
        .. " eventd *rollup.a-zero-row-cap-disables-rollups-immediately-without-changing-results"
        .. " eventd *rollup.the-three-rollup-controls-take-effect-live"
        .. " eventd *rollup.eligibility-requires-a-non-zero-adaptiverollupmaxrows",
}, function(t)
    set_cap(12)
    local name = eventd.marker("rraw")
    seed_samples(name)
    local truth = raw_avgs(name)
    local first = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    for ws, v in pairs(truth) do
        t:assert(close(first[ws], v), "the first (miss) query answers each window from raw: " .. ws)
    end
    wait_until(function() return #rollups(name) > 0 end,
        { timeout = 20, interval = 0.5, desc = "rollups to exist" })
    apply_cap(0)
    wait_until(function() return total_rollups() == 0 end,
        { timeout = 15, interval = 0.5, desc = "a zero cap to empty the cache at once" })
    local off = eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
    vm:run("sleep 3")
    t:assert_eq(total_rollups(), 0, "with the cap at zero a query writes nothing")
    local offv = by_window(off)
    for ws, v in pairs(first) do
        t:assert(close(offv[ws], v), "and every answer is unchanged: " .. ws)
    end
    set_cap(12)
end)

-- §5.6: "A query uses the cache only when every complete interior window for
-- the series has a valid row … otherwise it evaluates that series from raw."
-- A complete, valid cache whose values are offset by +1000 is served; remove
-- one interior row and the whole series is evaluated from raw instead.
test("a series is served from the cache only when every interior window has a valid row", {
    spec = "eventd *rollup.a-series-uses-the-cache-only-when-every-interior-window-has-a-valid-row",
}, function(t)
    set_cap(100000)
    local name = eventd.marker("rall")
    local base = seed_samples(name)
    local truth = raw_avgs(name)
    edit_store(cache_sql(name, 0, 0, "a + 1000"))
    local served = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    for ws, v in pairs(truth) do
        t:assert(close(served[ws], v + 1000), "a complete cache is served (offset values): " .. ws)
    end
    edit_store("DELETE FROM rollups WHERE window_start = " .. (base + 3 * W)
        .. " AND series_id = (SELECT id FROM series WHERE name = '" .. name .. "');")
    local raw = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    for ws, v in pairs(truth) do
        t:assert(close(raw[ws], v), "one missing interior row sends the whole series to raw: " .. ws)
    end
end)

-- §5.6 Validity proof: a row is served only if no greater sample id exists
-- in its window, so a backfilled sample makes it stale; the proof runs on
-- every read.
test("a backfilled sample makes its window's cached row stale", {
    spec = "eventd *rollup.a-backfilled-sample-makes-its-windows-row-stale"
        .. " eventd *rollup.a-row-is-served-only-if-its-window-has-no-greater-sample-id"
        .. " eventd *rollup.the-query-time-validity-proof-is-always-performed",
}, function(t)
    set_cap(100000)
    local name = eventd.marker("rstale")
    local base = seed_samples(name)
    edit_store(cache_sql(name, 0, 0, "a + 1000"))
    local truth = raw_avgs(name)
    local served = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    t:assert(close(served[base + 2 * W], truth[base + 2 * W] + 1000), "the cache is served first")
    -- Backfill into an old, cached window.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 500, timestamp = base + 2 * W + 1 })
    wait_until(function() return raw_avgs(name)[base + 2 * W] ~= truth[base + 2 * W] end,
        { timeout = 20, interval = 0.5, desc = "the backfilled sample to be stored" })
    local now_truth = raw_avgs(name)
    local after = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    t:assert(close(after[base + 2 * W], now_truth[base + 2 * W]),
        "the stale row was refused and the window recomputed with the backfill")
    t:assert(close(after[base + 5 * W], now_truth[base + 5 * W]),
        "and the series as a whole came from raw")
end)

-- §5.6: "An empty window records zero, so its first raw sample also
-- invalidates it."
test("an empty cached window's first sample invalidates it", {
    spec = "eventd *rollup.an-empty-window-records-zero-and-its-first-sample-invalidates-it",
}, function(t)
    set_cap(100000)
    local name = eventd.marker("rempty")
    local base = seed_samples(name)
    edit_store(cache_sql(name, 0, 0, "a + 1000"))
    local empty = base - 20 * W
    local row = eventd.sql(vm, eventd.DB.metrics, "SELECT value, source_max_sample_id FROM rollups "
        .. "WHERE window_start = " .. empty .. " AND series_id = (SELECT id FROM series WHERE name = '"
        .. name .. "')")
    t:assert(row[1][1] == nil and row[1][2] == 0, "the empty window is cached as null with id 0")
    local before = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
    t:assert(before[empty] == nil, "an empty window yields no result row")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 9, timestamp = empty + 5 })
    local after
    wait_until(function()
        after = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m"))
        return after[empty] ~= nil
    end, { timeout = 20, interval = 0.5, desc = "the formerly empty window to show its sample" })
    t:assert(close(after[empty], 9), "its first sample invalidated the cached empty row")
end)

-- §5.6: "Non-finite cached values are rejected."
test("a non-finite cached value is rejected", {
    spec = "eventd *rollup.non-finite-cached-values-are-rejected",
}, function(t)
    set_cap(100000)
    local name = eventd.marker("rinf")
    local base = seed_samples(name)
    edit_store(cache_sql(name, 0, 0, "a + 1000") .. " UPDATE rollups SET value = 1e999 WHERE window_start = "
        .. (base + 4 * W) .. " AND series_id = (SELECT id FROM series WHERE name = '" .. name .. "');")
    local truth = raw_avgs(name)
    local q = eventd.query(vm, "METRIC " .. name .. Q .. "AVG_OVER 1m")
    t:assert(q.ok, "the query succeeds: " .. tostring(q.stderr))
    local got = by_window(q)
    for ws, v in pairs(truth) do
        t:assert(close(got[ws], v), "the infinite row is refused and the series read from raw: " .. ws)
    end
end)

-- §5.6: RATE/DELTA rows record the immediately preceding sample's id as their
-- baseline (observed on eventd's own rows, with a surviving cap); a changed
-- baseline makes the row stale (observed with a written cache).
test("RATE rows record their baseline, and a changed baseline makes them stale", {
    spec = "eventd *rollup.rate-and-delta-rows-record-the-preceding-sample-id-as-their-baseline"
        .. " eventd *rollup.a-new-changed-or-deleted-baseline-makes-a-rate-or-delta-row-stale",
}, function(t)
    set_cap(12)
    local name = eventd.marker("rrate")
    local base = seed_samples(name, true)
    local rows
    wait_until(function()
        eventd.query(vm, "METRIC " .. name .. Q .. "RATE SUM_OVER 1m")
        rows = rollups(name)
        return #rows > 0
    end, { timeout = 30, interval = 1, desc = "RATE rollups" })
    for _, r in ipairs(rows) do
        t:assert_eq(r[4], 1, "transform is RATE (1)")
        t:assert_eq(r[5], 3, "function is SUM_OVER (3)")
        local pre = eventd.sql(vm, eventd.DB.metrics, "SELECT s.id FROM samples s JOIN series e ON "
            .. "e.id = s.series_id WHERE e.name = '" .. name .. "' AND s.timestamp < " .. r[1]
            .. " ORDER BY s.timestamp DESC, s.id DESC LIMIT 1")
        t:assert_eq(r[6], pre[1] and pre[1][1] or nil,
            "source_baseline_sample_id is the sample immediately before the window")
    end
    -- Now a written cache with a poisoned value, then a new sample between
    -- window 3's baseline and its start: the baseline changes, the row is
    -- stale, and the answer is computed from raw.
    set_cap(100000)
    edit_store(cache_sql(name, 1, 3, "777"))
    local served = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "RATE SUM_OVER 1m"))
    t:assert(close(served[base + 3 * W], 777), "the RATE cache is served first")
    eventd.send_metric(vm, { name = name, type = "counter", value = 3 * 90 + 1,
        timestamp = base + 3 * W - 1 })
    local after
    wait_until(function()
        after = by_window(eventd.query(vm, "METRIC " .. name .. Q .. "RATE SUM_OVER 1m"))
        return after[base + 3 * W] ~= nil and not close(after[base + 3 * W], 777)
    end, { timeout = 20, interval = 0.5, desc = "the changed baseline to invalidate the row" })
    t:assert(not close(after[base + 3 * W], 777), "the row with a changed baseline was refused")
end)

-- ---------------------------------------------------------------------------
-- Documented homes.
-- ---------------------------------------------------------------------------

-- Route closed: METRIC mode cannot stream (query_language.rs rejects STREAM
-- in METRIC mode) and only METRIC queries use rollups, so no streaming
-- continuation can use or seed the cache; there is nothing to observe.
test("streaming continuations neither use nor seed rollups (not observable)", {
    spec = "eventd *rollup.streaming-continuations-neither-use-nor-seed-rollups",
    skip = true,
    covered_by = "doc:not-observable METRIC mode cannot stream, and only METRIC queries use rollups",
}, function() end)

-- Route closed: the bounded non-blocking channel, the writer taking at most
-- one command only when its receive queue is idle and its batch committed,
-- and dropping a candidate on a full channel or writer pressure are internal
-- scheduling: every observable outcome is "fewer rows cached", which pruning
-- produces too and which never changes an answer.
test("the candidate channel, writer intake and drop-under-pressure (not observable)", {
    spec = "eventd *rollup.queries-submit-candidates-through-a-bounded-non-blocking-channel"
        .. " eventd *rollup.the-writer-takes-at-most-one-rollup-command-and-only-when-idle-and-committed"
        .. " eventd *rollup.a-full-channel-writer-pressure-or-write-failure-drops-the-candidate",
    skip = true,
    covered_by = "doc:not-observable internal rollup submission scheduling with no answer-visible effect",
}, function() end)
