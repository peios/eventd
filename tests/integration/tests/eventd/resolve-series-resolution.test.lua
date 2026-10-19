-- eventd TRM §5.3 — series resolution: turning each arriving sample into a
-- series_id on the single metric thread. This file proves the observable
-- steps and the cache's visible effects:
--
--   * every stored sample has a resolved series_id;
--   * the canonical label string is computed and hashed;
--   * a histogram's boundary blob keeps the producer's order (eventd never
--     sorts — it rejects a non-increasing set instead);
--   * counters and gauges have a null boundary blob and hash;
--   * a verified match reuses the series_id; a no-match inserts a new row; a
--     type mismatch against the matched series drops the record;
--   * the series cache is bounded by MetricSeriesCacheSize (read through the
--     eventd.metrics.series.cached health gauge) yet never caps series
--     creation, starts empty after a restart and warms on demand.
--
-- The hash-match verification and the by-(name,label_hash) lookup are proven
-- by a label-hash collision (two label sets that FNV-collide stay two
-- series); the changed-histogram-boundaries case is driven by a
-- boundary-blob collision. Both collisions were found with a
-- Pollard-rho search over the book's FNV-1a parameters and verified against
-- eventd's own hash. The cache's key structure, hit/miss cost and
-- undersized-cache thrash have no observable distinct from the result and are
-- documented as such.
--
-- Two VMs at peak: one file-scope, plus a dedicated VM for the boundary
-- collision, which would take eventd down if the collision were treated as
-- one series.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(2)

local vm = eventd.boot({ name = "ev-resolve" })

local function series_row(v, name)
    return eventd.sql(v, eventd.DB.metrics,
        "SELECT id, labels, type, label_hash, boundaries_hash, boundaries FROM series WHERE name = '" .. name .. "'")
end

local function await_series(v, name, n)
    local row
    wait_until(function()
        row = series_row(v, name)
        return #row == (n or 1)
    end, { timeout = 30, interval = 0.25, desc = "series row for " .. name })
    return row
end

-- §5.3: "Every arriving sample must be turned into a series_id before it can
-- be inserted."
test("every stored sample has a resolved series_id", {
    spec = "eventd *resolve.every-sample-is-resolved-to-a-series-id-before-insertion",
}, function(t)
    local name = eventd.marker("rid")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local s = await_series(vm, name)
    local samp = eventd.sql(vm, eventd.DB.metrics,
        "SELECT series_id FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")
    t:assert_eq(samp[1][1], s[1][1], "the sample's series_id is the resolved series' id")
end)

-- §5.3 steps 1-2.
test("resolution computes the canonical label string and hashes it", {
    spec = "eventd *resolve.first-the-canonical-label-string-is-computed"
        .. " eventd *resolve.then-the-canonical-label-string-is-hashed",
}, function(t)
    local name = eventd.marker("rlab")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1,
        labels = { zone = "z", app = "a" } })
    local s = await_series(vm, name)
    t:assert_eq(s[1][2], "app=a,zone=z", "the canonical label string is key-sorted and joined")
    t:assert_eq(s[1][4], eventd.hash_for_sql("app=a,zone=z"), "and label_hash is FNV-1a of that string")
end)

-- §5.3 step 3: producer order, never sorted; counters/gauges null.
test("a histogram keeps the producer boundary order, counters and gauges are null", {
    spec = "eventd *resolve.a-histograms-boundary-blob-and-hash-use-the-producer-supplied-order"
        .. " eventd *resolve.eventd-never-sorts-histogram-boundaries"
        .. " eventd *resolve.counters-and-gauges-have-a-null-boundary-blob-and-hash",
}, function(t)
    local h = eventd.marker("rho")
    eventd.send_metric(vm, { name = h, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.5), eventd.float(2.5), eventd.float(10.0) },
        counts = eventd.array{ 0, 1, 2 }, total_count = 2, sum = eventd.float(12.0),
    } })
    local hs = await_series(vm, h)
    local want = eventd.hex(string.pack("<I4", 3) .. string.pack("<d", 1.5)
        .. string.pack("<d", 2.5) .. string.pack("<d", 10.0))
    t:assert_eq(hs[1][6], want, "the blob holds the three boundaries in producer order")

    -- eventd never sorts: a non-increasing set is rejected, not reordered.
    local desc = eventd.marker("rdesc")
    eventd.send_metric(vm, { name = desc, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(2.0), eventd.float(1.0) },
        counts = eventd.array{ 1, 2 }, total_count = 2, sum = eventd.float(2.0),
    } })
    local ok = eventd.marker("rdok")
    eventd.send_metric(vm, { name = ok, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. ok .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(#series_row(vm, desc), 0, "descending boundaries are rejected, not sorted")

    -- A gauge has a null boundary blob and hash.
    local g = eventd.marker("rg")
    eventd.send_metric(vm, { name = g, type = "gauge", value = 1 })
    local gs = await_series(vm, g)
    t:assert(gs[1][5] == nil, "a gauge's boundaries_hash is null")
    t:assert(gs[1][6] == nil, "a gauge's boundaries blob is null")
end)

-- §5.3 steps 5-6: verified match reuses the id; no match inserts a new row;
-- type mismatch against the matched series drops the record.
test("a match reuses the series, a new name inserts a row, a type mismatch drops", {
    spec = "eventd *resolve.a-verified-match-reuses-the-existing-series-id"
        .. " eventd *resolve.no-match-inserts-a-new-series-row"
        .. " eventd *resolve.a-type-mismatch-with-the-matched-series-drops-the-record",
}, function(t)
    local name = eventd.marker("rmatch")
    -- Two samples, same name/labels/type: one series, reused id.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = { k = "v" } })
    eventd.send_metric(vm, { name = name, type = "gauge", value = 2, labels = { k = "v" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local s = series_row(vm, name)
    t:assert_eq(#s, 1, "a verified match reused the one series (no new row)")
    local ids = eventd.sql(vm, eventd.DB.metrics,
        "SELECT DISTINCT series_id FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")
    t:assert_eq(#ids, 1, "both samples share the reused series_id")

    -- A fresh name has no match: a new series row is inserted.
    local fresh = eventd.marker("rnew")
    eventd.send_metric(vm, { name = fresh, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. fresh .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(#series_row(vm, fresh), 1, "a no-match inserted a new series row")

    -- A counter against the matched gauge series drops the record.
    eventd.send_metric(vm, { name = name, type = "counter", value = 3, labels = { k = "v" } })
    eventd.send_metric(vm, { name = name, type = "gauge", value = 4, labels = { k = "v" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 3 end)
    local n = eventd.sql(vm, eventd.DB.metrics,
        "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")
    t:assert_eq(n[1][1], 3, "the mismatched counter was dropped: only the three gauge samples remain")
end)

-- §5.3: "The old one keeps its historical samples and the new one starts
-- accumulating" — for a non-colliding boundary change, a second series is
-- created and the first retains its sample.
test("a non-colliding boundary change makes a new series; the old keeps its samples", {
    spec = "eventd *resolve.the-old-histogram-series-keeps-its-historical-samples",
}, function(t)
    local name = eventd.marker("rold")
    eventd.send_metric(vm, { name = name, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.0), eventd.float(2.0) },
        counts = eventd.array{ 1, 1 }, total_count = 1, sum = eventd.float(1.0),
    } })
    await_series(vm, name, 1)
    eventd.send_metric(vm, { name = name, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.0), eventd.float(3.0) },
        counts = eventd.array{ 1, 1 }, total_count = 1, sum = eventd.float(1.0),
    } })
    local s = await_series(vm, name, 2)
    t:assert_eq(#s, 2, "the changed boundaries made a second series")
    -- The first series still has its sample.
    local first = eventd.sql(vm, eventd.DB.metrics,
        "SELECT COUNT(*) FROM samples WHERE series_id = " .. math.min(s[1][1], s[2][1]))
    t:assert_eq(first[1][1], 1, "the original series kept its historical sample")
end)

-- §5.3 steps 4-5 and §5.2: the lookup narrows by (name, label_hash) and then
-- verifies the full labels, so two label sets that FNV-collide stay two
-- distinct series. The two values below give the canonical strings "c=<v>"
-- with the SAME label_hash (6409608647051328177) under the book's FNV.
test("two label sets whose hashes collide are verified apart and stay two series", {
    spec = "eventd *resolve.lookup-is-by-name-label-hash-and-for-histograms-boundaries-hash"
        .. " eventd *resolve.a-hash-match-is-verified-against-the-full-labels-and-boundary-blob"
        .. " eventd *series.colliding-label-sets-are-still-distinct-series"
        .. " eventd *series.a-lookup-verifies-the-full-labels-and-boundaries-after-narrowing-by-hash",
}, function(t)
    local name = eventd.marker("rcollide")
    local v1, v2 = "0d886a48fed2ec9e", "5461728a45d0f6d3"
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = { c = v1 } })
    eventd.send_metric(vm, { name = name, type = "gauge", value = 2, labels = { c = v2 } })
    local s = await_series(vm, name, 2)
    t:assert_eq(#s, 2, "the two colliding label sets are two distinct series")
    t:assert_eq(s[1][4], 6409608647051328177, "both share the colliding label_hash")
    t:assert_eq(s[2][4], 6409608647051328177, "both share the colliding label_hash")
    t:assert(s[1][2] ~= s[2][2], "but their full canonical label strings differ, as verified: "
        .. s[1][2] .. " vs " .. s[2][2])
end)

-- §5.3: "It bounds memory, not the number of series … a new series is always
-- created in the database." Even with a small cache, every distinct series is
-- created. This is observable straight from the series table.
test("a full cache never prevents series creation; eventd does not cap it", {
    spec = "eventd *resolve.a-full-cache-never-prevents-series-creation"
        .. " eventd *resolve.eventd-does-not-cap-series-creation",
}, function(t)
    eventd.set(vm, "MetricSeriesCacheSize", "dword:1000"):assert_ok()
    local stem = eventd.marker("rcap")
    local now = os.time() * 1000000000
    -- 1100 distinct series in one datagram: more than the 1000-entry cache.
    local records = {}
    for i = 1, 1100 do
        records[i] = { name = stem .. "x" .. i, type = "gauge", value = i, timestamp = now + i }
    end
    eventd.send_metric(vm, eventd.array(records))
    local created
    wait_until(function()
        created = eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM series WHERE name LIKE '" .. stem .. "x%'")[1][1]
        return created >= 1100
    end, { timeout = 60, interval = 0.5, desc = "1100 series created" })
    t:assert_eq(created, 1100, "all 1100 series were created despite the 1000-entry cache")
    eventd.unset(vm, "MetricSeriesCacheSize")
end)

-- §5.3: "The bound is MetricSeriesCacheSize (§A), default 50000, with LRU
-- eviction." The only runtime observable of the cache's occupancy is the
-- eventd.metrics.series.cached health gauge (§5.7). (The 50000 default is not
-- written anywhere a guest can read; the bound itself is what is shown.)
test("the series cache is bounded by MetricSeriesCacheSize", {
    spec = "eventd *resolve.the-cache-is-bounded-by-metricseriescachesize-default-50000",
}, function(t)
    eventd.set(vm, "MetricSeriesCacheSize", "dword:1000"):assert_ok()
    eventd.set(vm, "HealthMetricIntervalSeconds", "dword:1"):assert_ok()
    local stem = eventd.marker("rbound")
    local now = os.time() * 1000000000
    local records = {}
    for i = 1, 1100 do
        records[i] = { name = stem .. "y" .. i, type = "gauge", value = i, timestamp = now + i }
    end
    eventd.send_metric(vm, eventd.array(records))
    wait_until(function()
        return eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM series WHERE name LIKE '" .. stem .. "y%'")[1][1] >= 1100
    end, { timeout = 60, interval = 0.5, desc = "1100 series created" })
    local cached
    wait_until(function()
        local rows = eventd.query(vm, "METRIC eventd.metrics.series.cached SINCE 10m ago")
        if rows.ok and #rows.rows > 0 then cached = rows.rows[#rows.rows].value end
        return cached ~= nil
    end, { timeout = 30, interval = 0.5, desc = "a series-cache health sample" })
    eventd.unset(vm, "MetricSeriesCacheSize")
    eventd.unset(vm, "HealthMetricIntervalSeconds")
    t:assert(cached <= 1000, "the cache holds at most MetricSeriesCacheSize (1000) entries: " .. tostring(cached))
end)

-- §5.3: "A histogram whose boundaries changed takes step 6: it is a new
-- series." and §5.2: "A hash is an index key, never an identity." Two
-- histograms with the SAME name and labels but DIFFERENT boundaries whose
-- blobs FNV-collide to the same boundaries_hash should still be two distinct
-- series — the full-blob verification is a no-match, so step 6 inserts a new
-- row. The boundary pairs below were found to collide under the book's own
-- FNV parameters (§5.2) and are each strictly increasing and finite. The
-- series table is unique on the full boundary blob, not its hash (§5.2,
-- schema version 3), so the second INSERT is no violation. Dedicated VM: a
-- failing commit would take eventd down.
test("two histogram series whose boundary hashes collide are still two series", {
    spec = "eventd *resolve.a-histogram-with-changed-boundaries-is-a-new-series"
        .. " eventd *series.the-series-table-is-unique-on-name-labels-and-boundaries",
}, function(t)
    local function dbl(bits) return (string.unpack("<d", string.pack("<I8", bits))) end
    -- boundaries_hash(both) == 5602808888786851467, blobs differ (see the
    -- collision search in the testset scratch).
    local A = { dbl(0x3ff740fe76b4fb21), dbl(0x40021c0000000000) }
    local B = { dbl(0x3ffc3e9005d714f8), dbl(0x4003040000000000) }
    local kb = eventd.boot({ name = "ev-resolve-collide" })
    local name = eventd.marker("rcoll")
    local function hist(bounds)
        return { name = name, type = "histogram", value = {
            boundaries = eventd.array{ eventd.float(bounds[1]), eventd.float(bounds[2]) },
            counts = eventd.array{ 0, 1 }, total_count = 1, sum = eventd.float(1.5),
        } }
    end
    -- First histogram: one series, committed.
    eventd.send_metric(kb, hist(A))
    wait_until(function() return #series_row(kb, name) == 1 end,
        { timeout = 30, interval = 0.25, desc = "the first histogram series" })
    -- Second histogram, different boundaries, colliding hash: a new series.
    eventd.send_metric(kb, hist(B))
    -- Give the store time to gain the second series.
    pcall(wait_until, function() return #series_row(kb, name) == 2 end,
        { timeout = 15, interval = 0.5, desc = "a second, distinct histogram series" })
    local count = #series_row(kb, name)
    t:assert_eq(count, 2,
        "a boundary-hash collision must yield two distinct series (book §5.2/§5.3)")
end)

-- ---------------------------------------------------------------------------
-- Unit / documented homes.
-- ---------------------------------------------------------------------------

-- Route closed: the metric thread's single ownership and the once-per-sample
-- resolution are internal control flow; a query sees the result, not where or
-- how often resolution ran.
test("resolution runs once per sample on the metric thread (not observable)", {
    spec = "eventd *resolve.resolution-runs-once-per-sample-on-the-metric-ingestion-thread",
    skip = true,
    covered_by = "doc:not-observable internal single-thread control flow with no interface-visible effect",
}, function() end)

-- Route closed: the cache key's composition is internal; only its effect
-- (reuse, bound) is visible, and those are covered above and by the collision
-- test. The mapping itself has no separate observable.
test("the series cache maps (name, labels, boundaries) to series_id (not observable)", {
    spec = "eventd *resolve.the-series-cache-maps-name-labels-and-boundaries-to-series-id",
    skip = true,
    covered_by = "doc:not-observable internal cache-key structure; its effects are tested via reuse and the bound",
}, function() end)

-- Route closed: whether a hit avoids SQLite is an internal performance
-- property; the stored result is identical either way.
test("a cache hit does not touch SQLite (not observable)", {
    spec = "eventd *resolve.a-cache-hit-does-not-touch-sqlite",
    skip = true,
    covered_by = "doc:not-observable internal fast-path with no interface-visible effect",
}, function() end)

-- Route closed: the one-SELECT cost and LRU eviction on a miss are internal;
-- no caller observes the SELECT count or the eviction order.
test("a cache miss costs one SELECT and evicts the LRU entry (not observable)", {
    spec = "eventd *resolve.a-cache-miss-costs-one-select-and-evicts-the-lru-entry-when-full",
    skip = true,
    covered_by = "cargo:eventd eventd-core metric_store::tests::a_cache_miss_costs_one_select_and_evicts_the_lru_entry_when_full",
}, function() end)

-- §5.3: "The cache starts empty after a restart and is warmed on demand …
-- There is no pre-warming pass." The store holds well over a thousand series
-- by now (the tests above); after a restart the cache gauge shows only what
-- has been resolved since — eventd's own health series — and grows as old
-- series are sampled again.
test("after a restart the cache starts empty and warms on demand, with no pre-warming", {
    spec = "eventd *resolve.the-cache-starts-empty-and-warms-on-demand"
        .. " eventd *resolve.there-is-no-cache-pre-warming-pass",
}, function(t)
    -- Make sure there are plenty of series to pre-warm, if anything did.
    local stem = eventd.marker("rwarm")
    local now = os.time() * 1000000000
    local recs = {}
    for i = 1, 600 do recs[i] = { name = stem .. "w" .. i, type = "gauge", value = i, timestamp = now + i } end
    eventd.send_metric(vm, eventd.array(recs))
    wait_until(function()
        return eventd.sql(vm, eventd.DB.metrics,
            "SELECT COUNT(*) FROM series WHERE name LIKE '" .. stem .. "w%'")[1][1] >= 600
    end, { timeout = 60, interval = 0.5, desc = "600 series" })
    local total = eventd.sql(vm, eventd.DB.metrics, "SELECT COUNT(*) FROM series")[1][1]

    eventd.set(vm, "HealthMetricIntervalSeconds", "dword:1"):assert_ok()
    eventd.restart(vm)
    local restarted = eventd.guest_ns(vm)
    local function cached_after(since)
        local found
        wait_until(function()
            local r = eventd.query(vm, "METRIC eventd.metrics.series.cached SINCE 10m ago")
            if not r.ok then return false end
            for _, row in ipairs(r.rows) do
                if row.timestamp > since then found = row.value end
            end
            return found ~= nil
        end, { timeout = 20, interval = 0.5, desc = "a cache gauge sample after " .. since })
        return found
    end
    local cold = cached_after(restarted)
    t:assert(cold < 100, "right after the restart the cache holds only freshly resolved "
        .. "series (" .. cold .. "), not the " .. total .. " in the store: no pre-warming")
    -- Sample 300 of the old series again: they are resolved and cached.
    local again = {}
    local later = os.time() * 1000000000
    for i = 1, 300 do again[i] = { name = stem .. "w" .. i, type = "gauge", value = i, timestamp = later + i } end
    eventd.send_metric(vm, eventd.array(again))
    local mark = eventd.guest_ns(vm)
    wait_until(function() return cached_after(mark) >= cold + 300 end,
        { timeout = 30, interval = 1, desc = "the cache to warm with the resampled series" })
    eventd.unset(vm, "HealthMetricIntervalSeconds")
end)

-- Route closed: that an undersized cache reloads its overflow every cycle is a
-- performance property; every query returns the same result whether the
-- SELECTs recur or not.
test("an undersized cache reloads the overflow each cycle (not observable)", {
    spec = "eventd *resolve.an-undersized-cache-reloads-the-overflow-every-collection-cycle",
    skip = true,
    covered_by = "doc:not-observable recurring cache misses have no interface-visible effect on results",
}, function() end)
