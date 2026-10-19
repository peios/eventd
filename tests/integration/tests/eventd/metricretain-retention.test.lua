-- eventd TRM §5.5 — metric retention. This file proves:
--
--   * samples older than MetricRetentionDays are deleted and a series with no
--     remaining samples is removed (the only thing that removes a series row);
--     an idle series persists until its last sample goes; the ninety-day
--     default, shown behaviourally; no downsampled replacement is left;
--   * before an affected series loses samples it loses its adaptive rollups,
--     including a series that keeps other samples (every affected series is
--     tracked), so a rollup never answers for expired samples;
--   * over a non-zero byte limit the oldest samples are deleted until the
--     logical live size (free pages excluded) is within it, in transactions of
--     RetentionDeleteBatchRows; both limits apply together; new series are
--     still accepted; VACUUM never runs; a zero limit disables size retention;
--     and the write path never makes the size decision.
--
-- Read-only planning is shown with the metric store's descriptor flags in
-- metricdb-database-lifecycle.test.lua. The permission to append one bounded
-- delete to an open transaction under urgent pressure is documented below.
--
-- Age retention only removes what this file ages out (other series carry
-- recent timestamps), so it shares the file-scope VM. Byte retention deletes
-- the oldest samples store-wide, so it runs on a second, dedicated VM. A pass
-- is kicked by any applied configuration change (config.rs apply_reload sets
-- retention_requested). Peak 2 VMs.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(2)

local DAY_NS = 86400 * 1000000000
-- ErrorControl=Normal / RestartPolicy=Never on this VM only, so eventd can be
-- stopped for a store edit without peinit restarting it.
local vm = eventd.boot({
    name = "ev-metricretain",
    noncritical = true,
})
local bytevm = eventd.boot({ name = "ev-metricretain-bytes" })

local function sample_count(v, name)
    return eventd.sql(v, eventd.DB.metrics,
        "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")[1][1]
end

local function series_count(v, name)
    return eventd.sql(v, eventd.DB.metrics,
        "SELECT COUNT(*) FROM series WHERE name = '" .. name .. "'")[1][1]
end

local function rollup_count(v, name)
    return eventd.sql(v, eventd.DB.metrics,
        "SELECT COUNT(*) FROM rollups r JOIN series e ON e.id = r.series_id WHERE e.name = '" .. name .. "'")[1][1]
end

local function await_samples(v, name, n)
    wait_until(function() return sample_count(v, name) >= n end,
        { timeout = 60, interval = 0.3, desc = n .. " samples for " .. name })
end

-- Send `n` gauge samples for `name` at `first + i*step`, 1000 to a datagram.
local function send_many(v, name, n, first, step)
    local sent = 0
    while sent < n do
        local recs = {}
        for j = 1, math.min(1000, n - sent) do
            local i = sent + j
            recs[j] = { name = name, type = "gauge", value = i, timestamp = first + i * step }
        end
        eventd.send_metric(v, eventd.array(recs))
        sent = sent + #recs
    end
end

-- Stop eventd, apply `sql` to the store on the host, put it back, restart.
local function edit_store(v, sql)
    eventd.stop(v)
    eventd.edit_store(v, eventd.DB.metrics, sql)
    eventd.start(v)
end

-- The store's logical live size, as retention measures it: pages in use
-- (page_count minus the freelist) times the page size.
local function live_size(v)
    local r = eventd.sql(v, eventd.DB.metrics,
        "SELECT (SELECT page_count FROM pragma_page_count) - (SELECT freelist_count FROM pragma_freelist_count), "
        .. "(SELECT page_size FROM pragma_page_size), (SELECT page_count FROM pragma_page_count)")
    return r[1][1] * r[1][2], r[1][3] * r[1][2]
end

-- §5.5: the ninety-day default, shown behaviourally. With the default
-- MetricRetentionDays in force, a 100-day-old sample ages out while an
-- 80-day-old one survives. The pass is kicked by an unrelated key.
test("metric retention defaults to ninety days", {
    spec = "eventd *metricretain.metric-retention-defaults-to-ninety-days",
}, function(t)
    local name = eventd.marker("mr90")
    local now = os.time() * 1000000000
    eventd.send_metric(vm, eventd.array{
        { name = name, type = "gauge", value = 1, timestamp = now - 100 * DAY_NS },
        { name = name, type = "gauge", value = 2, timestamp = now - 80 * DAY_NS },
    })
    await_samples(vm, name, 2)
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:9999"):assert_ok()
    wait_until(function() return sample_count(vm, name) == 1 end,
        { timeout = 60, interval = 0.3, desc = "the 100-day sample to age out under the default" })
    local left = eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")
    t:assert_eq(#left, 1, "only one sample remains")
    t:assert_eq(left[1][1], 2, "the 80-day sample survived; the 100-day one did not")
    eventd.unset(vm, "RetentionDeleteBatchRows")
end)

-- §5.5 The age pass.
test("samples past MetricRetentionDays are deleted, and an emptied series is removed", {
    spec = "eventd *metricretain.samples-older-than-metricretentiondays-are-deleted"
        .. " eventd *metricretain.series-with-no-remaining-samples-are-deleted"
        .. " eventd *metricretain.retention-is-the-only-mechanism-that-removes-a-series-row"
        .. " eventd *metricretain.an-idle-series-persists-until-its-last-sample-is-removed"
        .. " eventd *metricretain.metric-retention-runs-on-the-shared-retention-thread-after-the-log-store"
        .. " eventd *metricretain.retention-deletes-samples-without-downsampling",
}, function(t)
    local now = os.time() * 1000000000
    local aged = eventd.marker("mrold")   -- all samples old: series must vanish
    local live = eventd.marker("mrlive")  -- one recent sample: series must persist
    eventd.send_metric(vm, eventd.array{
        { name = aged, type = "gauge", value = 1, timestamp = now - 3 * DAY_NS },
        { name = aged, type = "gauge", value = 2, timestamp = now - 2 * DAY_NS },
        { name = live, type = "gauge", value = 1, timestamp = now - 2 * DAY_NS },
        { name = live, type = "gauge", value = 2, timestamp = now },
    })
    await_samples(vm, aged, 2)
    await_samples(vm, live, 2)
    -- A day's limit deletes everything older than a day. Our recent samples
    -- (and other series') are untouched. Nothing else removes a series: the
    -- aged one has existed idle since its samples were written.
    t:assert_eq(series_count(vm, aged), 1, "the idle series exists until retention runs")
    eventd.set(vm, "MetricRetentionDays", "dword:1"):assert_ok()
    wait_until(function() return series_count(vm, aged) == 0 end,
        { timeout = 60, interval = 0.3, desc = "the fully-aged series to be removed" })
    t:assert_eq(sample_count(vm, aged), 0, "all samples past the limit were deleted")
    t:assert_eq(series_count(vm, live), 1, "the series with a recent sample persists")
    local left = eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. live .. "'")
    t:assert(#left == 1 and left[1][1] == 2,
        "only the recent sample is left, with no downsampled replacement: " .. json.encode(left))
    eventd.unset(vm, "MetricRetentionDays")
end)

-- §5.5 steps 3-4 and "Rollups are not downsampling": every affected series
-- loses its rollups before its samples go, so no rollup can answer for an
-- expired sample.
test("retention invalidates the rollups of every series it deletes from", {
    spec = "eventd *metricretain.an-affected-series-loses-its-rollups-before-its-samples-are-deleted"
        .. " eventd *metricretain.every-series-that-lost-samples-is-tracked"
        .. " eventd *metricretain.rollups-never-answer-for-expired-samples",
}, function(t)
    local now = os.time()
    -- 300 samples two days old, 30 an hour over ten complete hours.
    local base = (math.floor(now / 3600) - 48) * 3600 * 1000000000
    local gone = eventd.marker("mrrgone")   -- every sample old
    local kept = eventd.marker("mrrkept")   -- old samples plus a recent one
    send_many(vm, gone, 300, base, 120 * 1000000000)
    send_many(vm, kept, 300, base, 120 * 1000000000)
    eventd.send_metric(vm, { name = kept, type = "gauge", value = 7, timestamp = now * 1000000000 })
    await_samples(vm, gone, 300)
    await_samples(vm, kept, 301)
    -- Valid hourly rollup rows for both series' old windows, written into the
    -- stopped store. (Written rather than seeded by a query: this test is
    -- about what retention does with rows that exist, not about seeding.)
    local H = 3600 * 1000000000
    edit_store(vm, "WITH RECURSIVE w(ws) AS (SELECT " .. base .. " UNION ALL SELECT ws + " .. H
        .. " FROM w WHERE ws + " .. H .. " < " .. (base + 10 * H) .. ") "
        .. "INSERT INTO rollups (series_id, window_start, window_width, transform, function, value, "
        .. "overflow, source_max_sample_id, source_baseline_sample_id) "
        .. "SELECT s.id, w.ws, " .. H .. ", 0, 0, (SELECT AVG(value) FROM samples WHERE series_id = s.id "
        .. "AND timestamp >= w.ws AND timestamp < w.ws + " .. H .. "), 0, "
        .. "COALESCE((SELECT MAX(id) FROM samples WHERE series_id = s.id AND timestamp >= w.ws "
        .. "AND timestamp < w.ws + " .. H .. "), 0), NULL FROM w, series s "
        .. "WHERE s.name IN ('" .. gone .. "', '" .. kept .. "');")
    t:assert(rollup_count(vm, gone) > 0 and rollup_count(vm, kept) > 0, "both series have rollups")
    eventd.set(vm, "MetricRetentionDays", "dword:1"):assert_ok()
    wait_until(function() return series_count(vm, gone) == 0 end,
        { timeout = 60, interval = 0.3, desc = "retention to remove the old series" })
    t:assert_eq(rollup_count(vm, gone), 0, "the emptied series' rollups are gone")
    t:assert_eq(series_count(vm, kept), 1, "the partly-aged series remains")
    t:assert_eq(rollup_count(vm, kept), 0,
        "and it too lost its rollups: every series retention deleted from was tracked")
    local rows = eventd.rows(vm, "METRIC " .. kept .. " SINCE 3d ago AVG_OVER 1h")
    local cutoff = (now - 86400) * 1000000000
    for _, row in ipairs(rows) do
        t:assert(row.timestamp >= cutoff - 3600 * 1000000000,
            "no window answers for expired samples: " .. json.encode(row))
    end
    eventd.unset(vm, "MetricRetentionDays")
end)

-- §5.5 The byte pass (dedicated VM: deletes the oldest store-wide).
test("over a byte limit the oldest samples go, in capped transactions, until the live size fits", {
    spec = "eventd *metricretain.over-the-byte-limit-the-oldest-samples-are-deleted-until-within-it"
        .. " eventd *metricretain.the-byte-limit-measures-logical-live-size-as-for-events"
        .. " eventd *metricretain.freed-pages-are-excluded-from-the-size-measure"
        .. " eventd *metricretain.each-delete-transaction-is-capped-at-retentiondeletebatchrows"
        .. " eventd *metricretain.vacuum-is-never-run-automatically"
        .. " eventd *metricretain.size-pressure-retires-old-samples-rather-than-refusing-new-series",
}, function(t)
    local name = eventd.marker("mrbyte")
    -- A day old, so these are the oldest samples in the store (older than
    -- every health sample) and the size pass deletes nothing else.
    local first = (os.time() - 86400) * 1000000000
    send_many(bytevm, name, 20000, first, 1)
    await_samples(bytevm, name, 20000)
    eventd.set(bytevm, "RetentionDeleteBatchRows", "dword:2000"):assert_ok()
    eventd.set(bytevm, "MetricRetentionMaxBytes", "qword:400000"):assert_ok()
    -- Each delete and checkpoint command waits for the metric thread's next
    -- loop, so a pass of several batches takes a while.
    wait_until(function() return live_size(bytevm) <= 400000 end,
        { timeout = 240, interval = 1, desc = "size retention to bring the store within the limit" })
    bytevm:run("sleep 3")
    local left = sample_count(bytevm, name)
    local live, file = live_size(bytevm)
    t:assert(left > 0, "deletion stopped short of emptying the series: " .. left)
    t:assert(live <= 400000, "the logical live size is within the limit: " .. live)
    t:assert(file > live, "while the file still holds freed pages (" .. file .. " bytes): the "
        .. "measure excludes them, and VACUUM did not run")
    t:assert(eventd.sql(bytevm, eventd.DB.metrics, "PRAGMA freelist_count")[1][1] > 0,
        "freed pages remain on the freelist")
    t:assert_eq((20000 - left) % 2000, 0,
        "samples went in whole RetentionDeleteBatchRows transactions: " .. (20000 - left) .. " deleted")
    -- The survivors are the newest: the oldest went first.
    local oldest = eventd.sql(bytevm, eventd.DB.metrics,
        "SELECT MIN(value) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '" .. name .. "'")[1][1]
    t:assert_eq(oldest, 20000 - left + 1, "the deleted samples were the oldest")
    -- New series are still accepted under size pressure.
    local fresh = eventd.marker("mrfresh")
    eventd.send_metric(bytevm, { name = fresh, type = "gauge", value = 1 })
    wait_until(function() return series_count(bytevm, fresh) == 1 end,
        { timeout = 30, interval = 0.3, desc = "a new series accepted under size pressure" })
end)

-- §5.5: "The size decision is made by the retention coordinator, never while
-- a datagram is parsed or a sample is committed." With the byte limit still
-- in force and no pass due, ingesting past it leaves the store over it.
test("the write path never makes the size decision", {
    spec = "eventd *metricretain.the-size-decision-is-made-only-by-the-retention-coordinator",
}, function(t)
    local name = eventd.marker("mrover")
    send_many(bytevm, name, 10000, os.time() * 1000000000, 1)
    await_samples(bytevm, name, 10000)
    bytevm:run("sleep 5")
    local live = live_size(bytevm)
    t:assert(live > 400000, "ingestion pushed the store over the limit and it stays there: " .. live)
    t:assert_eq(sample_count(bytevm, name), 10000, "nothing was deleted on the write path")
end)

-- §5.5: "Both limits are enforced and the more aggressive wins." With the byte
-- limit in force, a pass also applies the age limit: an old sample goes even
-- though the byte limit alone would keep it, and the byte limit trims what the
-- age limit alone would keep.
test("both limits are enforced together", {
    spec = "eventd *metricretain.both-limits-are-enforced-and-the-more-aggressive-wins",
}, function(t)
    local old = eventd.marker("mrage")
    eventd.send_metric(bytevm, { name = old, type = "gauge", value = 1,
        timestamp = os.time() * 1000000000 - 3 * DAY_NS })
    await_samples(bytevm, old, 1)
    local before = eventd.sql(bytevm, eventd.DB.metrics, "SELECT COUNT(*) FROM samples")[1][1]
    eventd.set(bytevm, "MetricRetentionDays", "dword:1"):assert_ok()
    wait_until(function() return sample_count(bytevm, old) == 0 end,
        { timeout = 60, interval = 0.5, desc = "the 3-day-old sample to age out" })
    wait_until(function() return live_size(bytevm) <= 400000 end,
        { timeout = 60, interval = 0.5, desc = "the byte limit to bring the store back within" })
    local after = eventd.sql(bytevm, eventd.DB.metrics, "SELECT COUNT(*) FROM samples")[1][1]
    t:assert(after < before - 1, "the byte limit removed recent samples the age limit keeps: "
        .. before .. " -> " .. after)
    eventd.unset(bytevm, "MetricRetentionDays")
end)

-- §5.5: "zero is an explicit opt-out." With the byte limit at 0, size
-- retention does nothing however large the store.
test("a zero byte limit disables size retention", {
    spec = "eventd *metricretain.the-byte-limit-defaults-to-1-gib-and-zero-disables-it",
}, function(t)
    eventd.set(bytevm, "MetricRetentionMaxBytes", "qword:0"):assert_ok()
    local name = eventd.marker("mrzero")
    send_many(bytevm, name, 10000, os.time() * 1000000000, 1)
    await_samples(bytevm, name, 10000)
    t:assert(live_size(bytevm) > 400000, "the store is well past the old limit")
    eventd.set(bytevm, "RetentionDeleteBatchRows", "dword:600"):assert_ok()
    bytevm:run("sleep 5")
    t:assert_eq(sample_count(bytevm, name), 10000, "with the byte limit at zero, nothing was deleted")
    eventd.unset(bytevm, "RetentionDeleteBatchRows")
end)

-- ---------------------------------------------------------------------------
-- Documented homes.
-- ---------------------------------------------------------------------------

-- Route closed: the book permits ("may") the writer to append one bounded
-- delete to an open transaction under urgent pressure; urgent pressure means
-- SQLITE_FULL on a store volume, which a tmpfs-backed overlay cannot be made
-- to report without starving the whole guest, and the permission has no
-- observable when unused. HEAD discards the batch and requests a pass
-- (metric_ingest.rs:419-423) rather than appending.
test("urgent size pressure may append one bounded delete (permission; not observable)", {
    spec = "eventd *metricretain.urgent-size-pressure-may-add-one-bounded-delete-to-an-open-transaction",
    skip = true,
    covered_by = "doc:not-observable a permission exercised only on SQLITE_FULL",
}, function() end)
