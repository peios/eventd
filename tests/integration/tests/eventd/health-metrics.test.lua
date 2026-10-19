-- eventd TRM §5.7 — eventd's own health, recorded as eventd.* metrics and
-- read back through the ordinary query channel (and, for the catalogue,
-- straight from the series table).
--
-- What each test proves:
--   * the metric thread samples every health series each interval, bypassing
--     the socket and the EVENTD_PUBLISH check;
--   * a zero interval turns them off;
--   * the counters restart from zero with eventd;
--   * the catalogue: events.stored (per shard), events.lost (per cpu),
--     kmes.ring.fill.percent (per cpu), events.index.sheds (per reason),
--     store.bytes (per store, with WALs), queries.refused (per slot limit);
--   * the default Metrics\eventd descriptor grants read and no publish;
--   * rejected ingestion input and kernel socket drops are NOT counted here.
--
-- One file-scope VM, 1 vCPU (on one CPU there is one shard and one cpu slot,
-- which is enough to see the per-shard/per-cpu labels). Health is sampled
-- every second for the duration via a live HealthMetricIntervalSeconds.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-health" })
eventd.set(vm, "MetricMaxBatchSize", "dword:100")
eventd.set(vm, "HealthMetricIntervalSeconds", "dword:1")

-- Rows of a health metric over the last ten minutes, waiting briefly for the
-- first sample.
local function health_rows(name, selector)
    local q = "METRIC " .. name .. (selector or "") .. " SINCE 10m ago"
    local rows
    wait_until(function()
        local r = eventd.query(vm, q)
        if r.ok and #r.rows > 0 then rows = r.rows; return true end
        return false
    end, { timeout = 10, interval = 0.5, desc = "health metric " .. name })
    return rows
end

-- §5.7: "Every HealthMetricIntervalSeconds, the metric thread takes one sample
-- of every series below and commits it," without a datagram, token or
-- EVENTD_PUBLISH check.
test("eventd samples its own health each interval, bypassing socket and publish check", {
    spec = "eventd *health.the-metric-thread-samples-every-health-series-each-interval"
        .. " eventd *health.health-samples-bypass-the-metric-socket-and-publication-check",
}, function(t)
    -- Nobody sent these: their presence proves the thread writes them straight
    -- into the store, with no socket and no publication check.
    local rows = health_rows("eventd.metrics.stored")
    t:assert(rows and #rows >= 1, "eventd.metrics.stored is sampled without any producer")
end)

-- §5.7: "An interval of 0 turns them off."
test("a zero interval turns health metrics off", {
    spec = "eventd *health.a-zero-interval-turns-health-metrics-off",
}, function(t)
    -- First confirm they are on, then turn them off and confirm no new sample
    -- arrives after a cut-off timestamp.
    health_rows("eventd.logs.stored")
    eventd.set(vm, "HealthMetricIntervalSeconds", "dword:0")
    -- Let any sample already being taken land, then take the cut-off from
    -- the guest's own clock.
    vm:run("sleep 2")
    local cutoff = eventd.guest_ns(vm)
    local stopped = true
    local ok = pcall(wait_until, function()
        local r = eventd.query(vm, "METRIC eventd.logs.stored SINCE 10m ago")
        if not r.ok then return false end
        for _, row in ipairs(r.rows) do if row.timestamp and row.timestamp > cutoff then stopped = false end end
        return not stopped
    end, { timeout = 4, interval = 0.5, desc = "a health sample after disabling" })
    eventd.set(vm, "HealthMetricIntervalSeconds", "dword:1")
    t:assert(stopped and not ok, "no health sample arrived once the interval was 0")
end)

-- §5.7: "The counters count from eventd's start, and so restart from zero
-- whenever eventd does."
test("health counters restart from zero with eventd", {
    spec = "eventd *health.health-counters-restart-from-zero-with-eventd",
}, function(t)
    -- Drive the metrics counter up, note it, restart, and see a lower value.
    for i = 1, 50 do eventd.send_metric(vm, { name = eventd.marker("hc"), type = "gauge", value = i }) end
    local high
    wait_until(function()
        local rows = health_rows("eventd.metrics.stored")
        high = rows[#rows].value
        return high >= 50
    end, { timeout = 15, interval = 0.5, desc = "the stored counter to count the 50 samples" })
    eventd.restart(vm)
    local restarted = eventd.guest_ns(vm)
    -- The first sample the new process takes.
    local first
    wait_until(function()
        for _, row in ipairs(health_rows("eventd.metrics.stored")) do
            if row.timestamp > restarted then first = row; return true end
        end
        return false
    end, { timeout = 15, interval = 0.5, desc = "a post-restart health sample" })
    t:assert(first.value < high,
        "the counter restarted from zero: " .. tostring(first.value) .. " < " .. tostring(high))
end)

-- §5.7 The catalogue of series and their labels.
test("the health catalogue counts events, logs and metrics stored per shard/store", {
    spec = "eventd *health.events-stored-counts-committed-events-per-shard"
        .. " eventd *health.events-lost-counts-the-sequences-committed-gap-records-name"
        .. " eventd *health.ring-fill-is-each-drains-last-observed-ring-occupancy"
        .. " eventd *health.index-sheds-count-indexes-dropped-by-reason"
        .. " eventd *health.store-bytes-is-bytes-on-disk-with-write-ahead-logs",
}, function(t)
    -- The latest sample of a labelled series taken after `since`.
    local function latest(name, label, value, since)
        local found
        wait_until(function()
            for _, r in ipairs(health_rows(name, "[]")) do
                if r[label] == value and r.timestamp > since then found = r end
            end
            return found ~= nil
        end, { timeout = 15, interval = 0.5, desc = name .. " " .. label .. "=" .. value })
        return found
    end

    -- events.stored counts events committed to the shard.
    local t0 = eventd.guest_ns(vm)
    local before = latest("eventd.events.stored", "shard", "0", t0).value
    for i = 1, 30 do eventd.emit(vm, "pt.health", { n = i }) end
    local t1 = eventd.guest_ns(vm)
    wait_until(function()
        return latest("eventd.events.stored", "shard", "0", t1).value >= before + 30
    end, { timeout = 20, interval = 1, desc = "the stored-events counter to count 30 more" })

    -- events.lost is the sum of the gap ranges committed; with no gap
    -- records this boot it is zero.
    local gaps = eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 1h ago")
    local lost = latest("eventd.events.lost", "cpu", "0", t0)
    if #gaps == 0 then t:assert_eq(lost.value, 0, "no gap records, nothing lost") end

    local ring = latest("eventd.kmes.ring.fill.percent", "cpu", "0", t0)
    t:assert(ring.value >= 0 and ring.value <= 100, "ring fill is a percentage: " .. ring.value)
    for _, reason in ipairs({ "pressure", "emergency" }) do
        latest("eventd.events.index.sheds", "reason", reason, t0)
    end

    -- store.bytes is bytes on disk, write-ahead log included: a sample taken
    -- after we measure metrics.db and its WAL is at least their sum.
    local function size(p) return tonumber(vm:run("wc -c < " .. p .. " 2>/dev/null || echo 0").stdout:match("%d+")) or 0 end
    local disk = size(eventd.DB.metrics) + size(eventd.DB.metrics .. "-wal")
    local t2 = eventd.guest_ns(vm)
    local mbytes = latest("eventd.store.bytes", "store", "metrics", t2)
    t:assert(mbytes.value >= disk, "metrics store.bytes covers the database and its WAL: "
        .. mbytes.value .. " >= " .. disk)
    for _, store in ipairs({ "events", "logs", "metadata" }) do
        t:assert(latest("eventd.store.bytes", "store", store, t0).value > 0,
            "store.bytes reports the " .. store .. " store")
    end
end)

-- §5.7: queries.refused counts each slot limit separately.
test("eventd.queries.refused breaks out each slot limit by reason", {
    spec = "eventd *health.queries-refused-counts-each-slot-limit-separately",
}, function(t)
    local rows = health_rows("eventd.queries.refused", "[]")
    local reasons = {}
    for _, r in ipairs(rows or {}) do reasons[r.reason] = true end
    t:assert(reasons.machine and reasons.streaming and reasons.user,
        "the refused counter carries the machine, streaming and user reasons")
end)

-- §5.7: "eventd creates Metrics\eventd on first boot, granting EVENTD_READ to
-- SYSTEM, Administrators and Authenticated Users and EVENTD_PUBLISH to
-- nobody." A SYSTEM caller can publish ordinary names but not under eventd.*.
test("the default Metrics\\eventd descriptor grants read and no publish", {
    spec = "eventd *health.the-default-eventd-metrics-descriptor-grants-read-and-no-publish",
}, function(t)
    local ls = vm:run("reg ls '" .. eventd.SECURITY .. "\\Metrics'")
    t:assert(ls.stdout:find("eventd", 1, true),
        "a Metrics\\eventd descriptor key exists: " .. ls.stdout .. ls.stderr)
    -- SYSTEM holds publish on Metrics\* but must be denied under eventd.*.
    local denied = "eventd." .. eventd.marker("hpub")
    local allowed = eventd.marker("hok")
    eventd.send_metric(vm, { name = denied, type = "gauge", value = 1 })
    eventd.send_metric(vm, { name = allowed, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. allowed .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local rows = eventd.rows(vm, "METRIC " .. denied .. " SINCE 10m ago")
    t:assert_eq(#rows, 0, "nobody, not even SYSTEM, may publish under eventd.*")
end)

-- §5.7 What is left out.
test("rejected ingestion input and kernel socket drops are not health metrics", {
    spec = "eventd *health.rejected-ingestion-input-is-not-among-the-health-metrics"
        .. " eventd *health.kernel-socket-drops-are-not-among-the-health-metrics",
}, function(t)
    -- Provoke every kind of rejected metric input the book lists: no token,
    -- truncated/garbage, a name denied by policy, and a wrong type.
    local name = eventd.marker("hrej")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.send_metric(vm, { name = name, type = "counter", value = 2 })              -- wrong type
    eventd.send_metric(vm, { name = name, type = "gauge", value = 3 }, { pass_token = false }) -- no token
    eventd.send_metric(vm, { raw = "\xc1\xc1\xc1" })                                  -- not MessagePack
    eventd.send_metric(vm, { name = "eventd." .. name, type = "gauge", value = 4 })   -- denied name
    eventd.send_log(vm, { origin = "", is_error = false, message = "bad origin" })    -- invalid origin
    -- Wait for a couple of health samples to be taken after all of that.
    vm:run("sleep 3")
    -- The eventd.* catalogue is exactly the documented table: nothing in it
    -- counts rejected input, and nothing counts kernel socket-queue drops.
    local documented = {}
    for _, n in ipairs({ "eventd.events.stored", "eventd.events.lost",
        "eventd.kmes.ring.fill.percent", "eventd.events.index.sheds", "eventd.logs.stored",
        "eventd.metrics.stored", "eventd.metrics.series.cached", "eventd.store.bytes",
        "eventd.store.write.errors", "eventd.retention.deleted", "eventd.queries.active",
        "eventd.queries.streaming", "eventd.queries.refused", "eventd.queries.failed" }) do
        documented[n] = true
    end
    local names = eventd.sql(vm, eventd.DB.metrics,
        "SELECT DISTINCT name FROM series WHERE name LIKE 'eventd.%'")
    t:assert(#names >= 10, "the health catalogue is present: " .. json.encode(names))
    for _, r in ipairs(names) do
        t:assert(documented[r[1]], "every eventd.* series is a documented health series, "
            .. "none counting rejected input or socket drops: " .. r[1])
    end
end)

-- Queries can coalesce several commits, so their row counts do not expose
-- transaction boundaries. The host regression exercises the writer's health
-- flush with real SQLite commits, including pending producer samples and
-- live increases/decreases of the cap, then checks all persisted sample IDs.
-- The native cases above run at the minimum supported batch size and check
-- ordinary health sampling, counters, catalogue and access control.
test("health samples obey the pending and current transaction size limits", {
    spec = "eventd *health.health-samples-respect-the-metric-transaction-size-cap",
    skip = true,
    covered_by = "cargo:eventd eventd metric_ingest::tests::health_samples_respect_transaction_caps_without_losing_or_reordering_records",
}, function() end)
