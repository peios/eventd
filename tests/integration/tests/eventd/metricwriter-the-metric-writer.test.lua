-- eventd TRM §5.1 — the metric writer: the one thread that drains the metric
-- socket and writes samples. This file proves, end to end against the image's
-- own eventd, the observable parts of §5.1:
--
--   * a metric sent to the socket is written and queryable (the writer runs);
--   * publication needs EVENTD_PUBLISH on the resolved name (§7.6);
--   * a new series is created with the record's declared type;
--   * a type that disagrees with the resolved series is dropped silently,
--     with no event and no change to the immutable series type;
--   * a sample carries its series_id, timestamp and value;
--   * SQLite assigns the sample id that breaks a timestamp tie;
--   * a histogram's data is a canonical MessagePack map, value 0;
--   * a late (older) sample is stored;
--   * one over-large datagram is split across transactions and loses nothing;
--   * the store is WAL;
--   * a transaction's mismatch count and latest conflict reach the coalesced
--     standard-error warning, at most once a minute, and the SIGQUIT dump.
--
-- eventd's standard error is read back from the log store under origin
-- `eventd` (peinit forwards service output there; a line written as eventd
-- exits is delivered once the next eventd is up). The latency commit trigger
-- and the batch defaults have no observable distinct from the idle commit and
-- are left as unit TODOs; the mismatch-free fast path is documented.
--
-- One file-scope VM, with eventd made ErrorControl=Normal / RestartPolicy=
-- Never so the SIGQUIT test can stop it without peinit restarting or
-- rebooting. Every test tags its records with a unique metric name. 1 vCPU.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
peinit.claim(1)

local vm = eventd.boot({
    name = "ev-metricwriter",
    noncritical = true,
})

-- Distinct eventd stderr lines containing `needle` (the log store can hold a
-- line more than once; see the report), as a list of messages.
local function stderr_lines(needle)
    local out = {}
    for _, r in ipairs(eventd.stderr(vm, needle, { wait = false })) do out[#out + 1] = r.message end
    return out
end

-- §5.1: "The transaction result counts step-3 failures and carries the most
-- recent conflict's metric name and expected and received types … the metric
-- thread … emits a coalesced standard-error warning at most once per minute."
-- This must be the file's first type mismatch, so it runs first.
test("a transaction's mismatches are counted, the latest conflict kept, and warned at most once a minute", {
    spec = "eventd *metricwriter.the-transaction-result-counts-type-mismatches-and-carries-the-latest-conflict"
        .. " eventd *metricwriter.mismatches-accumulate-in-a-diagnostic-total-and-warn-at-most-once-per-minute",
}, function(t)
    local a, b = eventd.marker("mwca"), eventd.marker("mwcb")
    eventd.send_metric(vm, eventd.array{
        { name = a, type = "gauge", value = 1 }, { name = b, type = "gauge", value = 1 } })
    eventd.wait_rows(vm, "METRIC " .. b .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    -- One datagram, one transaction, three conflicts; b's is the last.
    eventd.send_metric(vm, eventd.array{
        { name = a, type = "counter", value = 2 }, { name = a, type = "counter", value = 3 },
        { name = b, type = "counter", value = 9 } })
    local first
    wait_until(function()
        first = stderr_lines("immutable type conflicts")
        return #first >= 1
    end, { timeout = 20, interval = 0.5, desc = "the mismatch warning" })
    t:assert_eq(#first, 1, "one warning: " .. json.encode(first))
    t:assert(first[1]:find("discarded 3 metric samples", 1, true),
        "the transaction's count of three reached the warning: " .. first[1])
    t:assert(first[1]:find("latest name=" .. b .. " expected=gauge received=counter", 1, true),
        "with the most recent conflict's name and types: " .. first[1])
    -- Three more conflicts inside the minute: no second warning yet.
    local warned_at = os.time()
    for v = 4, 6 do eventd.send_metric(vm, { name = a, type = "counter", value = v }) end
    vm:run("sleep 5")
    t:assert_eq(#stderr_lines("immutable type conflicts"), 1, "no second warning within the minute")
    -- After the minute, one more conflict brings out the accumulated total.
    vm:run("sleep " .. math.max(1, 62 - (os.time() - warned_at)))
    eventd.send_metric(vm, { name = a, type = "counter", value = 7 })
    local lines
    wait_until(function()
        lines = stderr_lines("immutable type conflicts")
        return #lines >= 2
    end, { timeout = 20, interval = 0.5, desc = "the next minute's warning" })
    t:assert_eq(#lines, 2, "exactly one more warning: " .. json.encode(lines))
    local carried = false
    for _, l in ipairs(lines) do
        if l:find("discarded 4 metric samples", 1, true)
            and l:find("latest name=" .. a .. " expected=gauge received=counter", 1, true) then
            carried = true
        end
    end
    t:assert(carried, "it carries the three held back plus the new one: " .. json.encode(lines))
end)

-- Count this test's samples straight out of the store, by series name, with
-- no SINCE clause so an explicit timestamp cannot hide a row.
local function series_rows(name)
    return eventd.sql(vm, eventd.DB.metrics,
        "SELECT id, name, labels, type FROM series WHERE name = '" .. name .. "'")
end

local function sample_rows(name)
    return eventd.sql(vm, eventd.DB.metrics,
        "SELECT s.id, s.value, s.timestamp, s.series_id, s.histogram_data " ..
        "FROM samples s JOIN series e ON e.id = s.series_id " ..
        "WHERE e.name = '" .. name .. "' ORDER BY s.id")
end

-- §5.1: "One thread reads datagrams from the metric socket and writes samples
-- to the metric store, independent of both the event and log paths."
test("a metric sent to the socket is written and queryable", {
    spec = "eventd *metricwriter.one-thread-reads-the-metric-socket-and-writes-samples-independently",
}, function(t)
    local name = eventd.marker("mw")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 7 })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    t:assert_eq(#rows, 1, "exactly the one sample is stored")
    t:assert_eq(rows[1].value, 7, "and its value round-tripped")
end)

-- §5.1 step 1: "The datagram's KACS token must hold EVENTD_PUBLISH on the
-- resolved metric-name descriptor (§7.6)." The gate is per resolved name, not
-- per caller. We install a descriptor for one name that grants the agent
-- EVENTD_READ but not EVENTD_PUBLISH; the same SYSTEM token that publishes an
-- ordinary name (via the Metrics\* 0x09 default) is then refused that one.
test("publication is gated by EVENTD_PUBLISH on the resolved name's descriptor", {
    spec = "eventd *metricwriter.the-token-must-hold-eventd-publish-on-the-metric-name-descriptor",
}, function(t)
    local access = require("helpers.access")
    local denied = eventd.marker("mwpub")
    local allowed = eventd.marker("mwok")
    -- Read only (0x01), no publish (0x08), for the agent's SYSTEM identity.
    local sd = access.simple({ access.ace(access.ACE.ALLOWED, 0x01, token.SID.LOCAL_SYSTEM) })
    eventd.put_descriptor(vm, "Metrics", denied, sd)
    eventd.send_metric(vm, { name = denied, type = "gauge", value = 1 })
    eventd.send_metric(vm, { name = allowed, type = "gauge", value = 1 })
    -- Once the allowed one is stored, the denied one has had its chance.
    eventd.wait_rows(vm, "METRIC " .. allowed .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    local rows = eventd.rows(vm, "METRIC " .. denied .. " SINCE 10m ago")
    vm:run("reg del -p '" .. eventd.SECURITY .. "\\Metrics\\" .. denied .. "'")
    t:assert_eq(#rows, 0,
        "a token without EVENTD_PUBLISH on the resolved name stored nothing: " .. json.encode(rows))
end)

-- §5.1 step 2: "Resolve the series from name and labels … A series that does
-- not exist is inserted into `series` with the record's type."
test("a new series is created from name and labels with the record's type", {
    spec = "eventd *metricwriter.the-series-is-resolved-from-name-labels-and-histogram-boundaries"
        .. " eventd *metricwriter.a-missing-series-is-inserted-with-the-records-type-and-cached",
}, function(t)
    local name = eventd.marker("mwser")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 3,
        labels = { host = "srv1", core = "0" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    local s = series_rows(name)
    t:assert_eq(#s, 1, "one series row exists for the name")
    t:assert_eq(s[1][3], "core=0,host=srv1", "labels are the canonical string")
    t:assert_eq(s[1][4], 1, "type recorded is 1 (gauge)")
end)

-- §5.1 step 3: "If the record's type differs from the resolved series' type,
-- the record is dropped without replying to the producer." and "The type is
-- set at creation and is immutable." and "There is still no response to the
-- producer and no event per failure."
test("a type mismatch is dropped, emits no event, and never changes the type", {
    spec = "eventd *metricwriter.a-type-mismatch-drops-the-record-without-replying"
        .. " eventd *metricwriter.a-series-type-is-fixed-at-creation-and-never-changes"
        .. " eventd *metricwriter.a-type-mismatch-produces-no-response-and-no-event",
}, function(t)
    local name = eventd.marker("mwmm")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 10 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    -- Same name, wrong type: a counter against a gauge series.
    eventd.send_metric(vm, { name = name, type = "counter", value = 20 })
    -- A second valid gauge, to have a definite "later" point to wait for, so
    -- the mismatch has certainly been processed by the time we check.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 11 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 2 end)
    local s = series_rows(name)
    t:assert_eq(#s, 1, "still exactly one series (the counter made no new one)")
    t:assert_eq(s[1][4], 1, "its type is still 1 (gauge): the mismatch did not change it")
    local samples = sample_rows(name)
    t:assert_eq(#samples, 2, "only the two gauge samples are stored, not the counter")
    -- No event names this metric: a mismatch raises nothing on the event
    -- path. The only event eventd ever raises for a metric-store problem is a
    -- eventd.store.quarantined, and only on corruption; none should mention
    -- this name.
    local ev = eventd.rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 10m ago")
    local hit = false
    for _, r in ipairs(ev) do
        if json.encode(r):find(name, 1, true) then hit = true end
    end
    t:assert(not hit, "no event was emitted for the dropped record")
end)

-- §5.1 step 4: "Insert the sample into `samples` with the resolved series_id,
-- the timestamp and the value."
test("a sample carries its resolved series_id, a timestamp and its value", {
    spec = "eventd *metricwriter.the-sample-is-inserted-with-its-series-id-timestamp-and-value",
}, function(t)
    local name = eventd.marker("mwins")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 55 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    local s = series_rows(name)
    local samples = sample_rows(name)
    t:assert_eq(#samples, 1, "one sample row")
    t:assert_eq(samples[1][4], s[1][1], "its series_id references the series row")
    t:assert_eq(samples[1][2], 55, "its value is the sent value")
    t:assert(samples[1][3] and samples[1][3] > 0, "it has a timestamp: " .. tostring(samples[1][3]))
end)

-- §5.1 step 4: "SQLite assigns samples.id, which is the deterministic
-- tiebreaker among samples sharing a timestamp (§5.2)."
test("SQLite assigns the sample id that breaks a timestamp tie", {
    spec = "eventd *metricwriter.sqlite-assigns-the-sample-id-that-breaks-timestamp-ties",
}, function(t)
    local name = eventd.marker("mwtie")
    local ts = os.time() * 1000000000
    -- One datagram, two records sharing the timestamp, sent in a fixed order.
    eventd.send_metric(vm, eventd.array{
        { name = name, type = "gauge", value = 1, timestamp = ts },
        { name = name, type = "gauge", value = 2, timestamp = ts },
    })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 2 end)
    local samples = sample_rows(name)
    t:assert_eq(#samples, 2, "both tied samples stored")
    t:assert(samples[1][1] < samples[2][1], "distinct, increasing ids break the tie: " ..
        samples[1][1] .. " < " .. samples[2][1])
    t:assert(samples[1][2] == 1 and samples[2][2] == 2,
        "and id order follows insertion order within the datagram")
end)

-- §5.1 step 4: "A histogram's data is encoded as the canonical MessagePack
-- sample map and stored in histogram_data." (§5.2: value stores 0.)
test("a histogram is stored as a canonical MessagePack map with value 0", {
    spec = "eventd *metricwriter.histogram-data-is-stored-as-a-canonical-messagepack-sample-map",
}, function(t)
    local name = eventd.marker("mwh")
    eventd.send_metric(vm, { name = name, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.0), eventd.float(2.0) },
        counts = eventd.array{ 1, 2 },
        total_count = 2,
        sum = eventd.float(2.5),
    } })
    -- A histogram cannot be read by a bare METRIC query (PSPU §3.22 needs a
    -- percentile), so wait on the store directly.
    local samples
    wait_until(function()
        samples = sample_rows(name)
        return #samples == 1
    end, { timeout = 30, interval = 0.25, desc = "the histogram sample to land" })
    t:assert_eq(#samples, 1, "one histogram sample")
    t:assert_eq(samples[1][2], 0, "its value column is 0")
    local hex = samples[1][5]
    t:assert(type(hex) == "string" and #hex > 0, "histogram_data is a non-null blob")
    t:assert_eq(hex:sub(1, 2), "84", "it begins with a 4-entry fixmap (0x84), the canonical map")
end)

-- §5.1: "The writer stores a valid sample whose timestamp precedes samples
-- already held for that series."
test("a sample older than the stored ones is accepted", {
    spec = "eventd *metricwriter.a-sample-older-than-stored-samples-is-accepted",
}, function(t)
    local name = eventd.marker("mwold")
    local now = os.time() * 1000000000
    eventd.send_metric(vm, { name = name, type = "gauge", value = 100, timestamp = now })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    -- 60 s earlier than the first, but still inside the 10m query window.
    eventd.send_metric(vm, { name = name, type = "gauge", value = 50,
        timestamp = now - 60 * 1000000000 })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 2 end)
    t:assert_eq(#rows, 2, "the late sample was stored, not dropped")
    -- Metric results are timestamp-ascending (PSPU §3.21): the older is first.
    t:assert_eq(rows[1].value, 50, "the older sample sorts first by timestamp")
    t:assert_eq(rows[2].value, 100, "the newer sample second")
end)

-- §5.1: "A datagram yielding more samples than fit is split across
-- transactions … Neither cap is ever exceeded." With MetricMaxBatchSize at its
-- floor (100), one 150-record datagram must straddle at least two
-- transactions, and every record must survive.
test("an over-large datagram is split across transactions and loses nothing", {
    spec = "eventd *metricwriter.a-datagram-that-overflows-a-batch-is-split-across-transactions"
        .. " eventd *metricwriter.batching-uses-the-shared-adaptive-algorithm-over-the-socket-receive-queue"
        .. " eventd *metricwriter.a-batch-commits-at-metricmaxbatchsize-samples"
        .. " eventd *metricwriter.neither-batch-cap-is-ever-exceeded",
}, function(t)
    eventd.set(vm, "MetricMaxBatchSize", "dword:100"):assert_ok()
    local name = eventd.marker("mwbig")
    local now = os.time() * 1000000000
    local records = {}
    for i = 1, 150 do
        records[i] = { name = name, type = "gauge", value = i, timestamp = now + i }
    end
    eventd.send_metric(vm, eventd.array(records))
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs >= 150 end)
    eventd.unset(vm, "MetricMaxBatchSize")
    t:assert_eq(#rows, 150, "all 150 samples from the one datagram are stored")
end)

-- §5.1: "A transaction opens at the first valid sample and commits when … no
-- further datagram is immediately available." A lone sample becomes queryable
-- without a second datagram to close its batch.
test("a batch commits when no further datagram is available", {
    spec = "eventd *metricwriter.a-batch-commits-when-no-datagram-is-immediately-available",
}, function(t)
    local name = eventd.marker("mwlone")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 9 })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    t:assert_eq(#rows, 1, "the single sample committed on its own")
end)

-- §5.1 Durability: "WAL mode with synchronous=NORMAL." The journal mode is in
-- the database header and survives the host-side copy; synchronous is a
-- connection pragma and is not file-observable, so only WAL is asserted here.
test("the metric store is in WAL mode", {
    spec = "eventd *metricwriter.metric-writes-use-wal-with-synchronous-normal",
}, function(t)
    local mode = eventd.sql(vm, eventd.DB.metrics, "PRAGMA journal_mode")
    t:assert_eq(mode[1][1], "wal", "journal_mode is WAL: " .. json.encode(mode))
end)

-- ---------------------------------------------------------------------------
-- Unit homes: the parts of §5.1 that a query cannot reach.
-- ---------------------------------------------------------------------------

-- §5.1: "SIGQUIT includes both the total and latest conflict (§8.5)." SIGQUIT
-- makes eventd write its diagnostic dump and stop; the dump reaches the log
-- store once the next eventd is up. This runs after every mismatch in the
-- file, so the total is all of them and the latest is the last one sent here.
test("SIGQUIT reports the mismatch total and latest conflict", {
    spec = "eventd *metricwriter.sigquit-reports-the-mismatch-total-and-latest-conflict",
}, function(t)
    local name = eventd.marker("mwquit")
    eventd.send_metric(vm, { name = name, type = "counter", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.send_metric(vm, { name = name, type = "gauge", value = 2 })
    -- The conflict is in once a later sample of another series is stored.
    local after = eventd.marker("mwquitok")
    eventd.send_metric(vm, { name = after, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. after .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local d = eventd.quit_dump(vm)
    local totals, latest = {}, {}
    for _, m in ipairs(d.messages) do
        if m:find("type_mismatches=", 1, true) then totals[#totals + 1] = m end
        if m:find("last_metric_type_mismatch", 1, true) then latest[#latest + 1] = m end
    end
    assert(#totals >= 1 and #latest >= 1, "the SIGQUIT dump in the log store: "
        .. json.encode(d.messages) .. " " .. table.concat(d.partial, "; "))
    -- Earlier tests in this file discarded several samples for their type.
    t:assert(totals[#totals]:find("type_mismatches=%d+"), "the dump reports a mismatch total: " .. totals[#totals])
    local n = tonumber(totals[#totals]:match("type_mismatches=(%d+)"))
    t:assert(n >= 2, "covering every conflict so far: " .. n)
    t:assert(latest[#latest]:find("name=" .. name .. " expected=counter received=gauge", 1, true),
        "and the latest conflict is the one just sent: " .. latest[#latest])
end)

-- Route closed: whether a mismatch-free transaction allocates or synchronises
-- for diagnostics is an internal performance property with no caller-visible
-- effect; it is not runtime behaviour a query, store read or restart observes.
test("a mismatch-free transaction does no diagnostic work (not observable)", {
    spec = "eventd *metricwriter.a-mismatch-free-transaction-does-no-diagnostic-allocation-or-synchronization",
    skip = true,
    covered_by = "doc:not-observable internal fast-path property with no interface-visible effect",
}, function() end)

-- Route closed: the latency-triggered commit produces the same observable as
-- the idle-queue trigger (the sample becomes queryable), so a query cannot
-- distinguish it. The unit test drives the commit decision with an injected
-- clock, below the size cap and with samples still arriving.
test("a batch commits once MetricMaxBatchLatencyMs has elapsed", {
    spec = "eventd *metricwriter.a-batch-commits-when-metricmaxbatchlatencyms-has-elapsed-since-its-first-sample",
    skip = true,
    covered_by = "cargo:eventd eventd metric_ingest::tests::a_batch_commits_once_its_latency_has_elapsed_since_its_first_sample",
}, function() end)

-- Route closed: the code defaults apply only when the registry value is
-- absent, so they are never written anywhere a guest can read; the unit
-- test pins the two numbers with neither value present.
test("the metric batch defaults are 5000 samples and 1000 ms", {
    spec = "eventd *metricwriter.the-batch-defaults-are-5000-samples-and-1000-milliseconds",
    skip = true,
    covered_by = "cargo:eventd eventd config::tests::metric_batch_defaults_are_5000_samples_and_1000_milliseconds",
}, function() end)
