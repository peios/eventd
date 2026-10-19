-- eventd TRM §5.2 — the series and samples tables. The metric store is one
-- SQLite database organised around series, not records. This file proves:
--
--   * the series and samples table shapes and the write-time indexes, read
--     straight out of sqlite_master;
--   * the canonical label string (sorting, joining, the empty set, the
--     no-escaping rule enforced by ingestion);
--   * the boundary blob layout and that it is never returned to a query;
--   * that label_hash and boundaries_hash are 64-bit FNV-1a with the high bit
--     cleared (recomputed here and matched against the stored value);
--   * the UNIQUE constraint, and that type is not part of a series' identity;
--   * sample columns: the id tiebreaker, boot_id width, ns timestamps,
--     value-or-0, the histogram sample map's four keys, identical bytes for
--     equal histograms;
--   * ordering by (timestamp, id), duplicate timestamps, and that the sample
--     id is never a query result field.
--
-- Elsewhere: the label-hash collision anchors are proven in
-- resolve-series-resolution.test.lua (they are resolution behaviour); the
-- version-1 composition and the NULL-distinct UNIQUE constraint are proven in
-- metricdb-database-lifecycle.test.lua with crafted stores.
--
-- One file-scope VM, 1 vCPU. Records are tagged by unique metric name.
--
-- FNV-1a here is the same constants the book pins (§5.2) and eventd uses
-- (metric_store.rs hash_for_sql): offset 0xcbf29ce484222325, prime
-- 0x100000001b3, then `& 0x7fffffffffffffff`. Verified against the code's
-- own values.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-series" })

local function series_row(name)
    local r = eventd.sql(vm, eventd.DB.metrics,
        "SELECT id, name, labels, type, label_hash, boundaries_hash, boundaries " ..
        "FROM series WHERE name = '" .. name .. "'")
    return r
end

-- Wait until a named series' first sample has been committed, then return the
-- series row. Histograms cannot be read by a bare METRIC query, so poll the
-- store.
local function await_series(name)
    local row
    wait_until(function()
        row = series_row(name)
        return #row == 1
    end, { timeout = 30, interval = 0.25, desc = "series row for " .. name })
    return row[1]
end

-- §5.2 The series table.
test("the series table has the documented columns, UNIQUE constraint and indexes", {
    spec = "eventd *series.the-metric-store-is-one-sqlite-database-organised-around-series"
        .. " eventd *series.series-id-is-the-integer-primary-key-samples-reference"
        .. " eventd *series.series-name-is-the-metric-name"
        .. " eventd *series.series-labels-is-the-canonical-label-string-empty-for-no-labels"
        .. " eventd *series.series-type-is-0-counter-1-gauge-2-histogram"
        .. " eventd *series.series-boundaries-hash-hashes-the-boundary-blob-and-is-null-for-counters-and-gauges"
        .. " eventd *series.series-boundaries-holds-the-boundary-blob-and-is-null-for-counters-and-gauges"
        .. " eventd *series.the-series-table-is-unique-on-name-labels-and-boundaries"
        .. " eventd *series.idx-series-name-indexes-series-by-name"
        .. " eventd *series.idx-series-label-hash-indexes-series-by-label-hash"
        .. " eventd *series.idx-series-boundaries-hash-indexes-series-by-boundaries-hash",
}, function(t)
    local schema = eventd.schema(vm, eventd.DB.metrics)
    local s = schema.series
    t:assert(s, "there is a series table in the one metrics.db: " .. json.encode(schema and {} or {}))
    t:assert(s:find("id INTEGER PRIMARY KEY", 1, true), "id is the integer primary key")
    t:assert(s:find("name TEXT NOT NULL", 1, true), "name TEXT NOT NULL")
    t:assert(s:find("labels TEXT NOT NULL", 1, true), "labels TEXT NOT NULL")
    t:assert(s:find("type INTEGER NOT NULL CHECK (type IN (0, 1, 2))", 1, true),
        "type is 0/1/2")
    t:assert(s:find("label_hash INTEGER NOT NULL", 1, true), "label_hash INTEGER NOT NULL")
    t:assert(s:find("boundaries_hash INTEGER", 1, true), "boundaries_hash INTEGER (nullable)")
    t:assert(s:find("boundaries BLOB", 1, true), "boundaries BLOB")
    t:assert(s:find("UNIQUE(name, labels, boundaries)", 1, true),
        "UNIQUE(name, labels, boundaries), on the full boundary blob: " .. s)
    t:assert(schema.idx_series_name and schema.idx_series_name:find("series(name)", 1, true),
        "idx_series_name on series(name)")
    t:assert(schema.idx_series_label_hash and schema.idx_series_label_hash:find("series(label_hash)", 1, true),
        "idx_series_label_hash on series(label_hash)")
    t:assert(schema.idx_series_boundaries_hash
        and schema.idx_series_boundaries_hash:find("series(boundaries_hash)", 1, true),
        "idx_series_boundaries_hash on series(boundaries_hash), the hash being a lookup key")
end)

-- §5.2 The samples table, metadata table and its sample index.
test("the samples and metadata tables have the documented shape and index", {
    spec = "eventd *series.samples-id-breaks-ties-between-samples-sharing-a-series-and-timestamp"
        .. " eventd *series.samples-series-id-references-series-id"
        .. " eventd *series.samples-boot-id-is-a-16-byte-boot-id-guid"
        .. " eventd *series.samples-timestamp-is-nanoseconds-since-the-unix-epoch"
        .. " eventd *series.idx-samples-series-timestamp-indexes-series-id-timestamp-and-id"
        .. " eventd *series.the-metric-store-has-a-metadata-table-shaped-like-the-other-stores"
        .. " eventd *series.boot-id-is-per-sample-so-a-series-continues-across-reboots",
}, function(t)
    local schema = eventd.schema(vm, eventd.DB.metrics)
    local s = schema.samples
    t:assert(s, "there is a samples table")
    t:assert(s:find("id INTEGER PRIMARY KEY", 1, true), "id INTEGER PRIMARY KEY (the tiebreaker)")
    t:assert(s:find("series_id INTEGER NOT NULL REFERENCES series(id)", 1, true),
        "series_id references series(id)")
    t:assert(s:find("boot_id BLOB NOT NULL", 1, true), "boot_id BLOB NOT NULL")
    t:assert(s:find("timestamp INTEGER NOT NULL", 1, true), "timestamp INTEGER NOT NULL")
    t:assert(s:find("value REAL NOT NULL", 1, true), "value REAL NOT NULL")
    t:assert(s:find("histogram_data BLOB", 1, true), "histogram_data BLOB (nullable)")
    -- boot_id lives on the sample, not the series: that is why a series is
    -- continuous across a reboot (its identity carries no boot).
    t:assert(not schema.series:find("boot_id", 1, true),
        "the series table has no boot_id: identity does not include the boot")
    t:assert(schema.idx_samples_series_timestamp
        and schema.idx_samples_series_timestamp:find("samples(series_id, timestamp, id)", 1, true),
        "idx_samples_series_timestamp on (series_id, timestamp, id)")
    local m = schema.metadata
    t:assert(m and m:find("key TEXT PRIMARY KEY", 1, true) and m:find("value TEXT NOT NULL", 1, true),
        "metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
end)

-- §5.2 The canonical label string.
test("labels are key-sorted, comma-joined, and the empty set is the empty string", {
    spec = "eventd *series.canonical-labels-are-key-sorted-by-utf-8-bytes-and-comma-joined-as-key-value-pairs"
        .. " eventd *series.the-empty-label-set-is-the-empty-string"
        .. " eventd *series.series-label-hash-hashes-the-canonical-label-string",
}, function(t)
    local labelled = eventd.marker("sl")
    eventd.send_metric(vm, { name = labelled, type = "gauge", value = 1,
        labels = { host = "srv1", core = "0" } })
    local bare = eventd.marker("sb")
    eventd.send_metric(vm, { name = bare, type = "gauge", value = 1 })

    local lr = await_series(labelled)
    t:assert_eq(lr[3], "core=0,host=srv1", "labels sorted by key and comma-joined")
    t:assert_eq(lr[5], eventd.hash_for_sql("core=0,host=srv1"), "label_hash is FNV-1a of that string")

    local br = await_series(bare)
    t:assert_eq(br[3], "", "the empty label set is the empty string")
    t:assert_eq(br[5], eventd.hash_for_sql(""), "its label_hash is FNV-1a of the empty string")
end)

-- §5.2: "No escaping is performed and none is needed, because ingestion
-- rejects = and , inside a key or a value (PSPU §3.10)."
test("a label value containing a delimiter is rejected, so no escaping is needed", {
    spec = "eventd *series.the-canonical-label-string-is-not-escaped",
}, function(t)
    local bad = eventd.marker("sx")
    eventd.send_metric(vm, { name = bad, type = "gauge", value = 1,
        labels = { region = "a,b" } })
    -- A control gauge whose landing means the bad one has had its chance.
    local ok = eventd.marker("sxok")
    eventd.send_metric(vm, { name = ok, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. ok .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    t:assert_eq(#series_row(bad), 0, "the record with ',' in a label value was rejected, not escaped")
end)

-- §5.2 The boundaries blob, and §5.2 hashes with the high bit cleared.
test("the boundary blob is count-then-f64 little-endian, and hashes clear the high bit", {
    spec = "eventd *series.the-boundary-blob-starts-with-a-u32-little-endian-count"
        .. " eventd *series.the-boundary-blob-then-holds-little-endian-f64-values-in-producer-order"
        .. " eventd *series.series-hashes-are-64-bit-fnv-1a-over-the-canonical-bytes"
        .. " eventd *series.the-hash-high-bit-is-cleared-before-storage",
}, function(t)
    local hname = eventd.marker("shb")
    eventd.send_metric(vm, { name = hname, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.0), eventd.float(2.0) },
        counts = eventd.array{ 1, 2 }, total_count = 2, sum = eventd.float(2.5),
    } })
    local hr = await_series(hname)
    local blob = hr[7]
    t:assert_eq(blob:sub(1, 8), "02000000", "the blob starts with the u32 LE count 2")
    local b1 = eventd.hex(string.pack("<d", 1.0))
    local b2 = eventd.hex(string.pack("<d", 2.0))
    t:assert_eq(blob, "02000000" .. b1 .. b2, "then the two boundaries as LE f64 in producer order")
    -- boundaries_hash is FNV over the raw blob bytes.
    local raw = string.pack("<I4", 2) .. string.pack("<d", 1.0) .. string.pack("<d", 2.0)
    t:assert_eq(hr[6], eventd.hash_for_sql(raw), "boundaries_hash is FNV-1a over the blob bytes")

    -- A canonical label string whose *raw* 64-bit FNV has the high bit set:
    -- the stored value must be that hash with bit 63 cleared, so it is
    -- non-negative and equals `& 0x7fffffffffffffff`.
    local hbname = eventd.marker("shbh")
    eventd.send_metric(vm, { name = hbname, type = "gauge", value = 1, labels = { hb = "10" } })
    local hbr = await_series(hbname)
    t:assert_eq(hbr[3], "hb=10", "the label string is hb=10")
    t:assert_eq(hbr[5], 5715345424729079599,
        "whose raw FNV has bit 63 set, stored with the high bit cleared")
    t:assert(hbr[5] >= 0, "the stored hash is non-negative (fits SQLite's signed INTEGER)")
end)

-- §5.2: "type is not part of the identity. A record resolving to an existing
-- series with a different type resolves successfully and is then dropped."
test("type is not part of series identity", {
    spec = "eventd *series.type-is-not-part-of-series-identity",
}, function(t)
    local name = eventd.marker("sty")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1, labels = { k = "v" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    -- Same name and labels, different type: resolves to the one series (so no
    -- second series is made), then dropped for the type mismatch.
    eventd.send_metric(vm, { name = name, type = "counter", value = 2, labels = { k = "v" } })
    eventd.send_metric(vm, { name = name, type = "gauge", value = 3, labels = { k = "v" } })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local s = series_row(name)
    t:assert_eq(#s, 1, "the counter did not create a second series: type is not identity")
    t:assert_eq(s[1][4], 1, "and the one series is still a gauge")
end)

-- §5.2 The samples table in use.
test("samples store raw value or 0 for histograms, a 16-byte boot_id and ns timestamps", {
    spec = "eventd *series.samples-value-is-the-raw-value-or-0-for-histograms",
}, function(t)
    local g = eventd.marker("svg")
    eventd.send_metric(vm, { name = g, type = "gauge", value = 3.5 })
    local h = eventd.marker("svh")
    eventd.send_metric(vm, { name = h, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(1.0) }, counts = eventd.array{ 0 },
        total_count = 0, sum = eventd.float(0.0),
    } })
    eventd.wait_rows(vm, "METRIC " .. g .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    await_series(h)
    local gs = eventd.sql(vm, eventd.DB.metrics,
        "SELECT value, length(boot_id), timestamp, histogram_data FROM samples s " ..
        "JOIN series e ON e.id = s.series_id WHERE e.name = '" .. g .. "'")
    t:assert_eq(gs[1][1], 3.5, "a gauge stores its raw value")
    t:assert_eq(gs[1][2], 16, "boot_id is 16 bytes")
    t:assert(gs[1][3] > 1000000000000000000, "timestamp is nanoseconds since the epoch: " .. gs[1][3])
    t:assert(gs[1][4] == nil, "a gauge's histogram_data is null")
    local hs = eventd.sql(vm, eventd.DB.metrics,
        "SELECT value, histogram_data FROM samples s JOIN series e ON e.id = s.series_id " ..
        "WHERE e.name = '" .. h .. "'")
    t:assert_eq(hs[1][1], 0, "a histogram stores 0 in value")
    t:assert(type(hs[1][2]) == "string" and #hs[1][2] > 0, "and its data in histogram_data")
end)

-- §5.2: "a canonical MessagePack map … with exactly four keys: boundaries,
-- counts, total_count and sum." And equal histograms encode identically.
test("the histogram sample map has exactly the four keys, and equal histograms match byte for byte", {
    spec = "eventd *series.samples-histogram-data-is-the-sample-map-and-null-for-counters-and-gauges"
        .. " eventd *series.the-histogram-sample-map-has-exactly-boundaries-counts-total-count-and-sum"
        .. " eventd *series.equal-histogram-samples-encode-to-identical-bytes",
}, function(t)
    local name = eventd.marker("smap")
    local value = {
        boundaries = eventd.array{ eventd.float(1.0), eventd.float(2.0) },
        counts = eventd.array{ 1, 2 }, total_count = 2, sum = eventd.float(2.5),
    }
    -- Two identical histogram samples of one series.
    eventd.send_metric(vm, { name = name, type = "histogram", value = value })
    eventd.send_metric(vm, { name = name, type = "histogram", value = value })
    local rows
    wait_until(function()
        rows = eventd.sql(vm, eventd.DB.metrics,
            "SELECT histogram_data FROM samples s JOIN series e ON e.id = s.series_id " ..
            "WHERE e.name = '" .. name .. "' ORDER BY s.id")
        return #rows == 2
    end, { timeout = 30, interval = 0.25, desc = "two histogram samples" })
    local hex = rows[1][1]
    t:assert_eq(hex:sub(1, 2), "84", "the map is a 4-entry fixmap (exactly four keys)")
    for _, key in ipairs({ "boundaries", "counts", "total_count", "sum" }) do
        local kh = eventd.hex(key)
        t:assert(hex:find(kh, 1, true), "the map carries the key " .. key)
    end
    t:assert_eq(rows[1][1], rows[2][1], "two equal histograms encode to identical bytes")
end)

-- §5.2: "A histogram's value is never returned as a metric query value … the
-- boundary blob is never returned in a query result."
test("a histogram query returns the percentile, never the raw value or boundaries", {
    spec = "eventd *series.a-histogram-rows-value-is-never-returned-as-a-query-value"
        .. " eventd *series.the-boundary-blob-is-never-returned-in-a-query-result",
}, function(t)
    local name = eventd.marker("sret")
    eventd.send_metric(vm, { name = name, type = "histogram", value = {
        boundaries = eventd.array{ eventd.float(10.0), eventd.float(20.0) },
        counts = eventd.array{ 1, 2 }, total_count = 2, sum = eventd.float(25.0),
    } })
    await_series(name)
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " P50 SINCE 10m ago",
        function(rs) return #rs >= 1 end)
    local row = rows[1]
    t:assert(row.value ~= 0, "the returned value is the percentile, not the 0 placeholder: " ..
        json.encode(row))
    t:assert(row.boundaries == nil and row.counts == nil,
        "neither boundaries nor counts appear in the result: " .. json.encode(row))
end)

-- §5.2 Ordering: "(timestamp, id) ascending … Duplicate timestamps are
-- permitted and id gives them a stable order." And the sample id is never a
-- query result field.
test("samples order by (timestamp, id) ascending, duplicate timestamps allowed, id not exposed", {
    spec = "eventd *series.samples-in-a-series-are-ordered-by-timestamp-then-id-ascending"
        .. " eventd *series.duplicate-timestamps-are-permitted"
        .. " eventd *series.the-sample-id-is-never-exposed-as-a-query-result-access-control-or-label-field",
}, function(t)
    local name = eventd.marker("sord")
    local ts = os.time() * 1000000000
    -- Sent out of order, with a duplicate timestamp at ts.
    eventd.send_metric(vm, eventd.array{
        { name = name, type = "gauge", value = 30, timestamp = ts + 2 },
        { name = name, type = "gauge", value = 10, timestamp = ts },
        { name = name, type = "gauge", value = 20, timestamp = ts },
    })
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 3 end)
    t:assert_eq(rows[1].value, 10, "the two tied samples come first, in id order")
    t:assert_eq(rows[2].value, 20, "second of the tied pair")
    t:assert_eq(rows[3].value, 30, "then the later timestamp")
    t:assert(rows[1].id == nil, "no sample id is exposed as a result field: " .. json.encode(rows[1]))
    -- Duplicate timestamps really are both present.
    local n = eventd.sql(vm, eventd.DB.metrics,
        "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id " ..
        "WHERE e.name = '" .. name .. "' AND s.timestamp = " .. ts)
    t:assert_eq(n[1][1], 2, "both samples sharing the timestamp are stored")
end)

-- §5.2 Schema version. Version 2 added the rollups cache and left raw
-- storage alone; version 3 changed only the series table's uniqueness, to
-- the full boundary blob, and added idx_series_boundaries_hash. A fresh
-- store is the sum: version 1's tables shaped as before, the rollups cache,
-- and version 3's series constraint and index.
test("the metric store is at schema version 3: the rollups cache, and series unique on the boundary blob", {
    spec = "eventd *series.schema-version-2-adds-only-the-rollups-cache"
        .. " eventd *series.schema-version-3-makes-series-unique-on-the-full-boundary-blob",
}, function(t)
    local v = eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM metadata WHERE key = 'schema_version'")
    t:assert_eq(v[1][1], "3", "schema_version is 3")
    local schema = eventd.schema(vm, eventd.DB.metrics)
    t:assert(schema.rollups, "version 2 added the rollups cache table")
    t:assert(schema.samples and schema.samples:find("series_id INTEGER NOT NULL REFERENCES series(id)", 1, true),
        "samples is still shaped as version 1 left it: " .. tostring(schema.samples))
    t:assert(schema.series and schema.series:find("UNIQUE(name, labels, boundaries)", 1, true)
        and not schema.series:find("boundaries_hash)", 1, true),
        "version 3: series is unique on the boundary blob, not its hash: " .. tostring(schema.series))
    t:assert(schema.idx_series_boundaries_hash, "and version 3 added idx_series_boundaries_hash")
    local objects = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics,
        "SELECT type || ':' || name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")) do
        objects[#objects + 1] = r[1]
    end
    t:assert_eq(table.concat(objects, ","), "index:idx_rollups_window,index:idx_samples_series_timestamp," ..
        "index:idx_series_boundaries_hash,index:idx_series_label_hash,index:idx_series_name," ..
        "table:metadata,table:rollups,table:samples,table:series",
        "and nothing else: the version-1 objects, the rollups cache, and the boundary-hash index")
end)

