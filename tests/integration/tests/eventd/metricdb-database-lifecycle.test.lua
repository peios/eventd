-- eventd TRM §5.4 — the metric store's lifecycle: path, creation, opening,
-- concurrency and checkpointing.
--
-- One VM, broken and repaired in turn, in the style of
-- bootstrap-failure.test.lua: the seed makes eventd ErrorControl=Normal and
-- RestartPolicy=Never for this VM only, so a failed start is one attempt
-- that stays Failed until the test starts eventd again (in the image it is
-- Critical/OnFailure, which would retry and reboot). Nothing under test
-- reads either value.
--
-- The store-file cases need SQLite files the guest cannot build (it ships no
-- sqlite3), so they are built on the host with python's sqlite3, written into
-- the stopped store directory, and eventd is started on them: a version-1
-- and a version-2 store (migrated to version 3), an unknown and a missing
-- schema_version, and a v2 store missing a required index (all startup
-- failures), an unreadable file and one holding schema objects but no
-- metadata entries (quarantined and replaced), and one holding no schema
-- at all (created afresh in place).
--
-- The rest is read off the running store: its file name, its tables and
-- indexes and metadata, its WAL journal mode, the size of the database file
-- as WalCheckpointPages drives passive checkpoints, and the open-file flags
-- of eventd's descriptors on metrics.db (one read-write, the rest read-only).
--
-- Not observable here: synchronous=NORMAL is a per-connection pragma never
-- written to the file, and "the checkpoint is the durability boundary" needs
-- a power cut.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local METRICS_DIR = "/var/state/eventd/metrics/"

local vm = eventd.boot({
    name = "ev-metricdb",
    noncritical = true,
})

-- Ask peinit for a fresh eventd and return its status once the attempt has
-- settled (`restart` covers a running and a failed eventd alike).
local function attempt()
    vm:run("svctl restart eventd")
    local raw
    wait_until(function()
        raw = vm:run("svctl --json status eventd").stdout
        return raw:find('"current_operation":null', 1, true) ~= nil
    end, { timeout = 60, interval = 0.25, desc = "eventd's start attempt to settle" })
    return json.decode(raw)
end

local function answering()
    return eventd.query(vm, "EVENTS TAKE 1").ok
end

local function assert_failed(t, status, why)
    t:assert_eq(status.state, "failed", why .. ": the start failed: " .. json.encode(status))
    t:assert(not answering(), why .. ": and nothing answers on the query socket")
end

-- Build a SQLite database on the host from `script` and return its bytes.
local function host_db(script)
    local dir = eventd.host_tmpdir()
    eventd.host_write(dir .. "/s.sql", script)
    local run = assert(io.popen("python3 -c 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); "
        .. "c.executescript(open(sys.argv[2]).read()); c.commit(); c.close()' "
        .. dir .. "/db " .. dir .. "/s.sql 2>&1", "r"))
    local out = run:read("a")
    assert(run:close(), "building the crafted store failed: " .. out)
    local bytes = eventd.host_read(dir .. "/db")
    os.execute("rm -rf '" .. dir .. "'")
    return bytes
end

-- Stop eventd and replace the store with `bytes` (nil: no store at all).
local function install(bytes)
    eventd.stop(vm)
    eventd.remove_db(vm, eventd.DB.metrics)
    if bytes then vm:write_file(eventd.DB.metrics, bytes) end
end

-- After a failed crafted store, put a fresh one in place.
local function fresh_store()
    install(nil)
    eventd.start(vm)
end

-- A timestamp a minute behind the guest's clock: SINCE … ends at the guest's
-- now, and the host clock can run ahead of it, putting a host-stamped sample
-- in the query's future.
local function guest_past_ns()
    return eventd.guest_ns(vm) - 60 * 1000000000
end

local function file_size(path)
    return tonumber(vm:run("wc -c < " .. path).stdout:match("%d+")) or 0
end

local V1_SCHEMA = [[
CREATE TABLE series (id INTEGER PRIMARY KEY, name TEXT NOT NULL, labels TEXT NOT NULL,
    type INTEGER NOT NULL CHECK (type IN (0, 1, 2)), label_hash INTEGER NOT NULL,
    boundaries_hash INTEGER, boundaries BLOB, UNIQUE(name, labels, boundaries_hash));
CREATE TABLE samples (id INTEGER PRIMARY KEY, series_id INTEGER NOT NULL REFERENCES series(id),
    boot_id BLOB NOT NULL, timestamp INTEGER NOT NULL, value REAL NOT NULL, histogram_data BLOB);
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX idx_samples_series_timestamp ON samples(series_id, timestamp, id);
CREATE INDEX idx_series_name ON series(name);
CREATE INDEX idx_series_label_hash ON series(label_hash);
]]

local ROLLUPS = [[
CREATE TABLE rollups (series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    window_start INTEGER NOT NULL, window_width INTEGER NOT NULL CHECK (window_width > 0),
    transform INTEGER NOT NULL CHECK (transform IN (0, 1, 2, 50, 95, 99)),
    function INTEGER NOT NULL CHECK (function BETWEEN 0 AND 3), value REAL,
    overflow INTEGER NOT NULL CHECK (overflow IN (0, 1)),
    source_max_sample_id INTEGER NOT NULL CHECK (source_max_sample_id >= 0),
    source_baseline_sample_id INTEGER CHECK (source_baseline_sample_id > 0),
    CHECK (overflow = 0 OR value IS NULL),
    PRIMARY KEY (series_id, window_start, window_width, transform, function)) WITHOUT ROWID;
CREATE INDEX idx_rollups_window ON rollups(window_start);
]]

-- §5.4 Path.
test("MetricStorePath names the directory and the file is metrics.db", {
    spec = "eventd *metricdb.metricstorepath-names-the-metric-store-directory"
        .. " eventd *metricdb.the-database-file-is-named-metrics-db",
}, function(t)
    local r = vm:run("reg get '" .. eventd.KEY .. "' MetricStorePath")
    t:assert(r.stdout:find("/var/state/eventd/metrics", 1, true),
        "MetricStorePath names the metrics directory: " .. r.stdout .. r.stderr)
    local ls = vm:run("ls " .. METRICS_DIR).stdout
    t:assert(ls:find("metrics.db", 1, true), "the database inside it is metrics.db: " .. ls)
end)

-- §5.4 Creation (every boot starts on a fresh tmpfs, so this store was just
-- created).
test("a freshly created store is WAL with every table, index and metadata entry", {
    spec = "eventd *metricdb.a-new-store-is-created-in-wal-mode"
        .. " eventd *metricdb.a-new-store-creates-the-series-samples-rollups-and-metadata-tables"
        .. " eventd *metricdb.a-new-store-creates-every-write-time-index"
        .. " eventd *metricdb.a-new-store-records-schema-version-and-created-at",
}, function(t)
    local mode = eventd.sql(vm, eventd.DB.metrics, "PRAGMA journal_mode")
    t:assert_eq(mode[1][1], "wal", "the new store is in WAL mode")
    local schema = eventd.schema(vm, eventd.DB.metrics)
    for _, name in ipairs({ "series", "samples", "rollups", "metadata" }) do
        t:assert(schema[name], "the new store has the " .. name .. " table")
    end
    for _, name in ipairs({ "idx_samples_series_timestamp", "idx_series_name",
                            "idx_series_label_hash", "idx_rollups_window" }) do
        t:assert(schema[name], "the new store has the index " .. name)
    end
    local meta = eventd.sql(vm, eventd.DB.metrics,
        "SELECT key, value FROM metadata WHERE key IN ('schema_version', 'created_at') ORDER BY key")
    t:assert_eq(#meta, 2, "schema_version and created_at are recorded: " .. json.encode(meta))
    t:assert(meta[1][2]:match("^%d%d%d%d%-%d%d%-%d%dT"), "created_at is a timestamp: " .. meta[1][2])
end)

-- §5.4 Opening: an existing store reopens in WAL mode (NORMAL is a
-- connection pragma, not stored in the file, so only WAL is asserted).
test("an existing store reopens in WAL mode after a restart, its data intact", {
    spec = "eventd *metricdb.an-existing-store-opens-in-wal-mode-with-synchronous-normal",
}, function(t)
    local name = eventd.marker("mdb")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.restart(vm)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics, "PRAGMA journal_mode")[1][1], "wal",
        "the reopened store is still WAL")
    t:assert_eq(#eventd.rows(vm, "METRIC " .. name .. " SINCE 10m ago"), 1,
        "the pre-restart sample is still present (the store was opened, not recreated)")
end)

-- §5.4 Path: "A missing, invalid or unsafe directory is a startup failure",
-- with no degraded mode; eventd never creates the directory.
test("a missing or wrongly protected store directory fails startup, with no degraded mode", {
    spec = "eventd *metricdb.a-missing-invalid-or-unsafe-directory-is-a-startup-failure"
        .. " eventd *metricdb.the-metric-store-is-required-and-has-no-degraded-mode"
        .. " eventd *metricdb.the-directory-has-the-event-store-directory-requirements",
}, function(t)
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-absent-metrics"):assert_ok()
    assert_failed(t, attempt(), "metric store directory missing")
    -- No degraded mode: the event and log stores are fine, yet the whole
    -- daemon is down — no log or metric socket either.
    local sockets = vm:run("ls /run/eventd").stdout
    t:assert(not sockets:find("log.sock", 1, true) and not sockets:find("metric.sock", 1, true),
        "no socket of a partial eventd exists: " .. sockets)
    -- The event store's descriptor requirement applies: a directory that
    -- exists but carries an inherited rather than the protected descriptor.
    vm:run("mkdir -p /var/state/eventd/pt-plain-metrics"):assert_ok()
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-plain-metrics"):assert_ok()
    assert_failed(t, attempt(), "metric store directory with the wrong descriptor")
    eventd.set(vm, "MetricStorePath", "sz:" .. METRICS_DIR):assert_ok()
    eventd.start(vm)
end)

-- §5.4 Path: "eventd does not create it or any parent directory, never
-- follows a symbolic-link component".
test("the store directory is never created and never reached through a symlink", {
    spec = "eventd *metricdb.the-directory-is-never-created-or-traversed-through-symlinks-and-the-database-opens-relative-to-its-handle",
}, function(t)
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-never-made/metrics"):assert_ok()
    assert_failed(t, attempt(), "missing directory and parent")
    t:assert(vm:run("test -e /var/state/eventd/pt-never-made").exit_code ~= 0,
        "neither the directory nor its parent was created")
    -- A link whose target is the real, correctly protected metric store.
    vm:run("ln -sfn " .. METRICS_DIR .. " /var/state/eventd/pt-metrics-link"):assert_ok()
    eventd.set(vm, "MetricStorePath", "sz:/var/state/eventd/pt-metrics-link"):assert_ok()
    assert_failed(t, attempt(), "store reached through a final-component symlink")
    vm:run("ln -sfn /var/state/eventd /run/pt-metrics-varlink"):assert_ok()
    eventd.set(vm, "MetricStorePath", "sz:/run/pt-metrics-varlink/metrics"):assert_ok()
    assert_failed(t, attempt(), "store reached through an intermediate symlink")
    eventd.set(vm, "MetricStorePath", "sz:" .. METRICS_DIR):assert_ok()
    eventd.start(vm)
end)

-- §5.4 Opening step 2: "Version 1 is migrated transactionally to version 2
-- by adding the adaptive-rollup cache." (§5.6: the table and its pruning
-- index in one immediate transaction, then version 2, and on to version 3.)
test("a version-1 store is migrated to version 2, gaining the rollups table and index, then on to 3", {
    spec = "eventd *metricdb.a-version-1-store-is-migrated-transactionally-to-version-2"
        .. " eventd *rollup.a-version-1-store-gains-the-table-and-pruning-index-in-one-immediate-transaction"
        .. " eventd *series.schema-version-1-comprises-series-samples-and-metadata",
}, function(t)
    local now = guest_past_ns()
    install(host_db(V1_SCHEMA .. [[
INSERT INTO metadata VALUES ('schema_version', '1');
INSERT INTO metadata VALUES ('created_at', '2026-01-01T00:00:00Z');
INSERT INTO series (id, name, labels, type, label_hash) VALUES (1, 'ptmigrated', '', 1, 5472609002491880229);
INSERT INTO samples (series_id, boot_id, timestamp, value)
    VALUES (1, x'00000000000000000000000000000000', ]] .. now .. [[, 42.0);
]]))
    eventd.start(vm)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM metadata WHERE key = 'schema_version'")[1][1], "3",
        "the store now records version 3, the current one, by way of 2")
    local schema = eventd.schema(vm, eventd.DB.metrics)
    t:assert(schema.rollups, "the rollups table was added")
    t:assert(schema.idx_rollups_window, "with its pruning index")
    local rows = eventd.rows(vm, "METRIC ptmigrated SINCE 10m ago")
    t:assert(#rows == 1 and rows[1].value == 42, "the version-1 data survived: " .. json.encode(rows))
    fresh_store()
end)

-- §5.2 Uniqueness: "For counters and gauges boundaries is null, and SQLite
-- treats nulls as distinct in a unique constraint — so the constraint does
-- not enforce uniqueness for them. What does is the single-writer
-- resolution logic." A version-2 store holding two identical gauge series
-- is accepted, and migrated to version 3 with both kept; eventd itself,
-- resolving, never makes such a pair.
test("counter/gauge uniqueness comes from resolution, not the UNIQUE constraint", {
    spec = "eventd *series.counter-and-gauge-uniqueness-rests-on-single-writer-resolution-not-the-constraint",
}, function(t)
    local now = guest_past_ns()
    install(host_db(V1_SCHEMA .. ROLLUPS .. [[
INSERT INTO metadata VALUES ('schema_version', '2');
INSERT INTO metadata VALUES ('created_at', '2026-01-01T00:00:00Z');
INSERT INTO series (name, labels, type, label_hash) VALUES ('ptdup', '', 1, 5472609002491880229);
INSERT INTO series (name, labels, type, label_hash) VALUES ('ptdup', '', 1, 5472609002491880229);
]]))
    eventd.start(vm)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT COUNT(*) FROM series WHERE name = 'ptdup'")[1][1], 2,
        "the UNIQUE(name, labels, boundaries) constraint holds two identical gauge series")
    local name = eventd.marker("mdbuniq")
    for v = 1, 5 do
        eventd.send_metric(vm, { name = name, type = "gauge", value = v, timestamp = now + v })
    end
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 5 end)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT COUNT(*) FROM series WHERE name = '" .. name .. "'")[1][1], 1,
        "eventd's resolution kept five samples of one gauge in one series")
    fresh_store()
end)

-- §5.4 Opening step 2: "Missing or unrecognised versions are a startup
-- failure."
test("an unrecognised or missing schema version is a startup failure", {
    spec = "eventd *metricdb.opening-verifies-the-schema-version"
        .. " eventd *metricdb.a-missing-or-unrecognised-schema-version-is-a-startup-failure",
}, function(t)
    install(host_db(V1_SCHEMA .. ROLLUPS .. "INSERT INTO metadata VALUES ('schema_version', '7');"))
    assert_failed(t, attempt(), "schema_version 7")
    install(host_db(V1_SCHEMA .. ROLLUPS .. "INSERT INTO metadata VALUES ('created_at', 'x');"))
    assert_failed(t, attempt(), "no schema_version row")
    fresh_store()
end)

-- §5.4 Opening step 3: "Verify structural integrity — required tables and
-- indexes present. Failing this, with SQLite reporting no corruption, is a
-- startup failure."
-- §5.4 also: "a version 2 store missing a required table or index fails
-- startup instead" of being migrated.
test("a version-2 store missing a required index is a startup failure", {
    spec = "eventd *metricdb.opening-verifies-the-required-tables-and-indexes-are-present"
        .. " eventd *metricdb.a-structural-failure-without-reported-corruption-is-a-startup-failure"
        .. " eventd *metricdb.a-version-2-store-is-migrated-transactionally-to-version-3",
}, function(t)
    local schema = (V1_SCHEMA .. ROLLUPS):gsub("CREATE INDEX idx_series_name ON series%(name%);\n", "")
    install(host_db(schema .. "INSERT INTO metadata VALUES ('schema_version', '2');"))
    assert_failed(t, attempt(), "v2 store without idx_series_name")
    -- Not treated as corruption: nothing was quarantined.
    t:assert(not vm:run("ls " .. METRICS_DIR).stdout:find("corrupt", 1, true),
        "a structural gap is a failure, not a quarantine")
    fresh_store()
end)

-- §5.4 Opening step 4: "On SQLite reporting corruption, quarantine and
-- replace … and a fresh empty store created at the configured path."
test("a corrupt store is quarantined and replaced with a fresh empty store", {
    spec = "eventd *metricdb.a-corrupt-store-is-quarantined-and-replaced-with-a-fresh-empty-store",
}, function(t)
    install(string.rep("this is definitely not a SQLite database\n", 200))
    local status = attempt()
    t:assert_eq(status.state, "active", "eventd started on a replacement store: " .. json.encode(status))
    eventd.ready(vm)
    local ls = vm:run("ls " .. METRICS_DIR).stdout
    t:assert(ls:find("metrics%.db%.corrupt%.%d+"), "the unreadable file was quarantined: " .. ls)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics, "SELECT COUNT(*) FROM samples")[1][1] >= 0, true,
        "and metrics.db is a working store again")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM metadata WHERE key = 'schema_version'")[1][1], "3",
        "a fresh version-3 store")
end)

local function corrupt_files()
    local out = {}
    for name in vm:run("ls " .. METRICS_DIR).stdout:gmatch("[^\n]+") do
        if name:find("%.corrupt%.") then out[#out + 1] = name end
    end
    return out
end

--- The schema objects of a store, "type:name" sorted and joined.
local function objects()
    local out = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics,
        "SELECT type || ':' || name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")) do
        out[#out + 1] = r[1]
    end
    return table.concat(out, ",")
end

local V3_OBJECTS = "index:idx_rollups_window,index:idx_samples_series_timestamp,index:idx_series_boundaries_hash," ..
    "index:idx_series_label_hash,index:idx_series_name,table:metadata,table:rollups,table:samples,table:series"

-- §5.4 Opening step 2: "Version 2 is migrated transactionally to version 3
-- by rebuilding the series table under its new uniqueness constraint,
-- foreign keys off for the rebuild and every identifier kept". With foreign
-- keys on, dropping the old series table would cascade into the rollups
-- that reference it; a rollup row surviving is the observable of the
-- rebuild running with them off.
test("a version-2 store is migrated to version 3, the series table rebuilt with every identifier kept", {
    spec = "eventd *metricdb.a-version-2-store-is-migrated-transactionally-to-version-3"
        .. " eventd *series.schema-version-3-makes-series-unique-on-the-full-boundary-blob",
}, function(t)
    local now = guest_past_ns()
    install(host_db(V1_SCHEMA .. ROLLUPS .. [[
INSERT INTO metadata VALUES ('schema_version', '2');
INSERT INTO metadata VALUES ('created_at', '2026-01-01T00:00:00Z');
INSERT INTO series (id, name, labels, type, label_hash) VALUES (7, 'ptv2gauge', '', 1, 5472609002491880229);
INSERT INTO series (id, name, labels, type, label_hash, boundaries_hash, boundaries)
    VALUES (9, 'ptv2hist', '', 2, 5472609002491880229, 12345, x'000000000000f03f0000000000000040');
INSERT INTO samples (id, series_id, boot_id, timestamp, value)
    VALUES (100, 7, x'00000000000000000000000000000000', ]] .. now .. [[, 42.0);
INSERT INTO rollups (series_id, window_start, window_width, transform, function, value, overflow,
    source_max_sample_id, source_baseline_sample_id) VALUES (7, 0, 60000000000, 0, 0, 42.0, 0, 100, NULL);
]]))
    eventd.start(vm)
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM metadata WHERE key = 'schema_version'")[1][1], "3", "the store now records version 3")
    local schema = eventd.schema(vm, eventd.DB.metrics)
    t:assert(schema.series:find("UNIQUE(name, labels, boundaries)", 1, true),
        "the series table is unique on the boundary blob: " .. schema.series)
    t:assert_eq(objects(), V3_OBJECTS, "with idx_series_boundaries_hash added, and nothing lost")
    local kept = eventd.sql(vm, eventd.DB.metrics,
        "SELECT id || ':' || name || ':' || type || ':' || IFNULL(boundaries_hash, '-') || ':' || hex(boundaries) "
        .. "FROM series ORDER BY id")
    t:assert_eq(json.encode(kept), '[["7:ptv2gauge:1:-:"],["9:ptv2hist:2:12345:000000000000F03F0000000000000040"]]',
        "every series kept, under its own id, with its boundaries")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics, "SELECT series_id FROM samples WHERE id = 100")[1][1], 7,
        "the sample still references its series by the same id")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics, "SELECT COUNT(*) FROM rollups WHERE series_id = 7")[1][1], 1,
        "and the rollup row referencing it survived the rebuild: foreign keys were off")
    local rows = eventd.rows(vm, "METRIC ptv2gauge SINCE 10m ago")
    t:assert(#rows == 1 and rows[1].value == 42, "the version-2 data reads back: " .. json.encode(rows))
    fresh_store()
end)

-- §5.4 Creation: "A metric store that does not exist, or whose database
-- holds no schema at all (what a power cut leaves when it takes the
-- uncheckpointed creating transaction), is created". The crafted file is
-- exactly that: a WAL-mode header page and nothing else.
test("a metric store whose database holds no schema at all is created as new, not quarantined", {
    spec = "eventd *metricdb.a-metric-store-with-no-schema-is-created-as-new",
}, function(t)
    local bytes = host_db("PRAGMA journal_mode=WAL;")
    t:assert(#bytes > 0 and bytes:byte(19) == 2, "precondition: a WAL-mode database file of " .. #bytes .. " bytes")
    local before = #corrupt_files()
    install(bytes)
    local status = attempt()
    t:assert_eq(status.state, "active", "eventd started on it: " .. json.encode(status))
    eventd.ready(vm)
    t:assert_eq(#corrupt_files(), before, "nothing was quarantined: " .. table.concat(corrupt_files(), " "))
    t:assert_eq(objects(), V3_OBJECTS, "the store was created in the file, every table and index")
    t:assert_eq(eventd.sql(vm, eventd.DB.metrics,
        "SELECT value FROM metadata WHERE key = 'schema_version'")[1][1], "3", "at version 3")
end)

-- §5.4 Opening step 4: "A database holding schema objects but no metadata
-- table with entries is not a store eventd could have written; it is
-- quarantined and replaced like a corrupt one." Both shapes: tables without
-- a metadata table, and a metadata table with no entries.
test("a metric store with schema objects but no metadata entries is quarantined and replaced", {
    spec = "eventd *metricdb.unrecognised-contents-are-quarantined",
}, function(t)
    local cases = {
        { "no metadata table", "CREATE TABLE series (id INTEGER PRIMARY KEY);", "series" },
        { "an empty metadata table", "CREATE TABLE series (id INTEGER PRIMARY KEY);" ..
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;", "metadata,series" },
    }
    for _, c in ipairs(cases) do
        local bytes = host_db(c[2])
        local before = {}
        for _, f in ipairs(corrupt_files()) do before[f] = true end
        install(bytes)
        local status = attempt()
        t:assert_eq(status.state, "active", c[1] .. ": eventd started on a replacement: " .. json.encode(status))
        eventd.ready(vm)
        local new = {}
        for _, f in ipairs(corrupt_files()) do if not before[f] then new[#new + 1] = f end end
        t:assert_eq(#new, 1, c[1] .. ": the database was quarantined: " .. table.concat(new, " "))
        -- Not byte for byte: opening put the file in WAL mode before its
        -- contents were judged. What it held is aside, untouched.
        t:assert_eq(new[1] and eventd.sql(vm, METRICS_DIR .. new[1], "SELECT group_concat(name) FROM "
            .. "(SELECT name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name)")[1][1], c[3],
            c[1] .. ": the crafted database is what was set aside")
        t:assert_eq(objects(), V3_OBJECTS, c[1] .. ": a fresh store stands at the path")
    end
end)

-- §5.4 Checkpointing: "The metric writer checkpoints at WalCheckpointPages in
-- passive mode." eventd turns SQLite's own autocheckpoint off, so pages reach
-- metrics.db only through eventd's checkpoints: with a low threshold the
-- database file grows as samples arrive; with the threshold at its ceiling it
-- does not, and the samples stay in the WAL.
test("the writer checkpoints at WalCheckpointPages", {
    spec = "eventd *metricdb.the-writer-checkpoints-passively-at-walcheckpointpages-without-blocking",
}, function(t)
    local function flood(name)
        local now = os.time() * 1000000000
        for chunk = 0, 19 do
            local recs = {}
            for i = 1, 1000 do
                local n = chunk * 1000 + i
                recs[i] = { name = name, type = "gauge", value = n, timestamp = now + n }
            end
            eventd.send_metric(vm, eventd.array(recs))
        end
        local n = 0
        local ok, err = pcall(wait_until, function()
            n = eventd.sql(vm, eventd.DB.metrics,
                "SELECT COUNT(*) FROM samples s JOIN series e ON e.id = s.series_id WHERE e.name = '"
                .. name .. "'")[1][1]
            return n >= 20000
        end, { timeout = 90, interval = 0.5, desc = "20000 samples for " .. name })
        assert(ok, tostring(err) .. " (" .. n .. " landed)")
    end
    -- High threshold first: nothing is checkpointed while the samples land.
    eventd.set(vm, "WalCheckpointPages", "dword:100000"):assert_ok()
    vm:run("sleep 2") -- let the retention pass the change triggers checkpoint first
    local before = file_size(eventd.DB.metrics)
    flood(eventd.marker("mdbhi"))
    local after_hi = file_size(eventd.DB.metrics)
    t:assert(after_hi - before < 200000,
        "with the ceiling threshold metrics.db barely grows (" .. before .. " -> " .. after_hi .. ")")
    -- Low threshold: checkpoints copy pages into the database file.
    eventd.set(vm, "WalCheckpointPages", "dword:100"):assert_ok()
    vm:run("sleep 2")
    local base = file_size(eventd.DB.metrics)
    flood(eventd.marker("mdblo"))
    local after_lo = file_size(eventd.DB.metrics)
    t:assert(after_lo - base > 500000,
        "at 100 pages the writer checkpoints into metrics.db (" .. base .. " -> " .. after_lo .. ")")
    eventd.unset(vm, "WalCheckpointPages")
end)

-- §5.4 Concurrency: "Exactly one read-write connection, owned by the metric
-- writer … Retention and adaptive rollups plan or compute with read-only
-- connections … they never open another read-write connection." Read off the
-- open-file flags of eventd's descriptors on metrics.db while queries and
-- retention passes run.
test("eventd holds exactly one read-write descriptor on metrics.db; the rest are read-only", {
    spec = "eventd *metricdb.exactly-one-read-write-connection-owned-by-the-metric-writer"
        .. " eventd *metricdb.retention-and-rollups-use-read-only-connections-and-submit-commands-to-the-writer"
        .. " eventd *metricretain.retention-plans-read-only-and-submits-low-priority-deletes-to-the-writer",
}, function(t)
    eventd.set(vm, "AdaptiveRollupMinSamples", "dword:100"):assert_ok()
    local name = eventd.marker("mdbfd")
    local now = os.time() * 1000000000
    local recs = {}
    for i = 1, 400 do recs[i] = { name = name, type = "gauge", value = i, timestamp = now - 300 * 1000000000 + i * 500000000 } end
    eventd.send_metric(vm, eventd.array(recs))
    local saw_readonly, max_rw, samples = false, 0, 0
    for round = 1, 15 do
        -- Keep read-only work in flight: a seeding window query and a
        -- retention pass (kicked by a live change).
        vm:run("evctl 'METRIC " .. name .. " SINCE 10m ago AVG_OVER 10s' >/dev/null 2>&1 &")
        eventd.set(vm, "RetentionDeleteBatchRows", "dword:" .. (1000 + round))
        local _, _, on = eventd.fds_on(vm, nil, eventd.DB.metrics)
        local rw = 0
        for _, e in ipairs(on) do
            samples = samples + 1
            if e.mode == 2 then rw = rw + 1 elseif e.mode == 0 then saw_readonly = true end
        end
        if rw > max_rw then max_rw = rw end
    end
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert(samples > 0, "eventd's descriptors on metrics.db were read")
    t:assert_eq(max_rw, 1, "never more than one read-write descriptor on metrics.db")
    t:assert(saw_readonly, "while read-only descriptors (queries, retention planning) came and went")
end)

-- ---------------------------------------------------------------------------
-- Documented homes.
-- ---------------------------------------------------------------------------

-- Route closed: synchronous=NORMAL is a connection PRAGMA, never written into
-- the database file, and nothing in /proc or the store exposes it; only a
-- power-loss test could tell NORMAL from FULL.
test("a new store sets synchronous=NORMAL (not observable)", {
    spec = "eventd *metricdb.a-new-store-sets-synchronous-normal",
    skip = true,
    covered_by = "doc:not-observable connection pragma, never persisted; distinguishable only by power loss",
}, function() end)

-- Route closed: that the checkpoint is the durability boundary under
-- synchronous=NORMAL (§9.5) can only be shown by cutting power between commit
-- and checkpoint, which this harness cannot do to a tmpfs-backed store.
test("the checkpoint is the durability boundary (not observable)", {
    spec = "eventd *metricdb.the-checkpoint-is-the-durability-boundary",
    skip = true,
    covered_by = "doc:not-observable requires power-loss injection",
}, function() end)
