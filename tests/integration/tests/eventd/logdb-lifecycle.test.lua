-- eventd TRM §4.3 — the log store's database lifecycle: its directory,
-- creation, opening, quarantine, connections and checkpointing.
--
-- One file-scope VM. The chapter's startup failures (a bad directory, a
-- bad schema) are reached without letting the eventd service fail: a
-- failing start of the service would retry and then trip peinit's
-- Critical policy and take the VM to recovery. Instead the service is
-- stopped through the service manager and the eventd binary is run by
-- hand from the agent, against the same registry configuration, with
-- LogStorePath pointed at a scratch directory under /run that the test
-- has given the store directory's required descriptor (`sd set`) and
-- filled with a tampered copy of logs.db. eventd opens its event
-- directory, log directory and metric directory first (pipeline.rs:50-52)
-- and the log database before binding any socket (pipeline.rs:117 then
-- 123), so a log-store failure stops it before it touches anything the
-- service would have to clean up. The run is bounded (`eventd.run_by_hand`
-- kills it from the agent after 30 seconds); the service is started again
-- afterwards with LogStorePath restored.
--
-- Cases that the service survives (creation, reopening, quarantine) go
-- through the service itself: stop, change the files, start.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-logdb" })

local REQUIRED_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)"
    .. "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local SAVED_PATH = vm:run("reg get '" .. eventd.KEY .. "' LogStorePath").stdout:gsub("%s+$", "")

local function set_log_path(path)
    eventd.set(vm, "LogStorePath", "sz:" .. path):assert_ok()
end

local function restore_log_path()
    set_log_path(SAVED_PATH)
end

--- A fresh directory under /run with the store directory's descriptor.
local function scratch_dir(name)
    local dir = "/run/" .. name
    vm:run("rm -rf " .. dir .. " && mkdir -p " .. dir):assert_ok()
    vm:run("sd set " .. dir .. " '" .. REQUIRED_SDDL .. "'"):assert_ok()
    return dir
end

--- Run eventd by hand (the service must be stopped). Returns exit code
--- (124 when it was still running after 30 seconds and was killed) and
--- stderr.
local function hand_run()
    local r = eventd.run_by_hand(vm, {}, { timeout = 30 })
    return r.killed and 124 or r.exit_code, r.output
end

local function sha(path)
    return vm:run("sha256sum '" .. path .. "'").stdout:match("^(%x+)")
end

local function exists(path)
    return vm:run("test -e '" .. path .. "' -o -L '" .. path .. "'").exit_code == 0
end

local function wal_salt(db)
    local ok, wal = pcall(vm.read_file, vm, db .. "-wal")
    if not ok or #wal < 32 then return nil end
    return (string.unpack(">I4", wal, 17))
end


-- ---------------------------------------------------------------------------
-- Path
-- ---------------------------------------------------------------------------

test("LogStorePath names the provisioned log directory, and the database in it is logs.db", {
    spec = "eventd *logdb.logstorepath-names-a-provisioned-directory"
        .. " eventd *logdb.the-log-database-file-is-named-logs-db",
}, function(t)
    t:assert_eq(SAVED_PATH:gsub("/$", ""), "/var/state/eventd/logs", "the conventional path")
    local prov = vm:run("reg get 'Machine\\System\\Init\\ProvisionedPaths\\eventd-logs' Path")
    t:assert_eq(prov.stdout:gsub("%s+$", ""):gsub("/$", ""), "/var/state/eventd/logs",
        "which peinit provisions before Phase 2")
    local dbs = {}
    for _, e in ipairs(vm:listdir(eventd.STORE.logs)) do
        local n = type(e) == "table" and e.name or e
        if n:match("%.db$") then dbs[#dbs + 1] = n end
    end
    t:assert_eq(table.concat(dbs, ","), "logs.db", "the database is logs.db inside it")
end)

-- ---------------------------------------------------------------------------
-- Concurrency
-- ---------------------------------------------------------------------------

test("logs.db has one read-write connection; queries read through their own", {
    spec = "eventd *logdb.one-read-write-connection-owned-by-the-log-writer-and-any-number-of-read-only-ones",
}, function(t)
    local pid = eventd.pid(vm)
    local rw = eventd.fds_on(vm, pid, eventd.DB.logs)
    t:assert_eq(rw, 1, "one read-write descriptor on logs.db")
    -- Queries read concurrently with the writer and leave no writer behind.
    for _ = 1, 3 do eventd.rows(vm, "LOGS SINCE 1h ago TAKE 50") end
    t:assert_eq((eventd.fds_on(vm, pid, eventd.DB.logs)), 1, "still exactly one after queries")
end)

test("retention reaches logs.db only through the log writer", {
    spec = "eventd *logdb.retention-and-catalogue-cleanup-go-through-the-writer-and-never-open-a-read-write-connection",
}, function(t)
    -- Old rows for retention to delete, a pass triggered by an applied
    -- configuration change (config.rs:1031), and the descriptors sampled
    -- while it runs: never a second read-write connection.
    local origin = eventd.marker("ret")
    local recs = {}
    local old = math.tointeger(eventd.guest_ns(vm) - 40 * 86400 * 10 ^ 9)
    for i = 1, 3000 do recs[i] = { origin = origin, is_error = false, message = "r" .. i, timestamp = old + i } end
    eventd.send_log(vm, recs)
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " TAKE 10000", function(rs) return #rs == 3000 end)
    local pid = eventd.pid(vm)
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:100"):assert_ok()
    local most = 0
    for _ = 1, 20 do
        most = math.max(most, (eventd.fds_on(vm, pid, eventd.DB.logs)))
    end
    wait_until(function()
        return #eventd.sql(vm, eventd.DB.logs, "SELECT 1 FROM logs WHERE origin = '" .. origin .. "' LIMIT 1") == 0
    end, { timeout = 30, interval = 0.5, desc = "retention to delete the old rows" })
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert_eq(most, 1, "at most one read-write descriptor on logs.db throughout the pass")
end)

-- ---------------------------------------------------------------------------
-- Creation and opening
-- ---------------------------------------------------------------------------

test("a log store that does not exist is created with its schema, indexes and entries, in WAL mode", {
    spec = "eventd *logdb.a-log-store-that-does-not-exist-is-created"
        .. " eventd *logdb.creation-sets-wal-journal-mode"
        .. " eventd *logdb.creation-sets-synchronous-normal"
        .. " eventd *logdb.creation-creates-the-logs-log-origins-and-metadata-tables"
        .. " eventd *logdb.creation-creates-the-three-write-time-indexes"
        .. " eventd *logdb.creation-writes-the-schema-version-and-created-at-entries",
}, function(t)
    -- synchronous=NORMAL is per connection and leaves no mark in the
    -- file; everything else creation does is in it.
    eventd.stop(vm)
    local db = eventd.DB.logs
    vm:run("rm -f '" .. db .. "' '" .. db .. "-wal' '" .. db .. "-shm'"):assert_ok()
    eventd.start(vm)
    t:assert(vm:stat(db), "logs.db was created")
    local header = vm:read_file(db)
    t:assert_eq(header:byte(19) .. "," .. header:byte(20), "2,2", "in WAL mode")
    local objects = {}
    for _, r in ipairs(eventd.sql(vm, db,
        "SELECT type || ':' || name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")) do
        objects[#objects + 1] = r[1]
    end
    t:assert_eq(table.concat(objects, ","),
        "index:idx_logs_job_id,index:idx_logs_origin,index:idx_logs_timestamp,table:log_origins,table:logs,table:metadata",
        "with the three tables and three indexes")
    local meta = {}
    for _, r in ipairs(eventd.sql(vm, db, "SELECT key, value FROM metadata")) do meta[r[1]] = r[2] end
    t:assert_eq(meta.schema_version, "1", "schema_version 1")
    t:assert(meta.created_at and meta.created_at:match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$"),
        "and a created_at: " .. tostring(meta.created_at))
end)

test("an existing log store is reopened in WAL mode", {
    spec = "eventd *logdb.opening-uses-wal-mode"
        .. " eventd *logdb.opening-sets-synchronous-normal",
}, function(t)
    -- Take the file out of WAL mode offline (journal_mode is persistent),
    -- and the next open puts it back. synchronous=NORMAL is per connection
    -- and not visible from outside.
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.logs, "PRAGMA journal_mode=DELETE;")
    local header = vm:read_file(eventd.DB.logs)
    t:assert_eq(header:byte(19) .. "," .. header:byte(20), "1,1", "precondition: a rollback-journal file")
    eventd.start(vm)
    header = vm:read_file(eventd.DB.logs)
    t:assert_eq(header:byte(19) .. "," .. header:byte(20), "2,2", "opening switched it to WAL")
end)

-- ---------------------------------------------------------------------------
-- Checkpointing
-- ---------------------------------------------------------------------------

-- The writer checkpoints "when its write-ahead log reaches
-- WalCheckpointPages": the log's content, not the WAL file's size, which
-- SQLite does not shrink when it restarts a checkpointed log. Small
-- commits into a restarted log are not each followed by a checkpoint.
test("the log writer checkpoints when its WAL reaches WalCheckpointPages, and not before", {
    spec = "eventd *logdb.the-log-writer-checkpoints-passively-at-walcheckpointpages-and-retries-after-a-later-commit",
}, function(t)
    eventd.set(vm, "WalCheckpointPages", "dword:100"):assert_ok()
    vm:clock():sleep("2s")
    local origin = eventd.marker("cp")
    local function write(n)
        local recs = {}
        for i = 1, n do recs[i] = { origin = origin, is_error = false, message = string.rep("m", 200) .. i } end
        eventd.send_log(vm, recs)
        vm:clock():sleep("500ms")
    end
    write(1)
    local s0 = wal_salt(eventd.DB.logs)
    local crossed = false
    for _ = 1, 20 do
        write(200)
        if wal_salt(eventd.DB.logs) ~= s0 then crossed = true; break end
    end
    t:assert(crossed, "a WAL past 100 pages was checkpointed and restarted")
    -- The restarted log holds a few pages. Small commits now must not each
    -- be checkpointed.
    write(1)
    local s1 = wal_salt(eventd.DB.logs)
    write(1)
    write(1)
    local s2 = wal_salt(eventd.DB.logs)
    eventd.unset(vm, "WalCheckpointPages")
    t:assert_eq(s2, s1, "three one-line commits far below the threshold did not restart the WAL")
end)

-- ---------------------------------------------------------------------------
-- Startup failures (the service stopped; eventd run by hand)
-- ---------------------------------------------------------------------------

test("a missing, relative or unprotected log directory fails startup, and none is created", {
    spec = "eventd *logdb.a-missing-invalid-or-unsafe-log-store-directory-fails-startup"
        .. " eventd *logdb.the-log-store-directory-has-the-event-store-directory-requirements"
        .. " eventd *logdb.the-directory-is-never-created-symlinks-are-never-followed-and-the-database-is-opened-handle-relative"
        .. " eventd *logdb.the-log-store-is-required-and-eventd-never-runs-without-one",
}, function(t)
    -- The database being opened relative to the validated directory handle
    -- is not distinguishable from outside; the refusals around it are.
    vm:run("rm -rf /run/pt-unprot && mkdir -p /run/pt-unprot"):assert_ok()
    local good = scratch_dir("pt-good")
    vm:run("ln -sfn " .. good .. " /run/pt-link"):assert_ok()
    vm:run("ln -sfn /run /run/pt-runlink"):assert_ok()
    local cases = {
        { "missing", "/run/pt-absent/deeper" },
        { "relative", "run/pt-good" },
        { "without the required descriptor", "/run/pt-unprot" },
        { "a symbolic link as the directory", "/run/pt-link" },
        { "a symbolic link above it", "/run/pt-runlink/pt-good" },
    }
    eventd.stop(vm)
    local ok, err = pcall(function()
        for _, c in ipairs(cases) do
            set_log_path(c[2])
            local code, out = hand_run()
            t:assert(code ~= 0 and code ~= 124, c[1] .. ": eventd refused to start (exit " .. code .. "): " .. out)
            t:assert(not exists("/run/eventd/query.sock"),
                c[1] .. ": and never got as far as serving queries")
        end
        t:assert(not exists("/run/pt-absent"), "the missing directory, and its parent, were not created")
        t:assert(not exists(good .. "/logs.db"), "nothing was created through either link")
        -- The same directory named directly, with its descriptor, is fine.
        set_log_path(good)
    end)
    restore_log_path()
    eventd.start(vm)
    if not ok then error(err, 0) end
end)

test("a log store with a missing or unknown schema version fails startup and is left as it was", {
    spec = "eventd *logdb.a-missing-or-unrecognised-schema-version-fails-startup-without-migration"
        .. " eventd *logdb.missing-tables-or-indexes-without-reported-corruption-fail-startup"
        .. " eventd *logs.the-log-store-schema-is-checked-at-startup-and-never-migrated",
}, function(t)
    local cases = {
        { "schema_version missing", "DELETE FROM metadata WHERE key = 'schema_version';" },
        { "schema_version 2", "UPDATE metadata SET value = '2' WHERE key = 'schema_version';" },
        { "log_origins missing", "DROP TABLE log_origins;" },
        { "idx_logs_origin missing", "DROP INDEX idx_logs_origin;" },
        { "idx_logs_job_id missing", "DROP INDEX idx_logs_job_id;" },
    }
    local dir = scratch_dir("pt-schema")
    local derived = {}
    for i, c in ipairs(cases) do derived[i] = eventd.derive_store(vm, eventd.DB.logs, c[2]) end
    eventd.stop(vm)
    local ok, err = pcall(function()
        set_log_path(dir)
        for i, c in ipairs(cases) do
            vm:run("rm -f " .. dir .. "/*"):assert_ok()
            vm:write_file(dir .. "/logs.db", derived[i])
            local before = sha(dir .. "/logs.db")
            local code, out = hand_run()
            t:assert(code ~= 0 and code ~= 124, c[1] .. ": startup failed (exit " .. code .. "): " .. out)
            t:assert_eq(sha(dir .. "/logs.db"), before, c[1] .. ": the file was not migrated or rewritten")
            local names = vm:run("ls " .. dir).stdout
            t:assert(not names:find("corrupt", 1, true), c[1] .. ": nor quarantined as corrupt: " .. names)
        end
    end)
    restore_log_path()
    eventd.start(vm)
    if not ok then error(err, 0) end
end)

-- ---------------------------------------------------------------------------
-- A directory that meets the requirements, and quarantine
-- ---------------------------------------------------------------------------

--- Point the log store at a protected scratch directory holding a corrupt
--- logs.db and junk sidecars, restart eventd, and hand `check` the
--- directory listing (sorted) and the suffix each file was quarantined
--- under. LogStorePath is restored afterwards.
local function quarantine_case(t, name, check)
    local dir = scratch_dir(name)
    local junk = string.rep("this is not a database ", 400)
    vm:write_file(dir .. "/logs.db", junk)
    vm:write_file(dir .. "/logs.db-wal", "wal junk")
    vm:write_file(dir .. "/logs.db-shm", "shm junk")
    set_log_path(dir)
    local from = eventd.guest_ns(vm)
    local ok, err = pcall(function()
        eventd.restart(vm)
        local names = {}
        for _, e in ipairs(vm:listdir(dir)) do names[#names + 1] = type(e) == "table" and e.name or e end
        table.sort(names)
        local suffixes = {}
        for _, n in ipairs(names) do
            local base, suffix = n:match("^(logs%.db[%-%a]*)(%.corrupt%..+)$")
            if base then suffixes[base] = suffix end
        end
        check(dir, junk, names, suffixes, from)
    end)
    restore_log_path()
    eventd.restart(vm)
    if not ok then error(err, 0) end
end

test("a corrupt log store is quarantined and a fresh one created in its place", {
    spec = "eventd *logdb.a-corrupt-log-store-is-quarantined-and-replaced",
}, function(t)
    -- The directory is the test's own, given the required descriptor: a
    -- store directory other than the provisioned one is accepted when it
    -- meets the requirements.
    quarantine_case(t, "pt-quar1", function(dir, junk, names, suffixes, from)
        local q = suffixes["logs.db"]
        t:assert(q and q:match("^%.corrupt%.%d+$"),
            "the database was renamed .corrupt.<timestamp_ns>: " .. table.concat(names, ", "))
        t:assert_eq(vm:read_file(dir .. "/logs.db" .. (q or "")), junk, "the quarantined file is the original")
        local version = eventd.sql(vm, dir .. "/logs.db", "SELECT value FROM metadata WHERE key = 'schema_version'")
        t:assert_eq(version[1] and version[1][1], "1", "a fresh log store stands at the configured path")
        local origin = eventd.marker("q")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "after" })
        eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
        local errs = eventd.rows(vm, "EVENTS " .. eventd.T.storage_error .. " SINCE 10m ago")
        local seen = false
        for _, e in ipairs(errs) do
            if e["store.kind"] == "log" and e["event.time"] >= from then seen = true end
        end
        t:assert(seen, "and the replacement was recorded as a log storage error: " .. json.encode(errs))
    end)
end)

-- The book renames the database, -wal and -shm together: the sidecars,
-- possibly the only copy of the last commits, are moved aside as they
-- were, not lost to the failed open that led to the quarantine.
test("quarantine renames the database, -wal and -shm under one shared suffix", {
    spec = "eventd *logdb.quarantine-renames-all-three-files-with-a-shared-corrupt-suffix-and-creates-a-fresh-store",
}, function(t)
    -- The `.N` collision rule needs the nanosecond timestamp eventd will
    -- pick, which a test cannot arrange in advance.
    quarantine_case(t, "pt-quar2", function(dir, junk, names, suffixes)
        t:assert(suffixes["logs.db"] and suffixes["logs.db-wal"] and suffixes["logs.db-shm"],
            "all three files were quarantined: " .. table.concat(names, ", "))
        t:assert(suffixes["logs.db"] == suffixes["logs.db-wal"] and suffixes["logs.db"] == suffixes["logs.db-shm"],
            "under one shared suffix")
        t:assert_eq(vm:read_file(dir .. "/logs.db-wal" .. (suffixes["logs.db-wal"] or "")), "wal junk",
            "the quarantined -wal is the original")
    end)
end)

-- ---------------------------------------------------------------------------
-- A database with no schema, or with contents no eventd wrote
-- ---------------------------------------------------------------------------

--- Build a SQLite database on the host from `script` and return its bytes
--- (the guest ships no sqlite3).
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

--- Point the log store at a protected scratch directory holding `bytes` as
--- logs.db, restart eventd, and hand `check` the directory and its sorted
--- listing. LogStorePath is restored afterwards.
local function crafted_case(name, bytes, check)
    local dir = scratch_dir(name)
    vm:write_file(dir .. "/logs.db", bytes)
    set_log_path(dir)
    local ok, err = pcall(function()
        eventd.restart(vm)
        local names = {}
        for _, e in ipairs(vm:listdir(dir)) do names[#names + 1] = type(e) == "table" and e.name or e end
        table.sort(names)
        check(dir, names)
    end)
    restore_log_path()
    eventd.restart(vm)
    if not ok then error(err, 0) end
end

local LOG_OBJECTS = "index:idx_logs_job_id,index:idx_logs_origin,index:idx_logs_timestamp," ..
    "table:log_origins,table:logs,table:metadata"

local function objects(db)
    local out = {}
    for _, r in ipairs(eventd.sql(vm, db,
        "SELECT type || ':' || name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY 1")) do
        out[#out + 1] = r[1]
    end
    return table.concat(out, ",")
end

-- §4.3: "A log store that does not exist, or whose database holds no
-- schema at all (what a power cut leaves when it takes the uncheckpointed
-- creating transaction), is created". The crafted file is exactly that: a
-- WAL-mode header page and nothing else.
test("a log store whose database holds no schema at all is created as new, not quarantined", {
    spec = "eventd *logdb.a-log-store-with-no-schema-is-created-as-new",
}, function(t)
    local bytes = host_db("PRAGMA journal_mode=WAL;")
    t:assert(#bytes > 0 and bytes:byte(19) == 2, "precondition: a WAL-mode database file of " .. #bytes .. " bytes")
    crafted_case("pt-noschema", bytes, function(dir, names)
        t:assert_eq(table.concat(names, ","):find("corrupt", 1, true), nil,
            "nothing was quarantined: " .. table.concat(names, ", "))
        t:assert_eq(objects(dir .. "/logs.db"), LOG_OBJECTS, "the store was created in the file, every table and index")
        local version = eventd.sql(vm, dir .. "/logs.db", "SELECT value FROM metadata WHERE key = 'schema_version'")
        t:assert_eq(version[1] and version[1][1], "1", "at schema version 1")
        local origin = eventd.marker("ns")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "into the new store" })
        eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    end)
end)

-- §4.3: "A database holding schema objects but no metadata table with
-- entries is not a store eventd could have written; it is quarantined and
-- replaced like a corrupt one." Both shapes: tables without a metadata
-- table, and a metadata table with no entries.
test("a log store with schema objects but no metadata entries is quarantined and replaced", {
    spec = "eventd *logdb.unrecognised-contents-are-quarantined",
}, function(t)
    local cases = {
        { "no metadata table", "CREATE TABLE logs (id INTEGER PRIMARY KEY);", "logs" },
        { "an empty metadata table", "CREATE TABLE logs (id INTEGER PRIMARY KEY);" ..
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;", "logs,metadata" },
    }
    for i, c in ipairs(cases) do
        local bytes = host_db(c[2])
        crafted_case("pt-unrec" .. i, bytes, function(dir, names)
            local aside
            for _, n in ipairs(names) do
                if n:match("^logs%.db%.corrupt%.%d+$") then aside = n end
            end
            t:assert(aside, c[1] .. ": the database was quarantined: " .. table.concat(names, ", "))
            -- Not byte for byte: opening put the file in WAL mode before
            -- its contents were judged. What it held is aside, untouched.
            t:assert_eq(aside and eventd.sql(vm, dir .. "/" .. aside, "SELECT group_concat(name) FROM "
                .. "(SELECT name FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name)")[1][1], c[3],
                c[1] .. ": the crafted database is what was set aside")
            t:assert_eq(objects(dir .. "/logs.db"), LOG_OBJECTS, c[1] .. ": a fresh store stands at the path")
        end)
    end
end)
