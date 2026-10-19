-- eventd TRM §4.2 — the logs table: one unsharded database, its three
-- tables and write-time indexes, the origin grammar, the origin
-- catalogue, and why log queries take no part in adaptive indexing.
--
-- One file-scope VM, booted with two services of the test's own:
--
--   pt-logs     a oneshot whose ExecStartPre hook and main process each
--               write a line, so peinit forwards real output under a
--               one-component origin (the service) and a two-component
--               one (the hook), with stdout and stderr told apart;
--   pt-broker   a service that tries to send to the log socket itself
--               (pt-notify send-nocred), which the socket must refuse.
--
-- Everything else goes in through `eventd.send_log` as the agent (SYSTEM
-- without the Service group, which the log socket admits) and comes out
-- through evctl or a host-side sqlite copy of logs.db. The catalogue test
-- that removes an origin does so offline: eventd stopped through the
-- service manager, logs.db edited on the host, written back with its
-- -wal and -shm removed, eventd started again.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local LOGS_SH = [[#!/bin/sh
echo "pt-logs main out"
echo "pt-logs main err" >&2
]]
local HOOK_SH = [[#!/bin/sh
echo "pt-logs hook out"
]]

local SERVICES = {
    { path = [[Machine\System\Services]] },
    {
        path = [[Machine\System\Services\pt-logs]],
        values = {
            { name = "ImagePath", type = "sz", data = "/lcl/pt/logs.sh" },
            { name = "ExecStartPre", type = "multi", data = { "/bin/sh /lcl/pt/hook.sh" } },
            { name = "Type", type = "dword", data = 1 },
            { name = "RemainAfterExit", type = "dword", data = 1 },
            { name = "Identity", type = "sz", data = "SYSTEM" },
            { name = "Readiness", type = "dword", data = 1 },
        },
    },
    {
        path = [[Machine\System\Services\pt-broker]],
        values = {
            { name = "ImagePath", type = "sz", data = "/usr/bin/pt-notify" },
            { name = "Arguments", type = "multi", data = {
                "--socket", "/run/eventd/log.sock", "--log", "/run/pt-broker.log",
                "send-nocred", "pt-broker-direct", "sleep", "300",
            } },
            { name = "Identity", type = "sz", data = "SYSTEM" },
            { name = "Readiness", type = "dword", data = 1 },
        },
    },
}

local vm = eventd.boot({
    name = "ev-logs",
    files = peinit.merge(
        {
            ["lcl/pt/logs.sh"] = { LOGS_SH, exec = true },
            ["lcl/pt/hook.sh"] = { HOOK_SH, exec = true },
        },
        peinit.tool("pt-notify"),
        peinit.seed("zz-pt-logs-services", SERVICES)
    ),
})

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function shape(db, tbl)
    local parts = {}
    for _, c in ipairs(eventd.sql(vm, db,
        "SELECT name, type, \"notnull\", pk FROM pragma_table_info('" .. tbl .. "') ORDER BY cid")) do
        parts[#parts + 1] = string.format("%s %s%s%s", c[1], c[2],
            (c[3] == 1 and c[4] == 0) and " NOT NULL" or "", c[4] > 0 and " PK" or "")
    end
    return table.concat(parts, ", ")
end

local function sql_quote(s) return "'" .. s:gsub("'", "''") .. "'" end

local function rows_for(origin)
    return eventd.sql(vm, eventd.DB.logs,
        "SELECT id, hex(boot_id), timestamp, origin, is_error, message, hex(job_id) FROM logs WHERE origin = "
        .. sql_quote(origin) .. " ORDER BY id")
end

local function in_catalogue(origin)
    return #eventd.sql(vm, eventd.DB.logs,
        "SELECT 1 FROM log_origins WHERE origin = " .. sql_quote(origin)) == 1
end

-- ---------------------------------------------------------------------------
-- The database and its schema
-- ---------------------------------------------------------------------------

test("the log store is one database holding logs, log_origins and metadata", {
    spec = "eventd *logs.the-log-store-is-a-single-unsharded-database"
        .. " eventd *logs.the-log-store-holds-the-logs-log-origins-and-metadata-tables"
        .. " eventd *logs.log-store-schema-version-1-comprises-logs-log-origins-and-metadata"
        .. " eventd *logs.the-log-store-metadata-table-has-a-shards-two-column-structure"
        .. " eventd *logs.log-origins-has-a-single-origin-text-primary-key-column",
}, function(t)
    local dbs = {}
    for _, e in ipairs(vm:listdir(eventd.STORE.logs)) do
        local n = type(e) == "table" and e.name or e
        if n:match("%.db$") then dbs[#dbs + 1] = n end
    end
    t:assert_eq(table.concat(dbs, ","), "logs.db", "one database file in the log store directory")
    local tables = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.logs,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")) do
        tables[#tables + 1] = r[1]
    end
    t:assert_eq(table.concat(tables, ","), "log_origins,logs,metadata", "exactly the three tables")
    local version = eventd.sql(vm, eventd.DB.logs, "SELECT value FROM metadata WHERE key = 'schema_version'")
    t:assert_eq(version[1] and version[1][1], "1", "at schema version 1")
    t:assert_eq(shape(eventd.DB.logs, "metadata"), shape(eventd.shards(vm)[1], "metadata"),
        "metadata has a shard's two-column structure")
    t:assert_eq(shape(eventd.DB.logs, "metadata"), "key TEXT PK, value TEXT NOT NULL", "key and value")
    t:assert_eq(shape(eventd.DB.logs, "log_origins"), "origin TEXT PK", "log_origins is one TEXT key column")
end)

test("the logs table has exactly the narrow columns the chapter lists", {
    spec = "eventd *logs.a-log-record-has-no-payload-blob-and-no-origin-class"
        .. " eventd *logs.id-is-a-monotonic-rowid-primary-key",
}, function(t)
    t:assert_eq(shape(eventd.DB.logs, "logs"),
        "id INTEGER PK, boot_id BLOB NOT NULL, timestamp INTEGER NOT NULL, origin TEXT NOT NULL, "
        .. "is_error INTEGER NOT NULL, message TEXT NOT NULL, job_id BLOB",
        "seven columns: no payload, no origin class")
    local origin = eventd.marker("id")
    for i = 1, 5 do eventd.send_log(vm, { origin = origin, is_error = false, message = "n" .. i }) end
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 5 end)
    local rows = rows_for(origin)
    for i = 2, #rows do
        t:assert(rows[i][1] > rows[i - 1][1], "ids rise in the order the lines were written")
        t:assert_eq(rows[i][6], "n" .. i, "and line " .. i .. " has the next id")
    end
    local ipk = eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM pragma_index_list('logs') WHERE origin = 'pk'")
    t:assert_eq(ipk[1][1], 0, "id is the rowid itself, not a separate primary-key index")
end)

test("three write-time indexes come with the table: timestamp, origin, and job_id where set", {
    spec = "eventd *logs.three-write-time-indexes-are-created-with-the-table"
        .. " eventd *logs.idx-logs-timestamp-indexes-the-timestamp"
        .. " eventd *logs.idx-logs-origin-indexes-the-origin"
        .. " eventd *logs.idx-logs-job-id-is-a-partial-index-on-non-null-job-ids",
}, function(t)
    local idx = {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.logs,
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'logs' AND sql IS NOT NULL")) do
        idx[r[1]] = r[2]:gsub("%s+", " ")
    end
    local names = {}
    for k in pairs(idx) do names[#names + 1] = k end
    table.sort(names)
    t:assert_eq(table.concat(names, ","), "idx_logs_job_id,idx_logs_origin,idx_logs_timestamp",
        "exactly the three")
    t:assert(idx.idx_logs_timestamp and idx.idx_logs_timestamp:find("ON logs%(timestamp%)"), "timestamp")
    t:assert(idx.idx_logs_origin and idx.idx_logs_origin:find("ON logs%(origin%)"), "origin")
    t:assert(idx.idx_logs_job_id and idx.idx_logs_job_id:find("ON logs%(job_id%) WHERE job_id IS NOT NULL"),
        "job_id, partial on non-null: " .. tostring(idx.idx_logs_job_id))
end)

-- ---------------------------------------------------------------------------
-- What a row holds
-- ---------------------------------------------------------------------------

test("a service's forwarded output is stored under its name, stderr marked as an error", {
    spec = "eventd *logs.origin-is-the-producer-peinit-associated-with-the-output-pipe"
        .. " eventd *logs.a-one-component-origin-names-a-service-and-its-main-process"
        .. " eventd *logs.a-two-component-origin-names-a-producer-within-a-service"
        .. " eventd *logs.is-error-is-1-for-standard-error-or-an-explicitly-marked-error"
        .. " eventd *logs.job-id-is-a-16-byte-correlation-guid-or-null"
        .. " eventd *logs.boot-id-is-a-16-byte-guid-in-pcds-binary-layout"
        .. " eventd *logs.message-is-the-non-null-log-text",
}, function(t)
    vm:run("svctl start pt-logs"):assert_ok()
    eventd.wait_rows(vm, "LOGS FROM pt-logs SINCE 10m ago", function(rs) return #rs >= 2 end)
    local main = rows_for("pt-logs")
    local by = {}
    for _, r in ipairs(main) do by[r[6]] = r end
    local out, err = by["pt-logs main out"], by["pt-logs main err"]
    t:assert(out and err, "both lines of the main process are under the service's name: " .. json.encode(main))
    if not (out and err) then return end
    t:assert_eq(out[5], 0, "stdout is not an error")
    t:assert_eq(err[5], 1, "stderr is")
    t:assert_eq(out[2], eventd.boot_pcds_hex(vm), "boot_id is the kernel's, in PCDS layout")
    t:assert_eq(#out[7], 32, "peinit's correlation key is a 16-byte GUID: " .. out[7])

    local hook = rows_for("pt-logs/ExecStartPre[0]")
    t:assert_eq(#hook, 1, "the hook's line is under service/ExecStartPre[0]")
    t:assert_eq(hook[1] and hook[1][6], "pt-logs hook out", "with its text")

    -- An explicitly marked error, and a line with no correlation key.
    local origin = eventd.marker("e")
    eventd.send_log(vm, { origin = origin, is_error = true, message = "" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local r = rows_for(origin)[1]
    t:assert_eq(r[5], 1, "is_error true from the producer is stored as 1")
    t:assert_eq(r[7], "", "no job_id given: the column is NULL")
    t:assert_eq(r[6], "", "an empty message is stored as empty text, not NULL")
end)

test("timestamp is epoch nanoseconds: the producer's when it gave one, else receipt time", {
    spec = "eventd *logs.timestamp-is-epoch-nanoseconds-from-the-producer-or-eventds-receipt-clock",
}, function(t)
    local origin = eventd.marker("ts")
    local before = eventd.guest_ns(vm)
    eventd.send_log(vm, { origin = origin, is_error = false, message = "given", timestamp = 1600000000123456789 })
    eventd.send_log(vm, { origin = origin, is_error = false, message = "receipt" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin, function(rs) return #rs == 2 end)
    local after = eventd.guest_ns(vm)
    local by = {}
    for _, r in ipairs(rows_for(origin)) do by[r[6]] = r[3] end
    t:assert_eq(by.given, 1600000000123456789, "the producer's timestamp, to the nanosecond")
    t:assert(by.receipt and by.receipt >= before and by.receipt <= after,
        "the receipt clock, in nanoseconds, when none was given: " .. tostring(by.receipt))
end)

-- ---------------------------------------------------------------------------
-- The origin grammar
-- ---------------------------------------------------------------------------

test("an origin outside the grammar is discarded; everything inside it is kept", {
    spec = "eventd *logs.the-origin-grammar"
        .. " eventd *logs.a-bracketed-index-appears-only-after-the-slash-and-there-is-at-most-one-slash"
        .. " eventd *logs.a-record-whose-origin-is-outside-the-grammar-is-discarded"
        .. " eventd *logs.an-origin-never-accepts-a-wildcard-backslash-quote-or-whitespace",
}, function(t)
    local m = eventd.marker("g")
    local good = {
        m, "_" .. m, m .. ".a-b_c", m .. "/Hook", m .. "/ExecStartPre[0]", m .. "/ExecStartPost[12]",
        m .. "/x.y-z",
    }
    local bad = {
        m .. "/", "/" .. m, m .. "//h", m .. "/a/b", m .. "[0]", m .. "/h[]", m .. "/h[a]",
        m .. "/h[0", m .. "/h[0]x", "." .. m, "-" .. m, m .. "*", m .. "\\h", m .. " h", m .. "\"q\"",
        m .. "'q", m .. "\th", m .. "/h*",
    }
    local records = {}
    for _, o in ipairs(bad) do records[#records + 1] = { origin = o, is_error = false, message = "bad" } end
    for _, o in ipairs(good) do records[#records + 1] = { origin = o, is_error = false, message = "good" } end
    eventd.send_log(vm, records)
    local stored
    eventd.wait_rows(vm, "LOGS FROM " .. m .. " SINCE 10m ago", function(rs)
        stored = eventd.sql(vm, eventd.DB.logs,
            "SELECT origin FROM logs WHERE instr(origin, " .. sql_quote(m) .. ") > 0")
        return #stored >= #good
    end)
    local seen = {}
    for _, r in ipairs(stored) do seen[r[1]] = true end
    for _, o in ipairs(good) do t:assert(seen[o], "kept: " .. o) end
    for _, o in ipairs(bad) do
        t:assert(not seen[o], "discarded: " .. o)
        t:assert(not in_catalogue(o), "and never catalogued: " .. o)
    end
    t:assert_eq(#stored, #good, "only the grammatical records were stored")
end)

test("the log socket admits the broker and refuses a service's own token", {
    spec = "eventd *logs.origin-is-broker-attested-because-the-log-socket-admits-peinit-and-excludes-service-logon-tokens",
}, function(t)
    vm:run("svctl start pt-broker"):assert_ok()
    local sent = wait_until(function()
        local ok, text = pcall(function() return vm:read_file("/run/pt-broker.log") end)
        return ok and text:match("step=send%-nocred [^\n]*") or nil
    end, { timeout = 20, interval = 0.5, desc = "the service's direct send" })
    t:assert(sent:find("rc=-1 errno=13", 1, true),
        "a service's own send is refused with EACCES: " .. sent)
    local agent = vm:run("/usr/bin/pt-notify --socket /run/eventd/log.sock "
        .. "--log /run/pt-agent.log send-nocred pt-agent-direct")
    agent:assert_ok()
    t:assert(vm:read_file("/run/pt-agent.log"):find("step=send%-nocred rc=%d+ errno=0"),
        "the broker's kind of token (SYSTEM without the Service group) is admitted")
end)

-- ---------------------------------------------------------------------------
-- The origin catalogue
-- ---------------------------------------------------------------------------

test("a committed row's origin is already in the catalogue", {
    spec = "eventd *logs.every-committed-log-row-is-discoverable-immediately",
}, function(t)
    for i = 1, 5 do
        local origin = eventd.marker("cat")
        eventd.send_log(vm, { origin = origin, is_error = false, message = "x" })
        local snap
        wait_until(function()
            snap = eventd.sql(vm, eventd.DB.logs, "SELECT (SELECT count(*) FROM logs WHERE origin = "
                .. sql_quote(origin) .. "), (SELECT count(*) FROM log_origins WHERE origin = "
                .. sql_quote(origin) .. ")")
            return snap[1][1] > 0
        end, { timeout = 20, interval = 0.05, desc = "the row to commit" })
        t:assert_eq(snap[1][2], 1, "the same committed state that holds the row holds its origin (" .. i .. ")")
        t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago"), 1,
            "and a query finds it at once")
    end
end)

test("queries find origins through the catalogue, not by scanning the rows", {
    spec = "eventd *logs.query-planning-enumerates-origins-from-the-catalogue-not-the-log-rows",
}, function(t)
    local origin = eventd.marker("plan")
    for i = 1, 3 do eventd.send_log(vm, { origin = origin, is_error = false, message = "p" .. i }) end
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 3 end)
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.logs, "DELETE FROM log_origins WHERE origin = " .. sql_quote(origin) .. ";")
    eventd.start(vm)
    t:assert_eq(#rows_for(origin), 3, "precondition: the rows are still in the logs table")
    t:assert_eq(#eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago"), 0,
        "with the catalogue entry gone, a query does not find them")
    local all = eventd.rows(vm, "LOGS SINCE 10m ago TAKE 100000")
    for _, r in ipairs(all) do
        t:assert(r.origin ~= origin, "nor does an unfiltered query")
    end
    eventd.send_log(vm, { origin = origin, is_error = false, message = "p4" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 4 end)
    t:assert(in_catalogue(origin), "a new line re-catalogues the origin, and all four rows are found again")
end)

test("an origin stays in the catalogue after retention deletes its last row", {
    spec = "eventd *logs.catalogue-entries-survive-ordinary-retention",
}, function(t)
    local origin = eventd.marker("old")
    local old = eventd.guest_ns(vm) - 40 * 86400 * 10 ^ 9
    eventd.send_log(vm, { origin = origin, is_error = false, message = "old", timestamp = math.tointeger(old) })
    eventd.wait_rows(vm, "LOGS FROM " .. origin, function(rs) return #rs == 1 end)
    -- Any applied configuration change also asks for a retention pass
    -- (config.rs:1031); a row forty days old is past the fourteen-day
    -- default.
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:5000"):assert_ok()
    wait_until(function() return #rows_for(origin) == 0 end,
        { timeout = 30, interval = 0.5, desc = "retention to delete the old row" })
    eventd.unset(vm, "RetentionDeleteBatchRows")
    t:assert(in_catalogue(origin), "the origin is still catalogued with no row left")
end)

-- Not implemented, and permitted rather than required: nothing in eventd
-- removes a log_origins entry (no DELETE against it anywhere in the
-- tree), so there is no removal for a test to observe. The chapter says
-- maintenance *may* do it.
test("maintenance may remove an unreferenced origin through the log writer", {
    spec = "eventd *logs.maintenance-may-remove-an-unreferenced-origin-through-the-log-writer",
    skip = true,
}, function() end)

-- Conditional on the optional removal above, which eventd never performs:
-- with no removal there is no recheck or cache update to observe.
test("origin removal rechecks NOT EXISTS and updates the cache only after commit", {
    spec = "eventd *logs.origin-removal-rechecks-not-exists-and-updates-the-cache-only-after-commit",
    skip = true,
}, function() end)

-- ---------------------------------------------------------------------------
-- Querying it
-- ---------------------------------------------------------------------------

local function error_lines(t)
    local origin = eventd.marker("ie")
    eventd.send_log(vm, {
        { origin = origin, is_error = true, message = "e1" },
        { origin = origin, is_error = false, message = "o1" },
        { origin = origin, is_error = true, message = "e2" },
    })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 3 end)
    local function msgs(q)
        local out = {}
        for _, r in ipairs(eventd.rows(vm, q)) do
            out[#out + 1] = r.message
            t:assert_eq(r.is_error, true, "is_error comes back as a boolean")
        end
        table.sort(out)
        return table.concat(out, ",")
    end
    return "LOGS FROM " .. origin .. " SINCE 10m ago", msgs
end

test("ERROR ONLY selects exactly what WHERE is_error == true does", {
    spec = "eventd *logs.error-only-is-sugar-for-is-error-equals-true",
}, function(t)
    local base, msgs = error_lines(t)
    t:assert_eq(msgs(base .. " WHERE is_error == true"), "e1,e2", "== true")
    t:assert_eq(msgs(base .. " ERROR ONLY"), "e1,e2", "ERROR ONLY")
end)

-- The book (and PSPU §3.22, "compares against true/false or against 1/0")
-- lets a query write `is_error == 1`.
test("is_error is a boolean to queries that also compares equal to 1", {
    spec = "eventd *logs.is-error-is-queried-as-a-boolean-accepting-true-or-1",
}, function(t)
    local base, msgs = error_lines(t)
    t:assert_eq(msgs(base .. " WHERE is_error == true"), "e1,e2", "== true")
    t:assert_eq(msgs(base .. " WHERE is_error == 1"), "e1,e2", "== 1 selects the same lines")
end)

test("log queries leave the adaptive-index counters and the log store's indexes alone", {
    spec = "eventd *logs.the-log-store-does-not-participate-in-adaptive-indexing"
        .. " eventd *logs.log-queries-do-not-increment-query-frequency-counters",
}, function(t)
    eventd.set(vm, "AdaptiveIndexCreateThreshold", "dword:11"):assert_ok()
    vm:clock():sleep("2s")
    local origin = eventd.marker("ai")
    eventd.send_log(vm, { origin = origin, is_error = true, message = "x" })
    for _ = 1, 15 do
        eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago WHERE is_error == true")
        eventd.rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago CONTAINING \"x\"")
    end
    -- A further applied change makes the policy recompute and write now.
    eventd.set(vm, "RetentionDeleteBatchRows", "dword:4000"):assert_ok()
    vm:clock():sleep("2s")
    local counters = eventd.sql(vm, eventd.DB.meta,
        "SELECT field_path, query_count FROM index_counters WHERE field_path IN ('is_error', 'message', 'origin')")
    eventd.unset(vm, "RetentionDeleteBatchRows")
    eventd.unset(vm, "AdaptiveIndexCreateThreshold")
    t:assert_eq(#counters, 0, "thirty log queries counted nothing: " .. json.encode(counters))
    local idx = eventd.sql(vm, eventd.DB.logs,
        "SELECT count(*) FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")
    t:assert_eq(idx[1][1], 3, "and the log store still has only its three write-time indexes")
end)
