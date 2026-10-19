-- eventd TRM §8.2 — the bootstrap sequence as a healthy start walks it:
-- configuration, KMES attachment and shard sizing, the stores, the boot
-- boundary, the threads, and the startup record committed before
-- readiness.
--
-- One VM with two vCPUs, because half of this section is per-CPU
-- structure: a ring buffer, a drain thread and (by default) a shard per
-- attached CPU, and on one vCPU there is exactly one of each, which
-- proves nothing about "each". Tests run in file order and several build
-- on the one before: the shard count goes 2 (default) -> 3 -> 1 by
-- restarts, which is what creates a historical shard to read. The last
-- test takes a CPU offline.
--
-- What a test reads is the process (/proc thread names and descriptors),
-- the stores (copied to the host for sqlite), the startup records eventd
-- writes, and eventd's stderr in the log store. The failure half of §8.2
-- is bootstrap-failure.test.lua.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1, { cpus = 2 })

local vm = eventd.boot({ name = "ev-boot-seq", cpus = 2 })

-- SQL against a guest database while eventd is stopped is applied on the
-- host and written back (`eventd.edit_store`). A stopped eventd has
-- checkpointed and closed every database, so the file is the whole of it.

local function startups()
    return eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago")
end

--- A startup record's coverage points, `{cpu_id, sequence}` each, from
--- its parallel `store.resume.cpus` / `store.resume.sequences` arrays.
local function resume_points(s)
    local cpus, seqs = s["store.resume.cpus"], s["store.resume.sequences"]
    local points = {}
    if type(cpus) ~= "table" or type(seqs) ~= "table" then return points end
    for i, cpu in ipairs(cpus) do points[#points + 1] = { cpu_id = cpu, sequence = seqs[i] } end
    return points
end

local function count_prefix(list, prefix)
    local n = 0
    for _, v in ipairs(list) do
        if v:sub(1, #prefix) == prefix then n = n + 1 end
    end
    return n
end

local boot_uuid = eventd.boot_id(vm)

test("with StorageShards absent the default applies: one shard per attached KMES buffer", {
    spec = "eventd *bootstrap.absent-optional-keys-take-their-compiled-in-defaults"
        .. " eventd *bootstrap.storageshards-zero-means-one-shard-per-attached-kmes-buffer"
        .. " eventd *bootstrap.each-per-cpu-ring-buffer-is-mapped",
}, function(t)
    t:assert(not vm:run("reg get '" .. eventd.KEY .. "'").stdout:find("StorageShards", 1, true),
        "the image sets no StorageShards")
    local first = startups()
    local s = first[#first]
    t:assert_eq(s["store.shard-count"], 2, "two vCPUs, two buffers, two shards: " .. json.encode(s))
    t:assert_eq(#eventd.shards(vm), 2, "and two shard files")
    local fds = eventd.fd_listing(vm, eventd.pid(vm))
    local _, rings = fds:gsub("anon_inode:kmes%-cpu", "")
    t:assert_eq(rings, 2, "eventd holds one KMES ring descriptor per CPU: " .. fds)
    -- Mapped, not merely opened: events from both CPUs are being read.
    local cpus = {}
    for _, r in ipairs(eventd.rows(vm, "EVENTS SINCE 1h ago SELECT event.cpu TAKE 2000")) do
        local cpu = r["event.cpu"]
        if cpu ~= nil and cpu ~= json.null then cpus[cpu] = true end
    end
    t:assert(cpus[0] and cpus[1], "events from both CPUs' rings are stored: " .. json.encode(cpus))
end)

-- The EINVAL-hole branch of the walk is not reachable here: KMES gives
-- every *possible* CPU a ring, offline or not (PKM, kmes/failure-modes),
-- and QEMU offers no sparse possible-CPU mask, so every slot attaches.
-- What is observable is that every slot was walked and each attached CPU
-- got its own ordinal's worth of state — coverage, a drain, a ring.
test("every slot is walked and given a dense ordinal: each CPU has coverage, a drain and a ring", {
    spec = "eventd *bootstrap.every-kmes-slot-is-walked-skipping-einval-holes-and-given-a-dense-ordinal"
        .. " eventd *bootstrap.one-drain-thread-is-started-per-attached-logical-cpu",
}, function(t)
    local s = startups()
    s = s[#s]
    local seen = {}
    for _, p in ipairs(resume_points(s)) do seen[#seen + 1] = p.cpu_id end
    table.sort(seen)
    t:assert_eq(table.concat(seen, ","), "0,1", "the startup record names CPUs 0 and 1")
    local names = eventd.thread_names(vm, eventd.pid(vm))
    t:assert_eq(count_prefix(names, "eventd-drain-"), 2, "two drain threads: " .. table.concat(names, " "))
    local drains = {}
    for _, n in ipairs(names) do if n:find("^eventd%-drain%-") then drains[#drains + 1] = n end end
    table.sort(drains)
    t:assert_eq(table.concat(drains, ","), "eventd-drain-0,eventd-drain-1", "one per logical CPU")
end)

test("one writer per active shard, and one log, metric, retention and index-policy thread", {
    spec = "eventd *bootstrap.one-writer-thread-is-started-per-active-shard"
        .. " eventd *bootstrap.one-log-ingestion-thread-is-started"
        .. " eventd *bootstrap.one-metric-ingestion-thread-is-started"
        .. " eventd *bootstrap.one-retention-coordinator-thread-is-started"
        .. " eventd *bootstrap.one-adaptive-indexing-policy-thread-is-started",
}, function(t)
    -- Thread names are cut to 15 bytes by the kernel: eventd-writer-0000
    -- reads eventd-writer-0, eventd-retention eventd-retentio.
    local names = eventd.thread_names(vm, eventd.pid(vm))
    local all = table.concat(names, " ")
    t:assert_eq(count_prefix(names, "eventd-writer-"), 2, "a writer for each of two shards: " .. all)
    t:assert_eq(count_prefix(names, "eventd-log"), 1, "one log ingestion thread")
    t:assert_eq(count_prefix(names, "eventd-metric"), 1, "one metric ingestion thread")
    t:assert_eq(count_prefix(names, "eventd-retentio"), 1, "one retention coordinator")
    t:assert_eq(count_prefix(names, "eventd-index-po"), 1, "one adaptive index policy thread")
end)

test("the boot ID is read from the kernel and stored in PCDS GUID layout", {
    spec = "eventd *bootstrap.the-boot-id-is-read-and-converted-to-pcds-guid-layout",
}, function(t)
    local s = startups()
    s = s[#s]
    t:assert_eq(s["event.boot.guid"], "{" .. boot_uuid .. "}", "the startup record's boot is the kernel's")
    local stored = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT DISTINCT hex(boot_id) FROM events WHERE event_type = '"
        .. eventd.T.startup .. "'")
    t:assert_eq(#stored, 1, "one boot in the store")
    t:assert_eq(stored[1][1]:lower(), eventd.boot_pcds_hex(boot_uuid):lower(),
        "stored as PCDS GUID bytes (first three groups little-endian)")
end)

test("the startup record carries whether it is a restart, the shard count and per-CPU coverage", {
    spec = "eventd *bootstrap.an-eventd-daemon-started-event-records-restart-shard-count-and-per-cpu-coverage",
}, function(t)
    local s = startups()
    s = s[#s]
    t:assert_eq(s["event.boot.guid"], "{" .. boot_uuid .. "}", "stored under the boot it describes")
    t:assert_eq(s.boot_id, nil, "which the payload no longer repeats")
    t:assert_eq(type(s["store.restarted"]), "boolean", "whether this start is a restart")
    t:assert_eq(s["store.shard-count"], 2, "shard count")
    local points = resume_points(s)
    t:assert_eq(#points, 2, "a coverage point for each CPU: " .. json.encode(s))
    for _, p in ipairs(points) do
        t:assert(math.type(p.sequence) == "integer" and p.sequence >= 0,
            "CPU " .. tostring(p.cpu_id) .. " has a sequence coverage point")
    end
end)

test("the startup record is committed, and the sockets and threads up, by the time peinit sees ready", {
    spec = "eventd *bootstrap.the-startup-event-is-committed-before-readiness-is-signalled"
        .. " eventd *bootstrap.readiness-is-signalled-to-peinit-last",
}, function(t)
    local before = #startups()
    -- svctl restart waits for the operation: it returns once peinit has
    -- the new process's READY=1 (Readiness=Notify). One read, then — no
    -- polling: the claim is about that moment.
    local r = vm:run("svctl restart eventd")
    t:assert(r.stdout:find("eventd: active", 1, true), "peinit reports eventd ready: " .. r.stdout)
    local after = startups()
    t:assert_eq(#after, before + 1, "the new start's record was already committed")
    local listing = vm:run("ls /run/eventd").stdout
    for _, s in ipairs({ "query.sock", "log.sock", "metric.sock" }) do
        t:assert(listing:find(s, 1, true), s .. " existed at readiness")
    end
    local names = table.concat((eventd.thread_names(vm, eventd.pid(vm))), " ")
    for _, n in ipairs({ "eventd-writer-", "eventd-drain-", "eventd-log", "eventd-metric",
                         "eventd-retentio", "eventd-index-po" }) do
        t:assert(names:find(n, 1, true), n .. " was running at readiness: " .. names)
    end
end)

test("the boot's first start is told from a restart by what the store holds for the boot", {
    spec = "eventd *bootstrap.a-boot-id-with-no-committed-row-or-receipt-marks-the-boots-first-start",
}, function(t)
    local all = startups()
    t:assert(#all >= 2, "at least the boot's start and one restart: " .. #all)
    table.sort(all, function(a, b) return a["event.time"] < b["event.time"] end)
    t:assert_eq(all[1]["store.restarted"], false, "the first start of this boot is not a restart")
    for i = 2, #all do
        t:assert_eq(all[i]["store.restarted"], true, "every later start in the boot is a restart")
    end
end)

test("each drain starts from the merged receipts: covered sequences skipped, uncovered survivors read once", {
    spec = "eventd *bootstrap.each-drains-recovery-coverage-is-initialised-from-the-merged-receipts",
}, function(t)
    -- Frozen, eventd reads none of these; killed, it commits none. At the
    -- next start they are ring survivors no receipt covers, while every
    -- earlier event in the ring is covered by one. peinit restarts the
    -- killed eventd itself (the image's RestartPolicy).
    local since = eventd.guest_ns(vm)
    local pid = eventd.pid(vm)
    eventd.freeze(vm, pid)
    local tag = eventd.marker("cov")
    for i = 1, 12 do eventd.emit(vm, "pt.cov", { tag = tag, i = i }) end
    eventd.signal(vm, pid, "KILL")
    wait_until(function()
        local now = eventd.pid(vm)
        return now ~= nil and now ~= pid
    end, { timeout = 60, interval = 0.25, desc = "peinit to restart eventd" })
    eventd.ready(vm)
    local rows = eventd.wait_rows(vm, 'EVENTS pt.cov WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r >= 12 end)
    t:assert_eq(#rows, 12, "every uncovered survivor was ingested, once")
    -- Nothing already covered was ingested again: no (CPU, sequence) is
    -- stored twice for this boot, across all shards.
    local seen, dups = {}, 0
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard,
            "SELECT cpu_id, sequence FROM events WHERE sequence IS NOT NULL AND hex(boot_id) = '"
            .. eventd.boot_pcds_hex(boot_uuid) .. "'")) do
            local k = r[1] .. ":" .. r[2]
            if seen[k] then dups = dups + 1 end
            seen[k] = true
        end
    end
    t:assert_eq(dups, 0, "no covered sequence was stored a second time")
    local gaps = 0
    for _, g in ipairs(eventd.rows(vm, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")) do
        if g["event.time"] >= since then gaps = gaps + 1 end
    end
    t:assert_eq(gaps, 0, "and nothing was recorded as a gap: every sequence was in one source or the other")
end)

test("the configuration watch is persistent: successive changes are each applied", {
    spec = "eventd *bootstrap.a-persistent-watch-is-armed-on-the-configuration-subtree",
}, function(t)
    local function changes_to(value)
        return eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago",
            function(rows)
                for _, r in ipairs(rows) do
                    if r["config.name"] == "LogRetentionDays" and r["config.value"] == value then
                        return true
                    end
                end
                return false
            end, { desc = "a config change to LogRetentionDays=" .. value })
    end
    for _, v in ipairs({ 21, 22, 23 }) do
        eventd.set(vm, "LogRetentionDays", "dword:" .. v):assert_ok()
        local _, ok = changes_to(v)
        t:assert(ok, "change to " .. v .. " applied")
    end
    eventd.unset(vm, "LogRetentionDays")
end)

test("a configured StorageShards is read at startup and the CPU-to-shard assignment follows it", {
    spec = "eventd *bootstrap.every-key-under-machine-system-eventd-is-read-first"
        .. " eventd *bootstrap.the-shard-to-cpu-assignment-is-computed-after-the-shard-count"
        .. " eventd *bootstrap.each-active-event-shard-is-opened-or-created-in-wal-mode-with-synchronous-full",
}, function(t)
    eventd.set(vm, "StorageShards", "dword:3"):assert_ok()
    eventd.restart(vm)
    local s = startups()
    table.sort(s, function(a, b) return a["event.time"] < b["event.time"] end)
    t:assert_eq(s[#s]["store.shard-count"], 3, "the restart read StorageShards=3")
    local shards = eventd.shards(vm)
    t:assert_eq(#shards, 3, "and created a third shard: " .. json.encode(shards))
    -- The new shard was created as an event shard in WAL mode. (The
    -- synchronous level is a property of eventd's connection, not of the
    -- file, and cannot be read from outside the process.)
    local created = eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0002.db")
    t:assert_eq(eventd.sql(vm, created, "PRAGMA journal_mode")[1][1], "wal", "shard-0002 is WAL")
    t:assert(eventd.schema(vm, created).events, "with the events table")
    t:assert_eq(eventd.sql(vm, created,
        "SELECT value FROM metadata WHERE key = 'schema_version'")[1][1], "1", "and a schema version")
    -- Two CPUs over three shards: ordinal 0 owns shards 0 and 2, ordinal 1
    -- owns shard 1 (routing.rs assigned_shards). Emit enough to be sure
    -- both CPUs have written since the restart.
    for i = 1, 200 do eventd.emit(vm, "pt.assign", { i = i }) end
    eventd.wait_rows(vm, "EVENTS pt.assign SINCE 10m ago", function(rs) return #rs >= 200 end)
    local start_ts = s[#s]["event.time"]
    for idx, want in pairs({ [0] = 0, [1] = 1, [2] = 0 }) do
        local path = eventd.DB.meta:gsub("eventd%-meta%.db$", string.format("shard-%04d.db", idx))
        local rows = eventd.sql(vm, path, "SELECT DISTINCT cpu_id FROM events WHERE cpu_id IS NOT NULL"
            .. " AND timestamp > " .. start_ts)
        for _, r in ipairs(rows) do
            t:assert_eq(r[1], want, string.format("shard %d holds only CPU %d's events", idx, want))
        end
    end
end)

test("historical shards are read for their receipts and their rows; an unreadable one is excluded", {
    spec = "eventd *bootstrap.historical-shards-with-a-recognised-schema-open-read-only-and-the-rest-are-excluded"
        .. " eventd *bootstrap.receipt-ranges-for-the-boot-are-merged-from-every-readable-shard-by-cpu"
        .. " eventd *storagefail.a-historical-shard-with-a-bad-schema-version-is-excluded-from-queries",
}, function(t)
    local shard1 = eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0001.db")
    local shard0 = eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0000.db")
    -- An event CPU 1 wrote into shard 1 before it becomes historical.
    vm:run("svctl stop eventd"):assert_ok()
    local hi = eventd.sql(vm, shard1,
        "SELECT event_type, sequence FROM events WHERE cpu_id = 1 ORDER BY sequence DESC LIMIT 1")[1]
    t:assert(hi, "shard 1 holds CPU 1's events")
    local etype, seq = hi[1], hi[2]
    -- A copy of shard 1 whose schema version eventd does not know, and a
    -- file in the shard namespace that is not a database at all.
    local bad = eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0008.db")
    vm:write_file(bad, vm:read_file(shard1))
    eventd.edit_store(vm, bad, "UPDATE metadata SET value = '99' WHERE key = 'schema_version';")
    vm:write_file(eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0009.db"), "not a database\n")
    eventd.set(vm, "StorageShards", "dword:1"):assert_ok()
    eventd.start(vm)

    local s = startups()
    table.sort(s, function(a, b) return a["event.time"] < b["event.time"] end)
    local cpu1
    for _, p in ipairs(resume_points(s[#s])) do if p.cpu_id == 1 then cpu1 = p.sequence end end
    t:assert(cpu1 and cpu1 >= seq,
        "CPU 1's coverage comes from the now-historical shard's receipts: " .. tostring(cpu1)
        .. " >= " .. seq)
    local dup = eventd.sql(vm, shard0, "SELECT count(*) FROM events WHERE cpu_id = 1 AND sequence = " .. seq)
    t:assert_eq(dup[1][1], 0, "so that event was not ingested again into the active shard")

    local rows = eventd.rows(vm, "EVENTS " .. etype .. " WHERE event.cpu == 1 AND event.sequence == " .. seq
        .. " SINCE 1h ago")
    t:assert_eq(#rows, 1, "the historical row is queryable, once: the bad-schema copy is not read")
    local before = eventd.sql(vm, shard1, "SELECT count(*) FROM events")[1][1]
    for i = 1, 100 do eventd.emit(vm, "pt.hist", { i = i }) end
    eventd.wait_rows(vm, "EVENTS pt.hist SINCE 10m ago", function(r) return #r >= 100 end)
    t:assert_eq(eventd.sql(vm, shard1, "SELECT count(*) FROM events")[1][1], before,
        "nothing new is written to a historical shard")
    local excluded = eventd.wait_rows(vm, 'LOGS FROM eventd CONTAINING "excluding historical shard" SINCE 10m ago',
        function(r) return #r >= 2 end)
    t:assert(#excluded >= 2, "both unusable files were reported excluded: " .. #excluded)
    vm:run("rm -f " .. bad .. " " .. eventd.DB.meta:gsub("eventd%-meta%.db$", "shard-0009.db"))
    eventd.unset(vm, "StorageShards")
end)

test("the three databases are created when absent, in WAL mode", {
    spec = "eventd *bootstrap.logs-db-is-opened-or-created-in-wal-mode-with-synchronous-normal"
        .. " eventd *bootstrap.metrics-db-is-opened-or-created-in-wal-mode-with-synchronous-normal"
        .. " eventd *bootstrap.eventd-meta-db-is-opened-or-created",
}, function(t)
    vm:run("svctl stop eventd"):assert_ok()
    vm:run("rm -f " .. eventd.DB.logs .. "* " .. eventd.DB.metrics .. "* " .. eventd.DB.meta .. "*"):assert_ok()
    eventd.start(vm)
    for _, db in ipairs({ eventd.DB.logs, eventd.DB.metrics, eventd.DB.meta }) do
        t:assert_eq(eventd.sql(vm, db, "PRAGMA journal_mode")[1][1], "wal", db .. " recreated in WAL mode")
    end
    t:assert(eventd.schema(vm, eventd.DB.logs).logs, "logs.db has its logs table")
    t:assert(eventd.schema(vm, eventd.DB.metrics).series, "metrics.db has its series table")
    t:assert(eventd.schema(vm, eventd.DB.meta).sequence_checkpoints, "eventd-meta.db has its tables")
end)

test("the sequence checkpoints are not used for recovery", {
    spec = "eventd *bootstrap.sequence-checkpoints-are-loaded-for-diagnostics-only",
}, function(t)
    vm:run("svctl stop eventd"):assert_ok()
    local cps = eventd.sql(vm, eventd.DB.meta, "SELECT cpu_id, sequence FROM sequence_checkpoints")
    t:assert(#cps >= 1, "the stop wrote checkpoints")
    -- Lie: claim everything up to a sequence far in the future is covered.
    eventd.edit_store(vm, eventd.DB.meta, "UPDATE sequence_checkpoints SET sequence = 900000000;")
    eventd.start(vm)
    local s = startups()
    table.sort(s, function(a, b) return a["event.time"] < b["event.time"] end)
    local points = resume_points(s[#s])
    t:assert(#points >= 1, "the start recorded its coverage: " .. json.encode(s[#s]))
    for _, p in ipairs(points) do
        t:assert(p.sequence < 900000000, "CPU " .. p.cpu_id .. "'s coverage is from receipts, not the checkpoint")
    end
    local tag = eventd.marker("cp")
    eventd.emit(vm, "pt.cp", { tag = tag })
    local _, ok = eventd.wait_rows(vm, 'EVENTS pt.cp WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(r) return #r == 1 end)
    t:assert(ok, "and an event emitted afterwards is ingested, not treated as already covered")
end)

test("each shard's material indexes are discovered from its schema", {
    spec = "eventd *bootstrap.each-shards-material-indexes-are-discovered-from-its-schema",
}, function(t)
    -- An index eventd's policy does not want, planted in a shard while
    -- eventd is down. Nothing told eventd about it but the shard's own
    -- schema; when the policy converges it is found and dropped.
    vm:run("svctl stop eventd"):assert_ok()
    local shard0 = eventd.shards(vm)[1]
    eventd.edit_store(vm, shard0, "CREATE INDEX idx_events_process_guid ON events(process_guid);")
    t:assert(eventd.schema(vm, shard0).idx_events_process_guid, "the index is planted while eventd is down")
    eventd.start(vm)
    -- Any applied configuration change asks the policy to recompute (it
    -- may already have converged at start, which proves the same thing).
    eventd.set(vm, "LogRetentionDays", "dword:29"):assert_ok()
    local gone = pcall(wait_until, function()
        return eventd.schema(vm, shard0).idx_events_process_guid == nil
    end, { timeout = 60, interval = 1, desc = "the undesired material index to be dropped" })
    eventd.unset(vm, "LogRetentionDays")
    t:assert(gone, "eventd found the index in the schema and converged it away")
end)

test("the series cache starts empty", {
    spec = "eventd *bootstrap.the-series-cache-starts-empty",
}, function(t)
    local name = eventd.marker("sc")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(r) return #r == 1 end)
    -- A restart, and at once the diagnostic dump: what the new process's
    -- cache holds before anything has asked it for a series.
    eventd.restart(vm)
    local d = eventd.quit_dump(vm)
    local line
    for _, m in ipairs(d.messages) do if m:find("metric_series_cache:", 1, true) then line = m end end
    t:assert_eq(line and line:match("metric_series_cache:%s*(%d+)"), "0",
        "the restarted daemon's cache held nothing: " .. tostring(line))
end)

--- `sd show` of a socket path: its DACL lines, in order.
local function dacl(path)
    local out = vm:run("sd show " .. path).stdout
    local aces = {}
    for line in out:gmatch("[^\n]+") do
        -- "[0] deny  Service (S-1-5-6)  0x2"; the mask may also print as
        -- a letter code ("w").
        local kind, who, mask = line:match("^%s+%[%d+%]%s+(%a+)%s+(.-%))%s+(%S+)%s*$")
        if kind then aces[#aces + 1] = { kind = kind, who = who, mask = mask } end
    end
    return aces, out
end

local function has_ace(aces, kind, sid)
    for _, a in ipairs(aces) do
        if a.kind == kind and a.who:find("(" .. sid .. ")", 1, true) then return a end
    end
end

test("the log socket carries the broker descriptor: Service denied, SYSTEM allowed", {
    spec = "eventd *bootstrap.the-log-socket-gets-the-deny-service-allow-system-broker-descriptor",
}, function(t)
    local aces, raw = dacl(eventd.SOCKET.log)
    t:assert(raw:find("DACL_PROTECTED", 1, true), "a protected DACL, not an inherited one: " .. raw)
    t:assert(aces[1] and aces[1].kind == "deny" and aces[1].who:find("(S-1-5-6)", 1, true),
        "the first ACE denies the Service logon group: " .. raw)
    t:assert(has_ace(aces, "allow", "S-1-5-18"), "and SYSTEM is allowed")
    t:assert(not has_ace(aces, "allow", "S-1-5-11"), "and authenticated users in general are not")
end)

test("the query socket lets every authenticated caller connect", {
    spec = "eventd *bootstrap.the-query-socket-gets-the-authenticated-users-descriptor",
}, function(t)
    local aces, raw = dacl(eventd.SOCKET.query)
    t:assert(raw:find("DACL_PROTECTED", 1, true), "a protected DACL: " .. raw)
    t:assert(has_ace(aces, "allow", "S-1-5-11"), "allowing Authenticated Users: " .. raw)
end)

test("the metric socket lets every authenticated caller send", {
    spec = "eventd *bootstrap.the-metric-socket-gets-the-authenticated-users-descriptor",
}, function(t)
    local aces, raw = dacl(eventd.SOCKET.metric)
    t:assert(raw:find("DACL_PROTECTED", 1, true), "a protected DACL: " .. raw)
    t:assert(has_ace(aces, "allow", "S-1-5-11"), "allowing Authenticated Users: " .. raw)
end)

-- Route closed: each socket's descriptor is set and read back inside its
-- bind (datagram.rs IngestionSocket::bind -> establish_protection,
-- query/mod.rs QueryServer::bind), before the socket is handed to any
-- thread; there is no moment a client could probe between bind and
-- verification that would not also be before the socket exists for it.
test("socket descriptors are verified before any socket accepts or receives", {
    spec = "eventd *bootstrap.socket-descriptors-are-verified-before-any-socket-accepts-or-receives",
    skip = true,
    covered_by = "cargo:eventd eventd datagram::tests::a_socket_whose_descriptor_reads_back_differently_is_refused_before_it_can_receive",
}, function() end)

-- Route closed: KMES creates a ring for every possible CPU at
-- initialisation, offline or not (kmes/failure-modes.test.lua), so no
-- running kernel offers eventd a slot table with no buffer in it.
test("discovering no KMES buffers fails startup", {
    spec = "eventd *bootstrap.discovering-no-kmes-buffers-fails-startup",
    skip = true,
    covered_by = "cargo:eventd eventd kmes::tests::discovering_no_attachable_buffer_fails_startup",
}, function() end)
