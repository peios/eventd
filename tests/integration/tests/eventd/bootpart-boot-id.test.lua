-- eventd TRM §3.7 — boot partitioning: the boot_id every stored record
-- carries, where it lives, what makes an event unique, and how a start
-- tells a boot's first eventd from a restart within it.
--
-- One file-scope VM, and no reboot. The live root is a tmpfs, so a
-- reboot takes the stores with it; but nothing in this chapter needs one.
-- What startup decides between "first start" and "restart" is a question
-- about what the stores hold for the current kernel boot ID, so the tests
-- arrange the stores rather than the boot: eventd is stopped through the
-- service manager, a store is edited offline (a host-side sqlite copy
-- written back with its -wal and -shm removed) or deleted, and eventd is
-- started again under the same boot ID. Deleting every shard is exactly
-- the state a new boot presents to startup.
--
-- The tests that destroy evidence run in order of increasing damage, and
-- the historical-shard case, which changes StorageShards, runs last.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-bootpart" })

-- ---------------------------------------------------------------------------
-- Local helpers
-- ---------------------------------------------------------------------------

local function kernel_boot_id()
    return eventd.boot_id(vm)
end

local function startups()
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 1d ago")
    table.sort(rows, function(a, b) return a["event.time"] < b["event.time"] end)
    return rows
end

--- Where a startup record says `cpu` resumed: the entry of
--- `store.resume.sequences` parallel to `cpu` in `store.resume.cpus`.
local function resume_of(startup, cpu)
    local cpus = startup and startup["store.resume.cpus"]
    local seqs = startup and startup["store.resume.sequences"]
    if type(cpus) ~= "table" or type(seqs) ~= "table" then return nil end
    for i, c in ipairs(cpus) do
        if c == cpu then return seqs[i] end
    end
end

--- The highest sequence contiguously covered by this boot's committed
--- receipt ranges for CPU 0, across every shard file in the store.
local function covered_through(boot)
    local ranges = {}
    for _, shard in ipairs(eventd.shards(vm)) do
        for _, r in ipairs(eventd.sql(vm, shard,
            "SELECT first_sequence, last_sequence FROM receipt_ranges WHERE cpu_id = 0 AND hex(boot_id) = '"
            .. boot .. "'")) do
            ranges[#ranges + 1] = r
        end
    end
    table.sort(ranges, function(a, b) return a[1] < b[1] end)
    local high = 0
    for _, r in ipairs(ranges) do
        if r[1] <= high + 1 and r[2] > high then high = r[2] end
    end
    return high
end

-- ---------------------------------------------------------------------------
-- What carries it
-- ---------------------------------------------------------------------------

test("every stored event, log line and metric sample carries the kernel's boot ID in PCDS layout", {
    spec = "eventd *bootpart.every-stored-event-log-line-and-raw-metric-sample-carries-a-boot-id"
        .. " eventd *bootpart.the-boot-id-is-read-from-proc-at-startup-validated-and-stored-as-a-pcds-guid"
        .. " eventd *bootpart.every-events-row-carries-a-boot-id"
        .. " eventd *bootpart.every-logs-row-carries-a-boot-id"
        .. " eventd *bootpart.every-samples-row-carries-a-boot-id",
}, function(t)
    local tag = eventd.marker("b")
    eventd.emit(vm, "pt.bootpart", { tag = tag })
    eventd.send_log(vm, { origin = tag, is_error = false, message = "boot" })
    eventd.send_metric(vm, { name = tag, type = "gauge", value = 1 })
    eventd.wait_rows(vm, 'EVENTS pt.bootpart WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 1 end)
    eventd.wait_rows(vm, "LOGS FROM " .. tag .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.wait_rows(vm, "METRIC " .. tag .. " SINCE 10m ago", function(rs) return #rs == 1 end)

    local want = eventd.boot_pcds_hex(vm)
    local stores = {
        { "events", eventd.shards(vm)[1], "events" },
        { "logs", eventd.DB.logs, "logs" },
        { "samples", eventd.DB.metrics, "samples" },
    }
    for _, s in ipairs(stores) do
        local bad = eventd.sql(vm, s[2], "SELECT count(*) FROM " .. s[3]
            .. " WHERE boot_id IS NULL OR typeof(boot_id) <> 'blob' OR length(boot_id) <> 16")
        t:assert_eq(bad[1][1], 0, s[1] .. ": every row has a 16-byte boot_id")
        local mine = eventd.sql(vm, s[2], "SELECT count(*) FROM " .. s[3]
            .. " WHERE hex(boot_id) = '" .. want .. "'")
        t:assert(mine[1][1] >= 1, s[1] .. ": rows written this boot carry the kernel's ID in PCDS layout "
            .. want)
    end
    -- Read back through the query surface, the stored GUID formats as the
    -- very text /proc gave: the conversion went the PCDS way and back.
    local rows = eventd.rows(vm, "LOGS FROM " .. tag .. " SINCE 10m ago")
    t:assert_eq(rows[1].boot_id:lower(), "{" .. kernel_boot_id():lower() .. "}",
        "the boot ID round-trips to /proc's text")
end)

test("the metric boot ID is a sample column, not part of a series' identity", {
    spec = "eventd *bootpart.the-boot-id-is-recorded-per-sample-but-is-not-part-of-series-identity",
}, function(t)
    local series_cols, sample_cols = {}, {}
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics, "SELECT name FROM pragma_table_info('series')")) do
        series_cols[r[1]] = true
    end
    for _, r in ipairs(eventd.sql(vm, eventd.DB.metrics, "SELECT name FROM pragma_table_info('samples')")) do
        sample_cols[r[1]] = true
    end
    t:assert(sample_cols.boot_id, "samples has a boot_id column")
    t:assert(not series_cols.boot_id, "series has none, so a series cannot be keyed on it")
    local idx = eventd.sql(vm, eventd.DB.metrics,
        "SELECT il.name FROM pragma_index_list('series') il WHERE il.\"unique\" = 1")
    for _, r in ipairs(idx) do
        for _, c in ipairs(eventd.sql(vm, eventd.DB.metrics,
            "SELECT name FROM pragma_index_info('" .. r[1] .. "')")) do
            t:assert(c[1] ~= "boot_id", "no unique key on series includes a boot ID")
        end
    end
    -- A sample stamped with another boot lands in the same series as this
    -- boot's: the series is continuous across the boundary.
    local name = eventd.marker("m")
    eventd.send_metric(vm, { name = name, type = "gauge", value = 1 })
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.stop(vm)
    eventd.edit_store(vm, eventd.DB.metrics, string.format([[
INSERT INTO samples (series_id, boot_id, timestamp, value)
SELECT id, X'00112233445566778899AABBCCDDEEFF', (SELECT max(timestamp) FROM samples) - 1000000, 2
FROM series WHERE name = '%s';
]], name))
    eventd.start(vm)
    local series = eventd.sql(vm, eventd.DB.metrics,
        "SELECT count(*) FROM series WHERE name = '" .. name .. "'")
    t:assert_eq(series[1][1], 1, "one series")
    local boots = eventd.sql(vm, eventd.DB.metrics,
        "SELECT count(DISTINCT s.boot_id) FROM samples s JOIN series r ON r.id = s.series_id WHERE r.name = '"
        .. name .. "'")
    t:assert_eq(boots[1][1], 2, "holding samples from two boots")
    local rows = eventd.rows(vm, "METRIC " .. name .. " SINCE 1h ago")
    t:assert_eq(#rows, 2, "and a query over the series returns both without a boot filter")
end)

test("(cpu_id, sequence) identifies an event within a boot, and with boot_id across boots", {
    spec = "eventd *bootpart.cpu-id-and-sequence-identify-an-event-within-one-boot"
        .. " eventd *bootpart.the-boot-id-cpu-id-and-sequence-triple-is-globally-unique",
}, function(t)
    for i = 1, 20 do eventd.emit(vm, "pt.bootpart.uniq", { i = i }) end
    eventd.wait_rows(vm, "EVENTS pt.bootpart.uniq SINCE 10m ago", function(rs) return #rs == 20 end)
    for _, shard in ipairs(eventd.shards(vm)) do
        local dup = eventd.sql(vm, shard, [[
SELECT count(*) FROM (SELECT boot_id, cpu_id, sequence FROM events
WHERE sequence IS NOT NULL GROUP BY boot_id, cpu_id, sequence HAVING count(*) > 1)]])
        t:assert_eq(dup[1][1], 0, shard .. ": no (boot_id, cpu_id, sequence) appears twice")
        local within = eventd.sql(vm, shard, "SELECT count(*) FROM (SELECT cpu_id, sequence FROM events "
            .. "WHERE sequence IS NOT NULL AND hex(boot_id) = '" .. eventd.boot_pcds_hex(vm)
            .. "' GROUP BY cpu_id, sequence HAVING count(*) > 1)")
        t:assert_eq(within[1][1], 0, shard .. ": within this boot, (cpu_id, sequence) alone is unique")
    end
end)

-- ---------------------------------------------------------------------------
-- Detecting the boundary
-- ---------------------------------------------------------------------------

test("the boot's first eventd start covers from before sequence 1 and says restart false", {
    spec = "eventd *bootpart.no-committed-rows-or-receipts-for-the-boot-means-a-first-start"
        .. " eventd *bootpart.a-first-start-covers-from-before-sequence-1-writes-the-new-boot-id-and-emits-restart-false",
}, function(t)
    local s = startups()
    local first = s[1]
    t:assert(first, "a startup record exists")
    t:assert_eq(first["store.restarted"], false, "the boot's first start carries store.restarted false")
    t:assert_eq(first["event.boot.guid"]:lower(), "{" .. kernel_boot_id():lower() .. "}", "under the new boot ID")
    local boot = eventd.boot_pcds_hex(vm)
    local low = eventd.sql(vm, eventd.shards(vm)[1],
        "SELECT min(first_sequence) FROM receipt_ranges WHERE cpu_id = 0 AND hex(boot_id) = '" .. boot .. "'")
    t:assert_eq(low[1][1], 1, "coverage for the CPU begins at sequence 1, so nothing before it was missed")
end)

test("a restart in the same boot keeps the boot ID, merges coverage and says restart true", {
    spec = "eventd *bootpart.committed-rows-or-receipts-for-the-boot-mean-a-restart"
        .. " eventd *bootpart.a-restart-merges-and-reconciles-coverage-keeps-the-boot-id-and-emits-restart-true",
}, function(t)
    local boot = eventd.boot_pcds_hex(vm)
    local before = covered_through(boot)
    eventd.restart(vm)
    local s = startups()
    local last = s[#s]
    t:assert_eq(last["store.restarted"], true, "store.restarted true")
    t:assert_eq(last["event.boot.guid"]:lower(), "{" .. kernel_boot_id():lower() .. "}", "the same boot ID")
    local resume = resume_of(last, 0)
    t:assert(resume and resume >= before,
        "it resumed from the merged coverage (" .. tostring(resume) .. " >= " .. before .. ")")
    t:assert_eq(covered_through(boot) >= resume, true, "and the coverage is still contiguous from 1")
end)

test("neither the sequence checkpoints nor the last shutdown payload steer recovery", {
    spec = "eventd *bootpart.sequence-checkpoints-and-the-previous-shutdown-payload-are-not-consulted-for-recovery",
}, function(t)
    local boot = eventd.boot_pcds_hex(vm)
    eventd.stop(vm)
    local covered = covered_through(boot)
    eventd.edit_store(vm, eventd.DB.meta,
        "UPDATE sequence_checkpoints SET sequence = 999999999 WHERE hex(boot_id) = '" .. boot .. "';")
    local lie = eventd.msgpack({ store = { committed = {
        cpus = eventd.array({ 0 }), sequences = eventd.array({ 999999999 }) } } })
    local hex = eventd.hex(lie, true)
    for _, shard in ipairs(eventd.shards(vm)) do
        eventd.edit_store(vm, shard, "UPDATE events SET payload = X'" .. hex .. "' WHERE event_type = '"
            .. eventd.T.shutdown .. "';")
    end
    eventd.start(vm)
    local last = startups()
    last = last[#last]
    local resume = resume_of(last, 0)
    t:assert(resume and resume >= covered and resume < 999999999,
        "the resume point follows the receipts (" .. covered .. "), not the planted 999999999: "
        .. tostring(resume))
end)

test("committed receipt ranges, not stored rows, decide where ingestion resumes", {
    spec = "eventd *bootpart.committed-receipt-ranges-are-the-sequence-authority",
}, function(t)
    local boot = eventd.boot_pcds_hex(vm)
    eventd.stop(vm)
    local covered = covered_through(boot)
    -- Take away every KMES row of this boot but leave the receipts. If the
    -- rows were the authority, startup would find nothing ingested and
    -- take the ring's survivors again.
    for _, shard in ipairs(eventd.shards(vm)) do
        eventd.edit_store(vm, shard, "DELETE FROM events WHERE sequence IS NOT NULL AND hex(boot_id) = '" .. boot .. "';")
    end
    eventd.start(vm)
    local last = startups()
    last = last[#last]
    t:assert_eq(last["store.restarted"], true, "the receipts alone made it a restart")
    t:assert(resume_of(last, 0) >= covered, "resume point from the receipts: " .. tostring(resume_of(last, 0)))
    vm:clock():sleep("2s")
    local again = 0
    for _, shard in ipairs(eventd.shards(vm)) do
        again = again + eventd.sql(vm, shard, "SELECT count(*) FROM events WHERE sequence IS NOT NULL AND sequence <= "
            .. covered .. " AND hex(boot_id) = '" .. boot .. "'")[1][1]
    end
    t:assert_eq(again, 0, "nothing the receipts cover was ingested a second time")
end)

test("committed event rows alone show that eventd ran before in this boot", {
    spec = "eventd *bootpart.committed-event-rows-alone-establish-a-previous-run",
}, function(t)
    eventd.emit(vm, "pt.bootpart.rows", { n = 1 })
    eventd.wait_rows(vm, "EVENTS pt.bootpart.rows SINCE 10m ago", function(rs) return #rs >= 1 end)
    eventd.stop(vm)
    for _, shard in ipairs(eventd.shards(vm)) do
        eventd.edit_store(vm, shard, "DELETE FROM receipt_ranges;")
    end
    eventd.start(vm)
    local last = startups()
    last = last[#last]
    t:assert_eq(last["store.restarted"], true, "with no receipt at all, the rows made it a restart")
end)

test("first start and restart are told apart by the stored data, not a persisted flag", {
    spec = "eventd *bootpart.first-start-and-restart-are-distinguished-by-stored-data-not-a-persisted-flag",
}, function(t)
    -- eventd has started several times in this boot. Take its event data
    -- away and nothing else: the metadata database, the log and metric
    -- stores and every registry value stay. If any of them carried a
    -- "started this boot" flag, the next start would still be a restart.
    eventd.stop(vm)
    for _, shard in ipairs(eventd.shards(vm)) do
        vm:run("rm -f '" .. shard .. "' '" .. shard .. "-wal' '" .. shard .. "-shm'"):assert_ok()
    end
    eventd.start(vm)
    local s = startups()
    t:assert_eq(#s, 1, "the fresh shard holds only the new startup record")
    t:assert_eq(s[1]["store.restarted"], false, "and it reports a first start")
end)

test("startup looks for the current boot in historical shards too", {
    spec = "eventd *bootpart.startup-searches-every-readable-shard-including-historical-ones-for-the-current-boot-id",
}, function(t)
    -- Two shards, then one: shard-0001 becomes historical. With shard-0000
    -- deleted as well, the only evidence of this boot is in the
    -- historical shard, and only a search that includes it finds a restart.
    eventd.set(vm, "StorageShards", "dword:2"):assert_ok()
    eventd.restart(vm)
    t:assert_eq(#eventd.shards(vm), 2, "two shards")
    -- One CPU's events are striped across its shards 1024 sequences at a
    -- time (STRIPE_LENGTH), so it takes more than a stripe to reach the
    -- second shard.
    for i = 1, 1100 do eventd.emit(vm, "pt.bootpart.hist", { i = i }) end
    eventd.wait_rows(vm, "EVENTS pt.bootpart.hist SINCE 10m ago TAKE 5000",
        function(rs) return #rs == 1100 end)
    local boot = eventd.boot_pcds_hex(vm)
    local shard1 = eventd.STORE.events .. "/shard-0001.db"
    local evidence = eventd.sql(vm, shard1,
        "SELECT (SELECT count(*) FROM events WHERE hex(boot_id) = '" .. boot .. "') + "
        .. "(SELECT count(*) FROM receipt_ranges WHERE hex(boot_id) = '" .. boot .. "')")
    t:assert(evidence[1][1] > 0, "precondition: shard-0001 holds this boot's records")
    eventd.stop(vm)
    eventd.unset(vm, "StorageShards"):assert_ok()
    local shard0 = eventd.STORE.events .. "/shard-0000.db"
    vm:run("rm -f '" .. shard0 .. "' '" .. shard0 .. "-wal' '" .. shard0 .. "-shm'"):assert_ok()
    eventd.start(vm)
    local s = startups()
    table.sort(s, function(a, b) return a["event.time"] < b["event.time"] end)
    t:assert_eq(s[#s]["store.restarted"], true,
        "the evidence in the historical shard made it a restart")
end)
