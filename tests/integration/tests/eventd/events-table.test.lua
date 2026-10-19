-- eventd TRM §3.1 "The Event Shard Schema" (3--event-storage/1--the-events-table.md):
-- the events table and its columns, the event-type catalogue, the receipt
-- ranges, the shard metadata table and the write-time index.
--
-- Two VMs, both one vCPU, so there is one KMES buffer and one active shard
-- (shard-0000.db) and every real event the agent emits lands in it:
--
--   * `vm` is never stopped. Its cases emit a tagged KMES event, find it
--     through evctl (which gives its sequence), then read the very row out
--     of the shard with `eventd.sql`, so each column is asserted as stored,
--     not as rendered.
--   * `craft` is the one eventd here that is stopped and started. Between a
--     stop and a start the shard files are copied to the host, edited with
--     the host's sqlite3, and written back — the route to statements the
--     running daemon never lets a caller see:
--       - a BEFORE INSERT trigger on `event_types` records every catalogue
--         statement the writer executes (an INSERT OR IGNORE fires a BEFORE
--         trigger even when it inserts nothing), so "no catalogue statement"
--         becomes a row count;
--       - a crafted historical shard (shard-0005.db) carries rows the
--         planner can see only through its catalogue;
--       - an event dated forty days back, with its receipt and catalogue
--         row, is something age retention will delete;
--       - receipts for the current boot written into another shard show
--         what the startup merge does with them.
--     The cases run in file order and each leaves the daemon running; the
--     last one (receipt merge) leaves ingestion deliberately skewed, so
--     nothing follows it on `craft`.
--
-- Receipt compaction is not implemented (no compaction code anywhere in
-- eventd); its two anchors are documented skips below. A real event whose
-- identity was unavailable cannot be produced from a guest (the kernel
-- stamps the null GUID only with no task context), so that anchor is a
-- unit-test TODO.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local kacs = require("helpers.kacs")
local sys = require("helpers.sys")
peinit.claim(2) -- the shared vm and the crafting vm, both for the whole file

local vm = eventd.boot({ name = "ev-events" })
local craft = eventd.boot({ name = "ev-events-craft" })

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

local SHARD0 = eventd.STORE.events .. "/shard-0000.db"

--- Emit one tagged event and return its evctl record once it is stored.
local function emit_stored(v, who, event_type, payload, tag)
    local r = eventd.emit(who, event_type, payload)
    assert(r.ret == 0, "kmes_emit " .. event_type .. ": errno " .. tostring(r.errno))
    local rows = eventd.wait_rows(v,
        "EVENTS " .. event_type .. ' WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 1 end)
    return rows[1]
end

--- The stored row of a real event, by type and sequence, as a map.
local COLUMNS = { "id", "boot_id", "timestamp", "cpu_id", "sequence", "origin_class",
                  "event_type", "effective_token_guid", "true_token_guid", "process_guid", "payload" }
local function stored_row(v, event_type, sequence)
    local rows = eventd.sql(v, SHARD0,
        "SELECT id, hex(boot_id), timestamp, cpu_id, sequence, origin_class, event_type, " ..
        "hex(effective_token_guid), hex(true_token_guid), hex(process_guid), hex(payload), " ..
        "typeof(effective_token_guid), typeof(true_token_guid), typeof(process_guid), typeof(payload) " ..
        "FROM events WHERE event_type = '" .. event_type .. "' AND sequence = " .. sequence)
    assert(#rows == 1, "one stored row for " .. event_type .. " #" .. sequence .. ": " .. json.encode(rows))
    local out = {}
    for i, c in ipairs(COLUMNS) do out[c] = rows[1][i] end
    out.types = { effective = rows[1][12], true_ = rows[1][13], process = rows[1][14], payload = rows[1][15] }
    return out
end

--- The five types eventd writes itself (TRM §2.6), as an SQL list.
local OWN = "('" .. table.concat({ eventd.T.startup, eventd.T.shutdown, eventd.T.gap,
    eventd.T.config_change, eventd.T.storage_error }, "','") .. "')"

--- The columns of a synthetic row of `event_type` (the newest), with the
--- SQL type of each nullable column.
local function synthetic_row(v, event_type)
    local rows = eventd.sql(v, SHARD0,
        "SELECT typeof(cpu_id), typeof(sequence), typeof(origin_class), typeof(effective_token_guid), " ..
        "typeof(true_token_guid), typeof(process_guid), typeof(payload), hex(payload), cpu_id, hex(boot_id), timestamp " ..
        "FROM events WHERE event_type = '" .. event_type .. "' ORDER BY id DESC LIMIT 1")
    assert(#rows == 1, "a stored " .. event_type .. " row")
    local r = rows[1]
    return { cpu_id = r[1], sequence = r[2], origin_class = r[3], effective = r[4], true_ = r[5],
             process = r[6], payload_type = r[7], payload = r[8], cpu_value = r[9], boot_id = r[10],
             timestamp = r[11] }
end

--- One column's declaration from PRAGMA table_info: {type, notnull, pk}.
local function column_decl(v, db, tbl)
    local out = {}
    for _, r in ipairs(eventd.sql(v, db, "PRAGMA table_info(" .. tbl .. ")")) do
        out[r[2]] = { type = r[3], notnull = r[4], pk = r[6] }
    end
    return out
end

--- A crafted shard: shard-0000's schema with every row removed, then
--- `script` applied. Built from a stopped shard-0000.
local EMPTY = "DELETE FROM events; DELETE FROM event_types; DELETE FROM receipt_ranges; " ..
              "DROP TRIGGER IF EXISTS pt_catlog_trigger; DROP TABLE IF EXISTS pt_catlog; "

-- ---------------------------------------------------------------------------
-- The events table, on the shared vm
-- ---------------------------------------------------------------------------

test("every shard database holds exactly one events table", {
    spec = "eventd *events.every-shard-database-holds-one-events-table",
}, function(t)
    local shards = eventd.shards(vm)
    t:assert(#shards >= 1, "the store has a shard")
    for _, s in ipairs(shards) do
        local n = eventd.sql(vm, s, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'events'")
        t:assert_eq(n[1][1], 1, s .. " has one events table")
    end
end)

test("id is the INTEGER PRIMARY KEY rowid, increasing with each stored event", {
    spec = "eventd *events.id-is-a-rowid-monotonic-within-the-shard",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.id.type, "INTEGER", "id is declared INTEGER")
    t:assert_eq(decl.id.pk, 1, "and is the primary key (so the rowid)")
    local a, b = eventd.marker("ida"), eventd.marker("idb")
    local ra = emit_stored(vm, vm, "pt.ev.id", { tag = a }, a)
    local rb = emit_stored(vm, vm, "pt.ev.id", { tag = b }, b)
    local sa, sb = stored_row(vm, "pt.ev.id", ra["event.sequence"]), stored_row(vm, "pt.ev.id", rb["event.sequence"])
    t:assert(sb.id > sa.id, "the later event has the larger id: " .. sa.id .. " then " .. sb.id)
    local rowid = eventd.sql(vm, SHARD0, "SELECT rowid FROM events WHERE id = " .. sb.id)
    t:assert_eq(rowid[1][1], sb.id, "id is the rowid itself")
end)

test("boot_id is the current boot as a 16-byte PCDS-layout GUID", {
    spec = "eventd *events.boot-id-is-a-16-byte-guid-in-pcds-binary-layout",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.boot_id.type, "BLOB", "boot_id is a BLOB")
    t:assert_eq(decl.boot_id.notnull, 1, "declared NOT NULL")
    local tag = eventd.marker("boot")
    local r = emit_stored(vm, vm, "pt.ev.boot", { tag = tag }, tag)
    local row = stored_row(vm, "pt.ev.boot", r["event.sequence"])
    local want = eventd.boot_pcds_hex(vm)
    t:assert_eq(#row.boot_id, 32, "sixteen bytes")
    t:assert_eq(row.boot_id, want, "the kernel boot ID with its first three groups byte-reversed")
    t:assert_eq(synthetic_row(vm, eventd.T.startup).boot_id, want,
        "a synthetic row carries the same boot ID")
end)

-- The third case, a gap row stamped with the revealing event's timestamp,
-- is gap-detection.test.lua's (gap.a-gap-records-time-is-the-revealing-events-timestamp).
test("timestamp is epoch nanoseconds: the emission time for a real event, eventd's clock for a synthetic one", {
    spec = "eventd *events.timestamp-is-epoch-nanoseconds-from-the-header-or-eventds-clock",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.timestamp.type, "INTEGER", "timestamp is an INTEGER")
    t:assert_eq(decl.timestamp.notnull, 1, "declared NOT NULL")
    local tag = eventd.marker("ts")
    local before = eventd.guest_ns(vm)
    local r = eventd.emit(vm, "pt.ev.ts", { tag = tag })
    local after = eventd.guest_ns(vm)
    t:assert_eq(r.ret, 0, "emitted")
    local rows = eventd.wait_rows(vm, 'EVENTS pt.ev.ts WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 1 end)
    local row = stored_row(vm, "pt.ev.ts", rows[1]["event.sequence"])
    t:assert(row.timestamp >= before and row.timestamp <= after,
        "stamped at emission, inside the emit call: " .. before .. " <= " .. row.timestamp .. " <= " .. after)
    -- A synthetic one: a config change is generated now, by eventd.
    local cbefore = eventd.guest_ns(vm)
    eventd.set(vm, "LogRetentionDays", "dword:12"):assert_ok()
    local crow
    wait_until(function()
        local rs = eventd.sql(vm, SHARD0, "SELECT timestamp FROM events WHERE event_type = '" ..
            eventd.T.config_change .. "' AND timestamp >= " .. cbefore)
        crow = rs[1]
        return crow ~= nil
    end, { timeout = 30, interval = 0.25, desc = "a config_change record" })
    eventd.unset(vm, "LogRetentionDays")
    t:assert(crow[1] >= cbefore and crow[1] <= eventd.guest_ns(vm),
        "the synthetic record is stamped from eventd's clock when generated: " .. crow[1])
end)

test("sequence is the per-CPU header sequence of a real event and null for synthetic records", {
    spec = "eventd *events.sequence-is-the-per-cpu-per-boot-header-sequence-and-null-for-synthetics",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.sequence.type, "INTEGER", "sequence is an INTEGER")
    local a, b = eventd.marker("sqa"), eventd.marker("sqb")
    local ra = emit_stored(vm, vm, "pt.ev.seq", { tag = a }, a)
    local rb = emit_stored(vm, vm, "pt.ev.seq", { tag = b }, b)
    local sa, sb = ra["event.sequence"], rb["event.sequence"]
    t:assert(math.type(sa) == "integer" and sa > 0, "a positive sequence: " .. tostring(sa))
    t:assert(sb > sa, "later on the same CPU is higher: " .. sa .. " then " .. sb)
    t:assert_eq(stored_row(vm, "pt.ev.seq", sb).cpu_id, 0, "both on the one CPU")
    -- Per boot: this boot's receipts start at sequence 1.
    local first = eventd.sql(vm, SHARD0, "SELECT min(first_sequence) FROM receipt_ranges WHERE hex(boot_id) = '" ..
        eventd.boot_pcds_hex(vm) .. "'")
    t:assert_eq(first[1][1], 1, "the boot's sequence space begins at 1")
    t:assert_eq(synthetic_row(vm, eventd.T.startup).sequence, "null", "a synthetic record has a null sequence")
end)

test("origin_class is the header's origin, 0 for a userspace emitter, and null for synthetic records", {
    spec = "eventd *events.origin-class-is-0-userspace-1-kmes-2-kacs-3-lcs-and-null-for-synthetics",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.origin_class.type, "INTEGER", "origin_class is an INTEGER")
    local tag = eventd.marker("oc")
    local r = emit_stored(vm, vm, "pt.ev.origin", { tag = tag }, tag)
    t:assert_eq(stored_row(vm, "pt.ev.origin", r["event.sequence"]).origin_class, 0, "kmes_emit from userspace is class 0")
    local classes = eventd.sql(vm, SHARD0,
        "SELECT DISTINCT origin_class FROM events WHERE event_type NOT IN " .. OWN)
    -- The header's value, stored as it came: the kernel ABI's five
    -- assigned classes, 4 being NTFE (ntfe.policy.published at boot).
    for _, c in ipairs(classes) do
        t:assert(math.type(c[1]) == "integer" and c[1] >= 0 and c[1] <= 4,
            "every real row is one of the five assigned classes: " .. json.encode(classes))
    end
    t:assert_eq(synthetic_row(vm, eventd.T.startup).origin_class, "null", "a synthetic record has none")
end)

test("event_type is the header's type exactly, or one of the five types eventd writes itself", {
    spec = "eventd *events.event-type-is-the-header-type-or-one-of-the-five-eventd-types",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.event_type.type, "TEXT", "event_type is TEXT")
    t:assert_eq(decl.event_type.notnull, 1, "declared NOT NULL")
    local tag = eventd.marker("ty")
    local r = emit_stored(vm, vm, "pt.Ev.MixedCase", { tag = tag }, tag)
    local rows = eventd.sql(vm, SHARD0, "SELECT event_type FROM events WHERE sequence = " .. r["event.sequence"] ..
        " AND event_type NOT IN " .. OWN)
    t:assert_eq(rows[1][1], "pt.Ev.MixedCase", "stored byte for byte, case kept")
    local null_seq = eventd.sql(vm, SHARD0,
        "SELECT DISTINCT event_type FROM events WHERE sequence IS NULL")
    t:assert(#null_seq >= 1, "daemon records exist to check")
    for _, n in ipairs(null_seq) do
        t:assert(OWN:find("'" .. n[1] .. "'", 1, true),
            "each daemon record's type is one of eventd's five: " .. n[1])
    end
end)

test("the token and process GUID columns are the emitter's: effective follows impersonation, true does not", {
    spec = "eventd *events.effective-token-guid-is-the-effective-token-at-emission"
        .. " eventd *events.true-token-guid-is-the-primary-token-and-null-for-synthetics"
        .. " eventd *events.process-guid-is-the-emitting-process-and-null-for-synthetics",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    for _, c in ipairs({ "effective_token_guid", "true_token_guid", "process_guid" }) do
        t:assert_eq(decl[c].type, "BLOB", c .. " is a BLOB")
    end
    local tags = { before = eventd.marker("imb"), during = eventd.marker("imd"), other = eventd.marker("imo") }
    local worker = vm:spawn_worker()
    local other = vm:spawn_worker()
    local rows = {}
    local ok, err = pcall(function()
        rows.before = emit_stored(vm, worker, "pt.ev.ident", { tag = tags.before }, tags.before)
        local token = worker:syscall(kacs.SYS.OPEN_SELF_TOKEN, 0, kacs.TOKEN_ALL_ACCESS)
        assert(token.ret >= 0, "open_self_token")
        local dup = worker:syscall(sys.NR.ioctl, {
            args = { token.ret, kacs.IOC.DUPLICATE, 0 },
            bufs = { string.pack("<I4I4I4i4", kacs.TOKEN_ALL_ACCESS, kacs.TOKEN_TYPE_IMPERSONATION, 2, -1) },
            ptrs = { 2 },
        })
        assert(dup.ret == 0, "duplicate as impersonation: " .. sys.errname(dup.errno))
        local imp = string.unpack("<i4", dup.out_bufs[1], 13)
        assert(worker:syscall(sys.NR.ioctl, imp, kacs.IOC.IMPERSONATE, 0).ret == 0, "impersonate")
        rows.during = emit_stored(vm, worker, "pt.ev.ident", { tag = tags.during }, tags.during)
        worker:syscall(kacs.SYS.REVERT, 0)
        rows.other = emit_stored(vm, other, "pt.ev.ident", { tag = tags.other }, tags.other)
    end)
    worker:kill(); worker:join()
    other:kill(); other:join()
    if not ok then error(err, 0) end
    local b = stored_row(vm, "pt.ev.ident", rows.before["event.sequence"])
    local d = stored_row(vm, "pt.ev.ident", rows.during["event.sequence"])
    local o = stored_row(vm, "pt.ev.ident", rows.other["event.sequence"])
    for _, r in ipairs({ b, d, o }) do
        t:assert_eq(#r.effective_token_guid, 32, "effective token GUID is 16 bytes")
        t:assert_eq(#r.true_token_guid, 32, "true token GUID is 16 bytes")
        t:assert_eq(#r.process_guid, 32, "process GUID is 16 bytes")
    end
    t:assert_eq(b.effective_token_guid, b.true_token_guid, "not impersonating: effective is the primary token")
    t:assert_neq(d.effective_token_guid, d.true_token_guid, "impersonating: effective is the impersonation token")
    t:assert_eq(d.true_token_guid, b.true_token_guid, "the true token is the primary token throughout")
    t:assert_eq(d.process_guid, b.process_guid, "one process, one process GUID")
    t:assert_neq(o.process_guid, b.process_guid, "another process, another GUID")
    local s = synthetic_row(vm, eventd.T.startup)
    t:assert_eq(s.true_, "null", "a synthetic record has no true token")
    t:assert_eq(s.process, "null", "and no process")
end)

test("payload is the raw bytes for a KMES event and a MessagePack map for a synthetic one", {
    spec = "eventd *events.payload-is-the-raw-kmes-bytes-a-synthetic-map-or-null",
}, function(t)
    local decl = column_decl(vm, SHARD0, "events")
    t:assert_eq(decl.payload.type, "BLOB", "payload is a BLOB")
    local tag = eventd.marker("pl")
    local bytes = eventd.msgpack({ tag = tag, n = 3 })
    local r = emit_stored(vm, vm, "pt.ev.payload", { raw = bytes }, tag)
    local row = stored_row(vm, "pt.ev.payload", r["event.sequence"])
    t:assert_eq(row.types.payload, "blob", "stored as a blob")
    t:assert_eq(row.payload, eventd.hex(bytes, true), "the bytes emitted")
    local s = synthetic_row(vm, eventd.T.startup)
    t:assert_eq(s.payload_type, "blob", "a synthetic payload is a blob too")
    local first = tonumber(s.payload:sub(1, 2), 16)
    t:assert((first >= 0x80 and first <= 0x8f) or first == 0xde or first == 0xdf,
        "and it is a MessagePack map: " .. s.payload:sub(1, 16))
end)

test("every KMES header field is stored in its own column, not in the payload blob", {
    spec = "eventd *events.every-kmes-header-field-is-extracted-into-its-own-column",
}, function(t)
    local tag = eventd.marker("hdr")
    local bytes = eventd.msgpack({ tag = tag })
    local r = emit_stored(vm, vm, "pt.ev.header", { raw = bytes }, tag)
    local row = stored_row(vm, "pt.ev.header", r["event.sequence"])
    for _, c in ipairs({ "boot_id", "timestamp", "cpu_id", "sequence", "origin_class", "event_type",
                         "effective_token_guid", "true_token_guid", "process_guid" }) do
        t:assert(row[c] ~= nil and row[c] ~= "", c .. " has a value of its own")
    end
    t:assert_eq(row.payload, eventd.hex(bytes, true), "and the payload carries only what was emitted")
end)

test("a userspace emitter cannot make a record that event_type alone reads as synthetic", {
    spec = "eventd *events.event-type-alone-distinguishes-real-from-synthetic-records"
        .. " eventd *events.a-kmes-event-typed-as-one-of-the-five-eventd-types-is-counted-and-not-stored",
}, function(t)
    local cols = column_decl(vm, SHARD0, "events")
    local n = 0
    for _ in pairs(cols) do n = n + 1 end
    t:assert_eq(n, 11, "the eleven documented columns and no record-type column")
    local gaps = "SELECT count(*) FROM events WHERE event_type = '" .. eventd.T.gap .. "'"
    local gaps_before = eventd.sql(vm, SHARD0, gaps)[1][1]
    local tag = eventd.marker("spoof")
    -- A real event claiming to be eventd's own startup record, beside an
    -- event of another eventd.* type, which nothing reserves.
    local spoof = eventd.T.startup
    local free = "eventd.pt." .. tag
    local spoofs = "SELECT count(*) FROM events WHERE event_type = '" .. spoof .. "' AND sequence IS NOT NULL"
    local r = eventd.emit(vm, spoof, { tag = tag })
    local r2 = eventd.emit(vm, free, { tag = tag })
    t:assert_eq(r2.ret, 0, "emitted " .. free)
    -- A later event from the same CPU, once stored, means the spoof has
    -- been through the writer too.
    local mine = eventd.marker("after")
    emit_stored(vm, vm, "pt.ev.after", { tag = mine }, mine)
    local stored = eventd.sql(vm, SHARD0, spoofs)
    t:assert(r.ret ~= 0 or stored[1][1] == 0,
        "a real event typed " .. spoof .. " is refused or not stored as such (emit ret " .. tostring(r.ret) ..
        ", stored rows " .. stored[1][1] .. ")")
    t:assert_eq(eventd.sql(vm, SHARD0, "SELECT count(*) FROM events WHERE event_type = '" .. free .. "'")[1][1], 1,
        "while " .. free .. ", outside the five, is stored like any event")
    -- Its sequence is receipted, so the next event leaves no hole behind it.
    t:assert_eq(eventd.sql(vm, SHARD0, gaps)[1][1], gaps_before, "and no gap is written in its place")
end)

test("a KMES payload is stored byte for byte, never decoded, re-encoded or validated on the way in", {
    spec = "eventd *events.a-kmes-payload-is-stored-exactly-as-delivered"
        .. " eventd *events.the-write-path-never-decodes-re-encodes-or-validates-a-payload",
}, function(t)
    local tag = eventd.marker("raw")
    -- One map KMES accepts (one well-formed value) that no encoder would
    -- produce: 5 as uint16 (a re-encoder would write 0x05), a duplicate
    -- key, an integer key, and a 32-bit-length string for a short value.
    local bytes = "\x86" ..
        "\xa3tag" .. string.char(0xa0 + #tag) .. tag ..
        "\xa1n\xcd\x00\x05" ..
        "\xa1d\x01" .. "\xa1d\x02" ..
        "\x07\xa1x" ..
        "\xa1s\xdb\x00\x00\x00\x02hi"
    local r = eventd.emit(vm, "pt.ev.raw", { raw = bytes })
    t:assert_eq(r.ret, 0, "KMES takes it (errno " .. tostring(r.errno) .. ")")
    local rows = eventd.wait_rows(vm, 'EVENTS pt.ev.raw WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 1 end)
    local row = stored_row(vm, "pt.ev.raw", rows[1]["event.sequence"])
    t:assert_eq(row.payload, eventd.hex(bytes, true), "the stored blob is the emitted bytes exactly")
end)

test("payload fields are flattened only when read: the blob keeps the nesting, a query sees the path", {
    spec = "eventd *events.payloads-are-decoded-and-flattened-only-on-the-read-path",
}, function(t)
    local tag = eventd.marker("flat")
    local bytes = eventd.msgpack({ tag = tag, outer = { inner = 9 } })
    local r = emit_stored(vm, vm, "pt.ev.flat", { raw = bytes }, tag)
    t:assert_eq(r["outer.inner"], 9, "a query presents the flattened path: " .. json.encode(r))
    local row = stored_row(vm, "pt.ev.flat", r["event.sequence"])
    t:assert_eq(row.payload, eventd.hex(bytes, true), "the stored blob is still the nested map")
    local hit = eventd.rows(vm, 'EVENTS pt.ev.flat WHERE outer.inner == 9 WHERE tag == "' .. tag .. '" SINCE 10m ago')
    t:assert_eq(#hit, 1, "and a predicate on the path matches it")
end)

test("a payload field at a header path is hidden from queries but kept in the blob", {
    spec = "eventd *events.a-payload-field-colliding-with-a-header-name-is-suppressed-but-kept-in-the-blob",
}, function(t)
    local tag = eventd.marker("coll")
    -- event.type and event.cpu are header paths; emitter.process.guid is
    -- one too, beside the payload's own emitter.process.pid.
    local bytes = eventd.msgpack({ tag = tag, event = { type = "pt.spoofed", cpu = 77 },
        emitter = { process = { guid = "spoofed", pid = 4242 } } })
    local r = emit_stored(vm, vm, "pt.ev.collide", { raw = bytes }, tag)
    t:assert_eq(r["event.type"], "pt.ev.collide", "the header's event.type is what a query sees")
    t:assert_eq(r["event.cpu"], 0, "and the header's event.cpu")
    t:assert_neq(r["emitter.process.guid"], "spoofed", "and the header's emitter.process.guid")
    t:assert_eq(r["emitter.process.pid"], 4242, "while a payload field beside a header path resolves")
    local none = eventd.rows(vm, 'EVENTS pt.ev.collide WHERE event.cpu == 77 SINCE 10m ago')
    t:assert_eq(#none, 0, "the payload's event.cpu matches no predicate")
    none = eventd.rows(vm, 'EVENTS pt.ev.collide WHERE emitter.process.guid == "spoofed" SINCE 10m ago')
    t:assert_eq(#none, 0, "nor does its emitter.process.guid")
    t:assert_eq(stored_row(vm, "pt.ev.collide", r["event.sequence"]).payload, eventd.hex(bytes, true),
        "while the blob still holds every colliding field")
end)

test("event_types lists each concrete type committed to the shard", {
    spec = "eventd *events.event-types-lists-each-concrete-type-committed-to-the-shard",
}, function(t)
    local decl = column_decl(vm, SHARD0, "event_types")
    t:assert_eq(decl.event_type.type, "TEXT", "event_type is TEXT")
    t:assert_eq(decl.event_type.pk, 1, "and the primary key")
    local tag = eventd.marker("cat")
    local ty = "pt.ev.cat" .. tag
    emit_stored(vm, vm, ty, { tag = tag }, tag)
    local missing = eventd.sql(vm, SHARD0,
        "SELECT DISTINCT event_type FROM events WHERE event_type NOT IN (SELECT event_type FROM event_types)")
    t:assert_eq(#missing, 0, "every type in events is catalogued: " .. json.encode(missing))
    local has = eventd.sql(vm, SHARD0, "SELECT count(*) FROM event_types WHERE event_type = '" .. ty .. "'")
    t:assert_eq(has[1][1], 1, "including the one just committed, once")
end)

test("a receipt range has the four documented columns, keyed on all four, positive and ordered", {
    spec = "eventd *events.a-receipt-range-records-the-16-byte-kernel-boot-id"
        .. " eventd *events.a-receipt-range-records-the-logical-kmes-cpu"
        .. " eventd *events.a-receipt-range-first-sequence-is-inclusive"
        .. " eventd *events.a-receipt-range-last-sequence-is-inclusive"
        .. " eventd *events.receipt-ranges-are-keyed-on-all-four-columns-and-must-be-positive-and-ordered",
}, function(t)
    local decl = column_decl(vm, SHARD0, "receipt_ranges")
    t:assert_eq(decl.boot_id.type, "BLOB", "boot_id BLOB")
    t:assert_eq(decl.cpu_id.type, "INTEGER", "cpu_id INTEGER")
    t:assert_eq(decl.first_sequence.type, "INTEGER", "first_sequence INTEGER")
    t:assert_eq(decl.last_sequence.type, "INTEGER", "last_sequence INTEGER")
    for _, c in ipairs({ "boot_id", "cpu_id", "first_sequence", "last_sequence" }) do
        t:assert_eq(decl[c].notnull, 1, c .. " NOT NULL")
        t:assert(decl[c].pk >= 1, c .. " is part of the primary key")
    end
    local sql = eventd.schema(vm, SHARD0).receipt_ranges
    t:assert(sql:find("CHECK %(first_sequence > 0%)"), "first_sequence > 0 is enforced: " .. sql)
    t:assert(sql:find("CHECK %(last_sequence >= first_sequence%)"), "last >= first is enforced")
    -- An event's own sequence is inside a range for its boot and CPU,
    -- inclusive at both ends.
    local tag = eventd.marker("rc")
    local r = emit_stored(vm, vm, "pt.ev.receipt", { tag = tag }, tag)
    local cover = eventd.sql(vm, SHARD0, "SELECT length(boot_id), first_sequence, last_sequence FROM receipt_ranges " ..
        "WHERE hex(boot_id) = '" .. eventd.boot_pcds_hex(vm) .. "' AND cpu_id = 0 AND first_sequence <= " ..
        r["event.sequence"] .. " AND last_sequence >= " .. r["event.sequence"])
    t:assert_eq(#cover, 1, "one receipt for this boot and CPU 0 covers sequence " .. r["event.sequence"])
    t:assert_eq(cover[1][1], 16, "its boot_id is sixteen bytes")
    local edges = eventd.sql(vm, SHARD0, "SELECT count(*) FROM receipt_ranges r JOIN events e ON " ..
        "e.boot_id = r.boot_id AND e.cpu_id = r.cpu_id AND (e.sequence = r.first_sequence OR e.sequence = r.last_sequence)")
    t:assert(edges[1][1] >= 1, "range ends name stored sequences themselves (inclusive bounds)")
end)

test("identity absent twice over: a synthetic record's effective_token_guid is null", {
    spec = "eventd *events.a-null-effective-token-guid-means-a-synthetic-record",
}, function(t)
    t:assert_eq(synthetic_row(vm, eventd.T.startup).effective, "null", eventd.T.startup .. ": SQL NULL")
    local real_nulls = eventd.sql(vm, SHARD0,
        "SELECT count(*) FROM events WHERE effective_token_guid IS NULL AND event_type NOT IN " .. OWN)
    t:assert_eq(real_nulls[1][1], 0, "no real event has a NULL effective token")
end)

-- Route closed: the kernel stamps the null GUID only for an emission with
-- no task context (PKM kmes event model, "no task context"); kmes_emit
-- always runs in one, and no kernel emitter a test can trigger runs
-- outside one. eventd itself stores the header bytes verbatim, so the
-- proof is a writer unit test that commits a RealEvent with a zero
-- effective_token_guid and reads back sixteen zero bytes, not NULL.
test("a real event emitted without identity stores the null GUID, not NULL", {
    spec = "eventd *events.a-null-guid-effective-token-means-a-real-event-emitted-without-identity",
    skip = true,
    covered_by = "cargo:eventd eventd-core shard::tests::a_real_event_without_identity_stores_the_null_guid_not_null",
}, function() end)

test("idx_events_timestamp is the one index a shard is created with", {
    spec = "eventd *events.idx-events-timestamp-is-the-one-index-created-with-the-table",
}, function(t)
    -- Before any query has driven the adaptive policy, a shard's only
    -- explicit index on events is the timestamp one.
    local idx = eventd.sql(vm, SHARD0,
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'events' AND sql IS NOT NULL")
    t:assert_eq(#idx, 1, "one index on events: " .. json.encode(idx))
    t:assert_eq(idx[1][1], "idx_events_timestamp", "named idx_events_timestamp")
    t:assert(idx[1][2]:find("ON events%(timestamp%)"), "on events(timestamp): " .. idx[1][2])
end)

test("the shard metadata table is key TEXT PRIMARY KEY, value TEXT NOT NULL, with schema_version and a UTC created_at", {
    spec = "eventd *events.shard-metadata-key-is-the-text-primary-key"
        .. " eventd *events.shard-metadata-value-is-non-null-text"
        .. " eventd *events.shard-metadata-requires-schema-version-and-a-utc-created-at",
}, function(t)
    local decl = column_decl(vm, SHARD0, "metadata")
    t:assert_eq(decl.key.type, "TEXT", "key is TEXT")
    t:assert_eq(decl.key.pk, 1, "and the primary key")
    t:assert_eq(decl.value.type, "TEXT", "value is TEXT")
    t:assert_eq(decl.value.notnull, 1, "and NOT NULL")
    local meta = {}
    for _, r in ipairs(eventd.sql(vm, SHARD0, "SELECT key, value FROM metadata")) do meta[r[1]] = r[2] end
    t:assert(meta.schema_version ~= nil and meta.schema_version ~= "", "schema_version is present: " .. json.encode(meta))
    t:assert(meta.created_at and meta.created_at:match("^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%dZ$"),
        "created_at is YYYY-MM-DDTHH:MM:SSZ: " .. tostring(meta.created_at))
end)

-- ---------------------------------------------------------------------------
-- The catalogue, instrumented, on craft
-- ---------------------------------------------------------------------------

local PRELOADED = "pt.preloaded." .. eventd.marker("pre")

--- Rows the catalogue trigger has logged for `ty`.
local function catlog(v, ty)
    return eventd.sql(v, SHARD0, "SELECT count(*) FROM pt_catlog WHERE event_type = '" .. ty .. "'")[1][1]
end

local instrumented = false
local function instrument()
    if instrumented then return end
    eventd.stop(craft)
    eventd.edit_store(craft, SHARD0,
        "INSERT INTO event_types VALUES ('" .. PRELOADED .. "'); " ..
        "CREATE TABLE pt_catlog (event_type TEXT); " ..
        "CREATE TRIGGER pt_catlog_trigger BEFORE INSERT ON event_types " ..
        "BEGIN INSERT INTO pt_catlog VALUES (NEW.event_type); END;")
    eventd.start(craft)
    instrumented = true
end

test("the writer loads the type catalogue at startup: a type already on disk costs no catalogue statement", {
    spec = "eventd *events.the-writer-loads-the-type-catalogue-into-memory-at-startup",
}, function(t)
    instrument()
    -- PRELOADED was written into event_types while eventd was down; no
    -- event of it has ever been committed, so only a catalogue loaded
    -- from disk can make it known.
    local tag = eventd.marker("pl")
    emit_stored(craft, craft, PRELOADED, { tag = tag }, tag)
    t:assert_eq(catlog(craft, PRELOADED), 0, "its first event issued no catalogue statement")
end)

test("a known type adds no catalogue statement to an insert, real or synthetic", {
    spec = "eventd *events.a-known-type-adds-no-catalogue-statement-to-an-insert",
}, function(t)
    -- Synthetic and gap records consult the same in-memory set as real
    -- events do.
    instrument()
    local ty = "pt.known." .. eventd.marker("kn")
    local a, b = eventd.marker("ka"), eventd.marker("kb")
    emit_stored(craft, craft, ty, { tag = a }, a)
    emit_stored(craft, craft, ty, { tag = b }, b)
    t:assert_eq(catlog(craft, ty), 1, "two real events of one type, in two batches: one statement")
    -- eventd.daemon.started was catalogued before the trigger existed, so
    -- the startup record committed by the restart was a known-type insert.
    t:assert_eq(catlog(craft, eventd.T.startup), 0,
        "the restart's eventd.daemon.started (a known type) issued no catalogue statement")
end)

test("a new type is catalogued once with its first event, however many of it share the batch", {
    spec = "eventd *events.a-new-type-is-catalogued-with-its-first-event-and-interned-after-commit"
        .. " eventd *events.a-pending-set-stops-repeat-catalogue-inserts-in-a-batch-and-rollback-discards-it",
}, function(t)
    instrument()
    -- Three events of a fresh type emitted while eventd is frozen reach
    -- the writer together. That they shared one transaction is read back
    -- from the receipts: one batch's receipt is one merged range per CPU,
    -- and separate batches leave separate ranges.
    local ty, seqs
    for attempt = 1, 4 do
        ty = "pt.batch." .. eventd.marker("b" .. attempt)
        local tag = eventd.marker("bt")
        local entries = {}
        for i = 1, 3 do entries[i] = { type = ty, payload = eventd.msgpack({ tag = tag }) } end
        local pid = eventd.pid(craft)
        eventd.freeze(craft, pid)
        local r = kmes.emit_batch(craft, entries)
        eventd.thaw(craft, pid)
        t:assert_eq(r.emitted, 3, "three emitted")
        local rows = eventd.wait_rows(craft, 'EVENTS ' .. ty .. ' WHERE tag == "' .. tag .. '" SINCE 10m ago',
            function(rs) return #rs == 3 end)
        seqs = {}
        for _, row in ipairs(rows) do seqs[#seqs + 1] = row["event.sequence"] end
        table.sort(seqs)
        local one = eventd.sql(craft, SHARD0, "SELECT count(*) FROM receipt_ranges WHERE cpu_id = 0 AND " ..
            "first_sequence <= " .. seqs[1] .. " AND last_sequence >= " .. seqs[3])
        if one[1][1] == 1 then break end
        seqs = nil
    end
    t:assert(seqs, "the three events were committed in one batch within four attempts")
    t:assert_eq(catlog(craft, ty), 1, "one catalogue statement for three events of a new type in one batch")
    local cat = eventd.sql(craft, SHARD0, "SELECT count(*) FROM event_types WHERE event_type = '" .. ty .. "'")
    t:assert_eq(cat[1][1], 1, "and the type is catalogued")
end)

local HIST = eventd.STORE.events .. "/shard-0005.db"

test("catalogue pages count toward the event store's live size", {
    spec = "eventd *events.catalogue-pages-count-toward-the-logical-live-size",
}, function(t)
    -- Before the ring flood below, so that the stored events are small.
    instrument()
    eventd.stop(craft)
    -- About 6 MB of catalogue and nothing else, in a historical shard: 600
    -- names of 10,000 characters. (Typed queries only from here on: an
    -- untyped one fails its access check on a name this long.)
    eventd.edit_store(craft, SHARD0, EMPTY ..
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 600) " ..
        "INSERT INTO event_types SELECT 'pt.pad.' || i || '.' || hex(zeroblob(5000)) FROM n;", { to = HIST })
    eventd.start(craft)
    local function live(db)
        local r = eventd.sql(craft, db, "SELECT (SELECT page_count FROM pragma_page_count()) - " ..
            "(SELECT freelist_count FROM pragma_freelist_count()), (SELECT page_size FROM pragma_page_size())")
        return r[1][1] * r[1][2]
    end
    local catalogue = live(HIST)
    local events_only = 0
    for _, s in ipairs(eventd.shards(craft)) do
        if s ~= HIST then events_only = events_only + live(s) end
    end
    t:assert(catalogue > 5000000, "the catalogue shard is large: " .. catalogue)
    t:assert(events_only < 3000000, "every other shard together is under 3 MB: " .. events_only)
    local tag = eventd.marker("size")
    emit_stored(craft, craft, "pt.ev.size", { tag = tag }, tag)
    -- A 4 MB cap: the store is over it only if catalogue pages count.
    eventd.set(craft, "EventRetentionMaxBytes", "qword:4000000"):assert_ok()
    local ok = pcall(wait_until, function()
        return #eventd.rows(craft, 'EVENTS pt.ev.size WHERE tag == "' .. tag .. '" SINCE 10m ago') == 0
    end, { timeout = 60, interval = 0.5, desc = "size retention to delete events" })
    eventd.unset(craft, "EventRetentionMaxBytes")
    t:assert(ok, "size retention deleted events from a store over the cap only by its catalogue")
end)

-- ---------------------------------------------------------------------------
-- Planning, gaps and receipts, on craft
-- ---------------------------------------------------------------------------

local HTAG = eventd.marker("hist")
local GAP_FLOODED = false

test("planning finds types through the catalogues of every shard, never by scanning events", {
    spec = "eventd *events.planning-unions-type-catalogues-across-active-and-historical-shards"
        .. " eventd *events.planning-never-scans-the-events-table-for-distinct-types",
}, function(t)
    instrument()
    local now = eventd.guest_ns(craft)
    local boot = eventd.boot_pcds_hex(craft)
    eventd.stop(craft)
    -- A historical shard (index 5 >= the one active shard) with two rows:
    -- one whose type is catalogued there, one whose type is in no
    -- catalogue anywhere.
    local payload = eventd.hex(eventd.msgpack({ tag = HTAG }), true)
    local function ins(ty, seq)
        return "INSERT INTO events (boot_id, timestamp, cpu_id, sequence, origin_class, event_type, " ..
            "effective_token_guid, true_token_guid, process_guid, payload) VALUES (X'" .. boot .. "', " ..
            now .. ", 0, " .. seq .. ", 0, '" .. ty .. "', zeroblob(16), zeroblob(16), zeroblob(16), X'" .. payload .. "'); "
    end
    eventd.edit_store(craft, SHARD0, EMPTY ..
        ins("pt.hist.cat" .. HTAG, 900001) .. ins("pt.hist.uncat" .. HTAG, 900002) ..
        "INSERT INTO event_types VALUES ('pt.hist.cat" .. HTAG .. "');", { to = HIST })
    -- While it is down, overrun the ring so that the restart finds a gap
    -- (used by the gap cases below): 100 events of ~60 KB through a 4 MiB
    -- buffer.
    local big = eventd.msgpack({ blob = eventd.bin(string.rep("g", 60000)) })
    local entries = {}
    for i = 1, 100 do entries[i] = { type = "pt.flood", payload = big } end
    local fr = kmes.emit_batch(craft, entries)
    GAP_FLOODED = fr.emitted == 100
    eventd.start(craft)
    local stored = eventd.sql(craft, HIST, "SELECT count(*) FROM events WHERE event_type LIKE 'pt.hist.%'")
    t:assert_eq(stored[1][1], 2, "both rows are in the historical shard")
    local cat = eventd.rows(craft, "EVENTS pt.hist.cat" .. HTAG .. " SINCE 1h ago")
    t:assert_eq(#cat, 1, "the type catalogued in the historical shard is found there")
    local wild = eventd.rows(craft, "EVENTS pt.hist.* SINCE 1h ago")
    t:assert_eq(#wild, 1, "a pattern finds exactly the catalogued one: " .. json.encode(wild))
    local uncat = eventd.rows(craft, "EVENTS pt.hist.uncat" .. HTAG .. " SINCE 1h ago")
    t:assert_eq(#uncat, 0, "a type in no catalogue is never discovered from the events table")
end)

test("cpu_id is null on a daemon-wide synthetic record and set on a gap record", {
    spec = "eventd *events.cpu-id-is-null-for-daemon-wide-synthetic-events-but-set-for-gap-records",
}, function(t)
    t:assert(GAP_FLOODED, "the ring was overrun while eventd was down")
    local decl = column_decl(craft, SHARD0, "events")
    t:assert_eq(decl.cpu_id.type, "INTEGER", "cpu_id is an INTEGER")
    local gaps = eventd.wait_rows(craft, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago",
        function(rs) return #rs >= 1 end)
    t:assert(#gaps >= 1, "the restart recorded a gap")
    local g = synthetic_row(craft, eventd.T.gap)
    t:assert_eq(g.cpu_id, "integer", "a gap record's cpu_id is set")
    t:assert_eq(g.cpu_value, 0, "to the CPU of the gap")
    t:assert_eq(synthetic_row(craft, eventd.T.startup).cpu_id, "null", eventd.T.startup .. " has none")
end)

test("a receipt accounts for every stored event and every gap it commits with", {
    spec = "eventd *events.a-receipt-commits-in-the-transaction-that-stores-its-rows-or-gap-record",
}, function(t)
    t:assert(GAP_FLOODED, "the ring was overrun while eventd was down")
    local boot = eventd.boot_pcds_hex(craft)
    local uncovered = eventd.sql(craft, SHARD0,
        "SELECT e.sequence FROM events e WHERE hex(e.boot_id) = '" .. boot .. "' AND e.sequence IS NOT NULL " ..
        "AND NOT EXISTS (SELECT 1 FROM receipt_ranges r WHERE r.boot_id = e.boot_id AND r.cpu_id = e.cpu_id " ..
        "AND e.sequence BETWEEN r.first_sequence AND r.last_sequence)")
    t:assert_eq(#uncovered, 0, "every stored real event of this boot is receipted: " .. json.encode(uncovered))
    local gaps = eventd.rows(craft, "EVENTS " .. eventd.T.gap .. " SINCE 10m ago")
    t:assert(#gaps >= 1, "there is a gap record")
    for _, g in ipairs(gaps) do
        local cover = eventd.sql(craft, SHARD0, "SELECT count(*) FROM receipt_ranges WHERE hex(boot_id) = '" ..
            boot .. "' AND cpu_id = " .. g["event.cpu"] .. " AND first_sequence <= " .. g["loss.sequence"] ..
            " AND last_sequence >= " .. g["loss.sequence-last"])
        t:assert_eq(cover[1][1], 1,
            "the gap " .. g["loss.sequence"] .. "-" .. g["loss.sequence-last"] .. " is receipted")
    end
end)

-- ---------------------------------------------------------------------------
-- Retention and the catalogue, on craft
-- ---------------------------------------------------------------------------

local ORPHAN = "pt.orphan." .. eventd.marker("orph")
local OLD_BOOT = string.rep("A5", 16)

--- Plant a forty-day-old event of `ty` (its only one), sequence `seq` of
--- OLD_BOOT, with its catalogue row and receipt, and let age retention
--- delete it. Returns whether the planted event was seen stored first.
local function plant_old(ty, seq)
    instrument()
    local old = eventd.guest_ns(craft) - 40 * 86400 * 1000000000
    eventd.stop(craft)
    eventd.edit_store(craft, SHARD0,
        "INSERT INTO events (boot_id, timestamp, cpu_id, sequence, origin_class, event_type, " ..
        "effective_token_guid, true_token_guid, process_guid, payload) VALUES (X'" .. OLD_BOOT .. "', " ..
        old .. ", 0, " .. seq .. ", 0, '" .. ty .. "', zeroblob(16), zeroblob(16), zeroblob(16), X'80'); " ..
        "INSERT INTO event_types VALUES ('" .. ty .. "'); " ..
        "INSERT INTO receipt_ranges VALUES (X'" .. OLD_BOOT .. "', 0, " .. seq .. ", " .. seq .. ");")
    eventd.start(craft)
    local planted = eventd.sql(craft, SHARD0,
        "SELECT count(*) FROM events WHERE event_type = '" .. ty .. "'")[1][1] == 1
    -- Any configuration change requests a retention pass; the default
    -- EventRetentionDays of 30 then deletes the old event.
    eventd.set(craft, "LogRetentionDays", "dword:13"):assert_ok()
    wait_until(function()
        return eventd.sql(craft, SHARD0, "SELECT count(*) FROM events WHERE event_type = '" .. ty .. "'")[1][1] == 0
    end, { timeout = 60, interval = 0.5, desc = "age retention to delete the old event" })
    eventd.unset(craft, "LogRetentionDays")
    return planted
end

--- Once per file: plant_old for ORPHAN.
local orphan_planted
local function plant_orphan()
    if orphan_planted == nil then orphan_planted = plant_old(ORPHAN, 1) end
    return orphan_planted
end

--- Whether `ty` is in SHARD0's catalogue.
local function catalogued(ty)
    return eventd.sql(craft, SHARD0, "SELECT count(*) FROM event_types WHERE event_type = '" .. ty .. "'")[1][1] == 1
end

test("receipt rows survive the retention that deletes their events", {
    spec = "eventd *events.receipt-rows-survive-event-retention",
}, function(t)
    t:assert(plant_orphan(), "the forty-day-old event was stored, then deleted by retention")
    local receipt = eventd.sql(craft, SHARD0, "SELECT count(*) FROM receipt_ranges WHERE hex(boot_id) = '" .. OLD_BOOT .. "'")
    t:assert_eq(receipt[1][1], 1, "its receipt row is still there")
end)

test("retention removes a type its deletes orphaned, once rechecked, and never at ingestion's expense", {
    spec = "eventd *events.retention-offers-orphan-type-checks-for-types-its-deletes-touched"
        .. " eventd *events.an-orphan-type-is-rechecked-in-the-deletion-transaction-and-uninterned-after-commit"
        .. " eventd *events.an-unindexed-or-interrupted-orphan-check-is-skipped-rather-than-delaying-ingestion",
}, function(t)
    -- Unindexed: a shard starts with no idx_events_event_type, so the check
    -- retention offered after deleting ORPHAN's only event was skipped,
    -- and the stale row stays (safe: it can expose nothing).
    t:assert(plant_orphan(), "the type's only event was stored, then deleted by retention")
    local tag = eventd.marker("ing")
    emit_stored(craft, craft, "pt.ev.ingest", { tag = tag }, tag)
    craft:clock():sleep("3s")
    t:assert(catalogued(ORPHAN), "with idx_events_event_type not material, the orphan check was skipped")

    -- Indexed: query event.type 22 times, lower the create threshold to 20
    -- (the drop threshold's default is 10) — an applied change, which
    -- makes the policy run — and wait for the quiet writer to build the
    -- index.
    for _ = 1, 22 do
        local r = eventd.query(craft, 'EVENTS pt.ev.q WHERE event.type == "pt.x" SINCE 1h ago')
        t:assert(r.ok, "a query on event.type: " .. tostring(r.stderr))
    end
    local function applied(key, value)
        local since = eventd.guest_ns(craft)
        eventd.set(craft, key, "dword:" .. value):assert_ok()
        eventd.wait_rows(craft, "EVENTS " .. eventd.T.config_change
            .. ' WHERE config.name == "' .. key .. '" AND config.value == ' .. value .. ' SINCE 10m ago', function(rs)
                for _, r in ipairs(rs) do if r["event.time"] >= since then return true end end
                return false
            end)
    end
    applied("AdaptiveIndexCreateThreshold", 20)
    -- This one applied change recomputes with the new threshold (PEI-1447).
    local built = pcall(wait_until, function() return eventd.schema(craft, SHARD0).idx_events_event_type ~= nil end,
        { timeout = 60, interval = 0.5, desc = "idx_events_event_type to be built" })
    t:assert(built, "idx_events_event_type was built; counters "
        .. json.encode(eventd.sql(craft, eventd.DB.meta, "SELECT field_path, query_count FROM index_counters"))
        .. ", desired " .. json.encode(eventd.sql(craft, eventd.DB.meta, "SELECT field_path FROM desired_indexes")))
    local orphan2 = "pt.orphan2." .. eventd.marker("orph")
    t:assert(plant_old(orphan2, 2), "a second type's only event was stored, then deleted by retention")
    local gone = pcall(wait_until, function() return not catalogued(orphan2) end,
        { timeout = 20, interval = 0.5, desc = "the orphaned type to leave the catalogue" })
    eventd.unset(craft, "AdaptiveIndexCreateThreshold")
    t:assert(gone, "with the index material, the orphaned type has been removed from event_types")
    -- Uninterned too: its next event is catalogued again, with a statement.
    local before = catlog(craft, orphan2)
    local tag2 = eventd.marker("again")
    emit_stored(craft, craft, orphan2, { tag = tag2 }, tag2)
    t:assert(catalogued(orphan2), "a new event of the removed type puts it back in the catalogue")
    t:assert_eq(catlog(craft, orphan2), before + 1, "through a catalogue statement: it was no longer interned")
end)

-- Not observable: receipt compaction is optional ("may prepare", "may lag
-- indefinitely"), so no run can show that it never happens, and eventd
-- implements none (no compaction code in eventd/ or eventd-core/; only
-- writers insert receipt rows, shard.rs:210-224, 283-297). The ordering
-- and piggyback constraints bind a mechanism that does not exist.
test("receipt compaction inserts the merged range before deleting what it subsumes", {
    spec = "eventd *events.receipt-compaction-inserts-the-merged-range-before-deleting-what-it-subsumes",
    skip = true,
}, function() end)

test("receipt compaction only rides on commits a writer would make anyway", {
    spec = "eventd *events.receipt-compaction-only-piggybacks-on-writer-commits",
    skip = true,
}, function() end)

test("startup merges overlapping and adjacent receipts across every readable shard, compacted or not", {
    spec = "eventd *events.startup-merges-overlapping-and-adjacent-receipt-ranges-across-readable-shards"
        .. " eventd *events.startup-merge-correctness-never-depends-on-compaction",
}, function(t)
    instrument()
    local boot = eventd.boot_pcds_hex(craft)
    eventd.stop(craft)
    local frag = eventd.sql(craft, SHARD0, "SELECT first_sequence, last_sequence FROM receipt_ranges WHERE hex(boot_id) = '" ..
        boot .. "' AND cpu_id = 0 ORDER BY first_sequence")
    t:assert(#frag > 1, "the active shard's own receipts are uncompacted fragments: " .. #frag .. " rows")
    local s = frag[#frag][2]
    -- In another shard: a range overlapping the active shard's coverage
    -- and running far past it.
    eventd.edit_store(craft, SHARD0, EMPTY ..
        "INSERT INTO receipt_ranges VALUES (X'" .. boot .. "', 0, " .. (s - 5) .. ", " .. (s + 5000) .. ");",
        { to = eventd.STORE.events .. "/shard-0006.db" })
    eventd.start(craft)
    local rows = eventd.rows(craft, "EVENTS " .. eventd.T.startup .. " TAKE 1")
    local cpus, seqs = rows[1]["store.resume.cpus"], rows[1]["store.resume.sequences"]
    t:assert_eq(cpus[1], 0, "CPU 0's resume point")
    t:assert(seqs[1] >= s + 5000,
        "is contiguous from 1 through the other shard's range: " .. seqs[1] .. " (active ended at " .. s .. ")")
end)
