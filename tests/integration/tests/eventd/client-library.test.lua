-- eventd TRM §10.2 — The eventd-client Library.
--
-- eventd-client is a Rust library with no binary of its own. Nothing in the
-- image calls `query`, `Tail`, `text` or `access` except evctl, which uses
-- only its wire framing and output types (evctl/src/main.rs), and
-- whatever evctl does is §10.1's to prove. So the library's own guarantees
-- — whole results or none, how a tail reports, how text is quoted, what
-- `access` and `readable` compute — are proved by its unit tests, cited
-- below and run under ptcargo, or listed as TODO where none exists yet.
--
-- Two anchors are about eventd's behaviour as much as the library's, and
-- those run in a VM: that text a person typed must be quoted because a bare
-- keyword is read as the clause, and that a field-only grant shows records.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-client" })

-- ---------------------------------------------------------------------------
-- Unit homes
-- ---------------------------------------------------------------------------

-- Route closed: eventd-client is a library with no binary; the only program
-- in the image that links it is evctl, whose behaviour §10.1 covers.
test("eventd-client speaks the query channel: it frames the normative query request", {
    spec = "eventd *client.eventd-client-is-the-library-for-programs-that-query-eventd",
    skip = true,
    covered_by = "cargo:eventd eventd-client wire::tests::sends_the_normative_query_request",
}, function() end)

-- Route closed: no program in the image calls eventd_client::query; evctl
-- drives the wire itself (evctl/src/main.rs execute_query).
test("query() returns every record at end, or the error and no records", {
    spec = "eventd *client.query-returns-a-whole-result-or-none",
    skip = true,
    covered_by = "cargo:eventd eventd-client tests::a_query_that_fails_part_way_gives_nothing",
}, function() end)

-- Route closed: no program in the image uses Tail.
test("a Tail reports the initial result once, whole, at watch, then each live batch", {
    spec = "eventd *client.a-tail-reports-its-initial-result-once-complete",
    skip = true,
    covered_by = "cargo:eventd eventd-client tests::a_tail_reports_its_initial_result_whole_then_live_batches",
}, function() end)

-- Route closed: no program in the image uses Tail.
test("a Tail reads its socket on a thread of its own, through a channel of at most capacity reports", {
    spec = "eventd *client.a-tail-reads-its-socket-on-its-own-thread",
    skip = true,
    covered_by = "cargo:eventd eventd-client " ..
        "tests::a_tail_reads_its_socket_on_its_own_thread_until_capacity_reports_wait",
}, function() end)

-- Route closed: no program in the image uses Tail.
test("a Tail its program stops reading is ended by eventd, and reports that it ended", {
    spec = "eventd *client.a-tail-a-program-stops-reading-is-ended-by-eventd",
    skip = true,
    covered_by = "cargo:eventd eventd-client " ..
        "tests::a_tail_its_program_stops_reading_is_ended_by_eventd_and_says_so",
}, function() end)

-- Route closed: text::string is a pure function no image program calls.
test("text::string escapes the quote, the backslash and every control character", {
    spec = "eventd *client.text-string-escapes-quotes-backslashes-and-controls",
    skip = true,
    covered_by = "cargo:eventd eventd-client text::tests::strings_escape_quotes_backslashes_and_controls",
}, function() end)

-- Route closed: the access module runs in the calling program; no image
-- program calls it.
test("access resolves the descriptors eventd checks, at eventd's registry paths", {
    spec = "eventd *client.access-checks-the-descriptors-eventd-checks",
    skip = true,
    covered_by = "cargo:eventd eventd-client access::tests::descriptor_paths_sit_under_the_namespace_key",
}, function() end)

-- Route closed: which code eventd links is not observable from outside;
-- eventd's query/security.rs:17-19 imports the rights, generic mapping and
-- `candidates` walk from eventd_client::access, and the cited test pins the walk.
test("eventd and its clients share the rights, the generic mapping and the pattern walk", {
    spec = "eventd *client.eventd-and-its-clients-share-the-rights-mapping-and-pattern-walk",
    skip = true,
    covered_by = "cargo:eventd eventd-client access::tests::candidates_walk_from_the_identifier_to_the_wildcard",
}, function() end)

-- Route closed: field_grants runs in the calling program; eventd's own use
-- of it is the field-only grant, which the VM test below shows.
test("field_grants lists the object GUIDs of a descriptor's allowing object ACEs", {
    spec = "eventd *client.field-grants-are-the-object-guids-of-allowing-object-aces",
    skip = true,
    covered_by = "cargo:eventd eventd-client " ..
        "access::tests::a_descriptor_grants_by_name_the_fields_its_allowing_object_aces_name",
}, function() end)

-- Route closed: readable() runs in the calling program; no image program calls it.
test("readable() weighs every pattern: a specific grant under a denying * is Some, not Nothing", {
    spec = "eventd *client.readable-considers-every-pattern",
    skip = true,
    covered_by = "cargo:eventd eventd-client " ..
        "access::tests::readable_weighs_every_pattern_not_only_the_wildcard",
}, function() end)

-- Route closed: readable() runs in the calling program; no image program calls it.
test("readable() says Unknown, with the reason, when the policy cannot be read", {
    spec = "eventd *client.unreadable-policy-is-unknown-not-denied",
    skip = true,
    covered_by = "cargo:eventd eventd-client " ..
        "access::tests::a_policy_that_cannot_be_read_is_unknown_with_its_reason_never_nothing",
}, function() end)

-- ---------------------------------------------------------------------------
-- What eventd does with the text and the grants
-- ---------------------------------------------------------------------------

test("text a person typed must be quoted: bare, an event type called WHERE is read as the clause", {
    spec = "eventd *client.text-a-person-typed-is-always-quoted",
}, function(t)
    -- After EVENTS the pattern is optional, so a bare WHERE begins the
    -- clause; after LOGS FROM an origin is always expected, so the same
    -- word bare is the origin. Quoted, each is a value in both places.
    for _, origin in ipairs({ "WHERE", "STREAM" }) do
        eventd.send_log(vm, { origin = origin, is_error = false, message = "origin " .. origin })
    end
    eventd.wait_rows(vm, 'LOGS FROM "STREAM" SINCE 10m ago', function(rs) return #rs >= 1 end)
    t:assert(#eventd.rows(vm, 'LOGS FROM "WHERE" SINCE 10m ago') >= 1, "quoted, WHERE is an origin")
    t:assert(#eventd.rows(vm, 'LOGS FROM "STREAM" SINCE 10m ago') >= 1, "quoted, STREAM is an origin")
    local quoted = eventd.query(vm, 'EVENTS "WHERE" SINCE 10m ago')
    t:assert(quoted.ok, 'quoted, EVENTS "WHERE" names an event type: ' .. tostring(quoted.stderr))
    local bare = eventd.query(vm, "EVENTS WHERE SINCE 10m ago")
    t:assert(not bare.ok, "bare, EVENTS WHERE begins the clause, and SINCE is no predicate: "
        .. json.encode(bare.rows))
    for _, origin in ipairs({ "WHERE", "STREAM" }) do
        local r = eventd.query(vm, "LOGS FROM " .. origin .. " SINCE 10m ago")
        t:assert(r.ok and #r.rows >= 1 and r.rows[1].origin == origin,
            "bare after LOGS FROM, " .. origin .. " is the origin: " .. tostring(r.stderr))
    end
end)

test("records are visible with EVENTD_READ on the root, or on any field alone", {
    spec = "eventd *client.records-are-visible-with-read-on-the-root-or-any-field",
}, function(t)
    local origin = eventd.marker("fo")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "only this" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    local guid = eventd.field_guid("message")
    local key = eventd.SECURITY .. [[\Logs\]] .. origin
    local sd = access.simple({
        access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM),
        access.ace(access.ACE.ALLOWED_OBJECT, 0x1, token.SID.TEST_USER, 0, { object_type = guid }) })
    eventd.write_descriptor(vm, key, sd):assert_ok()
    vm:run("sleep 0.5")
    local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
    local out
    token.as_principal(t, vm, { privs_present = NOTIFY, privs_enabled = NOTIFY }, function(w)
        out = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", "LOGS FROM " .. origin .. " SINCE 10m ago" } })
    end)
    vm:run("reg del '" .. key .. "'")
    local line = out.stdout:match("[^\n]+")
    local row = line and json.decode(line)
    t:assert(row, "a grant on the message field alone shows the record: " .. tostring(out.stderr))
    t:assert_eq(row and row.message, "only this", "with the granted field")
    t:assert(row and row.origin == nil, "and not the ungranted ones: " .. tostring(line))
end)
