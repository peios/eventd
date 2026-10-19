-- eventd TRM §10.1 — Using evctl: the command line, the output formats, and
-- when a result is written.
--
-- One file-scope VM. Most tests run the image's /usr/bin/evctl against the
-- real eventd. The result-commitment tests need an eventd that misbehaves
-- on cue — records and then an error, records and a long pause before
-- `watch`, a stream that never stops — so the agent stands in for it: it
-- listens on a pathname socket under /tmp, evctl is pointed there with
-- --socket, and the agent answers with frames it built itself (a four-byte
-- little-endian length, then a MessagePack map: `{status = ok, records =
-- [...]}`, `{status = end}`, `{status = watch}`, `{status = error, error =
-- ...}`, as PSPU §3.16 and eventd-client's wire.rs frame them).
--
-- evctl's output is redirected to a file in those tests, so the test can
-- see what has been written while evctl is still running.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local token = require("helpers.token")
local access = require("helpers.access")
local us = require("helpers.unixsock")
peinit.claim(1)

local vm = eventd.boot({ name = "ev-evctl" })

local function q(s) return '"' .. s .. '"' end

--- evctl with `args` (a list), no shell in between.
local function evctl(args, stdin)
    return vm:run("/usr/bin/evctl", { args = args, stdin = stdin })
end

local function lines(s)
    local out = {}
    for l in s:gmatch("[^\n]+") do out[#out + 1] = l end
    return out
end

local function one_log()
    local origin = eventd.marker("ev")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "hello " .. origin })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    return origin
end

-- ---------------------------------------------------------------------------
-- The stand-in eventd
-- ---------------------------------------------------------------------------

local function frame(bytes) return string.pack("<I4", #bytes) .. bytes end
local function f_records(list) return frame(eventd.msgpack(eventd.map{ status = "ok", records = eventd.array(list) })) end
local function f_status(s) return frame(eventd.msgpack(eventd.map{ status = s })) end
local function f_error(msg) return frame(eventd.msgpack(eventd.map{ status = "error", error = msg })) end

local standin_seq = 0
--- Start evctl against a stand-in socket and accept its connection.
--- `pipeline` replaces the output redirection (default: to files).
local function standin(format, pipeline)
    standin_seq = standin_seq + 1
    local path = "/tmp/pt-standin-" .. standin_seq .. ".sock"
    local out, err = "/tmp/pt-standin-" .. standin_seq .. ".out", "/tmp/pt-standin-" .. standin_seq .. ".err"
    vm:run("rm -f " .. path)
    local srv = assert(us.socket(vm, us.AF_UNIX, us.SOCK.STREAM))
    assert(us.bind(vm, srv, path).ret == 0, "bind " .. path)
    assert(us.listen(vm, srv).ret == 0, "listen")
    local cmd = "exec /usr/bin/evctl --socket " .. path .. " --format " .. (format or "jsonl") ..
        " 'EVENTS pt.standin' " .. (pipeline or ("> " .. out .. " 2> " .. err))
    local proc = vm:run_async("/bin/sh", { args = { "-c", cmd } })
    local conn = assert(us.accept(vm, srv))
    local req = us.recvmsg(vm, conn, 65536, { cmsg = 0 })
    assert(req.ret and req.ret > 4, "evctl sent its query first")
    local s = { proc = proc, conn = conn, srv = srv, out = out, err = err, path = path }
    function s.send(bytes, flags)
        return us.sendmsg(vm, conn, bytes, { flags = flags })
    end
    function s.close()
        vm:syscall(3, conn)
        vm:syscall(3, srv)
        vm:run("rm -f " .. path)
    end
    return s
end

local function size(path)
    local r = vm:run("stat -L -c %s '" .. path .. "'")
    return tonumber((r.stdout:gsub("%s", ""))) or -1
end

local function fd_links(pid)
    return vm:run("ls -l /proc/" .. pid .. "/fd").stdout
end

-- ---------------------------------------------------------------------------
-- The command line
-- ---------------------------------------------------------------------------

test("the one positional argument is the query; there is no query subcommand", {
    spec = "eventd *evctl.the-one-positional-argument-is-a-query-with-no-subcommand",
}, function(t)
    local origin = one_log()
    local r = evctl({ "--format", "jsonl", "LOGS FROM " .. origin .. " SINCE 10m ago" })
    t:assert_eq(r.exit_code, 0, "the query as the sole argument runs: " .. r.stderr)
    t:assert_eq(#lines(r.stdout), 1, "and returns the record")
    local sub = evctl({ "query", "LOGS FROM " .. origin .. " SINCE 10m ago" })
    t:assert_eq(sub.exit_code, 2, "`evctl query '…'` is a usage error: " .. sub.stderr)
    t:assert_eq(sub.stdout, "", "and runs nothing")
end)

test("evctl takes PSPU query text and has no flag-based query language", {
    spec = "eventd *evctl.evctl-uses-pspu-query-syntax-and-has-no-flag-based-query-language",
}, function(t)
    for _, args in ipairs({ { "--type", "kacs.*" }, { "--since", "1h" }, { "--origin", "authd" },
                            { "--take", "5" }, { "LOGS", "FROM", "authd" } }) do
        local r = evctl(args)
        t:assert_eq(r.exit_code, 2, table.concat(args, " ") .. " is a usage error: " .. r.stderr)
    end
    local r = evctl({ "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1" })
    t:assert_eq(r.exit_code, 0, "while the same request in PSPU syntax runs")
end)

test("help and version are commands, as exact standalone words", {
    spec = "eventd *evctl.exact-standalone-words-such-as-help-and-version-are-commands",
}, function(t)
    local h = evctl({ "help" })
    t:assert_eq(h.exit_code, 0, "help runs")
    t:assert(h.stdout:find("Usage:", 1, true), "and prints usage")
    local v = evctl({ "version" })
    t:assert_eq(v.exit_code, 0, "version runs")
    t:assert(v.stdout:match("^evctl %d+%.%d+%.%d+"), "and prints the version: " .. v.stdout)
    for _, word in ipairs({ "Help", "VERSION", "help me", "status" }) do
        local r = evctl({ word })
        t:assert_eq(r.exit_code, 2, "'" .. word .. "' is neither a command nor a query: " .. r.stderr)
    end
end)

test("a first word of EVENTS, LOGS or METRIC, in any ASCII case, makes a query", {
    spec = "eventd *evctl.a-first-word-of-events-logs-or-metric-in-any-ascii-case-is-a-query",
}, function(t)
    for _, text in ipairs({ "events " .. eventd.T.startup .. " since 1h ago take 1",
                            "EvEnTs " .. eventd.T.startup .. " TAKE 1",
                            "logs since 1h ago take 1", "LoGs SINCE 1h ago TAKE 1",
                            "metric pt.none since 1h ago", "Metric pt.none SINCE 1h ago" }) do
        local r = evctl({ text })
        t:assert_eq(r.exit_code, 0, "'" .. text .. "' is sent to eventd as a query: " .. r.stderr)
    end
end)

test("the query can come from a UTF-8 file or from standard input", {
    spec = "eventd *evctl.the-query-can-be-read-from-a-utf-8-file-or-standard-input",
}, function(t)
    local origin = one_log()
    local text = "LOGS FROM " .. origin .. " SINCE 10m ago"
    vm:write_file("/tmp/pt-q.evq", text .. "\n")
    local f = evctl({ "--format", "jsonl", "--file", "/tmp/pt-q.evq" })
    t:assert_eq(f.exit_code, 0, "--file runs: " .. f.stderr)
    t:assert_eq(#lines(f.stdout), 1, "the file's query")
    local s = evctl({ "--format", "jsonl", "-" }, text .. "\n")
    t:assert_eq(s.exit_code, 0, "- reads standard input: " .. s.stderr)
    t:assert_eq(#lines(s.stdout), 1, "the same query")
    vm:write_file("/tmp/pt-bad.evq", "LOGS FROM \xff\xfe")
    local bad = evctl({ "--file", "/tmp/pt-bad.evq" })
    t:assert(bad.exit_code ~= 0, "a file that is not UTF-8 is refused")
    t:assert(bad.stderr:find("UTF%-8"), "as not UTF-8: " .. bad.stderr)
end)

test("the default socket is /run/eventd/query.sock, and --socket names another", {
    spec = "eventd *evctl.the-default-socket-is-run-eventd-query-sock"
        .. " eventd *evctl.socket-selects-a-nonstandard-query-socket",
}, function(t)
    local moved = "/run/eventd/pt-moved.sock"
    vm:run("mv /run/eventd/query.sock " .. moved):assert_ok()
    local default = evctl({ "EVENTS TAKE 1" })
    local chosen = evctl({ "--socket", moved, "EVENTS " .. eventd.T.startup .. " SINCE 1h ago TAKE 1" })
    vm:run("mv " .. moved .. " /run/eventd/query.sock"):assert_ok()
    t:assert_eq(default.exit_code, 1, "with nothing at the default path, evctl fails")
    t:assert(default.stderr:find("/run/eventd/query.sock", 1, true),
        "naming /run/eventd/query.sock: " .. default.stderr)
    t:assert_eq(chosen.exit_code, 0, "--socket reaches the same eventd at another path: " .. chosen.stderr)
    t:assert_eq(#lines(chosen.stdout), 1, "and gets its answer")
    t:assert_eq(evctl({ "EVENTS TAKE 1" }).exit_code, 0, "and with the socket back, the default works again")
end)

test("evctl opens no database and decides nothing: eventd answers by the caller's token", {
    spec = "eventd *evctl.evctl-never-opens-a-database-or-makes-its-own-authorization-decision",
}, function(t)
    -- An ordinary user — not an Administrator — who cannot open logs.db,
    -- granted one origin by a descriptor of its own.
    local granted, other = one_log(), one_log()
    local function put(origin, aces)
        local k = eventd.SECURITY .. [[\Logs\]] .. origin
        eventd.write_descriptor(vm, k, access.simple(aces)):assert_ok()
        return k
    end
    local key = put(granted, { access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM),
        access.ace(access.ACE.ALLOWED, 0x1, token.SID.TEST_USER) })
    local key2 = put(other, { access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM) })
    vm:run("sleep 0.5")
    local NOTIFY = token.bit(token.PRIV.CHANGE_NOTIFY)
    local cat, mine, theirs, fds
    token.as_principal(t, vm, { privs_present = NOTIFY, privs_enabled = NOTIFY }, function(w)
        cat = w:run("/usr/bin/cat", { args = { eventd.DB.logs } })
        mine = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", "LOGS FROM " .. granted .. " SINCE 10m ago" } })
        theirs = w:run("/usr/bin/evctl", { args = { "--format", "jsonl", "LOGS FROM " .. other .. " SINCE 10m ago" } })
        local s = w:run_async("/usr/bin/evctl", { args = { "LOGS FROM " .. granted .. " STREAM" } })
        vm:run("sleep 1")
        fds = fd_links(s:pid())
        pcall(function() s:kill("kill") end)
    end)
    vm:run("reg del '" .. key .. "'")
    vm:run("reg del '" .. key2 .. "'")
    t:assert(cat.exit_code ~= 0, "the user cannot read logs.db itself: " .. cat.stderr)
    t:assert_eq(mine.exit_code, 0, "yet evctl, as that user, runs: " .. tostring(mine.stderr))
    t:assert_eq(#lines(mine.stdout), 1, "and returns the granted origin's record")
    t:assert_eq(theirs.exit_code, 0, "the ungranted origin is no error to evctl")
    t:assert_eq(theirs.stdout, "", "eventd simply returned nothing for it")
    t:assert(not fds:find("%.db"), "a running evctl holds no database open: " .. fds)
    t:assert(fds:find("socket:", 1, true), "only its socket to eventd")
end)

-- ---------------------------------------------------------------------------
-- Output
-- ---------------------------------------------------------------------------

test("records go to standard output, diagnostics and query errors to standard error", {
    spec = "eventd *evctl.data-goes-to-standard-output-and-diagnostics-to-standard-error",
}, function(t)
    local origin = one_log()
    local ok = evctl({ "--format", "jsonl", "LOGS FROM " .. origin .. " SINCE 10m ago" })
    t:assert_eq(#lines(ok.stdout), 1, "the record on stdout")
    t:assert_eq(ok.stderr, "", "nothing on stderr")
    local bad = evctl({ "LOGS FROM " .. origin .. " NONSENSE" })
    t:assert_eq(bad.stdout, "", "a query eventd refuses writes nothing to stdout")
    t:assert(#bad.stderr > 0, "and says why on stderr: " .. bad.stderr)
    local usage = evctl({ "--bogus" })
    t:assert_eq(usage.stdout, "", "a usage error writes nothing to stdout")
    t:assert(#usage.stderr > 0, "and its diagnostic to stderr")
end)

test("pretty is the default format: one self-describing record per line", {
    spec = "eventd *evctl.pretty-is-the-default-format-with-one-record-per-line",
}, function(t)
    local origin = eventd.marker("pr")
    for i = 1, 3 do eventd.send_log(vm, { origin = origin, is_error = false, message = "line\n" .. i }) end
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 3 end)
    local r = evctl({ "LOGS FROM " .. origin .. " SINCE 10m ago" })
    local ls = lines(r.stdout)
    t:assert_eq(#ls, 3, "three records, three lines, though each message has a newline in it: " .. r.stdout)
    for _, l in ipairs(ls) do
        t:assert(l:find('origin="' .. origin .. '"', 1, true) and l:find("message=", 1, true)
            and l:find("timestamp=", 1, true), "each line names its fields: " .. l)
    end
    t:assert_eq(r.stdout, evctl({ "--format", "pretty", "LOGS FROM " .. origin .. " SINCE 10m ago" }).stdout,
        "and it is what --format pretty writes")
end)

test("jsonl is one object per record, with tagged objects for what JSON cannot carry", {
    spec = "eventd *evctl.jsonl-writes-one-object-per-record-and-tags-values-json-cannot-carry",
}, function(t)
    local etype = "pt.js" .. eventd.marker()
    eventd.emit(vm, etype, eventd.map{ b = eventd.bin("\0\255"), nan = eventd.float(0 / 0),
        inf = eventd.float(1 / 0), ninf = eventd.float(-1 / 0) })
    eventd.emit(vm, etype, { n = 2 })
    local rows = eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local r = evctl({ "--format", "jsonl", "EVENTS " .. etype .. " SINCE 10m ago" })
    t:assert_eq(#lines(r.stdout), 2, "two records, two lines")
    local tagged
    for _, row in ipairs(rows) do if row.b then tagged = row end end
    t:assert_eq(json.encode(tagged.b), '{"$binary":"00ff"}', "binary is {\"$binary\": hex}")
    t:assert_eq(json.encode(tagged.nan), '{"$float":"nan"}', "NaN is {\"$float\":\"nan\"}")
    t:assert_eq(json.encode(tagged.inf), '{"$float":"+inf"}', "+inf is tagged")
    t:assert_eq(json.encode(tagged.ninf), '{"$float":"-inf"}', "-inf is tagged")
    -- An extension value, which eventd does not produce itself: from the stand-in.
    local s = standin("jsonl")
    s.send(frame("\x82\xa6status\xa2ok\xa7records\x91\x81\xa1e\xd4\x05\x07"))
    s.send(f_status("end"))
    local done = s.proc:wait("10s")
    local out = vm:read_file(s.out)
    s.close()
    t:assert_eq(done.exit_code, 0, "evctl took the stand-in's answer")
    local parsed, row = pcall(json.decode, lines(out)[1] or "")
    t:assert(parsed, "the line holding an extension value is one JSON object: " .. out)
    t:assert_eq(parsed and row.e and json.encode(row.e), '{"$extension":{"data":"07","type":5}}',
        "an extension is {\"$extension\": {type, data}}: " .. out)
end)

test("msgpack frames each record with its length as four little-endian bytes", {
    spec = "eventd *evctl.msgpack-prefixes-each-record-with-a-four-byte-little-endian-length",
}, function(t)
    local origin = eventd.marker("mp")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "first" })
    eventd.send_log(vm, { origin = origin, is_error = true, message = "second" })
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local r = evctl({ "--format", "msgpack", "LOGS FROM " .. origin .. " SINCE 10m ago" })
    local out, at, frames = r.stdout, 1, {}
    while at <= #out do
        local len = string.unpack("<I4", out, at)
        frames[#frames + 1] = out:sub(at + 4, at + 3 + len)
        at = at + 4 + len
    end
    t:assert_eq(at, #out + 1, "the lengths account for every byte")
    t:assert_eq(#frames, 2, "two records, two frames")
    for i, f in ipairs(frames) do
        t:assert(f:byte(1) >= 0x80 and f:byte(1) <= 0x8f, "frame " .. i .. " is a MessagePack map")
        t:assert(f:find("first", 1, true) or f:find("second", 1, true), "holding a record")
    end
end)

test("records in one result need not share a schema", {
    spec = "eventd *evctl.records-in-one-result-do-not-share-a-uniform-schema",
}, function(t)
    local etype = "pt.sch" .. eventd.marker()
    eventd.emit(vm, etype, { alpha = 1 })
    eventd.emit(vm, etype, { beta = 2 })
    eventd.wait_rows(vm, "EVENTS " .. etype .. " SINCE 10m ago", function(rs) return #rs == 2 end)
    local ls = lines(evctl({ "--format", "jsonl", "EVENTS " .. etype .. " SINCE 10m ago" }).stdout)
    local a, b = json.decode(ls[1]), json.decode(ls[2])
    t:assert((a.alpha == nil) ~= (b.alpha == nil), "alpha is in one line and absent, not null, from the other")
    t:assert((a.beta == nil) ~= (b.beta == nil), "and beta the other way round")
end)

-- ---------------------------------------------------------------------------
-- Result commitment
-- ---------------------------------------------------------------------------

test("a result commits only at end or watch; an error before then voids what came first", {
    spec = "eventd *evctl.a-result-commits-only-on-end-or-watch-and-an-earlier-error-invalidates-it",
}, function(t)
    local s = standin("jsonl")
    s.send(f_records({ { n = 1 }, { n = 2 } }))
    s.send(f_records({ { n = 3 } }))
    s.send(f_error("query timed out"))
    local r = s.proc:wait("10s")
    local out, err = vm:read_file(s.out), vm:read_file(s.err)
    s.close()
    t:assert_eq(r.exit_code, 1, "evctl fails")
    t:assert_eq(out, "", "and writes none of the three records it received before the error")
    t:assert(err:find("query timed out", 1, true), "reporting eventd's message: " .. err)
    -- The same records followed by end are written.
    local s2 = standin("jsonl")
    s2.send(f_records({ { n = 1 }, { n = 2 } }))
    s2.send(f_status("end"))
    local r2 = s2.proc:wait("10s")
    local out2 = vm:read_file(s2.out)
    s2.close()
    t:assert_eq(r2.exit_code, 0, "with end, it succeeds")
    t:assert_eq(#lines(out2), 2, "and writes both")
end)

test("initial records go to an anonymous temporary spool, published only at end or watch", {
    spec = "eventd *evctl.initial-records-are-rendered-into-an-anonymous-temporary-spool"
        .. " eventd *evctl.the-spool-is-published-only-after-end-or-watch",
}, function(t)
    local s = standin("jsonl")
    s.send(f_records({ { n = 1 }, { n = 2 } }))
    vm:run("sleep 1")
    local pid = s.proc:pid()
    local fds = fd_links(pid)
    local before = size(s.out)
    local spool = fds:match("(/tmp/#%d+ %(deleted%))")
    s.send(f_status("watch"))
    local ok = pcall(wait_until, function() return size(s.out) > 0 end, { timeout = 5, interval = 0.1 })
    local after = vm:read_file(s.out)
    pcall(function() s.proc:kill("kill") end)
    s.close()
    t:assert(spool, "evctl holds an unlinked temporary file (O_TMPFILE in /tmp): " .. fds)
    t:assert_eq(before, 0, "and stdout is still empty though two records have arrived")
    t:assert(ok and #lines(after) == 2, "at watch both are published: " .. after)
end)

test("the spool keeps evctl's memory flat however large the result, and has no pathname", {
    spec = "eventd *evctl.the-spool-bounds-heap-use-and-has-no-pathname",
}, function(t)
    local s = standin("jsonl")
    local pad = string.rep("p", 60000)
    for i = 1, 500 do s.send(f_records({ { i = i, pad = pad } })) end -- about 30 MB
    vm:run("sleep 1")
    local pid = s.proc:pid()
    local rss = tonumber(vm:run("grep VmRSS /proc/" .. pid .. "/status").stdout:match("(%d+)"))
    local fds = fd_links(pid)
    local fdnum = fds:match("(%d+) %-> /tmp/#%d+ %(deleted%)")
    local spooled = fdnum and size("/proc/" .. pid .. "/fd/" .. fdnum) or -1
    local listed = vm:run("ls -a /tmp").stdout
    s.send(f_status("end"))
    local r = s.proc:wait("30s")
    local out = size(s.out)
    s.close()
    t:assert(spooled > 25000000, "about 30 MB of records are in the spool: " .. spooled)
    t:assert(rss and rss < 20000, "while evctl's resident memory stays under 20 MB: " .. tostring(rss) .. " kB")
    t:assert(not listed:find("#", 1, true), "the spool has no name in /tmp: " .. listed)
    t:assert_eq(r.exit_code, 0, "and the whole result is then written")
    t:assert(out > 25000000, "all of it: " .. out)
end)

test("after watch, each new record is written as it arrives", {
    spec = "eventd *evctl.after-watch-new-records-are-written-directly",
}, function(t)
    local s = standin("jsonl")
    s.send(f_records({ { n = 1 } }))
    s.send(f_status("watch"))
    pcall(wait_until, function() return size(s.out) > 0 end, { timeout = 5, interval = 0.1 })
    local initial = #lines(vm:read_file(s.out))
    s.send(f_records({ { n = 2 } }))
    local ok = pcall(wait_until, function() return #lines(vm:read_file(s.out)) == 2 end,
        { timeout = 5, interval = 0.1 })
    local running = s.proc:status() == "running"
    pcall(function() s.proc:kill("kill") end)
    s.close()
    t:assert_eq(initial, 1, "the initial record was published at watch")
    t:assert(ok, "the live record appeared with no end")
    t:assert(running, "while evctl was still following")
end)

test("closing evctl, with Ctrl-C too, closes the connection and ends the watch", {
    spec = "eventd *evctl.closing-evctl-closes-the-query-connection-and-ends-the-watch",
}, function(t)
    -- Against the real eventd with one streaming query allowed: while
    -- evctl is following, a second stream is refused; once it is closed,
    -- one is accepted again.
    eventd.set(vm, "MaxStreamingQueries", "dword:1"):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change ..
        ' WHERE config.name == "MaxStreamingQueries" AND config.value == 1 SINCE 1h ago',
        function(rs) return #rs >= 1 end)
    local function second_refused()
        local p = vm:run_async("/usr/bin/evctl", { args = { "LOGS FROM " .. eventd.marker("x") .. " STREAM" } })
        local r = p:wait("3s")
        return r.exit_code == 1
    end
    local a = vm:run_async("/usr/bin/evctl", { args = { "LOGS FROM " .. eventd.marker("a") .. " STREAM" } })
    vm:run("sleep 1")
    local blocked = second_refused()
    vm:run("kill -INT " .. a:pid())
    a:wait("5s")
    local freed = pcall(wait_until, function() return not second_refused() end, { timeout = 15, interval = 0.5 })
    eventd.unset(vm, "MaxStreamingQueries")
    t:assert(blocked, "while evctl follows, its stream holds the one slot")
    t:assert(freed, "after SIGINT ends evctl, eventd has ended the watch and the slot is free")
end)

test("a blocked output pipeline backs up into the socket", {
    spec = "eventd *evctl.a-blocked-output-pipeline-applies-socket-backpressure",
}, function(t)
    -- evctl writes into a pipe nobody reads. Once the pipe is full it stops
    -- reading the socket, and the far end's sends stop being taken.
    -- The reader is a sleep that never reads and is gone in 30 s, which
    -- also ends evctl (SIGPIPE) without the test having to find either.
    local s = standin("jsonl", "| /bin/sleep 30")
    s.send(f_status("watch"))
    local pad = string.rep("q", 4000)
    local sent, again = 0, false
    for _ = 1, 20000 do
        local r = s.send(f_records({ { pad = pad } }), 0x40) -- MSG_DONTWAIT
        if r.ret < 0 then
            again = r.errno == 11 -- EAGAIN
            break
        end
        sent = sent + r.ret
    end
    s.close()
    pcall(function() s.proc:wait("40s") end)
    t:assert(again, "the stand-in's sends would block (EAGAIN) after " .. sent .. " bytes")
    t:assert(sent < 16 * 1024 * 1024, "well before the 80 MB it offered: " .. sent)
end)
