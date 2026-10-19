-- The eventd testset's own machinery, asserted directly, so that a fault in
-- `helpers.eventd` shows up here rather than as a puzzling failure in a
-- chapter file. Every route a chapter test uses to put a record in or get
-- one out is exercised once: a KMES event, a log datagram and a metric
-- datagram in; evctl and the host-side sqlite copy out; a live registry
-- change; a seeded configuration; and a restart of the service. Then
-- every shared helper the chapter files lean on: eventd's process from
-- the agent, the raw query channel and decoder, the clock, the waits,
-- senders and descriptors, and — on a second VM whose eventd may fail —
-- stop/start, crash, store edits, a by-hand run and the SIGQUIT dump.
--
-- None of this cites the TRM. The anchors are the chapter files' to prove.

local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
local us = require("helpers.unixsock")
local sys = require("helpers.sys")
local access = require("helpers.access")
local token = require("helpers.token")
peinit.claim(2) -- two file-scope VMs

local vm = eventd.boot({ name = "ev-harness" })
-- The second VM is seeded twice: a configuration value (the seeded-value
-- test), and eventd's service as Normal / Never, so that the helpers
-- that stop, kill or fail eventd run where that reboots nothing — in the
-- image eventd is Critical and restarted on failure.
local nc = eventd.boot({
    name = "ev-harness-nc",
    noncritical = true,
    config = { { name = "LogRetentionDays", type = "dword", data = 11 } },
})

test("an eventd staged through PT_EVENTD_ROOT is the one that runs", {
    skip = not os.getenv("PT_EVENTD_ROOT"),
}, function(t)
    local staged = eventd.override_files()["usr/sbin/eventd"]
    t:assert(staged, "PT_EVENTD_ROOT carries usr/sbin/eventd")
    local running = vm:read_file("/proc/" .. eventd.pid(vm) .. "/exe")
    t:assert(running == staged[1], "the running eventd is the staged binary ("
        .. #running .. " bytes running, " .. #staged[1] .. " staged)")
    local psb = vm:read_file("/proc/" .. eventd.pid(vm) .. "/psb")
    t:assert(psb:find("pip_trust=8192", 1, true), "and it carries the TCB signature: " .. psb)
end)

test("eventd answers on its query socket once the boot is done", {}, function(t)
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " TAKE 5")
    t:assert(#rows >= 1, "a startup record is queryable: " .. #rows)
    t:assert_eq(rows[1]["event.type"], eventd.T.startup, "and it is the startup type")
end)

test("a KMES event emitted by the agent comes back through evctl", {}, function(t)
    local tag = eventd.marker("ev")
    local r = eventd.emit(vm, "pt.harness", { tag = tag, n = 7 })
    t:assert_eq(r.ret, 0, "kmes_emit succeeded (errno " .. tostring(r.errno) .. ")")
    local rows = eventd.wait_rows(vm,
        'EVENTS pt.harness WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 1 end)
    t:assert_eq(#rows, 1, "exactly the one event")
    t:assert_eq(rows[1].n, 7, "its payload field decoded: " .. json.encode(rows[1]))
end)

test("a log datagram sent to the log socket comes back through evctl", {}, function(t)
    local origin = eventd.marker("log")
    local r = eventd.send_log(vm, { origin = origin, is_error = false, message = "hello harness" })
    t:assert_eq(r.ret, #eventd.msgpack({ origin = origin, is_error = false, message = "hello harness" }),
        "sendto took the whole datagram (errno " .. tostring(r.errno) .. ")")
    local rows = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    t:assert_eq(rows[1].message, "hello harness", "the message round-tripped")
end)

test("a metric datagram sent to the metric socket comes back through evctl", {}, function(t)
    local name = eventd.marker("m")
    local r = eventd.send_metric(vm, { name = name, type = "gauge", value = 42 })
    t:assert(r.ret and r.ret > 0, "sendto succeeded (errno " .. tostring(r.errno) .. ")")
    local rows = eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    t:assert_eq(rows[1].value, 42, "the sample round-tripped: " .. json.encode(rows[1]))
end)

-- PEI-1336 item 7 / PEI-1452: fixture sends must not hide syscall failures
-- behind an unrelated eventd timeout or an apparent daemon-side rejection.
for _, channel in ipairs({ "log", "metric" }) do
    test(channel .. " helper reports send failures and supports explicit negative probes", {
        tags = { "datagram-helper" },
    }, function(t)
        local send = eventd["send_" .. channel]
        local missing = "/run/eventd/" .. eventd.marker("missing") .. ".sock"
        local bytes = "fixture"
        local ok, err = pcall(send, vm, { raw = bytes }, { path = missing })
        t:assert(not ok, "an absent socket must fail immediately")
        local message = tostring(err)
        t:assert(message:find(missing, 1, true), "error names the destination: " .. message)
        t:assert(message:find("/" .. #bytes .. " bytes", 1, true), "error names the size: " .. message)
        t:assert(message:find("errno " .. sys.E.NOENT, 1, true), "error names ENOENT: " .. message)
        local r = send(vm, { raw = bytes }, { path = missing, allow_error = true })
        t:assert_eq(r.errno, sys.E.NOENT, "an explicit negative probe retains the syscall result")
    end)
end

test("both datagram helpers surface EMSGSIZE instead of silently losing fixture input", {
    tags = { "datagram-helper" },
}, function(t)
    -- Deliberately beyond a fresh Unix datagram socket's send capacity.
    -- No producer buffer policy is changed by these test helpers.
    local bytes = string.rep("x", 1024 * 1024)
    for _, channel in ipairs({ "log", "metric" }) do
        local send = eventd["send_" .. channel]
        local raw = send(vm, { raw = bytes }, { allow_error = true })
        t:assert_eq(raw.errno, 90, "the guest refuses the oversized " .. channel .. " datagram with EMSGSIZE")
        local ok, err = pcall(send, vm, { raw = bytes })
        t:assert(not ok, channel .. " send must fail immediately")
        local message = tostring(err)
        t:assert(message:find(eventd.SOCKET[channel], 1, true), message)
        t:assert(message:find("/" .. #bytes .. " bytes", 1, true), message)
        t:assert(message:find("errno 90", 1, true), message)
    end
end)

test("the host-side sqlite copy reads a store's schema", {}, function(t)
    local schema = eventd.schema(vm, eventd.DB.logs)
    t:assert(schema.logs, "logs.db has a logs table; objects: " .. json.encode(schema))
    local shards = eventd.shards(vm)
    t:assert(#shards >= 1, "the event store holds at least one shard")
    local ev = eventd.schema(vm, shards[1])
    t:assert(ev.events, "a shard has an events table")
end)

test("a live registry change is applied and recorded as a config change", {}, function(t)
    eventd.set(vm, "LogRetentionDays", "dword:13"):assert_ok()
    local rows = eventd.wait_rows(vm,
        "EVENTS " .. eventd.T.config_change .. " SINCE 10m ago",
        function(rs)
            for _, r in ipairs(rs) do
                if json.encode(r):find("LogRetentionDays", 1, true) then return true end
            end
            return false
        end)
    t:assert(#rows >= 1, "a config change record names the key")
    eventd.unset(vm, "LogRetentionDays")
end)

test("a restart brings up a new eventd that still answers", {}, function(t)
    local before = eventd.pid(vm)
    t:assert(before, "eventd has a pid")
    local after = eventd.restart(vm)
    t:assert(after and after ~= before, "a new process: " .. tostring(before) .. " -> " .. tostring(after))
    local rows = eventd.rows(vm, "EVENTS " .. eventd.T.startup .. " SINCE 10m ago")
    t:assert(#rows >= 2, "both starts recorded a startup event: " .. #rows)
end)

test("a seeded value is in force from the first start", {}, function(t)
    local seeded = nc
    local r = seeded:run("reg get '" .. eventd.KEY .. "' LogRetentionDays")
    t:assert(r.stdout:find("11", 1, true), "the seed applied before Phase 2: " .. r.stdout .. r.stderr)
    t:assert(eventd.query(seeded, "EVENTS TAKE 1").ok, "and eventd started with it")
end)

-- ---------------------------------------------------------------------------
-- eventd's process, from the agent (PIP: the shell cannot signal eventd or
-- read its /proc; the agent can)
-- ---------------------------------------------------------------------------

test("signal, freeze and thaw act on eventd from the agent", {}, function(t)
    local pid = eventd.pid(vm)
    t:assert(eventd.alive(vm, pid), "eventd is alive")
    local shell = vm:run("kill -0 " .. pid)
    t:log("a shell kill -0 of eventd exits " .. shell.exit_code .. " (PIP refuses it once eventd is signed)")
    t:assert_eq(eventd.freeze(vm), pid, "freeze stops the running eventd")
    local state = vm:read_file("/proc/" .. pid .. "/stat"):match("%) (%S)")
    local tag = eventd.marker("frz")
    local ty = "pt.harness.frozen." .. tag
    eventd.emit(vm, ty, { tag = tag })
    vm:clock():sleep("1s")
    -- Read from the files: a frozen eventd answers no query.
    local ok_count, while_frozen = pcall(eventd.stored_count, vm, ty)
    eventd.thaw(vm, pid)
    t:assert(ok_count, "the shards read while frozen: " .. tostring(while_frozen))
    t:assert_eq(state, "T", "stopped, as the agent reads /proc/<pid>/stat")
    t:assert_eq(while_frozen, 0, "a frozen eventd stored nothing")
    local rows = eventd.wait_rows(vm, "EVENTS " .. ty .. " SINCE 10m ago",
        function(rs) return #rs == 1 end)
    t:assert_eq(#rows, 1, "thawed, it stored the event")
    local r = eventd.signal(vm, pid, "SIGCONT")
    t:assert_eq(r.ret, 0, "a signal by its SIG name")
    t:assert_eq(eventd.signal(vm, 999999, 0, { check = false }).errno, sys.E.SRCH,
        "check = false returns the raw result")
    t:assert(not eventd.alive(vm, 999999), "and alive() is false for no such process")
end)

test("fds, fds_on, fd_listing, threads and proc_status read eventd's /proc", {}, function(t)
    local pid = eventd.pid(vm)
    local fds = eventd.fds(vm, pid)
    t:assert(#fds >= 5, "eventd holds descriptors: " .. #fds)
    local rw, ro, on = eventd.fds_on(vm, nil, eventd.DB.logs)
    t:assert_eq(rw, 1, "logs.db has its writer's one read-write descriptor: " .. json.encode(on))
    t:assert(on[1].rdwr == (on[1].mode == 2) and on[1].flags & 3 == on[1].mode, "rdwr and mode agree with flags")
    t:assert_eq(#eventd.fd_numbers(fds, eventd.DB.logs, 2), 1, "fd_numbers finds the same descriptor")
    local by_fd = eventd.fds(vm, pid, { by_fd = true, ino = true })
    local writer = eventd.fd_numbers(fds, eventd.DB.logs, 2)[1]
    t:assert_eq(by_fd[writer].path, eventd.DB.logs, "by_fd is keyed by fd number")
    t:assert(math.type(by_fd[writer].ino) == "integer", "ino = true stats the target")
    local listing = eventd.fd_listing(vm, pid)
    t:assert(listing:find("anon_inode:kmes%-cpu"), "the listing shows the KMES ring: " .. listing)
    t:assert(listing:find("%d+ %-> " .. eventd.DB.logs:gsub("%p", "%%%0") .. "\n"), "and logs.db as `fd -> path`")
    t:assert(listing:find("socket:%[%d+%]"), "and its sockets")
    local threads, tpid = eventd.threads(vm)
    t:assert_eq(tpid, pid, "threads reads the running eventd")
    local names = {}
    for _, th in ipairs(threads) do names[#names + 1] = th.comm end
    t:assert(#threads >= 4 and table.concat(names, ","):find("eventd%-writer"),
        "its threads, by comm: " .. table.concat(names, ","))
    local st = eventd.proc_status(vm, pid)
    t:assert_eq(st.PPid, "1", "its parent is peinit")
    t:assert(st.VmRSS and st.VmRSS:match("^%d+ kB$"), "VmRSS: " .. tostring(st.VmRSS))
    eventd.set_policy(vm, threads[1].tid, 0, 0) -- SCHED_OTHER, as it already is
end)

-- ---------------------------------------------------------------------------
-- The raw query channel and the decoder
-- ---------------------------------------------------------------------------

test("the raw query client round-trips a query, as SYSTEM and as a minted user", {}, function(t)
    local c = eventd.rq.open(vm)
    eventd.rq.send(c, "EVENTS " .. eventd.T.startup .. " TAKE 2")
    local out = eventd.rq.collect(c)
    eventd.rq.close(c)
    t:assert_eq(out.status, "end", "the query ended: " .. tostring(out.error))
    t:assert(#out.records >= 1 and out.records[1]["event.type"] == eventd.T.startup, "with the startup record")
    t:assert(out.frames[1].size > 0, "and each frame carries its payload size")
    local asked = eventd.rq.ask(vm, "EVENTS " .. eventd.T.startup .. " TAKE 1")
    t:assert_eq(asked.status, "end", "ask: one whole query")
    t:assert_eq(#asked.records, 1, "ask: its record")
    local bad = eventd.rq.ask(vm, "EVENTS", { socket = "/run/eventd/pt-no-such.sock" })
    t:assert(bad.connect_error and bad.connect_error:find("^connect: "), "a refused connect: " .. tostring(bad.connect_error))
    local nope, why = eventd.rq.connect(vm, { socket = "/run/eventd/pt-no-such.sock" })
    t:assert(nope == nil and why:find("ENOENT"), "connect returns nil and why: " .. tostring(why))
    local w = vm:spawn_worker()
    local ok, err = pcall(function()
        local tok = eventd.rq.user(w, 4242)
        local uc = eventd.rq.open(w, tok)
        eventd.rq.timeout(uc, 10)
        eventd.rq.send(uc, "EVENTS " .. eventd.T.startup .. " TAKE 1")
        local m = eventd.rq.frame(uc)
        t:assert(m and (m.status == "ok" or m.status == "end"), "a minted user is answered: " .. json.encode(m))
        eventd.rq.close(uc)
    end)
    w:kill(); w:join()
    t:assert(ok, tostring(err))
end)

test("a frame whose length and payload arrive in separate reads is read whole", {}, function(t)
    local a, b = us.socketpair(vm, us.SOCK.STREAM)
    t:assert(a and b, "a socketpair")
    local body = eventd.msgpack({ status = "ok", records = { { n = 1, gone = eventd.NIL } } })
    local frame = string.pack("<I4", #body) .. body
    local c = { who = vm, w = vm, fd = b, buf = "" }
    eventd.rq.timeout(c, 5)
    -- The length alone, then the payload in two pieces, then a second
    -- frame and a third in one write.
    us.sendmsg(vm, a, frame:sub(1, 4))
    us.sendmsg(vm, a, frame:sub(5, 10))
    us.sendmsg(vm, a, frame:sub(11) .. frame .. frame)
    for i = 1, 3 do
        local m, why = eventd.rq.frame(c)
        t:assert(m, "frame " .. i .. ": " .. tostring(why))
        t:assert_eq(m.records[1].n, 1, "frame " .. i .. " decoded")
        t:assert_eq(m.records[1].gone, eventd.NIL, "with its nil-valued key kept")
        t:assert_eq(m.size, #body, "and its size")
    end
    sys.close(vm, a)
    local m, why = eventd.rq.frame(c)
    t:assert(m == nil and why == "eof", "then eof: " .. tostring(why))
    sys.close(vm, b)

    -- And a real answer bigger than one 64 KiB read.
    local tag = eventd.marker("big")
    local pad = eventd.bin(string.rep("z", 30000))
    for i = 1, 4 do eventd.emit(vm, "pt.harness.big", { tag = tag, i = i, pad = pad }) end
    eventd.wait_rows(vm, 'EVENTS pt.harness.big WHERE tag == "' .. tag .. '" SINCE 10m ago',
        function(rs) return #rs == 4 end)
    local rc = eventd.rq.open(vm)
    eventd.rq.send(rc, 'EVENTS pt.harness.big WHERE tag == "' .. tag .. '" SINCE 10m ago')
    local out = eventd.rq.collect(rc)
    eventd.rq.close(rc)
    t:assert_eq(out.status, "end", "the big answer ended")
    t:assert_eq(#out.records, 4, "with all four records")
    t:log("the answer came in " .. #out.frames .. " frames over " .. rc.reads .. " reads")
    t:assert_eq(#out.records[1].pad, 30000, "the payload arrived whole")
end)

test("decode keeps nil-valued keys and map order; tagged keeps bin and duplicates", {}, function(t)
    -- A stored KMES payload, read back raw from the shard.
    local ty = "pt.harness.nil." .. eventd.marker()
    eventd.emit(vm, ty, { a = 1, b = eventd.NIL, c = eventd.bin("\0\1") })
    wait_until(function() return eventd.stored_count(vm, ty) == 1 end,
        { timeout = 30, interval = 0.25, desc = "the event stored" })
    local hex
    for _, shard in ipairs(eventd.shards(vm)) do
        local r = eventd.sql(vm, shard, "SELECT hex(payload) FROM events WHERE event_type = '" .. ty .. "'")
        if r[1] then hex = r[1][1] end
    end
    local bytes = eventd.unhex(hex)
    local v = eventd.decode(bytes, 1, { whole = true })
    t:assert_eq(v.b, eventd.NIL, "the nil-valued key is kept, as eventd.NIL")
    t:assert_eq(table.concat(eventd.keys(v), ","), "a,b,c", "keys() gives wire order")
    t:assert_eq(v.c, "\0\1", "bin decodes to a string")
    local tg = eventd.decode(bytes, 1, { tagged = true })
    t:assert_eq(tg.map.c.bin, "\0\1", "tagged: bin is {bin = …}")
    t:assert_eq(tg.keys[2], "b", "tagged: keys in order")
    local dup = eventd.decode("\x82\xa1k\x01\xa1k\x02", 1, { tagged = true })
    t:assert_eq(#dup.keys, 2, "tagged: a duplicate key shows twice in keys")
    t:assert(not pcall(eventd.decode, bytes .. "\0", 1, { whole = true }), "whole = true refuses trailing bytes")
end)

-- ---------------------------------------------------------------------------
-- Clock, waits, senders, descriptors
-- ---------------------------------------------------------------------------

test("guest_ns is the guest's clock, and a stored timestamp is on it", {}, function(t)
    local before = eventd.guest_ns(vm)
    t:assert(math.type(before) == "integer", "an integer")
    local date = tonumber(vm:run("date +%s%N").stdout:match("%d+"))
    t:assert(math.abs(date - before) < 5e9, "within 5 s of the guest's date: " .. date .. " vs " .. before)
    local origin = eventd.marker("clk")
    eventd.send_log(vm, { origin = origin, is_error = false, message = "tick" })
    local row = eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)[1]
    t:assert(row.timestamp >= before and row.timestamp <= eventd.guest_ns(vm),
        "the record's timestamp lies between two guest_ns readings")
end)

test("wait_rows raises on timeout by default and returns rows, false with raise = false", {}, function(t)
    local text = "EVENTS pt.harness.never." .. eventd.marker() .. " SINCE 1m ago"
    local never = function(rs) return #rs > 0 end
    local raised, err = pcall(eventd.wait_rows, vm, text, never, { timeout = 1 })
    t:assert(not raised, "the default raises")
    t:assert(tostring(err):find("last rows"), "naming what it last saw: " .. tostring(err))
    local rows, ok = eventd.wait_rows(vm, text, never, { timeout = 1, raise = false })
    t:assert_eq(ok, false, "raise = false: ok is false")
    t:assert_eq(#rows, 0, "and rows the last answer")
    local got, fine = eventd.wait_rows(vm, "EVENTS " .. eventd.T.startup .. " TAKE 1",
        function(rs) return #rs == 1 end, { socket = eventd.SOCKET.query })
    t:assert(fine and #got == 1, "and on success rows, true (socket honoured)")
end)

test("a persistent sender and a non-blocking sendto deliver", {}, function(t)
    local name = eventd.marker("snd")
    local s = eventd.sender(vm)
    s.send({ { name = name, type = "gauge", value = 1 }, { name = name, type = "gauge", value = 2, timestamp = eventd.guest_ns(vm) + 1 } })
    s.send({ { name = name, type = "gauge", value = 3, timestamp = eventd.guest_ns(vm) + 2 } })
    s.close()
    eventd.wait_rows(vm, "METRIC " .. name .. " SINCE 10m ago", function(rs) return #rs >= 2 end)
    local origin = eventd.marker("nb")
    local bytes = eventd.msgpack({ origin = origin, is_error = false, message = "nb" })
    local r = eventd.sendto(vm, nil, bytes, eventd.SOCKET.log, eventd.DONTWAIT)
    t:assert_eq(r.ret, #bytes, "MSG_DONTWAIT sendto took the datagram: errno " .. tostring(r.errno))
    eventd.wait_rows(vm, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
end)

test("descriptor helpers write, read and drop a pattern's descriptor", {}, function(t)
    local origin = eventd.marker("sd")
    local sd = access.simple({ access.ace(access.ACE.ALLOWED, 0x1, token.SID.LOCAL_SYSTEM) })
    local key = eventd.put_descriptor(vm, "Logs", origin, sd)
    t:assert_eq(key, eventd.SECURITY .. "\\Logs\\" .. origin, "the key")
    t:assert_eq((eventd.descriptor_hex(vm, "Logs", origin) or ""):lower(), eventd.hex(sd), "read back as hex")
    t:assert_eq(eventd.read_descriptor(vm, key), sd, "and as bytes")
    eventd.drop_descriptor(vm, "Logs", origin)
    t:assert_eq(eventd.descriptor_hex(vm, "Logs", origin), nil, "dropped")
    local k2 = eventd.SECURITY .. "\\Logs\\" .. eventd.marker("sd2")
    eventd.write_descriptor(vm, k2, sd):assert_ok()
    t:assert_eq(eventd.read_descriptor(vm, k2), sd, "write_descriptor on a key")
    vm:run("reg del -r -y '" .. k2 .. "'")
end)

test("restart and ready honour a socket", {}, function(t)
    local pid = eventd.restart(vm, { socket = eventd.SOCKET.query })
    t:assert(pid, "a new eventd")
    eventd.ready(vm, { socket = eventd.SOCKET.query, timeout = 30 })
    eventd.ready(vm, 30)
end)

-- ---------------------------------------------------------------------------
-- The noncritical VM: stop, start, crash, store edits, by hand, the dump
-- ---------------------------------------------------------------------------

test("noncritical boot seeds ErrorControl Normal and RestartPolicy Never", {}, function(t)
    local def = nc:run("reg get '" .. eventd.SERVICE .. "'").stdout
    t:assert(def:match("ErrorControl[^\n]*0\n"), "ErrorControl 0: " .. def)
    t:assert(def:match("RestartPolicy[^\n]*0\n"), "RestartPolicy 0: " .. def)
    local old = eventd.crash(nc)
    t:assert(not eventd.alive(nc, old), "crash: the old process is gone")
    nc:clock():sleep("3s")
    t:assert_eq(eventd.pid(nc), nil, "and nothing restarted it")
    eventd.start(nc)
    t:assert(eventd.pid(nc) ~= old, "start brought up a new one")
end)

test("stop, start and start_fails", {}, function(t)
    eventd.stop(nc)
    t:assert_eq(eventd.pid(nc), nil, "stop: no process")
    t:assert_eq(eventd.settle(nc).current_operation, nil, "and no operation left")
    eventd.set(nc, "EventStorePath", "sz:/var/state/eventd/pt-nowhere"):assert_ok()
    local state, answered = eventd.start_fails(nc)
    -- Required (eventd-config.reg): put the image's value back, never unset.
    eventd.set(nc, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    t:assert(state ~= "active", "a start against a missing store fails: " .. tostring(state))
    t:assert(not answered, "and never answered")
    t:assert_eq(eventd.pid(nc), nil, "start_fails leaves it stopped")
    eventd.start(nc)
    t:assert(eventd.query(nc, "EVENTS TAKE 1").ok, "start: it answers")
end)

test("edit_store changes a stopped store and eventd starts on it", {}, function(t)
    local origin = eventd.marker("ed")
    eventd.send_log(nc, { origin = origin, is_error = false, message = "before" })
    eventd.wait_rows(nc, "LOGS FROM " .. origin .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    eventd.stop(nc)
    local bytes = eventd.edit_store(nc, eventd.DB.logs,
        "UPDATE logs SET message = 'after' WHERE origin = '" .. origin .. "';")
    t:assert(#bytes > 0, "the edited bytes are returned")
    t:assert(not pcall(nc.stat, nc, eventd.DB.logs .. "-wal"), "no stale -wal is left")
    eventd.start(nc)
    local rows = eventd.rows(nc, "LOGS FROM " .. origin .. " SINCE 10m ago")
    t:assert_eq(rows[1] and rows[1].message, "after", "eventd serves the edited row")
end)

test("run_by_hand runs /usr/sbin/eventd and ends it from the agent", {}, function(t)
    eventd.stop(nc)
    eventd.set(nc, "EventStorePath", "sz:/var/state/eventd/pt-nowhere"):assert_ok()
    local failed = eventd.run_by_hand(nc, {}, { timeout = 20 })
    eventd.set(nc, "EventStorePath", "sz:/var/state/eventd/events/"):assert_ok()
    t:assert(not failed.killed and failed.exit_code ~= 0,
        "a start that cannot open its store exits non-zero by itself: " .. json.encode(failed))
    t:assert(#failed.output > 0, "saying why: " .. failed.output)
    eventd.start(nc)
end)

test("quit_dump captures the whole SIGQUIT dump, and the stderr reader finds it", {}, function(t)
    local d = eventd.quit_dump(nc, { attempts = 3 })
    t:assert(d.complete, "a whole dump: " .. table.concat(d.partial, "; "))
    t:assert_eq(d.status.cause, "clean_exit", "eventd exited cleanly after it")
    t:assert_eq(d.headers, 1, "one header line")
    t:assert_eq(d.lines.boot_id, "{" .. eventd.boot_id(nc) .. "}", "the dump names the boot")
    t:assert(d.lines.queries and d.lines.queries:find("active="), "queries: " .. tostring(d.lines.queries))
    t:assert(eventd.query(nc, "EVENTS TAKE 1").ok, "and eventd was started again")
    local lines = eventd.stderr(nc, "eventd diagnostic dump", { since = d.since })
    t:assert_eq(#lines, 1, "stderr: the header once, de-duplicated")
    local line = eventd.stderr_line(nc, "last_write_errors", d.since)
    t:assert(line and line.message:find("last_write_errors", 1, true), "stderr_line: the dump's last line")
    t:assert_eq(#eventd.stderr(nc, "pt-never-written-" .. eventd.marker(), { timeout = 1 }), 0,
        "and an empty list, not an error, for a line never written")
end)

-- Last, because it leaves this VM's eventd unable to start: a by-hand
-- eventd runs as the agent (SYSTEM), not as eventd's service identity,
-- and what it writes to the stores is not what the service accepts.
test("run_by_hand's timeout ends a by-hand eventd that keeps running, from the agent", {}, function(t)
    eventd.stop(nc)
    local ran = eventd.run_by_hand(nc, {}, { timeout = 5 })
    t:log("a by-hand eventd with a good configuration: killed=" .. tostring(ran.killed)
        .. " exit=" .. tostring(ran.exit_code) .. " signal=" .. tostring(ran.signal)
        .. " output: " .. ran.output:sub(1, 800))
    t:assert(not eventd.alive(nc, ran.pid), "it is gone afterwards")
    if ran.killed then
        t:assert_eq(ran.signal, 9, "ended by the agent's SIGKILL")
    end
end)
