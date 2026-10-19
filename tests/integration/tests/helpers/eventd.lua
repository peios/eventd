-- Helpers for the eventd conformance testset.
--
-- eventd is tested where it runs: the image's own instance, booted by the
-- peinit profile as one Phase 2 service among the rest. A second instance
-- is not an option the way it was for loregd. eventd reads its whole
-- configuration from one fixed registry key and takes no arguments, so two
-- instances on one machine would share it. Everything here therefore drives
-- the real system sink, and a test varies it in three ways:
--
--   * at boot, by seeding values under `Machine\System\eventd` before
--     Phase 2 reads them (`boot{config = …}`) — the only route for a value
--     that applies at restart;
--   * live, with `set` and `unset`, which eventd's registry watch applies;
--   * by restarting the service (`restart`), which re-runs the whole
--     bootstrap sequence against the same stores.
--
-- What goes in comes through the three real channels: `emit` writes a KMES
-- event, `send_log` and `send_metric` send a datagram to the log and metric
-- sockets. What comes out is read through the fourth, the query socket,
-- with `query` (evctl, JSON lines), or straight out of the stores with
-- `sql`, which copies a database to the host and asks sqlite there — the
-- guest ships no sqlite3.
--
-- The agent runs as SYSTEM, so it is admitted to all three sockets
-- (`datagram.rs`: the log socket allows SY and denies only the Service
-- logon group; the metric and query sockets allow SY, BA and Authenticated
-- Users). A test that needs a less privileged caller mints one through
-- `helpers.token` / `helpers.peinit_client` as the rest of the suite does.
--
-- eventd is PIP-signed at the TCB tier, so a shell command cannot signal
-- it or read its /proc; everything that touches eventd's process goes
-- through the agent (`signal`, `freeze`, `fds`, `threads`, `crash`,
-- `run_by_hand` — see "eventd's process, from the agent" below). The
-- service is stopped and started through peinit (`stop`, `start`); a
-- file whose eventd fails or is killed boots with `noncritical`.
--
-- The stores live under /var/state on the live root, an overlay over a
-- tmpfs: they survive a restart of eventd and nothing survives a reboot. A
-- test about reboots or power loss mounts a disk under the store paths from
-- a Phase 1 autorun, which runs before path provisioning stamps them (peinit
-- §2.3 step 7, then step 8); see `boot{disk = …}`.

local peinit = require("helpers.peinit")
local kmes = require("helpers.kmes")
local unixsock = require("helpers.unixsock")
local sys = require("helpers.sys")
local token = require("helpers.token")

local M = {}

M.KEY = [[Machine\System\eventd]]
M.SECURITY = M.KEY .. [[\Security]]

M.SOCKET = {
    query = "/run/eventd/query.sock",
    log = "/run/eventd/log.sock",
    metric = "/run/eventd/metric.sock",
}

M.STORE = {
    events = "/var/state/eventd/events",
    logs = "/var/state/eventd/logs",
    metrics = "/var/state/eventd/metrics",
}

--- The one database file of each single-file store, and the metadata
--- database beside the event shards.
M.DB = {
    logs = M.STORE.logs .. "/logs.db",
    metrics = M.STORE.metrics .. "/metrics.db",
    meta = M.STORE.events .. "/eventd-meta.db",
}

--- The five event types eventd writes about itself (TRM §2.6), spelled
--- once. PEI-617 renamed the old `synthetic.*` family to these catalogue
--- types; the keys keep their old names so no test had to change for the
--- rename alone. Write `eventd.T.startup`, never the literal.
M.T = {
    startup = "eventd.daemon.started",
    shutdown = "eventd.daemon.stopped",
    gap = "eventd.events.lost",
    config_change = "eventd.config.changed",
    storage_error = "eventd.store.quarantined",
}

-- ---------------------------------------------------------------------------
-- MessagePack encoding
--
-- The log and metric channels carry MessagePack, and so does a KMES event's
-- payload. kmes.lua decodes; this encodes. Lua cannot tell an array from a
-- map, an integer-valued float from an integer, or a string from bytes, so
-- each of those is said explicitly when the default guess is wrong:
--
--   eventd.array{…}    an array, even when empty
--   eventd.map{…}      a map, even when empty or holding only [1]..[n]
--   eventd.bin(s)      bin 8/16/32 rather than str
--   eventd.float(x)    float 64 even for a whole number
--   eventd.NIL         nil, inside a table
--
-- Otherwise: a table with [1] is an array, any other table a map with its
-- keys sorted (so the bytes are deterministic), an integer is the smallest
-- integer form, a float is float 64, a string is str.
-- ---------------------------------------------------------------------------

local ARRAY, MAP, BIN, FLOAT = {}, {}, {}, {}
M.NIL = setmetatable({}, { __tostring = function() return "eventd.NIL" end })

function M.array(t) return { [ARRAY] = t or {} } end
function M.map(t) return { [MAP] = t or {} } end
function M.bin(s) return { [BIN] = s } end
function M.float(x) return { [FLOAT] = x } end

local encode

local function encode_int(n)
    if n >= 0 then
        if n < 0x80 then return string.char(n) end
        if n < 0x100 then return "\xcc" .. string.pack(">I1", n) end
        if n < 0x10000 then return "\xcd" .. string.pack(">I2", n) end
        if n < 0x100000000 then return "\xce" .. string.pack(">I4", n) end
        return "\xcf" .. string.pack(">I8", n)
    end
    if n >= -32 then return string.pack("b", n) end
    if n >= -0x80 then return "\xd0" .. string.pack(">i1", n) end
    if n >= -0x8000 then return "\xd1" .. string.pack(">i2", n) end
    if n >= -0x80000000 then return "\xd2" .. string.pack(">i4", n) end
    return "\xd3" .. string.pack(">i8", n)
end

local function encode_str(s, as_bin)
    local n = #s
    if as_bin then
        if n < 0x100 then return "\xc4" .. string.pack(">I1", n) .. s end
        if n < 0x10000 then return "\xc5" .. string.pack(">I2", n) .. s end
        return "\xc6" .. string.pack(">I4", n) .. s
    end
    if n < 32 then return string.char(0xa0 + n) .. s end
    if n < 0x100 then return "\xd9" .. string.pack(">I1", n) .. s end
    if n < 0x10000 then return "\xda" .. string.pack(">I2", n) .. s end
    return "\xdb" .. string.pack(">I4", n) .. s
end

local function encode_array(t)
    local n, parts = #t, {}
    if n < 16 then parts[1] = string.char(0x90 + n)
    elseif n < 0x10000 then parts[1] = "\xdc" .. string.pack(">I2", n)
    else parts[1] = "\xdd" .. string.pack(">I4", n) end
    for i = 1, n do parts[#parts + 1] = encode(t[i]) end
    return table.concat(parts)
end

local function encode_map(t)
    local keys = {}
    for k in pairs(t) do keys[#keys + 1] = k end
    table.sort(keys, function(a, b) return tostring(a) < tostring(b) end)
    local n, parts = #keys, {}
    if n < 16 then parts[1] = string.char(0x80 + n)
    elseif n < 0x10000 then parts[1] = "\xde" .. string.pack(">I2", n)
    else parts[1] = "\xdf" .. string.pack(">I4", n) end
    for _, k in ipairs(keys) do
        parts[#parts + 1] = encode(k)
        parts[#parts + 1] = encode(t[k])
    end
    return table.concat(parts)
end

encode = function(v)
    local ty = type(v)
    if v == nil or v == M.NIL then return "\xc0" end
    if ty == "boolean" then return v and "\xc3" or "\xc2" end
    if ty == "number" then
        if math.type(v) == "integer" then return encode_int(v) end
        return "\xcb" .. string.pack(">d", v)
    end
    if ty == "string" then return encode_str(v) end
    assert(ty == "table", "msgpack: cannot encode a " .. ty)
    if v[ARRAY] then return encode_array(v[ARRAY]) end
    if v[MAP] then return encode_map(v[MAP]) end
    if v[BIN] then return encode_str(v[BIN], true) end
    if v[FLOAT] then return "\xcb" .. string.pack(">d", v[FLOAT]) end
    if v[1] ~= nil or next(v) == nil then return encode_array(v) end
    return encode_map(v)
end

--- Encode a Lua value as MessagePack (see the section comment above).
M.msgpack = encode

-- ---------------------------------------------------------------------------
-- Boot, readiness and the service
-- ---------------------------------------------------------------------------

--- A registry seed setting values under `Machine\System\eventd`.
---
--- `values` is a list of `{name, type, data}` in the shape `reg apply`
--- reads (`type` is "sz", "dword", "qword", "multi", "binary"). The image's
--- own eventd-config.reg has created the key by the time an autoapply seed
--- runs, but every level is named anyway, as a seed must.
function M.config_seed(values, extra_keys)
    local keys = {
        { path = [[Machine\System]] },
        { path = M.KEY, values = values },
    }
    for _, k in ipairs(extra_keys or {}) do keys[#keys + 1] = k end
    return peinit.seed("zz-pt-eventd-config", keys)
end

--- An unreleased eventd, staged over the image's, when the run asks for one.
---
--- `PT_EVENTD_ROOT` names a host directory laid out as a package payload
--- (`usr/sbin/eventd`, `usr/bin/evctl`): normally the unpacked payloads of
--- a `pekit package --local` build, whose binaries are built and PIP-signed
--- exactly as a release's are. Each regular file under it is staged into
--- the root before peinit runs, so every boot of the run starts that eventd
--- from its first instruction. This is how a fix is proven against the
--- suite before it is released. Unset, it stages nothing.
---
---   PT_EVENTD_ROOT=/path/to/root ~/.local/bin/ptrun tests/eventd/
function M.override_files()
    local root = os.getenv("PT_EVENTD_ROOT")
    if not root or root == "" then return {} end
    local files = {}
    local list = assert(io.popen("cd '" .. root .. "' && find usr -type f", "r"))
    for rel in list:lines() do
        local f = assert(io.open(root .. "/" .. rel, "rb"))
        local bytes = f:read("*a")
        f:close()
        local exec = rel:match("^usr/s?bin/") ~= nil
        files[rel] = { bytes, exec = exec }
    end
    list:close()
    assert(next(files), "PT_EVENTD_ROOT " .. root .. " holds no files under usr/")
    return files
end

--- Boot a peinit VM and wait until eventd answers a query.
---
--- opts (all optional):
---   name, memory, cpus, append, boot, stage — as `peinit.boot`
---   config  a `config_seed` values list, applied before Phase 2
---   files   further `peinit.stage` files, merged with the seed
---   wait    false to skip waiting for eventd (a test that expects it
---           not to start)
---   noncritical  seed eventd's own service definition so that a failing,
---           killed or stopped eventd never reboots the machine nor is
---           restarted behind the test's back; see `service_seed`. `true`
---           is ErrorControl=0 (Normal) and RestartPolicy=0 (Never).
---   store_disk   see `store_disk_files`
---
--- Each boot is one VM against the file's `peinit.claim`. One vCPU unless
--- a test is about per-CPU structure: the KMES ring buffers, drain threads
--- and the default shard count are one per CPU, and on one vCPU there is
--- exactly one of each.
function M.boot(opts)
    opts = opts or {}
    local files = M.override_files()
    if opts.noncritical then
        local nc = opts.noncritical == true and {} or opts.noncritical
        for k, v in pairs(M.service_seed(nc)) do files[k] = v end
    end
    if opts.config or opts.config_keys then
        for k, v in pairs(M.config_seed(opts.config or {}, opts.config_keys)) do files[k] = v end
    end
    for k, v in pairs(opts.files or {}) do files[k] = v end
    local boot = opts.boot
    if opts.store_disk then
        for k, v in pairs(M.store_disk_files()) do files[k] = v end
        boot = {}
        for k, v in pairs(opts.boot or {}) do boot[k] = v end
        local disks = {}
        for _, d in ipairs(boot.disks or {}) do disks[#disks + 1] = d end
        for _, store in ipairs(M.STORE_DISK_ORDER) do
            disks[#disks + 1] = { id = M.STORE_DISK[store], mediated = true,
                                  scratch = opts.store_disk.size or "256M" }
        end
        boot.disks = disks
    end
    local vm = peinit.boot({
        name = opts.name or "ev",
        memory = opts.memory,
        cpus = opts.cpus,
        append = opts.append,
        boot = boot,
        stage = opts.stage,
        files = next(files) and files or nil,
    })
    if opts.wait ~= false then M.ready(vm) end
    return vm
end

--- eventd's service definition key.
M.SERVICE = [[Machine\System\Services\eventd]]

--- A registry seed (staged file `zz-pt-eventd-svc`) overriding values of
--- eventd's own service definition, for a file whose eventd is meant to
--- fail, be killed or be stopped. In the image eventd is ErrorControl
--- Critical with RestartPolicy OnFailure and RestartMaxRetries 5: killed
--- it is restarted behind the test's back, and the sixth failed start in
--- a row reboots the machine.
---
--- opts (all optional):
---   restart      RestartPolicy to seed (default 0, Never); `false` leaves
---                the image's own
---   max_retries  RestartMaxRetries to seed (default: not seeded)
---   values       further `{name, type, data}` values under the key
---                (StopTimeout, PostKillTimeout, …)
---
--- ErrorControl is always seeded 0 (Normal).
function M.service_seed(opts)
    opts = opts or {}
    local values = { { name = "ErrorControl", type = "dword", data = 0 } }
    local restart = opts.restart
    if restart == nil then restart = 0 end
    if restart ~= false then
        values[#values + 1] = { name = "RestartPolicy", type = "dword", data = restart }
    end
    if opts.max_retries then
        values[#values + 1] = { name = "RestartMaxRetries", type = "dword", data = opts.max_retries }
    end
    for _, v in ipairs(opts.values or {}) do values[#values + 1] = v end
    return peinit.seed("zz-pt-eventd-svc", {
        { path = [[Machine\System]] },
        { path = [[Machine\System\Services]] },
        { path = M.SERVICE, values = values },
    })
end

--- The ids of the three store disks `boot{store_disk = {…}}` attaches,
--- one per store, in attach order: /dev/vdb, /dev/vdc, /dev/vdd.
M.STORE_DISK = { events = "ev-events", logs = "ev-logs", metrics = "ev-metrics" }
M.STORE_DISK_ORDER = { "events", "logs", "metrics" }

--- The descriptor eventd requires on each store directory (`directory.rs`
--- REQUIRED_SDDL), used as the store disk's synthesis template so the
--- mount root and anything created without a descriptor inherit it.
M.STORE_SDDL = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)" ..
    "(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)"

--- The staged files that put eventd's stores on disks which survive a
--- power cut and a `vm:reset()`.
---
--- The live root is an overlay over tmpfs, so a store there is gone after
--- a reboot. `boot{store_disk = {size = "256M"}}` attaches one mediated
--- scratch disk per store and stages this Phase 1 autorun, which formats
--- each on the first boot only and mounts it over its store directory.
--- Autoruns run before path provisioning (peinit §2.3 steps 7 and 8), so
--- eventd starts in Phase 2 on the disks.
---
--- One disk per store, mounted at the store directory itself, because a
--- mount root takes the synthesis template verbatim — exactly the
--- descriptor eventd requires. A store directory *created* on a fresh
--- filesystem by path provisioning instead carries the generic-mapped form
--- (GENERIC_ALL as FILE_ALL_ACCESS), which eventd's exact comparison
--- refuses (PEI-1316).
---
--- ext4 is deny-missing under KACS, and a fresh filesystem carries no
--- descriptors, so each mount is adopted as synth-persist with eventd's
--- required descriptor as the template: a synthesised descriptor is
--- written back, and survives the cut with the data.
---
--- The profile attaches its ISO medium first, so the disks are /dev/vdb
--- (events), /dev/vdc (logs) and /dev/vdd (metrics).
--- `vm:disk(eventd.STORE_DISK.events):power_cut()` drops what that store
--- never flushed.
function M.store_disk_files()
    local lines = {
        "#!/bin/sh",
        "# Staged by helpers/eventd.lua: eventd's stores on mediated disks.",
        "set -eu",
        "store() {",
        "    blkid \"$1\" >/dev/null 2>&1 || mkfs.ext4 -F -q \"$1\"",
        "    mount -t ext4 -o policy=synth-persist --synth-sddl '" .. M.STORE_SDDL ..
            "' \"$1\" \"/var/state/eventd/$2\"",
        "}",
        "store /dev/vdb events",
        "store /dev/vdc logs",
        "store /dev/vdd metrics",
        "",
    }
    return { ["lcl/policy/autorun.d/05-pt-eventd-store.sh"] = { table.concat(lines, "\n"), exec = true } }
end

--- Write `text` to a fresh guest file and return its path.
local seq = 0
local function guest_tmp(vm, text, stem)
    seq = seq + 1
    local path = string.format("/tmp/pt-eventd-%s-%d", stem or "f", seq)
    vm:write_file(path, text)
    return path
end
M.guest_tmp = guest_tmp

--- Run a query through evctl and decode its JSON lines.
---
--- Returns a table:
---   ok         exit status 0
---   exit_code  evctl's status: 0 success, 1 eventd answered with an error
---              or the channel failed, 2 a usage error
---   rows       the decoded records, in order (empty when not ok)
---   stdout, stderr
---
--- The query is passed by file, never on a command line, so a test can
--- send any text — quotes, backslashes, newlines — without a shell
--- between it and eventd. `opts.socket` overrides the query socket.
function M.query(vm, text, opts)
    opts = opts or {}
    local path = guest_tmp(vm, text, "q")
    local cmd = "evctl --format jsonl --file " .. path
    if opts.socket then cmd = cmd .. " --socket '" .. opts.socket .. "'" end
    local r = vm:run(cmd)
    local out = { exit_code = r.exit_code, ok = r.exit_code == 0,
                  stdout = r.stdout, stderr = r.stderr, rows = {} }
    if out.ok then
        for line in r.stdout:gmatch("[^\n]+") do
            local okd, row = pcall(json.decode, line)
            assert(okd, "evctl emitted a line that is not JSON: " .. line)
            out.rows[#out.rows + 1] = row
        end
    end
    return out
end

--- `query`, asserting success, returning the rows.
function M.rows(vm, text, opts)
    local r = M.query(vm, text, opts)
    assert(r.ok, "query failed (exit " .. tostring(r.exit_code) .. "): " ..
        text .. "\nstderr: " .. tostring(r.stderr))
    return r.rows
end

--- Poll a query until `pred(rows)` is true. Ingestion is asynchronous on
--- every channel — a record is queryable once its writer commits, not
--- when the sender returns — so a test that sends and then reads must
--- wait rather than read once.
---
--- Returns `rows, true`: the rows that satisfied `pred`. On timeout it
--- RAISES (the wait's `desc` and the last failure in the message), so a
--- bare call is a barrier that fails the test. With `opts.raise = false`
--- it returns `rows, false` instead, `rows` being the last answer read
--- (`{}` if no query succeeded).
---
--- opts: timeout (s, default 30), interval (default 0.25), desc,
--- socket (the query socket, as `query`), raise (default true).
function M.wait_rows(vm, text, pred, opts)
    opts = opts or {}
    local last, err = {}, nil
    local function probe()
        local r = M.query(vm, text, { socket = opts.socket })
        if not r.ok then err = r.stderr; return false end
        last = r.rows
        return pred(r.rows)
    end
    local wopts = { timeout = opts.timeout or 30, interval = opts.interval or 0.25,
                    desc = opts.desc or ("rows for: " .. text) }
    if opts.raise == false then
        local ok = pcall(wait_until, probe, wopts)
        return last, ok
    end
    local ok, why = pcall(wait_until, probe, wopts)
    if not ok then
        error(tostring(why) .. (err and ("\nlast query error: " .. tostring(err)) or "")
            .. "\nlast rows: " .. json.encode(last), 2)
    end
    return last, true
end

--- Wait until eventd answers a query on its socket: the service has
--- signalled readiness, which by §8.2 is after every store and socket is
--- open and `eventd.daemon.started` is durably committed.
---
--- The second argument is a timeout in seconds (default 90) or an opts
--- table `{timeout = s, socket = path}`; `socket` probes a query socket
--- other than the default.
function M.ready(vm, opts)
    if type(opts) ~= "table" then opts = { timeout = opts } end
    -- Typed, so the probe plans against one catalogued type rather than
    -- all of them: an untyped EVENTS query fails outright when any
    -- catalogued type name is unusable as a descriptor path, and a test
    -- that plants one would otherwise never see eventd come ready.
    wait_until(function()
        return M.query(vm, "EVENTS " .. M.T.startup .. " TAKE 1", { socket = opts.socket }).ok
    end, { timeout = opts.timeout or 90, interval = 0.5,
           desc = "eventd to answer on " .. (opts.socket or "its query socket") })
end

--- `svctl --json status eventd`, decoded.
function M.status(vm)
    local r = vm:run("svctl --json status eventd")
    assert(r.exit_code == 0, "svctl status eventd: " .. r.stdout .. r.stderr)
    return json.decode(r.stdout)
end

--- eventd's main pid, or nil when it is not running. From the service
--- manager rather than a process scan: the image ships no pidof.
function M.pid(vm)
    local r = vm:run("svctl --json status eventd")
    if r.exit_code ~= 0 then return nil end
    return tonumber(r.stdout:match('"pid":(%d+)'))
end

--- Restart eventd through the service manager and wait until it answers
--- again with a new process. Returns the new pid.
---
--- opts (optional): socket — wait for the answer on this query socket
--- (a restart that moves QuerySocketPath); timeout — for the readiness
--- wait (default 90).
function M.restart(vm, opts)
    opts = opts or {}
    local before = M.pid(vm)
    vm:run("svctl restart eventd"):assert_ok()
    wait_until(function()
        local now = M.pid(vm)
        return now ~= nil and now ~= before
    end, { timeout = 60, interval = 0.25, desc = "a new eventd process" })
    M.ready(vm, { socket = opts.socket, timeout = opts.timeout })
    return M.pid(vm)
end

-- ---------------------------------------------------------------------------
-- Configuration at runtime
-- ---------------------------------------------------------------------------

--- `reg set` one value under the eventd key (or `opts.key`). `value` is
--- `reg`'s typed form: "dword:64", "sz:/run/x", "qword:5".
function M.set(vm, name, value, opts)
    local key = (opts and opts.key) or M.KEY
    return vm:run(string.format("reg set '%s' '%s' '%s'", key, name, value))
end

--- `reg del` one value under the eventd key.
function M.unset(vm, name, opts)
    local key = (opts and opts.key) or M.KEY
    return vm:run(string.format("reg del '%s' '%s'", key, name))
end

-- ---------------------------------------------------------------------------
-- The three ingestion channels
-- ---------------------------------------------------------------------------

--- Emit a KMES event as `who` (default the agent, SYSTEM). `payload` is a
--- Lua value encoded with `msgpack`, or raw bytes via `{raw = "…"}`.
--- Returns the kmes_emit result (`r.ret == 0` on success).
function M.emit(who, event_type, payload)
    local bytes
    if type(payload) == "table" and payload.raw then
        bytes = payload.raw
    elseif payload == nil then
        bytes = M.msgpack(M.map{})
    else
        bytes = M.msgpack(payload)
    end
    return kmes.emit(who, event_type, bytes)
end

local function send(who, path, bytes, pass_token, allow_error)
    local fd, errno = unixsock.socket(who, unixsock.AF_UNIX, unixsock.SOCK.DGRAM)
    assert(fd, "socket: " .. tostring(errno))
    if pass_token then
        local p = unixsock.set_pass_token(who, fd, true)
        assert(p.ret == 0, "KACS_SO_PASS_TOKEN: errno " .. tostring(p.errno))
    end
    local r = unixsock.sendto(who, fd, bytes, path)
    who:syscall(3, fd) -- close
    assert(allow_error or r.ret == #bytes, string.format(
        "sendto %s: sent %s/%d bytes; errno %s", path, tostring(r.ret), #bytes, tostring(r.errno)))
    return r
end

--- Send one log datagram. `record` is a record map, a list of them (one
--- datagram, an array), or `{raw = "…"}` bytes. Returns the sendto result.
---
--- No token is attached: the log channel's only producer is peinit, and
--- the socket's descriptor is its whole access control (§7.1).
--- `opts.path` overrides the socket, `opts.pass_token` attaches one.
--- Requires the whole datagram to be sent; failure reports the path, byte
--- counts and errno after closing the temporary socket. Negative syscall
--- tests may pass `opts.allow_error = true` and must check the returned result.
function M.send_log(who, record, opts)
    opts = opts or {}
    local bytes = record.raw or M.msgpack(record)
    return send(who, opts.path or M.SOCKET.log, bytes, opts.pass_token, opts.allow_error)
end

--- Send one metric datagram; as `send_log`, except that the sender's
--- token IS attached by default. eventd checks EVENTD_PUBLISH per record
--- against that token (§7.6) and counts a datagram arriving without one as
--- `metric_missing_identity`, storing nothing — so a test that wants the
--- record stored must send with `KACS_SO_PASS_TOKEN` set, and a test about
--- that rejection passes `{pass_token = false}`.
function M.send_metric(who, record, opts)
    opts = opts or {}
    local bytes = record.raw or M.msgpack(record)
    local pass = opts.pass_token
    if pass == nil then pass = true end
    return send(who, opts.path or M.SOCKET.metric, bytes, pass, opts.allow_error)
end

--- A process-unique marker, for telling this test's records from
--- everything else the system writes into the same stores: `pt`, the
--- stem, then digits. A stem that already starts with `pt` is not given
--- a second one (`marker("ptval")` is `ptval…`, not `ptptval…`).
local marker_seq = 0
function M.marker(stem)
    marker_seq = marker_seq + 1
    stem = stem or ""
    if stem:sub(1, 2) == "pt" then stem = stem:sub(3) end
    return string.format("pt%s%d%d", stem, os.time() % 100000, marker_seq)
end

-- ---------------------------------------------------------------------------
-- The stores, read directly
-- ---------------------------------------------------------------------------

--- A fresh temporary directory on the host; the caller removes it.
local function host_tmpdir()
    local p = assert(io.popen("mktemp -d", "r"))
    local dir = p:read("l")
    p:close()
    return dir
end
M.host_tmpdir = host_tmpdir

--- Write `bytes` to a host file.
local function host_write(path, bytes)
    local f = assert(io.open(path, "wb"))
    f:write(bytes)
    f:close()
end
M.host_write = host_write

--- The whole of a host file.
local function host_read(path)
    local f = assert(io.open(path, "rb"))
    local s = f:read("a")
    f:close()
    return s
end
M.host_read = host_read

--- Run SQL against a copy of a guest SQLite database, on the host.
---
--- `db` is a guest path (`eventd.DB.logs`, a shard under
--- `eventd.STORE.events`). The database and its `-wal`, if present, are
--- copied out together, so committed transactions still in the WAL are
--- seen. Returns a list of rows, each a list of column values (JSON
--- types; blobs as lowercase hex).
---
--- The copy is not a snapshot: a writer committing while the two files
--- are read can leave them inconsistent. Read schema, metadata and rows a
--- test has already waited for, not a store under load.
local sql_once

function M.sql(vm, db, query)
    -- The two files are read one after the other, so a writer committing
    -- in between can leave a copy SQLite rejects. That is a torn copy,
    -- not a broken store: take another, several times, before giving up.
    -- A store under a flood tears often, so the budget is generous.
    local err
    for _ = 1, 10 do
        local ok, rows = pcall(sql_once, vm, db, query)
        if ok then return rows end
        err = rows
        vm:clock():sleep("250ms")
    end
    error(err, 2)
end

sql_once = function(vm, db, query)
    local dir = host_tmpdir()
    host_write(dir .. "/db", vm:read_file(db))
    local okw, wal = pcall(vm.read_file, vm, db .. "-wal")
    if okw and wal and #wal > 0 then host_write(dir .. "/db-wal", wal) end
    host_write(dir .. "/q.sql", query)
    local script = [[
import json, sqlite3, sys
d = sys.argv[1]
c = sqlite3.connect(d + "/db")
q = open(d + "/q.sql").read()
rows = c.execute(q).fetchall()
def v(x):
    return x.hex() if isinstance(x, (bytes, bytearray)) else x
print(json.dumps([[v(x) for x in r] for r in rows]))
]]
    host_write(dir .. "/run.py", script)
    local p = assert(io.popen("python3 " .. dir .. "/run.py " .. dir .. " 2>&1", "r"))
    local out = p:read("a")
    local ok = p:close()
    os.execute("rm -rf '" .. dir .. "'")
    assert(ok, "sqlite on host failed: " .. out)
    return json.decode(out)
end

--- The CREATE statements of a guest database, by object name.
function M.schema(vm, db)
    local out = {}
    for _, r in ipairs(M.sql(vm, db, "SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL")) do
        out[r[1]] = r[2]
    end
    return out
end

--- The event shard files currently in the event store, sorted.
function M.shards(vm)
    local out = {}
    for _, name in ipairs(vm:listdir(M.STORE.events)) do
        local n = type(name) == "table" and name.name or name
        if n:match("^shard%-%d+%.db$") then out[#out + 1] = M.STORE.events .. "/" .. n end
    end
    table.sort(out)
    return out
end

--- Events of `event_type` committed to the store, counted in the shard
--- files themselves (a query would itself be an access check).
function M.stored_count(vm, event_type)
    local n = 0
    for _, shard in ipairs(M.shards(vm)) do
        n = n + M.sql(vm, shard, "SELECT count(*) FROM events WHERE event_type = '"
            .. event_type .. "'")[1][1]
    end
    return n
end

-- ---------------------------------------------------------------------------
-- Editing a store while eventd is stopped
--
-- The guest ships no sqlite3, so a store is changed on the host: copied
-- out with its WAL, edited by the host's Python sqlite3, checkpointed so
-- the main file is the whole database, and written back with the guest's
-- stale -wal and -shm removed. eventd must be stopped (`stop`) first: a
-- running eventd holds the file open and would write over the edit.
-- ---------------------------------------------------------------------------

local EDIT_PY = [[
import sqlite3, sys
d = sys.argv[1]
c = sqlite3.connect(d + "/db")
c.executescript(open(d + "/edit.sql").read())
c.commit()
c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
if len(sys.argv) > 2 and sys.argv[2] == "delete":
    c.execute("PRAGMA journal_mode=DELETE")
c.close()
]]

--- A copy of guest database `db` with `script` (SQL, any number of
--- statements) applied on the host, returned as bytes with its WAL folded
--- in. Nothing in the guest changes.
---
--- opts: journal = "delete" to leave the copy in rollback-journal mode
--- rather than WAL.
function M.derive_store(vm, db, script, opts)
    opts = opts or {}
    local dir = host_tmpdir()
    host_write(dir .. "/db", vm:read_file(db))
    local okw, wal = pcall(vm.read_file, vm, db .. "-wal")
    if okw and wal and #wal > 0 then host_write(dir .. "/db-wal", wal) end
    host_write(dir .. "/edit.sql", script)
    host_write(dir .. "/run.py", EDIT_PY)
    local p = assert(io.popen("python3 " .. dir .. "/run.py " .. dir ..
        (opts.journal == "delete" and " delete" or "") .. " 2>&1", "r"))
    local out = p:read("a")
    local ok = p:close()
    local bytes = ok and host_read(dir .. "/db") or nil
    os.execute("rm -rf '" .. dir .. "'")
    assert(ok, "editing " .. db .. " on the host failed: " .. out)
    return bytes
end

--- Remove guest database `db` and its -wal and -shm, whichever exist.
function M.remove_db(vm, db)
    for _, suffix in ipairs({ "", "-wal", "-shm" }) do pcall(vm.unlink, vm, db .. suffix) end
end

--- Apply `script` to guest database `db` while eventd is stopped (see the
--- section comment), writing the result over `opts.to` (default `db`).
--- Returns the bytes written.
---
--- opts: to — the guest path to write (a copy elsewhere, a new shard);
--- journal = "delete" — leave the file in rollback-journal mode.
function M.edit_store(vm, db, script, opts)
    opts = opts or {}
    local bytes = M.derive_store(vm, db, script, opts)
    local to = opts.to or db
    pcall(vm.unlink, vm, to .. "-wal")
    pcall(vm.unlink, vm, to .. "-shm")
    vm:write_file(to, bytes)
    return bytes
end

-- ---------------------------------------------------------------------------
-- The service: stop, start, settle
--
-- Through `svctl`, which talks to peinit; never by signalling the process
-- from the shell (see "eventd's process" below).
-- ---------------------------------------------------------------------------

--- Wait until peinit has no operation in progress on eventd
--- (`current_operation` null): after a stop that ended in a kill, the
--- stop outlives the process by peinit's post-kill check, and a start
--- issued inside that window is refused. Returns the decoded status.
function M.settle(vm, timeout)
    local raw
    wait_until(function()
        raw = vm:run("svctl --json status eventd").stdout
        return raw:find('"current_operation":null', 1, true) ~= nil
    end, { timeout = timeout or 60, interval = 0.25, desc = "peinit's operation on eventd to finish" })
    return json.decode(raw)
end

--- Stop eventd through peinit and wait until its process is gone and the
--- stop operation has finished. Returns the `svctl stop` result.
---
--- svctl's exit status is not asserted (stopping a failed or stopped
--- eventd is not an error here); the process being gone is.
---
--- opts: wait = false — issue the stop and return at once (a test about
--- a stop in progress); timeout — for the process to go (default 60).
function M.stop(vm, opts)
    opts = opts or {}
    local r = vm:run("svctl stop eventd")
    if opts.wait == false then return r end
    wait_until(function() return M.pid(vm) == nil end,
        { timeout = opts.timeout or 60, interval = 0.25, desc = "eventd to stop" })
    M.settle(vm)
    return r
end

--- Start eventd through peinit (after `settle`) and wait until it
--- answers (`ready`). Returns the `svctl start` result, whose exit status
--- is not asserted: readiness is the check.
---
--- opts: wait = false — do not wait for readiness; timeout, socket — as
--- `ready`.
function M.start(vm, opts)
    opts = opts or {}
    M.settle(vm)
    local r = vm:run("svctl start eventd")
    if opts.wait ~= false then M.ready(vm, { timeout = opts.timeout, socket = opts.socket }) end
    return r
end

--- Start eventd expecting the start to fail. Waits for the attempt to
--- resolve, notes whether eventd answered a query, then stops it and
--- resets its restart budget (`svctl reset`), so it is left stopped.
---
--- Needs a `boot{noncritical = …}` VM: in the image a failed start is
--- retried and the sixth reboots the machine.
---
--- Returns the state the start reached ("failed", "active", …) and
--- whether eventd answered a query.
function M.start_fails(vm, opts)
    opts = opts or {}
    vm:run("svctl start eventd")
    local state
    pcall(wait_until, function()
        state = M.status(vm).state
        return state ~= "starting" and state ~= "activating"
    end, { timeout = opts.timeout or 30, interval = 0.25, desc = "eventd's start to resolve" })
    local answered = M.query(vm, "EVENTS TAKE 1").ok
    vm:run("svctl stop eventd")
    vm:run("svctl reset eventd")
    return state, answered
end

-- ---------------------------------------------------------------------------
-- eventd's process, from the agent
--
-- eventd is PIP-signed at the TCB tier. PIP refuses a process that does
-- not dominate it every signal and every /proc read of it. The provium
-- agent is signed at the same tier, so the agent's OWN operations —
-- `vm:syscall`, `vm:read_file`, `vm:listdir`, `vm:stat`, `vm:write_file`
-- and a worker's syscalls — dominate eventd; a command run through
-- `vm:run`/`vm:run_async`/`w:run` is a fresh, unsigned exec and is
-- refused. So `kill`, `cat /proc/<pid>/…`, `ls /proc/<pid>/fd` and
-- `timeout N /usr/sbin/eventd` in the shell do not work against eventd;
-- everything below goes through the agent.
--
-- `who` is a VM (or a worker of it); `pid` is eventd's main pid, and
-- where it is optional, nil means `eventd.pid(vm)` now.
-- ---------------------------------------------------------------------------

local NR_KILL = 62

--- x86_64 signal numbers, by name.
M.SIG = {
    HUP = 1, INT = 2, QUIT = 3, KILL = 9, USR1 = 10, USR2 = 12, PIPE = 13,
    ALRM = 14, TERM = 15, CONT = 18, STOP = 19,
}

local function signum(sig)
    if math.type(sig) == "integer" then return sig end
    local name = tostring(sig):upper():gsub("^SIG", "")
    return assert(M.SIG[name], "eventd: unknown signal " .. tostring(sig))
end

local function pid_of(who, pid)
    return pid or assert(M.pid(who), "eventd is not running")
end

--- kill(2) from the agent: send `sig` (a number, or a name such as
--- "TERM", "KILL", "STOP", "CONT", "QUIT", "HUP", "INT", "USR1", "PIPE",
--- with or without "SIG") to `pid`. Asserts success unless
--- `opts.check == false`. Returns the raw result (`ret`, `errno`).
function M.signal(who, pid, sig, opts)
    pid = pid_of(who, pid)
    local r = who:syscall(NR_KILL, { args = { pid, signum(sig) } })
    if not (opts and opts.check == false) then
        assert(r.ret == 0, "kill(" .. pid .. ", " .. tostring(sig) .. "): " .. sys.errname(r.errno or 0))
    end
    return r
end

--- SIGSTOP eventd (default: the running one). Returns the pid stopped.
function M.freeze(who, pid)
    pid = pid_of(who, pid)
    M.signal(who, pid, "STOP")
    return pid
end

--- SIGCONT eventd (default: the running one). Returns the pid.
function M.thaw(who, pid)
    pid = pid_of(who, pid)
    M.signal(who, pid, "CONT")
    return pid
end

--- Whether process `pid` exists (kill(pid, 0); a zombie not yet reaped
--- still counts, as `test -d /proc/<pid>` did).
function M.alive(who, pid)
    local r = who:syscall(NR_KILL, { args = { pid, 0 } })
    return r.ret == 0 or r.errno == sys.E.PERM
end

--- Wait until process `pid` is gone. Returns true when it is, false
--- when `timeout` (seconds, default 30) lapsed first. Never raises.
function M.wait_gone(who, pid, timeout)
    return (pcall(wait_until, function() return not M.alive(who, pid) end,
        { timeout = timeout or 30, interval = 0.1, desc = "process " .. pid .. " to exit" }))
end

--- Crash eventd: SIGKILL it and wait until it is gone. Returns the pid
--- killed. Use a `boot{noncritical = true}` VM, or peinit restarts it at
--- once (and repeated kills count toward a Critical reboot).
---
--- opts: pid (default the running eventd); timeout (default 10).
function M.crash(vm, opts)
    opts = opts or {}
    local pid = pid_of(vm, opts.pid)
    M.signal(vm, pid, "KILL")
    assert(M.wait_gone(vm, pid, opts.timeout or 10), "eventd (" .. pid .. ") to die on SIGKILL")
    return pid
end

--- eventd's open descriptors, from /proc/<pid>/fd and fdinfo read by the
--- agent. A list sorted by fd of
---   { fd = n, path = readlink target ("socket:[123]", "anon_inode:…",
---     "/var/state/…"), flags = the fdinfo flags as a number,
---     mode = flags & 3 (0 O_RDONLY, 1 O_WRONLY, 2 O_RDWR),
---     rdwr = mode == 2, ino = the target's inode (only with opts.ino) }
--- A descriptor closed between the listing and its read is left out.
---
--- opts: ino = true to stat each target; by_fd = true to return a map
--- fd -> entry instead of a list.
function M.fds(who, pid, opts)
    opts = opts or {}
    pid = pid_of(who, pid)
    local base = "/proc/" .. pid
    local ok, names = pcall(who.listdir, who, base .. "/fd")
    assert(ok, "eventd.fds: cannot list " .. base .. "/fd: " .. tostring(names))
    local list, map = {}, {}
    for _, e in ipairs(names) do
        local n = type(e) == "table" and e.name or e
        local path = sys.readlink(who, base .. "/fd/" .. n)
        local okf, info = pcall(who.read_file, who, base .. "/fdinfo/" .. n)
        local flags = okf and info and info:match("flags:%s*(%d+)")
        if path and flags then
            flags = tonumber(flags, 8)
            local entry = { fd = tonumber(n), path = path, flags = flags,
                            mode = flags & 3, rdwr = (flags & 3) == 2 }
            if opts.ino then
                local st = sys.stat(who, base .. "/fd/" .. n)
                entry.ino = st and st.ino
            end
            list[#list + 1] = entry
            map[entry.fd] = entry
        end
    end
    table.sort(list, function(a, b) return a.fd < b.fd end)
    if opts.by_fd then return map end
    return list
end

--- Read-write and read-only descriptor counts eventd holds on `path`
--- itself (not its -wal or -shm), and the matching `fds` entries.
--- Returns rw, ro, entries. `opts` as `fds` (`{ino = true}`).
---
--- Three values: where only the read-write count is wanted inside a
--- call (`math.max(n, …)`), parenthesise it: `(eventd.fds_on(…))`.
function M.fds_on(who, pid, path, opts)
    local rw, ro, entries = 0, 0, {}
    for _, e in ipairs(M.fds(who, pid, { ino = opts and opts.ino })) do
        if e.path == path then
            entries[#entries + 1] = e
            if e.mode == 2 then rw = rw + 1 elseif e.mode == 0 then ro = ro + 1 end
        end
    end
    return rw, ro, entries
end

--- The fd numbers in `fds` (a list or map from `fds`) open on `path`
--- with access mode `mode` (0, 1 or 2; nil for any), sorted.
function M.fd_numbers(fds, path, mode)
    local out = {}
    for _, e in pairs(fds) do
        if e.path == path and (mode == nil or e.mode == mode) then out[#out + 1] = e.fd end
    end
    table.sort(out)
    return out
end

--- eventd's descriptors as text, one `<fd> -> <target>` line each in fd
--- order: the arrow part of `ls -l /proc/<pid>/fd`, so a pattern written
--- against that output (`socket:%[(%d+)%]`, `anon_inode:kmes%-cpu`,
--- `(%d+) %-> /path$`, `%-> (/proc/%S+)`) matches unchanged.
function M.fd_listing(who, pid)
    local lines = {}
    for _, e in ipairs(M.fds(who, pid)) do lines[#lines + 1] = e.fd .. " -> " .. e.path end
    return table.concat(lines, "\n") .. (#lines > 0 and "\n" or "")
end

--- eventd's threads: a list of `{tid = n, comm = name}` sorted by tid
--- (comm is the kernel's, truncated to 15 bytes), and the pid read.
function M.threads(who, pid)
    pid = pid_of(who, pid)
    local base = "/proc/" .. pid .. "/task"
    local ok, names = pcall(who.listdir, who, base)
    assert(ok, "eventd.threads: cannot list " .. base .. ": " .. tostring(names))
    local out = {}
    for _, e in ipairs(names) do
        local n = type(e) == "table" and e.name or e
        local okc, comm = pcall(who.read_file, who, base .. "/" .. n .. "/comm")
        if okc and comm then
            out[#out + 1] = { tid = tonumber(n), comm = (comm:gsub("\n$", "")) }
        end
    end
    table.sort(out, function(a, b) return a.tid < b.tid end)
    return out, pid
end

--- eventd's thread names (comm) in tid order, as a list and as one
--- newline-joined string (what `cat /proc/<pid>/task/*/comm` printed).
function M.thread_names(who, pid)
    local names = {}
    for _, th in ipairs(M.threads(who, pid)) do names[#names + 1] = th.comm end
    return names, table.concat(names, "\n") .. (#names > 0 and "\n" or "")
end

--- /proc/<pid>/status, parsed: a map from field name to its value with
--- surrounding whitespace removed (`st.PPid == "1"`, `st.VmRSS ==
--- "1234 kB"`), and the raw text.
function M.proc_status(who, pid)
    pid = pid_of(who, pid)
    local text = who:read_file("/proc/" .. pid .. "/status")
    local out = {}
    for k, v in text:gmatch("([^:\n]+):%s*([^\n]*)") do out[k] = (v:gsub("%s+$", "")) end
    return out, text
end

--- sched_setscheduler(2) on thread `tid` from the agent; asserts
--- success. `policy` is SCHED_OTHER 0, SCHED_FIFO 1, SCHED_RR 2,
--- SCHED_BATCH 3, SCHED_IDLE 5.
function M.set_policy(who, tid, policy, priority)
    local r = who:syscall(144, {
        args = { tid, policy, 0 }, bufs = { string.pack("<i4", priority or 0) }, ptrs = { 2 },
    })
    assert(r.ret == 0, "sched_setscheduler(" .. tid .. ", " .. policy .. "): " .. sys.errname(r.errno or 0))
    return r
end

M.BINARY = "/usr/sbin/eventd"

--- Run /usr/sbin/eventd by hand, for a startup that is expected to fail
--- (the service must be stopped). Replaces `timeout N /usr/sbin/eventd`:
--- the signed binary is protected even started by hand, so neither
--- `timeout` nor `kill` in the shell can end it. It is started with
--- `vm:run_async`, its exit waited for (`opts.timeout` seconds, default
--- 30), and SIGKILLed from the agent if it is still running then.
---
--- Returns { exit_code, stdout, stderr, output = stderr .. stdout,
--- signal (the killing signal, or nil), killed = true when the timeout
--- ended it, pid }.
function M.run_by_hand(vm, args, opts)
    opts = opts or {}
    local proc = vm:run_async(M.BINARY, { args = args or {} })
    local pid = proc:pid()
    local exited = pcall(wait_until, function() return proc:status() == "exited" end,
        { timeout = opts.timeout or 30, interval = 0.1, desc = "eventd run by hand to exit" })
    local killed = false
    if not exited then
        M.signal(vm, pid, "KILL", { check = false })
        killed = true
    end
    local r = proc:wait("30s")
    return { exit_code = r.exit_code, stdout = r.stdout, stderr = r.stderr,
             output = (r.stderr or "") .. (r.stdout or ""), signal = r.signal,
             killed = killed, pid = pid }
end

-- ---------------------------------------------------------------------------
-- Clock, boot ID, hashes and GUIDs
-- ---------------------------------------------------------------------------

--- The guest's realtime clock in nanoseconds, an integer. Every
--- timestamp eventd stores is the guest's; the host's `os.time()` runs
--- ahead of it. `who` is a VM (a worker has no clock: pass its VM).
function M.guest_ns(who)
    local ok, ns = pcall(function() return who:clock():get_ns() end)
    if ok and ns then return math.tointeger(ns) end
    return math.tointeger(tonumber(who:run("date +%s%N").stdout:match("%d+")))
end

--- The kernel boot ID in its canonical text form (as read now, so a bind
--- mount over /proc/sys/kernel/random/boot_id is honoured).
function M.boot_id(who)
    return (who:read_file("/proc/sys/kernel/random/boot_id"):match("[%x%-]+"))
end

--- A boot ID in the PCDS byte layout eventd stores (the first three
--- groups byte-reversed), as the uppercase hex SQLite's hex() prints.
--- `x` is a VM (its current boot ID) or a canonical UUID string.
function M.boot_pcds_hex(x)
    local c = (type(x) == "string" and x or M.boot_id(x)):gsub("%-", ""):lower()
    assert(#c == 32, "boot_pcds_hex: not a UUID: " .. tostring(c))
    local function rev(h)
        local out = {}
        for i = #h - 1, 1, -2 do out[#out + 1] = h:sub(i, i + 1) end
        return table.concat(out)
    end
    return (rev(c:sub(1, 8)) .. rev(c:sub(9, 12)) .. rev(c:sub(13, 16)) .. c:sub(17)):upper()
end

--- Bytes as hex: lowercase, or uppercase (SQLite's hex()) when `upper`.
function M.hex(s, upper)
    local fmt = upper and "%02X" or "%02x"
    return (s:gsub(".", function(c) return string.format(fmt, c:byte()) end))
end

--- Hex (either case, whitespace ignored) as bytes.
function M.unhex(h)
    return (h:gsub("%s", ""):gsub("%x%x", function(x) return string.char(tonumber(x, 16)) end))
end

--- 64-bit FNV-1a of `bytes`, as a signed Lua integer (all 64 bits; the
--- multiplication wraps modulo 2^64 as the Rust does).
function M.fnv1a64(bytes)
    local h = -3750763034362895579 -- 0xcbf29ce484222325
    for i = 1, #bytes do
        h = (h ~ bytes:byte(i)) * 0x100000001b3
    end
    return h
end

--- eventd's `hash_for_sql` (metric_store.rs): FNV-1a 64 with bit 63
--- cleared, as stored in `series.label_hash` and `boundaries_hash`.
function M.hash_for_sql(bytes)
    return M.fnv1a64(bytes) & 0x7fffffffffffffff
end

--- EVENTD_FIELD_NAMESPACE (§B): the uuid5 namespace of field GUIDs.
M.FIELD_NAMESPACE = "e7d3a1b0-5c2f-4e8a-9b1d-0a6f3c8e2d4b"

local function shq(s) return "'" .. tostring(s):gsub("'", "'\\''") .. "'" end

local guid_cache = {}
--- uuid5(namespace, name) in PCDS byte order (Python's `bytes_le`): the
--- 16 raw bytes an object ACE carries for a field. `namespace` defaults
--- to FIELD_NAMESPACE. Computed by the host's Python, and cached.
function M.field_guid(name, namespace)
    namespace = namespace or M.FIELD_NAMESPACE
    local key = namespace .. "\0" .. name
    if not guid_cache[key] then
        local p = assert(io.popen("python3 -c " ..
            shq("import sys, uuid; print(uuid.uuid5(uuid.UUID(sys.argv[1]), sys.argv[2]).bytes_le.hex())")
            .. " " .. shq(namespace) .. " " .. shq(name), "r"))
        local h = p:read("l")
        p:close()
        assert(h and #h == 32, "field_guid(" .. name .. "): python said " .. tostring(h))
        guid_cache[key] = M.unhex(h)
    end
    return guid_cache[key]
end

-- ---------------------------------------------------------------------------
-- MessagePack decoding
--
-- `json.decode` drops a null-valued key, and kmes.lua's decoder keeps no
-- order; eventd's answers and payloads need both. `decode` keeps every
-- key, a nil value as `eventd.NIL`, and remembers each map's wire order
-- (`eventd.keys(map)`).
-- ---------------------------------------------------------------------------

local KEYS = setmetatable({}, { __mode = "k" })

--- A decoded map's keys in the order they were on the wire (nil for a
--- table `decode` did not produce).
function M.keys(map) return KEYS[map] end

local function decode_at(s, i, tagged)
    local b = s:byte(i)
    assert(b, "msgpack: ran off the end")
    local function list(count, at)
        local out = {}
        for k = 1, count do out[k], at = decode_at(s, at, tagged) end
        if tagged then return { array = out }, at end
        return out, at
    end
    local function dict(count, at)
        local out, keys = {}, {}
        for _ = 1, count do
            local k, v
            k, at = decode_at(s, at, tagged)
            v, at = decode_at(s, at, tagged)
            keys[#keys + 1] = k
            out[k] = v
        end
        if tagged then return { map = out, keys = keys }, at end
        KEYS[out] = keys
        return out, at
    end
    local function bytes(len, at) return s:sub(at, at + len - 1), at + len end
    local function bin(len, at)
        local v, nxt = bytes(len, at)
        if tagged then return { bin = v }, nxt end
        return v, nxt
    end
    local function ext(len, at)
        local ty = string.unpack("i1", s, at)
        return { ext = ty, data = s:sub(at + 1, at + len) }, at + 1 + len
    end
    local function num(fmt, size) return (string.unpack(fmt, s, i + 1)), i + 1 + size end
    if b <= 0x7f then return b, i + 1 end
    if b >= 0xe0 then return b - 0x100, i + 1 end
    if b <= 0x8f then return dict(b - 0x80, i + 1) end
    if b <= 0x9f then return list(b - 0x90, i + 1) end
    if b <= 0xbf then return bytes(b - 0xa0, i + 1) end
    if b == 0xc0 then return M.NIL, i + 1 end
    if b == 0xc2 then return false, i + 1 end
    if b == 0xc3 then return true, i + 1 end
    if b == 0xc4 then return bin(s:byte(i + 1), i + 2) end
    if b == 0xc5 then return bin((string.unpack(">I2", s, i + 1)), i + 3) end
    if b == 0xc6 then return bin((string.unpack(">I4", s, i + 1)), i + 5) end
    if b == 0xc7 then return ext(s:byte(i + 1), i + 2) end
    if b == 0xc8 then return ext((string.unpack(">I2", s, i + 1)), i + 3) end
    if b == 0xc9 then return ext((string.unpack(">I4", s, i + 1)), i + 5) end
    if b == 0xca then return num(">f", 4) end
    if b == 0xcb then return num(">d", 8) end
    if b == 0xcc then return num(">I1", 1) end
    if b == 0xcd then return num(">I2", 2) end
    if b == 0xce then return num(">I4", 4) end
    if b == 0xcf then return num(">i8", 8) end -- u64: above 2^63-1 wraps negative
    if b == 0xd0 then return num(">i1", 1) end
    if b == 0xd1 then return num(">i2", 2) end
    if b == 0xd2 then return num(">i4", 4) end
    if b == 0xd3 then return num(">i8", 8) end
    if b >= 0xd4 and b <= 0xd8 then return ext(1 << (b - 0xd4), i + 1) end
    if b == 0xd9 then return bytes(s:byte(i + 1), i + 2) end
    if b == 0xda then return bytes((string.unpack(">I2", s, i + 1)), i + 3) end
    if b == 0xdb then return bytes((string.unpack(">I4", s, i + 1)), i + 5) end
    if b == 0xdc then return list((string.unpack(">I2", s, i + 1)), i + 3) end
    if b == 0xdd then return list((string.unpack(">I4", s, i + 1)), i + 5) end
    if b == 0xde then return dict((string.unpack(">I2", s, i + 1)), i + 3) end
    if b == 0xdf then return dict((string.unpack(">I4", s, i + 1)), i + 5) end
    error(string.format("msgpack: unsupported tag 0x%02x", b))
end

--- Decode one MessagePack value from `bytes` at `at` (default 1).
--- Returns the value and the index just past it.
---
--- Plain (default): a map is a Lua table with every key kept — a nil
--- value is `eventd.NIL` — and its wire order in `eventd.keys(map)`; an
--- array is a list; str and bin are both strings; an ext is
--- `{ext = type, data = bytes}`.
---
--- opts.tagged = true: shapes that keep everything visible — a map is
--- `{map = {k = v}, keys = {k1, k2, …}}` (duplicates show in `keys`), an
--- array `{array = {…}}`, a bin `{bin = bytes}`.
--- opts.whole = true: raise unless the value is all of `bytes`.
function M.decode(bytes, at, opts)
    local v, nxt = decode_at(bytes, at or 1, opts and opts.tagged)
    if opts and opts.whole then assert(nxt == #bytes + 1, "msgpack: trailing bytes") end
    return v, nxt
end

-- ---------------------------------------------------------------------------
-- The query channel, spoken directly (PSPU §3.15–§3.17): `eventd.rq`
--
-- For what evctl hides: where one response frame ends and the next
-- begins, the order frames arrive in, a caller other than SYSTEM, and a
-- reader that stops reading. A request is a u32 little-endian length and
-- a MessagePack map {query = text}; every answer is the same framing
-- around a map whose `status` is ok (with `records`), end, watch or
-- error. A frame's length and payload may arrive in separate reads, and
-- one read may carry several frames, so frames are read whole from a
-- buffer.
--
--   local c = eventd.rq.open(w, tok)       -- connect (as tok), or raise
--   eventd.rq.send(c, "EVENTS … TAKE 5")
--   local m, why = eventd.rq.frame(c)      -- one decoded frame
--   local all = eventd.rq.collect(c)       -- frames until a non-ok one
--   eventd.rq.close(c)
--
-- Stopping reading is just not calling `frame`: the connection stays
-- open, unread, until `close`.
-- ---------------------------------------------------------------------------

local rq = {}
M.rq = rq

--- SO_RCVTIMEO on `fd`, in seconds (fractions allowed).
local function rcvtimeo(who, fd, seconds)
    return who:syscall(unixsock.NR.setsockopt, {
        args = { fd, 1, 20, 0, 16 },
        bufs = { string.pack("<i8i8", math.floor(seconds), math.floor((seconds % 1) * 1e6)) },
        ptrs = { 3 },
    })
end

--- Connect `who` to the query socket. Returns a connection
--- `{who, w, fd, buf}`, or nil and why ("socket: …", "impersonate: …",
--- "connect: …"); the socket is closed on failure.
---
--- `opts` is a token fd (impersonated around connect() only) or a table:
---   as       a token fd, as above
---   level    KACS_SO_IMPERSONATION_LEVEL, set before connecting
---   timeout  SO_RCVTIMEO seconds (default 30; false for none), so a
---            read nobody answers fails with EAGAIN rather than hanging
---   socket   the socket path (default eventd.SOCKET.query)
function rq.connect(who, opts)
    if type(opts) ~= "table" then opts = { as = opts } end
    local fd, e = unixsock.socket(who, unixsock.AF_UNIX, unixsock.SOCK.STREAM)
    if not fd then return nil, "socket: " .. unixsock.errname(e) end
    local timeout = opts.timeout
    if timeout == nil then timeout = 30 end
    if timeout then rcvtimeo(who, fd, timeout) end
    if opts.level then unixsock.set_level(who, fd, opts.level) end
    if opts.as then
        local r = token.impersonate(who, opts.as)
        if r.ret ~= 0 then
            sys.close(who, fd)
            return nil, "impersonate: " .. unixsock.errname(r.errno)
        end
    end
    local r = unixsock.connect(who, fd, opts.socket or M.SOCKET.query)
    if opts.as then token.revert(who) end
    if r.ret ~= 0 then
        sys.close(who, fd)
        return nil, "connect: " .. unixsock.errname(r.errno)
    end
    return { who = who, w = who, fd = fd, buf = "" }
end

--- `connect`, raising on failure.
function rq.open(who, opts)
    local c, why = rq.connect(who, opts)
    assert(c, "connect to the query socket: " .. tostring(why))
    return c
end

--- Change the connection's SO_RCVTIMEO (seconds, fractions allowed).
function rq.timeout(c, seconds) return rcvtimeo(c.who, c.fd, seconds) end

--- Send raw bytes on the connection; the sendmsg result.
function rq.send_raw(c, bytes) return unixsock.sendmsg(c.who, c.fd, bytes) end

--- The framed request for `text`: u32-LE length and msgpack {query=text}.
function rq.request(text)
    local body = M.msgpack({ query = text })
    return string.pack("<I4", #body) .. body
end

--- Send the request for `text`, asserting the whole frame went.
function rq.send(c, text)
    local frame = rq.request(text)
    local r = rq.send_raw(c, frame)
    assert(r.ret == #frame, "sending the request: ret " .. tostring(r.ret) ..
        " errno " .. tostring(r.errno))
end

--- Read until the buffer holds `n` bytes. true, or false and why.
local function fill(c, n)
    while #c.buf < n do
        local r = unixsock.recvmsg(c.who, c.fd, 65536, { cmsg = 0 })
        if r.ret < 0 then return false, unixsock.errname(r.errno) end
        if r.ret == 0 then return false, "eof" end
        c.buf = c.buf .. r.data
        c.reads = (c.reads or 0) + 1
    end
    return true
end

--- The next response frame, decoded (`decode`, so nil values are
--- `eventd.NIL`), with `size` set to its payload length; or nil and
--- "eof" or an errno name (`EAGAIN (11)` when the receive timed out).
function rq.frame(c)
    local ok, why = fill(c, 4)
    if not ok then return nil, why end
    local len = string.unpack("<I4", c.buf)
    ok, why = fill(c, 4 + len)
    if not ok then return nil, why end
    local msg = M.decode(c.buf:sub(5, 4 + len))
    c.buf = c.buf:sub(5 + len)
    if type(msg) == "table" then msg.size = len end
    return msg
end

--- Frames until one whose status is not "ok", or the stream ends.
--- Returns { frames, records (every ok frame's, in order), status (the
--- last frame's, or "eof"/errno name when no final frame came), error,
--- closed (set, to that same "eof"/errno name, only when no final frame
--- came) }.
function rq.collect(c)
    local out = { frames = {}, records = {} }
    while true do
        local m, why = rq.frame(c)
        if not m then out.status, out.closed = why, why; return out end
        out.frames[#out.frames + 1] = m
        if m.status == "ok" then
            for _, r in ipairs(m.records) do out.records[#out.records + 1] = r end
        else
            out.status, out.error = m.status, m.error
            return out
        end
    end
end

function rq.close(c) sys.close(c.who, c.fd) end

--- One whole query from `who` (opts as `connect`): connect, send, read
--- to the final frame, close. Returns { records, frames, status (the
--- final frame's: "end", "error", "watch"; nil when none came), error,
--- closed ("eof" or errno name when the stream ended without a final
--- frame), connect_error (why connect failed; nothing else is set) }.
function rq.ask(who, text, opts)
    local c, why = rq.connect(who, opts)
    if not c then return { records = {}, frames = {}, connect_error = why } end
    rq.send(c, text)
    local out = rq.collect(c)
    rq.close(c)
    if out.closed then out.status = nil end
    return out
end

--- A token for an ordinary user, `rid` naming which one (minted by
--- `who`, which must hold the privilege to mint). Each call is a fresh
--- logon session. In Administrators because /run/eventd admits SYSTEM,
--- Administrators and eventd's own service SID and nothing else.
function rq.user(who, rid)
    local pc = require("helpers.peinit_client")
    local tok = pc.mint_admin(who, token.sid(5, 21, 1278, 6, 6, rid))
    assert(tok, "minting user " .. rid)
    return tok
end

-- ---------------------------------------------------------------------------
-- eventd's standard error
--
-- peinit forwards eventd's stderr into the log store under origin
-- `eventd` — once an eventd is running to take it, so the lines of an
-- eventd that has exited are read after the next one starts. The store
-- can hold a line more than once; these de-duplicate.
-- ---------------------------------------------------------------------------

--- eventd's stderr lines containing `needle` (nil: every line), at or
--- after `opts.since` (guest ns, default 0), de-duplicated and oldest
--- first: a list of log rows (`message`, `timestamp`, `job_id`, …).
--- Waits up to `opts.timeout` seconds (default 20) for at least
--- `opts.count` (default 1) of them; never raises — the list may be
--- short or empty. `opts.wait = false` reads once.
---
--- `needle` goes inside double quotes in the query, so it must not hold
--- one. `opts.window` is the SINCE window (default "30m").
function M.stderr(who, needle, opts)
    opts = opts or {}
    local since, want = opts.since or 0, opts.count or 1
    local text = "LOGS FROM eventd" .. (needle and (' CONTAINING "' .. needle .. '"') or "")
        .. " SINCE " .. (opts.window or "30m") .. " ago" .. (needle and "" or " TAKE 100000")
    local out = {}
    local function read()
        local r = M.query(who, text)
        if not r.ok then return false end
        local seen = {}
        out = {}
        for _, row in ipairs(r.rows) do
            local key = tostring(row.timestamp) .. "\0" .. tostring(row.message)
            if (row.timestamp or 0) >= since and not seen[key] then
                seen[key] = true
                out[#out + 1] = row
            end
        end
        table.sort(out, function(a, b) return a.timestamp < b.timestamp end)
        return #out >= want
    end
    if opts.wait == false then
        read()
    else
        pcall(wait_until, read, { timeout = opts.timeout or 20, interval = 0.25,
            desc = "eventd's stderr: " .. tostring(needle) })
    end
    return out
end

--- The first (oldest) stderr line containing `needle` at or after
--- `since` (guest ns), waiting as `stderr` does; nil if none arrives.
function M.stderr_line(who, needle, since, opts)
    local o = {}
    for k, v in pairs(opts or {}) do o[k] = v end
    o.since = since
    return M.stderr(who, needle, o)[1]
end

--- SIGQUIT eventd and capture the diagnostic dump it writes to stderr.
---
--- From the agent: SIGQUIT, wait for the process to go (the dump is
--- written, then a graceful shutdown), note `eventd.status`, start eventd
--- again (`start`) and read the dump back from the log store, waiting
--- until its last line — `opts.last`, default "last_write_errors" — has
--- arrived. A line peinit delivers after the dying eventd's final drain is
--- lost, so a dump can arrive incomplete; `opts.attempts` (default 1)
--- repeats the whole procedure until one is complete.
---
--- Use a `boot{noncritical = true}` VM. Returns
---   { complete = bool, lines = {label = rest} (each indented
---     `  label: rest` line, e.g. lines.queries, lines["cpu[0]"]),
---     messages = every dump line in order, header = the "eventd
---     diagnostic dump" row (or nil), headers = how many such rows,
---     job_id, since (guest ns before the signal), pid (the eventd
---     signalled), status (eventd.status after it exited),
---     attempts, partial = per failed attempt, the labels it got }
---
--- opts:
---   prepare(attempt)  called before each attempt's signal (material for
---                     the counters); may return a function called once
---                     the process is gone
---   last              the label that ends a dump
---   attempts          tries (default 1)
---   timeout           for eventd to exit (default 30)
---   wait              seconds to wait for the dump's lines (default 30)
---   start = false     leave eventd stopped; the dump is then NOT read
function M.quit_dump(vm, opts)
    opts = opts or {}
    local last = opts.last or "last_write_errors"
    local d
    local partial = {}
    for attempt = 1, opts.attempts or 1 do
        local after = opts.prepare and opts.prepare(attempt)
        local since = M.guest_ns(vm)
        local pid = assert(M.pid(vm), "eventd is not running")
        M.signal(vm, pid, "QUIT")
        assert(M.wait_gone(vm, pid, opts.timeout or 30), "eventd (" .. pid .. ") to exit on SIGQUIT")
        if type(after) == "function" then pcall(after) end
        d = { since = since, pid = pid, status = M.status(vm), lines = {}, messages = {},
              attempts = attempt, partial = partial, complete = false }
        if opts.start == false then return d end
        M.start(vm)
        local header = M.stderr(vm, "eventd diagnostic dump", { since = since })
        d.header, d.headers = header[1], #header
        d.job_id = header[1] and header[1].job_id
        d.complete = pcall(wait_until, function()
            local r = M.query(vm, "LOGS FROM eventd SINCE 10m ago TAKE 100000")
            if not r.ok then return false end
            local lines, msgs, seen = {}, {}, {}
            for _, row in ipairs(r.rows) do
                local key = tostring(row.timestamp) .. "\0" .. tostring(row.message)
                if row.timestamp >= since and (d.job_id == nil or row.job_id == d.job_id)
                    and not seen[key] then
                    seen[key] = true
                    msgs[#msgs + 1] = row.message
                    local label, rest = row.message:match("^%s+([%w_%[%]]+):%s*(.*)$")
                    if label then lines[label] = rest end
                end
            end
            d.lines, d.messages = lines, msgs
            return lines[last] ~= nil
        end, { timeout = opts.wait or 30, interval = 0.5, desc = "the whole dump to reach the log store" })
        if d.complete then break end
        local got = {}
        for k in pairs(d.lines) do got[#got + 1] = k end
        table.sort(got)
        partial[#partial + 1] = "try " .. attempt .. " got: " .. table.concat(got, ",")
    end
    return d
end

-- ---------------------------------------------------------------------------
-- Senders, and descriptors under the Security key
-- ---------------------------------------------------------------------------

--- MSG_DONTWAIT, for `sendto`.
M.DONTWAIT = unixsock.MSG.DONTWAIT

--- sendto(2) of `bytes` to the socket at `path`, with `flags` (e.g.
--- `eventd.DONTWAIT`, so a full receive queue refuses with EAGAIN instead
--- of blocking). `fd` is a datagram socket of `who`'s; nil opens a fresh
--- one and closes it after. Returns the raw result (`ret`, `errno`).
function M.sendto(who, fd, bytes, path, flags)
    local own = fd == nil
    if own then fd = assert(unixsock.socket(who, unixsock.AF_UNIX, unixsock.SOCK.DGRAM)) end
    local addr, len = unixsock.sockaddr(path)
    local r = who:syscall(unixsock.NR.sendto, {
        args = { fd, 0, #bytes, flags or 0, 0, len }, bufs = { bytes, addr }, ptrs = { 1, 4 },
    })
    if own then sys.close(who, fd) end
    return r
end

--- A persistent datagram sender for `who`: one socket, with
--- KACS_SO_PASS_TOKEN set once, as §7.6 has a metric producer do (KACS
--- reuses the captured token while the socket keeps one identity, so its
--- datagrams share a token_id; `send_metric` makes a fresh socket, a
--- fresh capture, every time).
---
--- opts: channel = "metric" (default) or "log"; path overrides the
--- socket; pass_token (default true for metric, false for log).
---
--- Returns s with
---   s.send(records)          a list of records: one is sent as a map,
---                            more as one array datagram; asserts all sent
---   s.sendto(bytes, flags)   raw bytes, raw result (flags e.g. DONTWAIT)
---   s.close()
---   s.fd, s.path
function M.sender(who, opts)
    opts = opts or {}
    local channel = opts.channel or "metric"
    local path = opts.path or M.SOCKET[channel]
    local fd = assert(unixsock.socket(who, unixsock.AF_UNIX, unixsock.SOCK.DGRAM))
    local pass = opts.pass_token
    if pass == nil then pass = channel == "metric" end
    if pass then
        local p = unixsock.set_pass_token(who, fd, true)
        assert(p.ret == 0, "KACS_SO_PASS_TOKEN: errno " .. tostring(p.errno))
    end
    local s = { fd = fd, path = path }
    function s.sendto(bytes, flags) return M.sendto(who, fd, bytes, path, flags) end
    function s.send(records)
        local bytes = M.msgpack(#records == 1 and records[1] or M.array(records))
        local r = s.sendto(bytes)
        assert(r.ret == #bytes, "sendto: errno " .. tostring(r.errno))
        return r
    end
    function s.close() sys.close(who, fd) end
    return s
end

--- The key of a pattern's descriptor: `Security\<ns>\<pattern>`, or
--- `Security\<pattern>` when `ns` is nil (the Admin key).
function M.key_of(ns, pattern)
    return M.SECURITY .. (ns and ("\\" .. ns) or "") .. "\\" .. pattern
end

--- Write `sd` (descriptor bytes) as the default value of the pattern's
--- key, creating it (`reg set -p`). Asserts; returns the key.
function M.put_descriptor(who, ns, pattern, sd)
    local key = M.key_of(ns, pattern)
    who:run("reg set -p '" .. key .. "' @ hex:" .. M.hex(sd)):assert_ok()
    return key
end

--- Remove a pattern's key and everything under it. Not asserted (it may
--- not exist); returns the `reg del` result.
function M.drop_descriptor(who, ns, pattern)
    return who:run("reg del -r -y '" .. M.key_of(ns, pattern) .. "'")
end

--- A pattern's descriptor value as hex, or nil when there is none.
function M.descriptor_hex(who, ns, pattern)
    local r = who:run("reg get '" .. M.key_of(ns, pattern) .. "' @")
    if r.exit_code ~= 0 then return nil end
    return (r.stdout:gsub("%s", ""))
end

--- Write `sd` as the default value of any key (`reg new`, then
--- `set(… "@" …)`). Returns the set result, NOT asserted.
function M.write_descriptor(who, key, sd)
    who:run("reg new '" .. key .. "'")
    return M.set(who, "@", "hex:" .. M.hex(sd), { key = key })
end

--- A key's default value as bytes (nil when it has none), and the raw
--- `reg get` output.
function M.read_descriptor(who, key)
    local r = who:run("reg get '" .. key .. "'")
    local h = r.stdout:match("%(default%) = REG_BINARY (%x+)")
    return h and M.unhex(h), r.stdout .. r.stderr
end

return M
