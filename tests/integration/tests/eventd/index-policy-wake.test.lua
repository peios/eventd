-- PEI-1447: one applied threshold change must use the new policy values.
local eventd = require("helpers.eventd")
local peinit = require("helpers.peinit")
peinit.claim(1)
local vm = eventd.boot({ name = "ev-policy-wake", config = {
    { name = "AdaptiveIndexCreateThreshold", type = "dword", data = 100 },
    { name = "AdaptiveIndexDropThreshold", type = "dword", data = 1 },
    { name = "AdaptiveIndexPolicyIntervalMinutes", type = "dword", data = 1440 },
} })

local function applied(key, value)
    local since = eventd.guest_ns(vm)
    eventd.set(vm, key, "dword:" .. value):assert_ok()
    eventd.wait_rows(vm, "EVENTS " .. eventd.T.config_change
        .. ' WHERE config.name == "' .. key .. '" AND config.value == ' .. value .. ' SINCE 10m ago', function(rs)
            for _, row in ipairs(rs) do if row["event.time"] >= since then return true end end
            return false
        end)
end

test("each threshold wake judges the desired set using the current configuration", {
    spec = "eventd *index.a-policy-run-reads-current-configuration-after-waking",
}, function(t)
    local name, field = eventd.marker("policyEvent"), eventd.marker("policyField")
    t:assert_eq(eventd.emit(vm, name, { [field] = 1 }).ret, 0)
    eventd.wait_rows(vm, "EVENTS " .. name .. " SINCE 10m ago", function(rs) return #rs == 1 end)
    for _ = 1, 22 do
        t:assert_eq(#eventd.rows(vm, "EVENTS " .. name .. " WHERE " .. field .. " == 1 SINCE 10m ago"), 1)
    end
    local function desired()
        return #eventd.sql(vm, eventd.DB.meta,
            "SELECT field_path FROM desired_indexes WHERE field_path = '" .. field .. "'") == 1
    end
    t:assert(not desired(), "22 queries do not meet the original create threshold of 100")
    applied("AdaptiveIndexCreateThreshold", 20)
    wait_until(desired, { timeout = 15, interval = 0.25,
        desc = "the single threshold change to create the desired index" })

    -- Restore a create threshold above the new drop threshold while the
    -- existing index is retained by the old drop threshold of one.
    applied("AdaptiveIndexCreateThreshold", 100)
    t:assert(desired(), "raising only the creation threshold retains an existing index")
    applied("AdaptiveIndexDropThreshold", 30)
    wait_until(function() return not desired() end, { timeout = 15, interval = 0.25,
        desc = "the single drop-threshold change to remove the desired index" })
end)
