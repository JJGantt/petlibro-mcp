#!/usr/bin/env python3
"""MCP server for a PetLibro PLAF203 camera-feeder.

Local LAN control via the Tuya protocol (see feeder_core for the why and the datapoint map).
Tools: feed, feeder_status, food_level, get_schedule. After a feed, the device_status mirror is
refreshed so the hub tile updates immediately.
"""

import time

from mcp.server.fastmcp import FastMCP

import feeder_core as fc

mcp = FastMCP("petlibro-feeder")


@mcp.tool()
def feed(portions: int = 1) -> str:
    """Dispense food now from the cat feeder.

    Args:
        portions: Number of portions to dispense (1 portion ≈ one app "Feed Now"). Default 1.
    """
    if portions < 1 or portions > 12:
        return "portions must be between 1 and 12."
    res = fc.feed(portions)
    fc.mirror_to_supabase()  # keep the hub tile current
    if res["confirmed"]:
        return f"Fed {res['portions']} portion(s). ✅"
    return ("Sent the feed command but couldn't confirm it from the feed log — "
            "check the bowl; the feeder may have been mid-cycle or briefly offline.")


@mcp.tool()
def feeder_status() -> str:
    """Get the cat feeder's current status: motor state, food level, and last feed."""
    s = fc.snapshot()
    if not s.get("online"):
        return "Feeder is not reachable on the local network right now."
    last = "no record"
    if s.get("last_fed_at"):
        ago = max(0, int(time.time()) - int(s["last_fed_at"]))
        last = f"{s.get('last_fed_portions', '?')} portion(s) {ago // 60}m ago"
    return (f"Feeder status:\n"
            f"  Motor: {s['motor']}\n"
            f"  Food bin: {s['food_level']}\n"
            f"  Last feed: {last}")


@mcp.tool()
def food_level() -> str:
    """Check how much food is left in the feeder's hopper."""
    s = fc.snapshot()
    if not s.get("online"):
        return "Feeder is not reachable on the local network right now."
    return f"Food bin level: {s['food_level']}"


@mcp.tool()
def get_schedule() -> str:
    """View the feeder's automatic feeding schedule (set in the PetLibro app; read-only here)."""
    s = fc.snapshot()
    if not s.get("online"):
        return "Feeder is not reachable on the local network right now."
    sched = s.get("schedule", [])
    if not sched:
        return "No feeding schedule is set on the feeder."
    lines = [f"  {e['time']}  {e['portions']} portion(s)  [{e['days']}]"
             + ("" if e["enabled"] else "  (disabled)") for e in sched]
    return "Feeding schedule (device local time):\n" + "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
