# petlibro-mcp

Local LAN control + MCP server for a PetLibro PLAF203 camera-feeder (Tuya under the hood).

The cloud cannot feed or read feeder state (it schema-validates and only exposes the camera), so
everything talks directly to the feeder on the LAN via tinytuya using its device id + local key.

- `feeder_core.py` — shared: connect / read / decode schedule / feed / snapshot / mirror-to-Supabase.
- `petlibro_mcp.py` — FastMCP server `petlibro-feeder`: tools `feed`, `feeder_status`, `food_level`, `get_schedule`.
- `collector.py` — always-on loop on the Pi (`petlibro-collector.service`), the LAN host. Two jobs:
  execute pending `feeder_commands` rows (the relay behind the hub's "Feed Now" and the data MCP's
  `feed_cats` — Fly can't reach the LAN, so it enqueues and a machine at home dispenses), and mirror
  feeder state into Supabase `device_status` (device='cat-feeder') for the hub tile.
- The Mac runs the same collector as the Pi's fallback: launchd `com.jaredgantt.petlibro-collector`,
  from `launchd/` (copy it to `~/Library/LaunchAgents`, then `launchctl bootstrap gui/$(id -u)` it),
  log `~/Library/Logs/petlibro-collector.log`, reporting as `mac/petlibro-collector`. It answers only
  while the Mac is awake and at home.

Tests: `.venv/bin/python -m unittest discover -s tests` (who dispenses a command, and when it must not).

Config (device id/key/ip, Supabase URL + service key) lives in `.env-petlibro` (gitignored). The IP line
is rewritten in place when the feeder moves, so that file is state as well as config.

Confirmed datapoints: dp232=manual feed (portions, write), dp247=last-feed log, dp233=motor,
dp241=food level, dp231=schedule (5 bytes/entry: day_mask, hour, min, portions, enabled).

[CLAUDE.md](CLAUDE.md) has what to know before changing any of it.
