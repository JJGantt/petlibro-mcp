# petlibro-mcp

Local LAN control + MCP server for a PetLibro PLAF203 camera-feeder (Tuya under the hood).

The cloud cannot feed or read feeder state (it schema-validates and only exposes the camera), so
everything talks directly to the feeder on the LAN via tinytuya using its device id + local key.

- `feeder_core.py` — shared: connect / read / decode schedule / feed / snapshot / mirror-to-Supabase.
- `petlibro_mcp.py` — FastMCP server `petlibro-feeder`: tools `feed`, `feeder_status`, `food_level`, `get_schedule`.
- `collector.py` — always-on loop on the Pi (`petlibro-collector.service`), the LAN host. Two jobs:
  execute pending `feeder_commands` rows (the hub's "Feed Now" relay — Fly can't reach the LAN, so it
  enqueues and the Pi dispenses), and mirror feeder state into Supabase `device_status`
  (device='cat-feeder') for the hub tile.

Config (device id/key/ip, Supabase URL + service key) lives in `.env-petlibro` (gitignored). The IP line
is rewritten in place when the feeder moves, so that file is state as well as config.

Confirmed datapoints: dp232=manual feed (portions, write), dp247=last-feed log, dp233=motor,
dp241=food level, dp231=schedule (5 bytes/entry: day_mask, hour, min, portions, enabled).

[CLAUDE.md](CLAUDE.md) has what to know before changing any of it.
