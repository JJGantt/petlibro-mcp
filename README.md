# petlibro-mcp

Local LAN control + MCP server for a PetLibro PLAF203 camera-feeder (Tuya under the hood).

The cloud cannot feed or read feeder state (it schema-validates and only exposes the camera), so
everything talks directly to the feeder on the LAN via tinytuya using its device id + local key.

- `feeder_core.py` — shared: connect / read / decode schedule / feed / snapshot / mirror-to-Supabase.
- `petlibro_mcp.py` — FastMCP server `petlibro-feeder`: tools `feed`, `feeder_status`, `food_level`, `get_schedule`.
- `collector.py` — always-on loop: polls the feeder and mirrors state into Supabase `device_status`
  (device='cat-feeder') so the status-hub tile stays current. Runs on the Pi (LAN host).

Config (device id/key/ip + Supabase service key) lives in `.env-petlibro` (gitignored).

Confirmed datapoints: dp3=manual feed (portions, write-only), dp247=last-feed log, dp233=motor,
dp241=food level, dp231=schedule (5 bytes/entry: day_mask, hour, min, portions, enabled).
