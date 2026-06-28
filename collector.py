#!/usr/bin/env python3
"""PetLibro feeder collector — polls the feeder over the LAN and mirrors its state into Supabase
`device_status` (device='cat-feeder') so the status-hub tile stays current via realtime.

Runs as an always-on loop (systemd on the Pi / launchd on the Mac). Food level and last-fed change
slowly, so a relaxed poll is fine; the MCP feed tool also refreshes the mirror instantly on demand.
"""

import time

import feeder_core as fc

POLL_SECONDS = 120


def main():
    while True:
        try:
            ok = fc.mirror_to_supabase()
            print(f"[collector] mirrored ok={ok}", flush=True)
        except Exception as e:  # noqa: BLE001 — never let the loop die
            print(f"[collector] error: {e}", flush=True)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
