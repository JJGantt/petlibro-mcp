#!/usr/bin/env python3
"""PetLibro feeder collector — runs always-on on the Pi (the LAN host).

Two jobs in one loop:
  1. Execute pending feed commands from Supabase `feeder_commands` (the web "Feed Now" relay — the
     Fly hub can't reach the LAN-only feeder, so it enqueues a row and we dispense here). Checked
     every few seconds so a tap feeds promptly.
  2. Mirror feeder state into `device_status` (device='cat-feeder') so the hub tile stays current.
     Done on startup, after any feed, and every MIRROR_EVERY ticks.
"""

import json
import time

import feeder_core as fc

TICK_SECONDS = 5
MIRROR_EVERY = 24  # ticks between routine state mirrors (~120s)


def process_commands() -> bool:
    """Execute any pending feed commands. Returns True if at least one was handled."""
    cmds = fc.fetch_pending_commands()
    if not cmds:
        return False
    for c in cmds:
        if c.get("action") != "feed":
            fc.mark_command(c["id"], "failed", f"unknown action {c.get('action')!r}")
            continue
        portions = int(c.get("portions") or 1)
        portions = max(1, min(12, portions))
        try:
            res = fc.feed(portions)
            status = "done" if res.get("confirmed") else "failed"
            fc.mark_command(c["id"], status, json.dumps(res))
            print(f"[collector] fed {portions} via command {c['id']} -> {status}", flush=True)
        except Exception as e:  # noqa: BLE001
            fc.mark_command(c["id"], "failed", str(e))
            print(f"[collector] feed command {c['id']} error: {e}", flush=True)
    return True


def main():
    tick = 0
    while True:
        try:
            fed = process_commands()
            if fed or tick % MIRROR_EVERY == 0:
                fc.mirror_to_supabase()
        except Exception as e:  # noqa: BLE001 — never let the loop die
            print(f"[collector] error: {e}", flush=True)
        tick += 1
        time.sleep(TICK_SECONDS)


if __name__ == "__main__":
    main()
