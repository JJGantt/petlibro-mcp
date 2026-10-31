#!/usr/bin/env python3
"""PetLibro feeder collector — runs always-on on the Pi (the LAN host).

Two jobs in one loop:
  1. Execute pending feed commands from Supabase `feeder_commands` (the web "Feed Now" relay — the
     Fly hub can't reach the LAN-only feeder, so it enqueues a row and we dispense here). A realtime
     INSERT subscription wakes the loop the moment a command lands, so a tap feeds instantly; the
     poll is only a missed-event backstop. (The old 5s poll was ~17k Supabase reads/day of empty
     [] — a real chunk of the free-plan egress quota — for worse latency than realtime gives.)
  2. Mirror feeder state into `device_status` (device='cat-feeder') so the hub tile stays current.
     Done on startup, after any feed, and every MIRROR_EVERY_S.
"""

import asyncio
import json
import logging
import threading
import time

import feeder_core as fc
import ops_report as ops

POLL_BACKSTOP_S = 60   # realtime subscribed — poll is just a missed-event safety net
POLL_DEGRADED_S = 5    # realtime down — polling is the pickup path, keep the old prompt cadence
MIRROR_EVERY_S = 120   # routine state mirror period

_wake = threading.Event()
_rt_healthy = False


def _rt_thread() -> None:
    """Realtime waker: set _wake on any feeder_commands INSERT. Reconnects forever; never raises out.
    While unhealthy (lib missing, disconnected, not yet subscribed) the main loop polls at the old
    prompt cadence, so realtime being down never delays a feed beyond the pre-realtime behavior."""
    global _rt_healthy
    try:
        from realtime import AsyncRealtimeClient
    except Exception as e:  # noqa: BLE001 — missing lib must not break feeding; just poll
        print(f"[collector] realtime unavailable ({e}); polling every {POLL_DEGRADED_S}s", flush=True)
        return

    def _on_sub(state, err=None) -> None:
        global _rt_healthy
        _rt_healthy = "SUBSCRIBED" in str(state).upper()
        print(f"[collector] realtime subscription: {state}{(' err=' + str(err)) if err else ''}", flush=True)

    async def _serve() -> None:
        global _rt_healthy
        while True:
            sock = None
            try:
                sock = AsyncRealtimeClient(f"{fc.SUPABASE_URL}/realtime/v1", fc.SUPABASE_KEY,
                                           auto_reconnect=True)
                await sock.connect()
                ch = sock.channel("feeder-collector")
                ch.on_postgres_changes("INSERT", callback=lambda _p: _wake.set(),
                                       schema="public", table="feeder_commands")
                await ch.subscribe(_on_sub)
                while sock.is_connected:
                    await asyncio.sleep(1.0)
            except Exception as e:  # noqa: BLE001
                print(f"[collector] realtime error: {e}; retrying in 5s (poll fallback active)", flush=True)
            _rt_healthy = False
            try:
                if sock:
                    await sock.close()
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(5.0)

    asyncio.run(_serve())


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
            if status == "failed":
                ops.report(f"feed not confirmed by the feeder: {json.dumps(res)[:300]}")
        except Exception as e:  # noqa: BLE001
            fc.mark_command(c["id"], "failed", str(e))
            print(f"[collector] feed command {c['id']} error: {e}", flush=True)
            ops.report(e)
    return True


def main():
    # Errors and the heartbeat go to the ops responder (health/responder); the beat is from this loop,
    # which comes round at least every POLL_BACKSTOP_S, so a wedged loop goes quiet.
    ops.install(machine="pi", service="petlibro-collector")
    # The realtime lib logs every socket drop (e.g. 1006 from Supabase) at ERROR and then reconnects
    # on its own (auto_reconnect=True); a dropped socket that recovers is not a fault, and while it is
    # down the main loop polls at POLL_DEGRADED_S. Reconnect failures still surface as other records.
    logging.getLogger("realtime._async.client").addFilter(
        lambda r: not r.getMessage().startswith("WebSocket connection closed with code"))
    threading.Thread(target=_rt_thread, daemon=True).start()
    last_mirror = 0.0   # monotonic epoch start → forces the startup mirror
    while True:
        try:
            fed = process_commands()
            if fed or time.monotonic() - last_mirror >= MIRROR_EVERY_S:
                fc.mirror_to_supabase()
                last_mirror = time.monotonic()
        except Exception as e:  # noqa: BLE001 — never let the loop die
            print(f"[collector] error: {e}", flush=True)
            ops.report(e)
        ops.beat(POLL_BACKSTOP_S)
        _wake.wait(POLL_BACKSTOP_S if _rt_healthy else POLL_DEGRADED_S)
        _wake.clear()


if __name__ == "__main__":
    main()
