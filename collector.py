#!/usr/bin/env python3
"""PetLibro feeder collector — runs always-on on the Pi, and on the Mac as its fallback.

Two jobs in one loop:
  1. Execute pending feed commands from Supabase `feeder_commands` (the web "Feed Now" relay — the
     Fly hub can't reach the LAN-only feeder, so it enqueues a row and we dispense here). A realtime
     INSERT subscription wakes the loop the moment a command lands, so a tap feeds instantly; the
     poll is only a missed-event backstop. (The old 5s poll was ~17k Supabase reads/day of empty
     [] — a real chunk of the free-plan egress quota — for worse latency than realtime gives.)
  2. Mirror feeder state into `device_status` (device='cat-feeder') so the hub tile stays current.
     Done on startup, after any feed, and every MIRROR_EVERY_S.
  3. Record every feed, scheduled and manual, into `feeder_feeds`. The feeder keeps only its latest
     feed of each kind, so a listener thread holds a LAN connection open and the feeder pushes each
     new log value as it is written; the mirror's read offers the same values again as the backstop
     (the table dedupes on kind+fed_at). A scheduled slot that passes with no scheduled feed logged
     within MISSED_GRACE_S is reported.

The Pi is the primary. The Mac runs the same script with FEED_DEFER_S set: it leaves a command alone
until it is that old, so while the Pi is up the Pi has always claimed it first. A command the Mac does
claim means the Pi is not answering, which it reports as an error. The fallback mirrors only after a
feed of its own, so the Pi stays the one writer of the routine mirror.
"""

import asyncio
import json
import logging
import os
import socket
import threading
import time
import urllib.error
from datetime import datetime, timedelta, timezone

import feeder_core as fc
import ops_report as ops

POLL_BACKSTOP_S = 60   # realtime subscribed — poll is just a missed-event safety net
POLL_DEGRADED_S = 5    # realtime down — polling is the pickup path, keep the old prompt cadence
RT_UNJOINED_GRACE_S = 30  # a rejoin takes about a second; longer than this, the channel is dead
MIRROR_EVERY_S = 120   # routine state mirror period
# A command this old is not dispensed: a feed asked for while every collector was down must not land
# hours later on top of the next one.
MAX_AGE_S = 600
# The primary hears a command over realtime within a second or two. One it first sees this late was
# found by the backstop poll, which means the subscription missed it: reported, since a deaf channel
# looks healthy otherwise.
LATE_S = 20
# Every Mac wake starts with a few seconds of failed lookups and connects, and the loop retries on its
# own, so a link failure (DNS, refused, reset, timeout, 5xx) is a warning until it has lasted this long
# straight; then it is reported once for the streak. Anything else is reported at once.
LINK_RAISE_S = 300

# The feed listener's connection: the feeder drops an idle local socket, so it is kept open with a
# heartbeat; a dropped one is reopened with backoff up to LISTEN_BACKOFF_MAX_S.
LISTEN_HEARTBEAT_S = 20
LISTEN_BACKOFF_MAX_S = 300
# A scheduled slot with no scheduled feed logged this long after it is reported as missed. A cycle
# takes ~25s and dp 237 is stamped about 10s after the slot.
MISSED_GRACE_S = 600

MACHINE = os.environ.get("COLLECTOR_MACHINE", "pi")
FEED_DEFER_S = float(os.environ.get("FEED_DEFER_S", "0"))   # >0 makes this the fallback
CLAIMANT = f"{MACHINE}:{socket.gethostname()}"

_wake = threading.Event()
_rt_healthy = False
_last_scheduled = 0       # epoch of the latest scheduled feed seen, from the listener or the mirror read
_record_streak = None     # LinkStreak for feeder_feeds writes, made in main()


def is_link_failure(e: BaseException) -> bool:
    if isinstance(e, urllib.error.HTTPError):
        return e.code >= 500
    return isinstance(e, OSError)   # URLError, socket.gaierror, ConnectionError, TimeoutError


class LinkStreak:
    """Report a run of link failures once, after LINK_RAISE_S; other failures at once. ok() ends a run."""

    def __init__(self, label: str):
        self.label, self.since, self.raised = label, 0.0, False

    def ok(self) -> None:
        self.since, self.raised = 0.0, False

    def fail(self, e: BaseException) -> None:
        if not is_link_failure(e):
            print(f"[collector] {self.label} error: {e}", flush=True)
            ops.report(e)
            return
        now = time.monotonic()
        self.since = self.since or now
        if not self.raised and now - self.since >= LINK_RAISE_S:
            self.raised = True
            ops.report(f"{self.label} failing for {int(now - self.since)}s: {e}")
        else:
            print(f"[collector] {self.label} failed (will retry): {e}", flush=True)


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

    streak = LinkStreak("realtime connect")

    def _on_sub(state, err=None) -> None:
        global _rt_healthy
        _rt_healthy = "SUBSCRIBED" in str(state).upper()
        if _rt_healthy:
            streak.ok()
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
                # A connected socket is not a subscribed one. realtime-py's auto-reconnect rejoins only
                # channels that were JOINED when the socket dropped, and with none it crashes in
                # asyncio.wait([]), leaving a live socket carrying no subscription. So the channel's own
                # state is watched too, and a channel left unjoined past the grace gets a fresh client.
                unjoined_since = None
                while sock.is_connected:
                    if ch.is_joined:
                        unjoined_since = None
                    elif unjoined_since is None:
                        unjoined_since = time.monotonic()
                    elif time.monotonic() - unjoined_since > RT_UNJOINED_GRACE_S:
                        ops.report(f"realtime channel unjoined for {RT_UNJOINED_GRACE_S}s on a connected "
                                   f"socket (state {ch.state}); rebuilding the client")
                        break
                    await asyncio.sleep(1.0)
            except Exception as e:  # noqa: BLE001 — poll fallback is active meanwhile; retried in 5s
                streak.fail(e)
            _rt_healthy = False
            try:
                if sock:
                    await sock.close()
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(5.0)

    asyncio.run(_serve())


def record(feeds: list) -> None:
    """Offer feed log values to feeder_feeds. Duplicates are skipped by the table, and a failed write is
    retried for free by the next offer of the same value (the feeder keeps it until the next feed)."""
    global _last_scheduled
    for f in feeds:
        if f["kind"] == "scheduled":
            _last_scheduled = max(_last_scheduled, f["fed_at"])
    try:
        for r in fc.record_feeds(feeds, CLAIMANT):
            print(f"[collector] recorded {r['kind']} feed of {r['portions']} at {r['fed_at']}", flush=True)
        if _record_streak:
            _record_streak.ok()
    except Exception as e:  # noqa: BLE001 — the next offer retries it
        if _record_streak:
            _record_streak.fail(e)


def _feed_listener() -> None:
    """Hold a LAN connection to the feeder and record each feed log value it pushes. Never raises out."""
    streak = LinkStreak("feeder listener")
    backoff = 5
    while True:
        d = None
        try:
            d = fc.device()
            d.set_socketPersistent(True)
            d.set_socketTimeout(LISTEN_HEARTBEAT_S)
            first = d.status() or {}
            if not first.get("dps"):
                raise ConnectionError(f"feeder status failed: {str(first)[:200]}")
            streak.ok()
            backoff = 5
            record(fc.feeds_in(first["dps"]))
            last_hb = time.monotonic()
            while True:
                msg = d.receive()
                if isinstance(msg, dict) and msg.get("Err"):
                    raise ConnectionError(f"feeder connection dropped: {str(msg)[:200]}")
                if isinstance(msg, dict) and msg.get("dps"):
                    record(fc.feeds_in(msg["dps"]))
                if time.monotonic() - last_hb >= LISTEN_HEARTBEAT_S:
                    d.heartbeat(nowait=True)
                    last_hb = time.monotonic()
        except Exception as e:  # noqa: BLE001 — reconnect below; the mirror read backstops meanwhile
            streak.fail(e)
        finally:
            try:
                if d:
                    d.close()
            except Exception:  # noqa: BLE001
                pass
        time.sleep(backoff)
        backoff = min(backoff * 2, LISTEN_BACKOFF_MAX_S)


def missed_slot(schedule: list, last_scheduled: float, since: datetime, now: datetime) -> datetime | None:
    """The latest enabled schedule slot that is more than MISSED_GRACE_S past, no earlier than `since`,
    with no scheduled feed logged at or after it — or None. Times are local wall clock, as the feeder
    keeps them; `since` and `now` are naive local datetimes."""
    latest = None
    for e in schedule:
        if not e.get("enabled"):
            continue
        for back in range(2):   # today's slot, or yesterday's for a slot later in the day than now
            slot = (now - timedelta(days=back)).replace(hour=e["hour"], minute=e["minute"], second=0,
                                                        microsecond=0)
            if not (e.get("mask", 0x7F) >> slot.weekday() & 1):
                continue
            if (now - slot).total_seconds() > MISSED_GRACE_S and slot >= since:
                latest = max(latest, slot) if latest else slot
                break
    if latest is None:
        return None
    return None if last_scheduled >= latest.timestamp() - 60 else latest


def _age_s(c: dict) -> float:
    created = datetime.fromisoformat(str(c["created_at"]).replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - created).total_seconds()


def process_commands() -> bool:
    """Claim and execute pending feed commands. Returns True if this collector fed at least once."""
    fed = False
    for c in fc.fetch_pending_commands():
        age = _age_s(c)
        if age < FEED_DEFER_S:
            continue   # the Pi's to take; the next pass looks again
        if not fc.claim_command(c["id"], CLAIMANT):
            continue   # another collector has it
        if age > MAX_AGE_S:
            fc.mark_command(c["id"], "failed", f"expired: {int(age)}s old when claimed")
            ops.report(f"feed command {c['id']} expired unanswered after {int(age)}s; not dispensed")
            continue
        if not FEED_DEFER_S and age > LATE_S:
            ops.report(f"feed command {c['id']} reached the collector {int(age)}s late: realtime missed it, the backstop poll found it")
        if FEED_DEFER_S:
            ops.report(f"feed command {c['id']} unclaimed after {int(age)}s: dispensing from the {MACHINE}, the Pi is not")
        if c.get("action") != "feed":
            fc.mark_command(c["id"], "failed", f"unknown action {c.get('action')!r}")
            continue
        portions = int(c.get("portions") or 1)
        portions = max(1, min(12, portions))
        try:
            res = fc.feed(portions)
            status = "done" if res.get("confirmed") else "failed"
            fc.mark_command(c["id"], status, json.dumps(res))
            fed = True
            print(f"[collector] fed {portions} via command {c['id']} -> {status}", flush=True)
            if status == "failed":
                ops.report(f"feed not confirmed by the feeder: {json.dumps(res)[:300]}")
        except Exception as e:  # noqa: BLE001
            fc.mark_command(c["id"], "failed", str(e))
            print(f"[collector] feed command {c['id']} error: {e}", flush=True)
            ops.report(e)
    return fed


def _demote_connect_retry(record: logging.LogRecord) -> bool:
    if record.levelno >= logging.ERROR and record.getMessage().startswith(
            ("Connection attempt failed:", "Connection failed permanently after")):
        record.levelno, record.levelname = logging.WARNING, "WARNING"
    return True


def main():
    # Errors and the heartbeat go to the ops responder (health/responder); the beat is from this loop,
    # which comes round at least every POLL_BACKSTOP_S, so a wedged loop goes quiet.
    ops.install(machine=MACHINE, service="petlibro-collector")
    # The realtime lib logs every socket drop (e.g. 1006 from Supabase) at ERROR and then reconnects
    # on its own (auto_reconnect=True); a dropped socket that recovers is not a fault, and while it is
    # down the main loop polls at POLL_DEGRADED_S. Reconnect failures still surface as other records.
    logging.getLogger("realtime._async.client").addFilter(
        lambda r: not r.getMessage().startswith("WebSocket connection closed with code"))
    # Its per-attempt "Connection attempt failed" and its "Connection failed permanently" are re-levelled
    # to WARNING: connect() re-raises the failure to _serve, whose LinkStreak reports one that lasts.
    logging.getLogger("realtime._async.client").addFilter(_demote_connect_retry)
    # That reconnect's asyncio.wait([]) crash surfaces only as asyncio's GC-time "Task exception was
    # never retrieved". The unjoined-channel watch in _rt_thread reports the outage it causes, so this
    # record would only report the same thing twice.
    logging.getLogger("asyncio").addFilter(
        lambda r: "Set of Tasks/Futures is empty" not in r.getMessage())
    global _record_streak
    _record_streak = LinkStreak("feed record")
    threading.Thread(target=_rt_thread, daemon=True).start()
    if not FEED_DEFER_S:   # the primary records feeds; the fallback's own feeds land via its mirror
        threading.Thread(target=_feed_listener, daemon=True).start()
    started = datetime.now()
    schedule: list = []
    reported_missed = None
    last_mirror = 0.0   # monotonic epoch start → forces the startup mirror
    loop = LinkStreak("loop")
    while True:
        try:
            fed = process_commands()
            routine = not FEED_DEFER_S and time.monotonic() - last_mirror >= MIRROR_EVERY_S
            if fed or routine:
                snap = fc.snapshot()
                fc.mirror_to_supabase(snap)
                last_mirror = time.monotonic()
                if snap.get("online"):
                    schedule = snap.get("schedule", [])
                    record(snap.get("feeds", []))
            if not FEED_DEFER_S:
                slot = missed_slot(schedule, _last_scheduled, started, datetime.now())
                if slot and slot != reported_missed:
                    reported_missed = slot
                    ops.report(f"scheduled feed at {slot:%Y-%m-%d %H:%M} not logged by the feeder "
                               f"{MISSED_GRACE_S // 60} min later")
            loop.ok()
        except Exception as e:  # noqa: BLE001 — never let the loop die
            loop.fail(e)
        ops.beat(POLL_BACKSTOP_S)
        if _wake.wait(POLL_BACKSTOP_S if _rt_healthy else POLL_DEGRADED_S) and FEED_DEFER_S:
            time.sleep(FEED_DEFER_S)   # give the Pi its head start on the command that woke us
        _wake.clear()


if __name__ == "__main__":
    main()
