#!/usr/bin/env python3
"""Shared core for the PetLibro PLAF203 feeder — used by both the MCP server and the collector.

Control is LOCAL over the LAN via the Tuya protocol (tinytuya). The device's *cloud* datapoint
schema only exposes the camera; the cloud rejects the feed command it schema-validates. The feed
command (dp 3 = portions) and all feeder state live on the local channel only.

Confirmed datapoint map (tinytuya.Device(..., version=3.4)):
  * dp 232 (write) — manual feed, integer = portions. The device ACKs it (echoes 233='feeding')
                     and resets it to 0 between feeds, so repeat same-count feeds work (no dedupe).
                     Cycle takes ~25s; the feed log (247) updates when it completes.
  * dp 247 (read)  — last-feed log JSON {"value": portions, "time": epoch}
  * dp 233 (read)  — motor state: "standby" / "feeding"
  * dp 241 (read)  — food bin level (e.g. "full")
  * dp 231 (read)  — feeding schedule, raw hex; 5 bytes/entry: [day_mask, hour, min, portions, enabled]
"""

import json
import os
import time
from pathlib import Path

import tinytuya
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env-petlibro")

DEVICE_ID = os.environ["FEEDER_DEVICE_ID"]
LOCAL_KEY = os.environ["FEEDER_LOCAL_KEY"]
FEEDER_IP = os.environ["FEEDER_IP"]
VERSION = float(os.environ.get("FEEDER_VERSION", "3.4"))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://<project-ref>.supabase.co").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

DP_FEED = "232"
DP_LAST_FEED = "247"
DP_MOTOR = "233"
DP_FOOD = "241"
DP_SCHEDULE = "231"

DEVICE_KEY = "cat-feeder"  # device_status primary key for the hub
_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def device() -> tinytuya.Device:
    d = tinytuya.Device(DEVICE_ID, FEEDER_IP, LOCAL_KEY, version=VERSION)
    d.set_socketTimeout(6)
    return d


def read_dps() -> dict:
    """Current datapoints, retrying once on a flaky connect. Empty dict if unreachable."""
    for _ in range(2):
        data = device().status()
        if isinstance(data, dict) and data.get("dps"):
            return data["dps"]
        time.sleep(1)
    return {}


def parse_last_feed(dps: dict) -> dict:
    raw = dps.get(DP_LAST_FEED)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {}


def decode_schedule(dps: dict) -> list:
    """Decode dp 231 into a list of {time, hour, minute, portions, days, enabled}, sorted by time."""
    raw = dps.get(DP_SCHEDULE)
    if not raw:
        return []
    try:
        b = bytes.fromhex(raw)
    except ValueError:
        return []
    out = []
    for i in range(0, len(b) - 4, 5):
        mask, hour, minute, portions, enabled = b[i], b[i + 1], b[i + 2], b[i + 3], b[i + 4]
        days = "every day" if mask == 0x7F else ",".join(d for j, d in enumerate(_DAYS) if mask >> j & 1)
        out.append({"time": f"{hour:02d}:{minute:02d}", "hour": hour, "minute": minute,
                    "portions": portions, "days": days, "enabled": bool(enabled)})
    return sorted(out, key=lambda e: (e["hour"], e["minute"]))


def feed(portions: int = 1) -> dict:
    """Dispense `portions` now. Returns {ok, portions, confirmed, fed_at}.

    Writes dp 232 (the device ACKs with 233='feeding'). The mechanical cycle takes ~25s and the
    feed log (247) updates on completion, so we hold one persistent connection and poll up to ~36s,
    returning as soon as the log advances. If the motor engaged but the log hasn't posted by then,
    we still report confirmed (the feed is in progress)."""
    d = device()
    d.set_socketPersistent(True)
    try:
        before = parse_last_feed((d.status() or {}).get("dps", {})).get("time", 0)
        resp = d.set_value(DP_FEED, portions)
        started = isinstance(resp, dict) and (resp.get("dps") or {}).get(DP_MOTOR) == "feeding"
        for _ in range(24):
            time.sleep(1.5)
            dps = (d.status() or {}).get("dps", {})
            if dps.get(DP_MOTOR) == "feeding":
                started = True
            log = parse_last_feed(dps)
            if log.get("time", 0) > before:
                return {"ok": True, "confirmed": True, "portions": log.get("value", portions),
                        "fed_at": log.get("time")}
        return {"ok": True, "confirmed": bool(started), "portions": portions, "fed_at": None}
    finally:
        try:
            d.close()
        except Exception:
            pass


def snapshot() -> dict:
    """Full feeder state for the hub / status mirror. online=False if unreachable."""
    dps = read_dps()
    if not dps:
        return {"online": False}
    last = parse_last_feed(dps)
    return {
        "online": True,
        "motor": dps.get(DP_MOTOR, "unknown"),
        "food_level": dps.get(DP_FOOD, "unknown"),
        "last_fed_at": last.get("time"),
        "last_fed_portions": last.get("value"),
        "schedule": decode_schedule(dps),
    }


def mirror_to_supabase(snap: dict | None = None) -> bool:
    """Upsert the feeder's state into device_status so the hub tile updates. No-op without a key."""
    if not SUPABASE_KEY:
        return False
    import urllib.request
    snap = snap or snapshot()
    last_iso = None
    if snap.get("last_fed_at"):
        last_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(snap["last_fed_at"]))
    row = {
        "device": DEVICE_KEY,
        "name": "Cat Feeder",
        "status": snap.get("motor") if snap.get("online") else "OFFLINE",
        "online": bool(snap.get("online")),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "data": {
            "food_level": snap.get("food_level"),
            "last_fed_at": last_iso,
            "last_fed_portions": snap.get("last_fed_portions"),
            "schedule": snap.get("schedule", []),
        },
    }
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/device_status?on_conflict=device",
        data=json.dumps(row).encode(), method="POST",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"})
    try:
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception:
        return False


def _sb(method: str, path: str, body=None, extra_headers=None):
    """Minimal Supabase REST call returning parsed JSON (or None). Raises on transport error."""
    import urllib.request
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
               "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{SUPABASE_URL}/rest/v1/{path}", data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def fetch_pending_commands() -> list:
    """Pending feeder_commands rows (oldest first). Empty list without a key or on error."""
    if not SUPABASE_KEY:
        return []
    try:
        return _sb("GET", "feeder_commands?status=eq.pending&order=created_at.asc&select=*") or []
    except Exception:
        return []


def mark_command(cmd_id: str, status: str, result: str = "") -> None:
    """Mark a command row done/failed with the result + execution time."""
    if not SUPABASE_KEY:
        return
    body = {"status": status, "result": result[:500],
            "executed_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())}
    try:
        _sb("PATCH", f"feeder_commands?id=eq.{cmd_id}", body)
    except Exception:
        pass
