# Working in petlibro-mcp

## The cloud cannot feed — everything is LAN-only, on purpose

Tuya's cloud schema for this device exposes the camera and **rejects the feed command**; the feed
datapoint and all feeder state exist only on the local channel. So control is tinytuya straight to
the feeder over the LAN, with the device id + local key.

The consequence to keep in mind: whatever runs this has to be on the feeder's network. The Fly hub
cannot be, which is why "Feed Now" on the phone writes a `feeder_commands` row and the Pi's
`collector.py` dispenses. Nothing here should try to reach the feeder from off-LAN.

It also means the PetLibro app working is not evidence that this does — the app goes via the cloud.

## `dp 232` is the feed write, not `dp 3`

The confirmed map is in `feeder_core.py`'s header and that file is the authority: 232 write =
portions, 247 = last-feed log, 233 = motor state, 241 = food level, 231 = schedule (5 bytes per
entry: day mask, hour, minute, portions, enabled). A feed cycle takes ~25s; 247 only updates when it
completes, so an immediate re-read looks like nothing happened.

## DHCP moves the feeder, and it fails silently

A lease shift breaks every LAN call with no error anywhere. `rediscover()` UDP-scans for
the device id and writes the new address back into `.env-petlibro` so the next process starts
correct. It is rate-limited by `SCAN_COOLDOWN_S` deliberately — an unplugged feeder must not add a
scan to every call. Keep both properties if you touch it.

Because of that write-back, `.env-petlibro` is state, not just config. It is gitignored and holds the
device id, local key and Supabase service key.

## The collector is realtime-driven

`collector.py` wakes on a Supabase realtime INSERT on `feeder_commands`; its poll is only a
missed-event backstop. A 5s poll would cost ~17k empty reads a day against the free-plan egress
quota, for worse latency. If a tap feels slow, fix the subscription rather than shortening the poll.

## Two collectors, one dispense

The Pi's collector is the primary; the Mac runs the same script as its fallback (`FEED_DEFER_S` set in
its launchd plist, `COLLECTOR_MACHINE=mac`). Every command is **claimed** (`claimed_by`, a PATCH that
only one caller can win) before it is dispensed, and the fallback leaves a command alone until it is
`FEED_DEFER_S` old, so while the Pi is up the Mac finds every row taken. A row the Mac wins is reported
as an error, because it means the Pi is not answering. Dispensing without the claim, or a second
collector without the defer, is a double feed. A command older than `MAX_AGE_S` is failed instead of
dispensed, so a feed queued while both are down never lands hours later on top of the next one. A
restart of the Pi's unit is needed after pulling a collector change (`sudo systemctl restart
petlibro-collector`); launchd `kickstart -k` for the Mac's.

It runs as `petlibro-collector.service` on the Pi, and `pi-ops` watches that unit. A failed or
unconfirmed feed and any loop error go to the ops responder (health/responder) through
the `ops-report` package (github.com/JJGantt/ops-report, pinned to a tag in `requirements.txt`, which
the Pi's `.venv` is installed from), and the loop beats there each pass.

## A rejected local key cannot be refreshed from here right now

When status() returns `914 Check device key or version` (or `904 Unexpected Payload` under 3.3), the
feeder has most likely re-provisioned and rotated its local key. The `TUYA_API_*` credentials in
`.env-petlibro` would fetch the new one from `/v1.0/devices/<id>`, but that call returns
`28841002 IoT Core service subscription has expired` until the Tuya IoT Platform trial is renewed
(free, from the Tuya developer console). Without that, the new key has to come from re-pairing or
another key-extraction route; nothing on the Pi can recover it.
