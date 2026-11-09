"""collector.process_commands: who dispenses a feed command, and when it must not be dispensed.

The feeder and Supabase are stubbed; the claim/defer/expiry rules are the real code."""
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

for k, v in {"FEEDER_DEVICE_ID": "d", "FEEDER_LOCAL_KEY": "k", "FEEDER_IP": "0.0.0.0",
             "SUPABASE_URL": "http://sb.invalid", "SUPABASE_KEY": ""}.items():
    os.environ.setdefault(k, v)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import collector  # noqa: E402


def row(age_s, id="c1", portions=1):
    created = datetime.now(timezone.utc) - timedelta(seconds=age_s)
    return {"id": id, "action": "feed", "portions": portions, "created_at": created.isoformat()}


class ProcessCommands(unittest.TestCase):
    def run_with(self, rows, *, defer=0, claim=True, confirmed=True):
        self.marks, self.reports = [], []
        self.feed = mock.Mock(return_value={"confirmed": confirmed})
        with mock.patch.object(collector, "FEED_DEFER_S", defer), \
             mock.patch.object(collector.fc, "fetch_pending_commands", return_value=rows), \
             mock.patch.object(collector.fc, "claim_command", return_value=claim), \
             mock.patch.object(collector.fc, "feed", self.feed), \
             mock.patch.object(collector.fc, "mark_command", lambda i, s, r="": self.marks.append((i, s))), \
             mock.patch.object(collector.ops, "report", lambda m, **k: self.reports.append(str(m))):
            return collector.process_commands()

    def test_primary_feeds_at_once_and_reports_nothing(self):
        self.assertTrue(self.run_with([row(1)]))
        self.feed.assert_called_once_with(1)
        self.assertEqual(self.marks, [("c1", "done")])
        self.assertEqual(self.reports, [])

    def test_primary_reports_a_command_realtime_missed(self):
        self.assertTrue(self.run_with([row(collector.LATE_S + 10)]))
        self.feed.assert_called_once()
        self.assertTrue(any("realtime missed it" in r for r in self.reports))

    def test_fallback_leaves_a_young_command_for_the_pi(self):
        self.assertFalse(self.run_with([row(2)], defer=8))
        self.feed.assert_not_called()
        self.assertEqual(self.marks, [])

    def test_fallback_feeds_an_unclaimed_old_command_and_reports_the_pi_down(self):
        self.assertTrue(self.run_with([row(9)], defer=8))
        self.feed.assert_called_once()
        self.assertTrue(any("the Pi is not" in r for r in self.reports))

    def test_lost_claim_never_feeds(self):
        self.assertFalse(self.run_with([row(9)], claim=False))
        self.feed.assert_not_called()
        self.assertEqual(self.marks, [])

    def test_expired_command_is_failed_not_dispensed(self):
        self.assertFalse(self.run_with([row(collector.MAX_AGE_S + 5)]))
        self.feed.assert_not_called()
        self.assertEqual(self.marks, [("c1", "failed")])
        self.assertEqual(len(self.reports), 1)

    def test_unconfirmed_feed_is_failed_and_reported(self):
        self.run_with([row(1)], confirmed=False)
        self.assertEqual(self.marks, [("c1", "failed")])
        self.assertEqual(len(self.reports), 1)

    def test_portions_are_clamped(self):
        self.run_with([row(1, portions=50)])
        self.feed.assert_called_once_with(12)


if __name__ == "__main__":
    unittest.main()


class LinkFailures(unittest.TestCase):
    """A link failure (DNS, refused, reset, 5xx: every Mac wake starts with some) is reported only once it
    has lasted LINK_RAISE_S, once per streak; anything else is reported at once; ok() ends a streak."""

    def setUp(self):
        self.reports, self.now = [], [1000.0]
        for p in (mock.patch.object(collector.ops, "report", lambda m, **k: self.reports.append(str(m))),
                  mock.patch.object(collector.time, "monotonic", lambda: self.now[0])):
            p.start()
            self.addCleanup(p.stop)

    def gai(self):
        import socket, urllib.error
        return urllib.error.URLError(socket.gaierror(8, "nodename nor servname provided"))

    def test_link_failure_reports_once_after_the_streak_is_long(self):
        s = collector.LinkStreak("loop")
        s.fail(self.gai())
        self.now[0] += 299
        s.fail(self.gai())
        self.assertEqual(self.reports, [])
        self.now[0] += 2
        s.fail(self.gai())
        self.now[0] += 60
        s.fail(self.gai())
        self.assertEqual(len(self.reports), 1)

    def test_ok_ends_the_streak(self):
        s = collector.LinkStreak("loop")
        s.fail(self.gai())
        self.now[0] += 200
        s.ok()
        self.now[0] += 200
        s.fail(self.gai())
        self.assertEqual(self.reports, [])

    def test_other_failures_report_at_once(self):
        import io, urllib.error
        s = collector.LinkStreak("loop")
        s.fail(ValueError("bad row"))
        s.fail(urllib.error.HTTPError("u", 401, "Unauthorized", {}, io.BytesIO()))
        self.assertEqual(len(self.reports), 2)
        s.fail(urllib.error.HTTPError("u", 502, "Bad Gateway", {}, io.BytesIO()))
        self.assertEqual(len(self.reports), 2)

    def test_realtime_retry_and_give_up_records_are_warnings(self):
        import logging
        for msg, level in [("Connection attempt failed: x", logging.WARNING),
                           ("Connection failed permanently after 5 attempts. Error: x", logging.WARNING),
                           ("something else", logging.ERROR)]:
            r = logging.LogRecord("realtime._async.client", logging.ERROR, "", 0, msg, None, None)
            collector._demote_connect_retry(r)
            self.assertEqual(r.levelno, level, msg)
