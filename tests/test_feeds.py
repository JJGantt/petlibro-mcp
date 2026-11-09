"""Feed history: which log values become records, and when a scheduled slot counts as missed."""
import os
import sys
import unittest
from datetime import datetime
from pathlib import Path

for k, v in {"FEEDER_DEVICE_ID": "d", "FEEDER_LOCAL_KEY": "k", "FEEDER_IP": "0.0.0.0",
             "SUPABASE_URL": "http://sb.invalid", "SUPABASE_KEY": ""}.items():
    os.environ.setdefault(k, v)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import collector  # noqa: E402
import feeder_core as fc  # noqa: E402

EVERY_4H = [{"hour": h, "minute": 0, "mask": 0x7F, "enabled": True} for h in (0, 4, 8, 12, 16, 20)]
SINCE = datetime(2026, 11, 1)


def ts(*a):
    return datetime(*a).timestamp()


class FeedsIn(unittest.TestCase):
    def test_both_logs_become_records(self):
        dps = {"237": '{"value":1,"time":1794258011}', "247": '{"value":2,"time":1794251555}'}
        self.assertEqual(fc.feeds_in(dps), [
            {"kind": "scheduled", "portions": 1, "fed_at": 1794258011},
            {"kind": "manual", "portions": 2, "fed_at": 1794251555}])

    def test_pushed_update_with_one_log_or_none(self):
        self.assertEqual(fc.feeds_in({"247": '{"value":1,"time":5}'}),
                         [{"kind": "manual", "portions": 1, "fed_at": 5}])
        self.assertEqual(fc.feeds_in({"233": "feeding"}), [])
        self.assertEqual(fc.feeds_in({"237": "not json", "247": ""}), [])


class MissedSlot(unittest.TestCase):
    def test_slot_fed_on_time_is_not_missed(self):
        now = datetime(2026, 11, 9, 16, 30)
        self.assertIsNone(collector.missed_slot(EVERY_4H, ts(2026, 11, 9, 16, 0, 11), SINCE, now))

    def test_slot_with_no_feed_after_grace_is_missed(self):
        now = datetime(2026, 11, 9, 16, 30)
        self.assertEqual(collector.missed_slot(EVERY_4H, ts(2026, 11, 9, 12, 0, 10), SINCE, now),
                         datetime(2026, 11, 9, 16, 0))

    def test_within_grace_is_not_yet_missed(self):
        now = datetime(2026, 11, 9, 16, 5)
        self.assertIsNone(collector.missed_slot(EVERY_4H, ts(2026, 11, 9, 12, 0, 10), SINCE, now))

    def test_slot_before_the_collector_started_is_not_judged(self):
        now = datetime(2026, 11, 9, 16, 30)
        self.assertIsNone(collector.missed_slot(EVERY_4H, 0, datetime(2026, 11, 9, 16, 20), now))

    def test_midnight_slot_judged_across_the_day_boundary(self):
        now = datetime(2026, 11, 10, 0, 15)
        self.assertEqual(collector.missed_slot(EVERY_4H, ts(2026, 11, 9, 20, 0, 9), SINCE, now),
                         datetime(2026, 11, 10, 0, 0))

    def test_disabled_and_off_day_slots_are_ignored(self):
        now = datetime(2026, 11, 9, 16, 30)   # a Monday
        sched = [{"hour": 16, "minute": 0, "mask": 0x7F, "enabled": False},
                 {"hour": 16, "minute": 0, "mask": 0x7F & ~1, "enabled": True},
                 {"hour": 12, "minute": 0, "mask": 0x7F, "enabled": True}]
        self.assertIsNone(collector.missed_slot(sched, ts(2026, 11, 9, 12, 0, 10), SINCE, now))

    def test_no_schedule_known_reports_nothing(self):
        self.assertIsNone(collector.missed_slot([], 0, SINCE, datetime(2026, 11, 9, 16, 30)))


if __name__ == "__main__":
    unittest.main()
