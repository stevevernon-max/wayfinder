import json
import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from darwin_feed import build_feed, process_message, future_viaduct_crossings, planned_works  # noqa: E402


NOW = datetime(2026, 11, 19, 11, 0, tzinfo=timezone.utc)


def kafka_record(*, cancel_reason=None, event_ts="2026-11-19T11:00:00Z"):
    ts = {
        "rid": "202611190000001",
        "uid": "W12345",
        "trainId": "1A23",
        "ssd": "2026-11-19",
        "Location": [
            {"tpl": "MORAR", "arr": {"at": "10:55"}},
            {"tpl": "ARISAIG", "arr": {"et": "11:03"}},
            {"tpl": "LCHALRT", "arr": {"et": "11:06"}},
            {"tpl": "GLNFNNN", "pass": {"et": "11:08"}},
            {"tpl": "FRTWLM", "arr": {"et": "11:22"}},
        ],
    }
    if cancel_reason is not None:
        ts["cancelReason"] = cancel_reason
    payload = {"ts": event_ts, "version": "18.0", "uR": {"TS": ts}}
    return json.dumps({"destination": {"name": "sample"}, "bytes": json.dumps(payload)})


class DarwinFeedTests(unittest.TestCase):
    def test_unwraps_rdm_envelope_and_maps_forecasts_without_gps(self):
        state = {"services": {}}
        self.assertEqual(process_message(kafka_record(), state), 1)

        feed = build_feed(state, NOW)
        self.assertEqual(feed["status"], "ok")
        train = feed["trains"][0]
        self.assertEqual(train["id"], "1A23")
        self.assertEqual(train["direction"], "out")
        self.assertAlmostEqual(train["progress"], 86.5004475, places=4)
        self.assertEqual(train["positionLabel"], "Estimated Morar–Arisaig")
        self.assertEqual(train["viewpoints"]["glenfinnan"]["etaMinutes"], 8.0)
        self.assertFalse(train["locationVerified"])
        self.assertIsNone(train["lat"])
        self.assertIsNone(train["lon"])

    def test_ignores_stale_reports(self):
        state = {"services": {}}
        process_message(kafka_record(event_ts="2026-11-19T10:49:00Z"), state)
        self.assertEqual(build_feed(state, NOW)["trains"], [])

    def test_tomorrows_crossing_is_retained_after_midnight(self):
        state = {"services": {
            "future": {
                "rid": "future",
                "serviceDate": "2026-11-20",
                "scheduleAt": {
                    "LCHALRT": "2026-11-20T09:50:00Z",
                    "GLNFNNN": "2026-11-20T10:00:00Z",
                    "LCHESDE": "2026-11-20T10:09:00Z",
                },
                "route": ["LCHALRT", "GLNFNNN", "LCHESDE"],
                "observedAt": NOW.isoformat().replace("+00:00", "Z"),
                "scheduled": True,
            }
        }}
        feed = build_feed(state, NOW)
        self.assertTrue(feed["futureCrossings"])
        self.assertEqual(feed["futureCrossings"][0]["at"], "2026-11-20T10:01:00Z")
        self.assertEqual(feed["futureCrossings"][0]["direction"], "fort_william")

    def test_works_unknown_without_a_checked_source(self):
        feed = build_feed({"services": {}}, NOW)
        self.assertEqual(feed["plannedWorks"]["status"], "unavailable")
        self.assertEqual(feed["plannedWorks"]["items"], [])

    def test_future_feed_uses_explicit_crossings_not_repeating_today(self):
        state = {"services": {}, "futureTimetable": {
            "updated": NOW.isoformat().replace("+00:00", "Z"),
            "crossings": [
                {"at": "2026-11-21T10:30:00Z", "direction": "mallaig", "id": "scheduled-1"}
            ]
        }}
        feed = build_feed(state, NOW)
        self.assertEqual(feed["futureCrossings"][0]["at"], "2026-11-21T10:30:00Z")
        self.assertEqual(len(feed["futureCrossings"]), 1)

    def test_preserves_cancellation_flag(self):
        state = {"services": {}}
        process_message(kafka_record(cancel_reason=4), state)
        self.assertTrue(build_feed(state, NOW)["trains"][0]["cancelled"])


if __name__ == "__main__":
    unittest.main()
