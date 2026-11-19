import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from darwin_feed import build_feed, process_message  # noqa: E402


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
            {"tpl": "LOCHAILORT", "arr": {"et": "11:06"}},
            {"tpl": "GLENFINNAN", "pass": {"et": "11:08"}},
            {"tpl": "FTWILLIAM", "arr": {"et": "11:22"}},
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
        self.assertEqual(train["progress"], 80)
        self.assertEqual(train["positionLabel"], "Last actual report: Morar")
        self.assertEqual(train["viewpoints"]["glenfinnan"]["etaMinutes"], 8.0)
        self.assertFalse(train["locationVerified"])
        self.assertIsNone(train["lat"])
        self.assertIsNone(train["lon"])

    def test_ignores_stale_reports(self):
        state = {"services": {}}
        process_message(kafka_record(event_ts="2026-11-19T10:49:00Z"), state)
        self.assertEqual(build_feed(state, NOW)["trains"], [])

    def test_preserves_cancellation_flag(self):
        state = {"services": {}}
        process_message(kafka_record(cancel_reason=4), state)
        self.assertTrue(build_feed(state, NOW)["trains"][0]["cancelled"])


if __name__ == "__main__":
    unittest.main()
