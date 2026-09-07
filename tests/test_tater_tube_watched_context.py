import json
import unittest

from test_tater_tube_core import load_tater_tube_core


class TaterTubeWatchedContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_tater_tube_core()

    def test_late_live_tune_in_retains_played_time_separately_from_position(self):
        compact, _ = self.core._compact_activity_events([
            {
                "source": "tube_tv",
                "media_type": "movie",
                "title": "A Long Movie",
                "state": "stopped",
                "position_ms": 7_140_000,
                "duration_ms": 7_200_000,
                "metadata_json": json.dumps({"watched_ms": 60_000}),
            }
        ])
        self.assertEqual(compact[0]["progress"], 99)
        self.assertEqual(compact[0]["watched_ms"], 60_000)
        self.assertEqual(compact[0]["duration_ms"], 7_200_000)

    def test_legacy_events_keep_their_existing_progress_without_inferred_watch_time(self):
        compact, _ = self.core._compact_activity_events([
            {
                "source": "local_media",
                "media_type": "episode",
                "title": "Episode Two",
                "series_title": "Harbor Street",
                "state": "completed",
                "position_ms": 1_200_000,
                "duration_ms": 1_200_000,
            }
        ])
        self.assertEqual(compact[0]["progress"], 100)
        self.assertEqual(compact[0]["state"], "completed")
        self.assertEqual(compact[0]["series_title"], "Harbor Street")
        self.assertNotIn("watched_ms", compact[0])
        self.assertNotIn("duration_ms", compact[0])

    def test_zero_watched_time_is_distinct_from_missing_or_invalid_measurements(self):
        compact, _ = self.core._compact_activity_events([
            {"title": "A Movie", "metadata": {"watched_ms": 0}}
        ])
        self.assertEqual(compact[0]["watched_ms"], 0)
        self.assertNotIn("duration_ms", compact[0])
        for invalid in (None, True, -1, "unknown", float("nan"), float("inf")):
            with self.subTest(invalid=invalid):
                compact, _ = self.core._compact_activity_events([
                    {"title": "A Movie", "metadata": {"watched_ms": invalid}}
                ])
                self.assertNotIn("watched_ms", compact[0])


if __name__ == "__main__":
    unittest.main()
