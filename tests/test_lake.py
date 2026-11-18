import tempfile
import unittest

from pipeline.lake import Lake

LOADED_AT = "2026-11-17T12:00:00Z"


def row(event_id: str) -> tuple:
    return (event_id, 1, "wave_started", LOADED_AT, LOADED_AT, "ses_1", "player_1", 1, "2.0.0", 1, "test", "{}", "batch_x", LOADED_AT)


class LakeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.lake = Lake(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, batch_hash="abc123"):
        return self.lake.write_batch(
            batch_hash=batch_hash, batch_id="batch_x", source_file="test.jsonl", payload=b"{}\n",
            loaded_at=LOADED_AT, valid_rows=[row("e1"), row("e2")],
            quarantined_rows=[("batch_x", 3, "unsupported_schema_version:2", "{}", LOADED_AT)],
        )

    def test_batch_commit_is_idempotent(self):
        self.assertTrue(self.write())
        self.assertFalse(self.write())
        self.assertEqual(len(self.lake.manifests()), 1)

    def test_bronze_round_trips_through_parquet(self):
        self.write()
        manifest = self.lake.manifests()[0]
        self.assertEqual(self.lake.read_bronze(manifest).num_rows, 2)
        self.assertEqual(len(self.lake.read_quarantine(manifest)), 1)
        self.assertIn("ingest_date=2026-11-17", manifest["bronze_path"])

    def test_uncommitted_batch_is_not_visible(self):
        self.assertFalse(self.lake.is_committed("never_written"))
        self.assertEqual(self.lake.manifests(), [])


if __name__ == "__main__":
    unittest.main()
