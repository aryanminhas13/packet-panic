import json
import tempfile
import unittest
from pathlib import Path

from generator.generate_events import generate_sessions, inject_failure
from pipeline.run_pipeline import validate_event


class EventContractTests(unittest.TestCase):
    def test_generated_events_follow_contract(self):
        events = generate_sessions(5, 42)
        self.assertGreater(len(events), 100)
        self.assertTrue(all(validate_event(event) is None for event in events))

    def test_event_ids_are_unique_for_clean_batch(self):
        events = generate_sessions(8, 42)
        ids = [event["event_id"] for event in events]
        self.assertEqual(len(ids), len(set(ids)))

    def test_schema_drift_is_quarantinable(self):
        events = inject_failure(generate_sessions(3, 42), "schema-drift", 42)
        rejected = [event for event in events if validate_event(event)]
        self.assertGreater(len(rejected), 0)
        self.assertTrue(all("schema" in validate_event(event) for event in rejected))

    def test_duplicate_injection_preserves_event_identity(self):
        events = inject_failure(generate_sessions(5, 42), "duplicates", 42)
        ids = [event["event_id"] for event in events]
        self.assertGreater(len(ids), len(set(ids)))


if __name__ == "__main__":
    unittest.main()

