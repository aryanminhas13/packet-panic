"""Object-storage landing zone for Packet Panic.

The lake is the durable source of truth; the DuckDB warehouse is disposable and
can be rebuilt from it at any time (see ``run_pipeline.py --rebuild-from-lake``).

Layout (Hive-style partitions so engines can prune by date)::

    raw/events/ingest_date=YYYY-MM-DD/<batch_hash>.jsonl        original bytes, immutable
    bronze/game_events/ingest_date=YYYY-MM-DD/<batch_id>.parquet validated rows, typed
    quarantine/ingest_date=YYYY-MM-DD/<batch_id>.jsonl          rejected rows + reason
    _manifests/<batch_hash>.json                                commit marker, written last

Configuration (environment):

    LAKE_URI            file path or s3://bucket[/prefix]   (default: ./data/lake)
    AWS_ENDPOINT_URL    S3-compatible endpoint, e.g. http://localhost:9000 for MinIO
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAKE = ROOT / "data" / "lake"

RAW_EVENT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("schema_version", pa.int32()),
    ("event_type", pa.string()),
    ("client_time", pa.string()),
    ("received_at", pa.string()),
    ("session_id", pa.string()),
    ("anonymous_player_id", pa.string()),
    ("sequence_number", pa.int32()),
    ("game_version", pa.string()),
    ("wave", pa.int32()),
    ("source", pa.string()),
    ("properties_json", pa.string()),
    ("batch_id", pa.string()),
    ("loaded_at", pa.timestamp("us")),
])


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


class Lake:
    def __init__(self, uri: str | None = None) -> None:
        self.uri = (uri or os.environ.get("LAKE_URI") or str(DEFAULT_LAKE)).rstrip("/")
        protocol = self.uri.split("://", 1)[0] if "://" in self.uri else "file"
        options: dict = {}
        if protocol == "s3":
            endpoint = os.environ.get("AWS_ENDPOINT_URL")
            if endpoint:
                options["client_kwargs"] = {"endpoint_url": endpoint}
        self.fs = fsspec.filesystem(protocol, **options)
        self.base = self.uri.split("://", 1)[1] if "://" in self.uri else self.uri
        if protocol == "file":
            Path(self.base).mkdir(parents=True, exist_ok=True)

    def _path(self, *parts: str) -> str:
        return "/".join([self.base, *parts])

    def _write(self, path: str, data: bytes) -> None:
        parent = path.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)
        with self.fs.open(path, "wb") as handle:
            handle.write(data)

    def is_committed(self, batch_hash: str) -> bool:
        return self.fs.exists(self._path("_manifests", f"{batch_hash}.json"))

    def write_batch(
        self,
        *,
        batch_hash: str,
        batch_id: str,
        source_file: str,
        payload: bytes,
        loaded_at: str,
        valid_rows: list[tuple],
        quarantined_rows: list[tuple],
    ) -> bool:
        """Persist a batch to the lake. Returns False if it was already committed."""
        if self.is_committed(batch_hash):
            return False
        partition = f"ingest_date={loaded_at[:10]}"

        self._write(self._path("raw", "events", partition, f"{batch_hash}.jsonl"), payload)

        columns = list(zip(*valid_rows)) if valid_rows else [[] for _ in RAW_EVENT_SCHEMA]
        arrays = []
        for field, values in zip(RAW_EVENT_SCHEMA, columns):
            values = list(values)
            if field.name == "loaded_at":
                values = [_parse_ts(v) for v in values]
            arrays.append(pa.array(values, type=field.type))
        table = pa.Table.from_arrays(arrays, schema=RAW_EVENT_SCHEMA)
        bronze_path = self._path("bronze", "game_events", partition, f"{batch_id}.parquet")
        self.fs.makedirs(bronze_path.rsplit("/", 1)[0], exist_ok=True)
        with self.fs.open(bronze_path, "wb") as handle:
            pq.write_table(table, handle, compression="zstd")

        quarantine_path = None
        if quarantined_rows:
            lines = "".join(
                json.dumps({"batch_id": r[0], "line_number": r[1], "reason": r[2], "raw_payload": r[3]}) + "\n"
                for r in quarantined_rows
            )
            quarantine_path = self._path("quarantine", partition, f"{batch_id}.jsonl")
            self._write(quarantine_path, lines.encode("utf-8"))

        manifest = {
            "batch_hash": batch_hash,
            "batch_id": batch_id,
            "source_file": source_file,
            "ingested_at": loaded_at,
            "valid_rows": len(valid_rows),
            "quarantined_rows": len(quarantined_rows),
            "bronze_path": bronze_path,
            "quarantine_path": quarantine_path,
        }
        # Manifest last: a crash mid-batch leaves no manifest, so the batch is retried.
        self._write(self._path("_manifests", f"{batch_hash}.json"), json.dumps(manifest, indent=2).encode())
        return True

    def manifests(self) -> list[dict]:
        pattern = self._path("_manifests", "*.json")
        result = []
        for path in sorted(self.fs.glob(pattern)):
            with self.fs.open(path, "rb") as handle:
                result.append(json.loads(handle.read()))
        return sorted(result, key=lambda m: m["ingested_at"])

    def read_bronze(self, manifest: dict) -> pa.Table:
        with self.fs.open(manifest["bronze_path"], "rb") as handle:
            return pq.read_table(handle)

    def read_quarantine(self, manifest: dict) -> list[dict]:
        if not manifest.get("quarantine_path"):
            return []
        with self.fs.open(manifest["quarantine_path"], "rb") as handle:
            return [json.loads(line) for line in handle.read().decode("utf-8").splitlines() if line]
