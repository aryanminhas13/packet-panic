#!/usr/bin/env python3
"""Incrementally load raw events, run dbt, and publish dashboard metrics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "warehouse"
DB_PATH = WAREHOUSE / "packet_panic.duckdb"
DEFAULT_INPUT = ROOT / "data" / "raw" / "demo-events.jsonl"
OUTPUT = ROOT / "dist" / "data" / "analytics.json"
QUARANTINE_DIR = ROOT / "data" / "quarantine"

VALID_EVENT_TYPES = {
    "session_started", "packet_spawned", "pulse_fired", "clean_collected",
    "corrupt_destroyed", "corrupt_leaked", "damage_taken", "wave_started", "session_ended",
}
REQUIRED_FIELDS = {
    "event_id", "schema_version", "event_type", "client_time", "received_at",
    "session_id", "anonymous_player_id", "sequence_number", "game_version", "wave", "properties",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def initialize(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("""
        create table if not exists raw_game_events (
            event_id varchar,
            schema_version integer,
            event_type varchar,
            client_time varchar,
            received_at varchar,
            session_id varchar,
            anonymous_player_id varchar,
            sequence_number integer,
            game_version varchar,
            wave integer,
            source varchar,
            properties_json varchar,
            batch_id varchar,
            loaded_at timestamp
        )
    """)
    connection.execute("""
        create table if not exists quarantined_events (
            batch_id varchar,
            line_number integer,
            reason varchar,
            raw_payload varchar,
            quarantined_at timestamp
        )
    """)
    connection.execute("""
        create table if not exists ingested_batches (
            batch_hash varchar primary key,
            batch_id varchar,
            source_file varchar,
            ingested_at timestamp,
            valid_rows integer,
            quarantined_rows integer
        )
    """)
    connection.execute("""
        create table if not exists pipeline_runs (
            run_id varchar,
            batch_id varchar,
            started_at timestamp,
            completed_at timestamp,
            raw_rows integer,
            valid_rows integer,
            quarantined_rows integer,
            duplicate_rows integer,
            late_rows integer,
            status varchar,
            elapsed_ms integer
        )
    """)


def validate_event(event: dict) -> str | None:
    if event.get("schema_version") != 1:
        return f"unsupported_schema_version:{event.get('schema_version')}"
    missing = sorted(REQUIRED_FIELDS - event.keys())
    if missing:
        return "missing_fields:" + ",".join(missing)
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return f"unknown_event_type:{event.get('event_type')}"
    if not isinstance(event.get("wave"), int) or event["wave"] < 1:
        return "invalid_wave"
    if not isinstance(event.get("properties"), dict):
        return "invalid_properties"
    return None


def load_batch(connection: duckdb.DuckDBPyConnection, input_path: Path) -> dict:
    payload = input_path.read_bytes()
    batch_hash = hashlib.sha256(payload).hexdigest()
    previous = connection.execute("select batch_id from ingested_batches where batch_hash = ?", [batch_hash]).fetchone()
    if previous:
        return {"batch_id": previous[0], "skipped": True, "raw": 0, "valid": 0, "quarantined": 0, "duplicates": 0, "late": 0}

    batch_id = "batch_" + batch_hash[:12]
    loaded_at = utc_now()
    valid: list[tuple] = []
    quarantined: list[tuple] = []
    seen_ids: set[str] = set()
    duplicate_count = 0
    late_count = 0

    for line_number, line in enumerate(payload.decode("utf-8").splitlines(), 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            quarantined.append((batch_id, line_number, f"invalid_json:{error.msg}", line, loaded_at))
            continue
        reason = validate_event(event)
        if reason:
            quarantined.append((batch_id, line_number, reason, line, loaded_at))
            continue
        if event["event_id"] in seen_ids:
            duplicate_count += 1
        seen_ids.add(event["event_id"])
        try:
            client = datetime.fromisoformat(event["client_time"].replace("Z", "+00:00"))
            received = datetime.fromisoformat(event["received_at"].replace("Z", "+00:00"))
            if (received - client).total_seconds() > 3600:
                late_count += 1
        except ValueError:
            quarantined.append((batch_id, line_number, "invalid_timestamp", line, loaded_at))
            continue
        valid.append((
            event["event_id"], event["schema_version"], event["event_type"], event["client_time"],
            event["received_at"], event["session_id"], event["anonymous_player_id"],
            event["sequence_number"], event["game_version"], event["wave"], event.get("source", "unknown"),
            json.dumps(event["properties"], separators=(",", ":")), batch_id, loaded_at,
        ))

    if valid:
        connection.executemany("insert into raw_game_events values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", valid)
    if quarantined:
        connection.executemany("insert into quarantined_events values (?, ?, ?, ?, ?)", quarantined)
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        quarantine_path = QUARANTINE_DIR / f"{batch_id}.jsonl"
        with quarantine_path.open("w", encoding="utf-8") as handle:
            for row in quarantined:
                handle.write(json.dumps({"batch_id": row[0], "line_number": row[1], "reason": row[2], "raw_payload": row[3]}) + "\n")

    connection.execute("insert into ingested_batches values (?, ?, ?, ?, ?, ?)", [batch_hash, batch_id, str(input_path), loaded_at, len(valid), len(quarantined)])
    return {
        "batch_id": batch_id,
        "skipped": False,
        "raw": len(payload.decode("utf-8").splitlines()),
        "valid": len(valid),
        "quarantined": len(quarantined),
        "duplicates": duplicate_count,
        "late": late_count,
    }


def run_dbt() -> None:
    dbt = shutil.which("dbt")
    if not dbt:
        candidate = Path(sys.executable).parent / "dbt"
        dbt = str(candidate) if candidate.exists() else None
    if not dbt:
        raise RuntimeError("dbt was not found. Install dependencies with: pip install -r requirements.txt")
    environment = os.environ.copy()
    environment["DBT_PROFILES_DIR"] = str(WAREHOUSE)
    environment["PACKET_PANIC_DUCKDB_PATH"] = str(DB_PATH)
    subprocess.run([dbt, "run", "--project-dir", str(WAREHOUSE)], check=True, env=environment)
    subprocess.run([dbt, "test", "--project-dir", str(WAREHOUSE)], check=True, env=environment)


def export_dashboard(connection: duckdb.DuckDBPyConnection, run: dict, elapsed_ms: int) -> dict:
    totals = connection.execute("""
        select
            count(*) as sessions,
            round(avg(duration_seconds), 1) as avg_survival,
            round(100.0 * sum(corrupt_packets_destroyed) / nullif(sum(corrupt_packets_destroyed + damage_events), 0), 1) as intercept_rate,
            round(avg(final_score), 0) as avg_score
        from fct_sessions
    """).fetchone()
    funnel = [
        {"wave": row[0], "sessions": row[1], "rate": float(row[2])}
        for row in connection.execute("select wave, sessions_reached, reach_rate_pct from agg_wave_funnel order by wave").fetchall()
    ]
    distribution = [
        {"event_type": row[0], "count": row[1]}
        for row in connection.execute("select event_type, event_count from agg_event_distribution order by event_count desc").fetchall()
    ]
    total_raw = connection.execute("select count(*) from raw_game_events").fetchone()[0]
    unique_events = connection.execute("select count(*) from stg_game_events").fetchone()[0]
    quarantine_total = connection.execute("select count(*) from quarantined_events").fetchone()[0]
    orphan_sessions = connection.execute("""
        select count(distinct e.session_id)
        from stg_game_events e
        left join (select distinct session_id from stg_game_events where event_type = 'session_started') s using (session_id)
        where s.session_id is null
    """).fetchone()[0]
    duplicate_total = max(total_raw - unique_events, 0)
    quality_rate = round(100.0 * unique_events / max(total_raw + quarantine_total, 1), 1)
    largest_drop = None
    for current, following in zip(funnel, funnel[1:]):
        drop = round(current["rate"] - following["rate"], 1)
        if largest_drop is None or drop > largest_drop["drop"]:
            largest_drop = {"wave": current["wave"], "next_wave": following["wave"], "drop": drop}

    output = {
        "generated_at": utc_now(),
        "source": "DuckDB warehouse transformed by dbt",
        "pipeline": {
            "run_id": run["run_id"],
            "batch_id": run["batch_id"],
            "status": "passed",
            "elapsed_ms": elapsed_ms,
            "incremental_rows": run["valid"],
            "batch_replay_skipped": run["skipped"],
        },
        "metrics": {
            "sessions": totals[0],
            "avg_survival_seconds": float(totals[1] or 0),
            "intercept_rate_pct": float(totals[2] or 0),
            "avg_score": int(totals[3] or 0),
            "event_quality_pct": quality_rate,
            "normalized_events": unique_events,
        },
        "funnel": funnel,
        "event_distribution": distribution,
        "insight": largest_drop,
        "quality_checks": [
            {"name": "Unique event identifiers", "status": "pass", "value": f"{unique_events:,} unique", "failed_rows": duplicate_total},
            {"name": "Supported schema version", "status": "pass" if quarantine_total == 0 else "warn", "value": f"{quarantine_total:,} quarantined", "failed_rows": quarantine_total},
            {"name": "Sessions resolve to a start", "status": "pass" if orphan_sessions == 0 else "fail", "value": f"{orphan_sessions:,} orphan sessions", "failed_rows": orphan_sessions},
            {"name": "Accepted event vocabulary", "status": "pass", "value": f"{len(VALID_EVENT_TYPES)} types", "failed_rows": 0},
        ],
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    return output


def execute(input_path: Path) -> dict:
    started = time.perf_counter()
    run_id = "run_" + uuid.uuid4().hex[:12]
    connection = duckdb.connect(str(DB_PATH))
    initialize(connection)
    batch = load_batch(connection, input_path)
    connection.close()
    run_dbt()
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    record = {"run_id": run_id, **batch}
    connection = duckdb.connect(str(DB_PATH))
    connection.execute("insert into pipeline_runs values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
        run_id, batch["batch_id"], utc_now(), utc_now(), batch["raw"], batch["valid"],
        batch["quarantined"], batch["duplicates"], batch["late"], "passed", elapsed_ms,
    ])
    output = export_dashboard(connection, record, elapsed_ms)
    connection.close()
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--reset", action="store_true", help="Delete the local warehouse before loading.")
    args = parser.parse_args()
    if args.reset and DB_PATH.exists():
        DB_PATH.unlink()
    output = execute(args.input)
    print(json.dumps({"status": "passed", "sessions": output["metrics"]["sessions"], "events": output["metrics"]["normalized_events"], "output": str(OUTPUT)}))


if __name__ == "__main__":
    main()
