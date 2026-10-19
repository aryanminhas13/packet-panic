#!/usr/bin/env python3
"""Generate deterministic Packet Panic telemetry, including optional bad batches."""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "demo-events.jsonl"
EVENT_NAMESPACE = uuid.UUID("fba605dd-13b7-4b44-a62f-3cbd60fcb875")


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def make_event(
    session_id: str,
    player_id: str,
    sequence: int,
    event_type: str,
    client_time: datetime,
    wave: int,
    properties: dict | None = None,
) -> dict:
    event_id = str(uuid.uuid5(EVENT_NAMESPACE, f"{session_id}:{sequence}:{event_type}"))
    received = client_time + timedelta(milliseconds=120 + (sequence % 9) * 17)
    return {
        "event_id": event_id,
        "schema_version": 1,
        "event_type": event_type,
        "client_time": iso(client_time),
        "received_at": iso(received),
        "session_id": session_id,
        "anonymous_player_id": player_id,
        "sequence_number": sequence,
        "game_version": "2.0.0",
        "wave": wave,
        "source": "synthetic",
        "properties": properties or {},
    }


def generate_sessions(session_count: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    cohort_start = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)

    for index in range(session_count):
        session_id = f"ses_demo_{index:04d}"
        player_id = f"player_{index % max(24, session_count // 3):03d}"
        started = cohort_start + timedelta(minutes=index * 11 + rng.randint(0, 7))
        sequence = 1
        score = 0
        integrity = 3
        max_wave = rng.choices([1, 2, 3, 4, 5, 6], weights=[14, 22, 24, 18, 13, 9])[0]
        current = started
        events.append(make_event(session_id, player_id, sequence, "session_started", current, 1, {"device": rng.choice(["desktop", "desktop", "touch"])}))

        for wave in range(1, max_wave + 1):
            sequence += 1
            current += timedelta(milliseconds=rng.randint(300, 900))
            events.append(make_event(session_id, player_id, sequence, "wave_started", current, wave))
            interactions = 8 + wave * 3
            for _ in range(interactions):
                current += timedelta(milliseconds=rng.randint(380, 1300))
                corrupt = rng.random() < min(0.28 + wave * 0.045, 0.62)
                sequence += 1
                events.append(make_event(session_id, player_id, sequence, "packet_spawned", current, wave, {"packet_type": "corrupt" if corrupt else "clean"}))
                if corrupt:
                    if rng.random() < max(0.82 - wave * 0.055, 0.42):
                        sequence += 1
                        current += timedelta(milliseconds=rng.randint(80, 620))
                        events.append(make_event(session_id, player_id, sequence, "pulse_fired", current, wave, {"player_x": rng.randint(20, 900)}))
                        sequence += 1
                        current += timedelta(milliseconds=rng.randint(40, 180))
                        points = 150 * rng.randint(1, 4)
                        score += points
                        events.append(make_event(session_id, player_id, sequence, "corrupt_destroyed", current, wave, {"points": points, "score": score}))
                    else:
                        sequence += 1
                        current += timedelta(milliseconds=rng.randint(500, 1400))
                        integrity -= 1
                        events.append(make_event(session_id, player_id, sequence, "damage_taken", current, wave, {"integrity": max(integrity, 0), "score": score}))
                else:
                    sequence += 1
                    current += timedelta(milliseconds=rng.randint(130, 720))
                    points = 100 * rng.randint(1, 3)
                    score += points
                    events.append(make_event(session_id, player_id, sequence, "clean_collected", current, wave, {"points": points, "score": score}))
                if integrity <= 0:
                    break
            if integrity <= 0:
                break

        sequence += 1
        current += timedelta(milliseconds=rng.randint(250, 900))
        events.append(make_event(session_id, player_id, sequence, "session_ended", current, max_wave, {"score": score, "integrity": max(integrity, 0), "reason": "damage" if integrity <= 0 else "run_complete"}))

    return events


def inject_failure(events: list[dict], failure: str, seed: int) -> list[dict]:
    rng = random.Random(seed + 99)
    output = [dict(event) for event in events]
    if failure == "none":
        return output
    if failure == "duplicates":
        output.extend(dict(event) for event in output[50:75])
    elif failure == "late-events":
        for event in rng.sample(output, min(40, len(output))):
            event["client_time"] = iso(datetime.fromisoformat(event["client_time"].replace("Z", "+00:00")) - timedelta(hours=8))
    elif failure == "schema-drift":
        for event in rng.sample(output, min(30, len(output))):
            event["schema_version"] = 2
            event["level"] = event.pop("wave")
    elif failure == "orphan-events":
        for event in rng.sample([e for e in output if e["event_type"] != "session_started"], min(25, len(output))):
            event["session_id"] = f"orphan_{event['session_id']}"
    else:
        raise ValueError(f"Unknown failure: {failure}")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessions", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--failure", choices=["none", "duplicates", "late-events", "schema-drift", "orphan-events"], default="none")
    args = parser.parse_args()

    events = inject_failure(generate_sessions(args.sessions, args.seed), args.failure, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    print(json.dumps({"output": str(args.output), "events": len(events), "sessions": args.sessions, "failure": args.failure}))


if __name__ == "__main__":
    main()

