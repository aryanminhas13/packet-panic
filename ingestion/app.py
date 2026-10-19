"""FastAPI event collector for Packet Panic's append-only Bronze layer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Literal

from fastapi import FastAPI, status
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
EVENT_FILE = ROOT / "data" / "raw" / "live-events.jsonl"
EVENT_FILE.parent.mkdir(parents=True, exist_ok=True)
WRITE_LOCK = Lock()

EventType = Literal[
    "session_started", "packet_spawned", "pulse_fired", "clean_collected",
    "corrupt_destroyed", "corrupt_leaked", "damage_taken", "wave_started", "session_ended",
]


class GameEvent(BaseModel):
    event_id: str = Field(min_length=8)
    schema_version: Literal[1]
    event_type: EventType
    client_time: datetime
    session_id: str = Field(min_length=4)
    anonymous_player_id: str = Field(min_length=4)
    sequence_number: int = Field(ge=1)
    game_version: str
    wave: int = Field(ge=1)
    properties: dict[str, Any] = Field(default_factory=dict)


app = FastAPI(title="Packet Panic Event Collector", version="1.0.0")


def known_event_ids() -> set[str]:
    if not EVENT_FILE.exists():
        return set()
    ids: set[str] = set()
    for line in EVENT_FILE.read_text(encoding="utf-8").splitlines():
        try:
            ids.add(json.loads(line)["event_id"])
        except (json.JSONDecodeError, KeyError):
            continue
    return ids


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "layer": "bronze-ingestion"}


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
def ingest_event(event: GameEvent) -> dict[str, str]:
    payload = event.model_dump(mode="json")
    payload["received_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    payload["source"] = "web"
    with WRITE_LOCK:
        if event.event_id in known_event_ids():
            return {"status": "duplicate", "event_id": event.event_id}
        with EVENT_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
    return {"status": "accepted", "event_id": event.event_id}

