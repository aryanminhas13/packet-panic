"""Dagster asset graph for the Packet Panic batch pipeline."""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset

from generator.generate_events import DEFAULT_OUTPUT, generate_sessions
from pipeline.run_pipeline import execute


@asset(group_name="bronze", compute_kind="python")
def synthetic_game_events(context: AssetExecutionContext) -> str:
    events = generate_sessions(session_count=120, seed=42)
    DEFAULT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_OUTPUT.open("w", encoding="utf-8") as handle:
        import json
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
    context.log.info("Generated %s immutable game events", len(events))
    return str(DEFAULT_OUTPUT)


@asset(group_name="gold", compute_kind="duckdb+dbt", deps=[synthetic_game_events])
def gameplay_analytics(context: AssetExecutionContext) -> dict:
    result = execute(Path(DEFAULT_OUTPUT))
    context.log.info("Published metrics for %s sessions", result["metrics"]["sessions"])
    return result["metrics"]


defs = Definitions(assets=[synthetic_game_events, gameplay_analytics])

