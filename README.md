# Packet Panic

**A retro arcade game with a gameplay telemetry pipeline.**

[Play the live demo](https://packet-panic-telemetry.byww6j8ky8.chatgpt.site)

Packet Panic began with a question: **can a small game generate enough meaningful behavioral data to explain where players struggle, quit, and improve?**

The project controls the full data lifecycle. The browser game emits structured events for player actions and outcomes, reconstructs those events into sessions, validates their quality, and transforms them into a progression funnel and gameplay insights.

## What it demonstrates

- Event-driven instrumentation with stable event and session identifiers
- Session reconstruction from immutable gameplay events
- Funnel analysis across increasingly difficult waves
- Data-quality checks for uniqueness, referential integrity, timestamps, and event types
- A Bronze → Silver → Gold transformation model
- A responsive analytics interface that connects metrics to product decisions

## Try it

1. Open the [live demo](https://packet-panic-telemetry.byww6j8ky8.chatgpt.site).
2. Use **A/D** or the arrow keys to move and **Space** to fire.
3. Collect clean packets and destroy corrupted packets.
4. Open **Analytics** to see your session added to the cohort.

## Event model

Every interaction is stored as an append-only event:

```json
{
  "event_id": "evt_8f31...",
  "session_id": "ses_mg...",
  "event_type": "corrupt_destroyed",
  "event_time": "2026-10-04T14:32:18Z",
  "wave": 3,
  "properties": {
    "combo": 4,
    "points": 600
  }
}
```

The current demo processes events locally so it can be played without an account or backend. A production version would preserve the same contract while moving ingestion and transformation to Kafka, object storage, dbt or Spark, and a warehouse.

## Pipeline

```text
Browser events
    ↓
Raw append-only event store (Bronze)
    ↓
Schema validation, deduplication, sessionization (Silver)
    ↓
Session facts, progression funnels, gameplay KPIs (Gold)
    ↓
Analytics console and product insights
```

## Run locally

The project is a dependency-free static application:

```bash
python3 -m http.server 4173 --directory dist
```

Then open `http://localhost:4173`.

## Technology

- HTML5 Canvas
- JavaScript
- CSS
- Browser local storage
- OpenAI Sites hosting

