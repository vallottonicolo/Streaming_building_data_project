from __future__ import annotations

import json
from pathlib import Path

from .config import AppConfig
from .kafka_utils import topic_offsets


def _weather_source_rows(cfg: AppConfig) -> int | None:
    """Return the number of source weather rows when the CSV is readable."""
    path = Path(cfg.paths["weather_csv"])
    if not path.exists():
        return None
    try:
        return sum(1 for _ in path.open("r", encoding="utf-8")) - 1
    except OSError:
        return None


def _read_producer_state(cfg: AppConfig) -> dict | None:
    """Read the producer progress state file if it exists and is valid JSON."""
    path = cfg.runtime_dir / "producer_state.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"status": "unreadable", "message": f"invalid JSON in {path}"}


def run_monitor(cfg: AppConfig) -> None:
    """Print producer progress, Kafka offsets, and Hudi table size summaries."""
    producer_state = _read_producer_state(cfg)
    expected_weather_rows = _weather_source_rows(cfg)
    print("Producer")
    if not producer_state:
        print("  state: not started")
        if expected_weather_rows is not None:
            print(f"  source weather rows: {expected_weather_rows}")
    else:
        status = producer_state.get("status", "unknown")
        rows_sent = producer_state.get("rows_sent", 0)
        total_rows = producer_state.get("total_rows", 0)
        percent = producer_state.get("percent_complete", 0)
        date_start = producer_state.get("current_start_date")
        date_end = producer_state.get("current_end_date")
        latest_source_timestamp = producer_state.get("latest_source_timestamp")
        updated = producer_state.get("last_update_time")
        message = producer_state.get("message")
        print(f"  state: {status}")
        print(f"  rows: {rows_sent}/{total_rows} ({percent}%)")
        print(f"  current batch dates: {date_start}..{date_end}")
        print(f"  latest source timestamp: {latest_source_timestamp}")
        print(f"  updated: {updated}")
        if message:
            print(f"  message: {message}")

    print("Kafka offsets")
    offsets_by_topic = topic_offsets(cfg)
    for topic, offsets in offsets_by_topic.items():
        total = sum(offsets.values())
        print(f"  {topic}: total={total} partitions={offsets}")

    weather_topic = cfg.topics["weather"]
    weather_total = sum(offsets_by_topic.get(weather_topic, {}).values())
    if expected_weather_rows is not None and weather_total >= expected_weather_rows and not producer_state:
        print("\nPipeline state: weather topic has at least one full source file of input; downstream services may still be catching up.")
    elif producer_state and producer_state.get("status") == "completed":
        print("\nPipeline state: producer completed; downstream services may be catching up or idle.")
    elif producer_state and int(producer_state.get("rows_sent", 0)) > weather_total:
        print("\nPipeline state: producer has sent more rows than Kafka currently reports; check Kafka/producer logs.")
    elif producer_state and producer_state.get("status") == "running":
        print("\nPipeline state: producer is running; Kafka offsets should continue increasing.")
    else:
        print("\nPipeline state: no active producer state found; output topics will not advance unless new input is sent.")

    base = Path(cfg.hudi["base_path"])
    print("\nHudi tables")
    if not base.exists():
        print(f"  missing: {base}")
        return
    for table in sorted(p for p in base.iterdir() if p.is_dir()):
        parquet_files = list(table.rglob("*.parquet"))
        commits = list((table / ".hoodie").glob("*commit")) if (table / ".hoodie").exists() else []
        size_mb = sum(p.stat().st_size for p in parquet_files) / 1024 / 1024
        print(f"  {table.name}: parquet_files={len(parquet_files)} commits={len(commits)} size_mb={size_mb:.2f}")
