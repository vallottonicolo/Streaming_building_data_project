from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import AppConfig
from .kafka_utils import ensure_topics, make_producer, wait_for_kafka


def _jsonable(row: pd.Series) -> dict:
    """Convert a pandas row into JSON-safe Python scalar values."""
    result = {}
    for key, value in row.items():
        if pd.isna(value):
            result[key] = None
        elif hasattr(value, "item"):
            result[key] = value.item()
        else:
            result[key] = value
    return result


def site_partition(site_id: object, partition_count: int) -> int:
    """Map a numeric site id to its deterministic Kafka partition."""
    site = int(site_id)
    if site < 0:
        raise ValueError(f"site_id must be non-negative, got {site_id}")
    if site >= partition_count:
        raise ValueError(f"site_id {site_id} requires at least {site + 1} Kafka partitions")
    return site


def _write_state(cfg: AppConfig, state: dict) -> None:
    """Atomically write producer progress state into the runtime directory."""
    cfg.runtime_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.runtime_dir / "producer_state.json"
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _state(
    *,
    status: str,
    current_start_date: object | None,
    current_end_date: object | None,
    latest_source_timestamp: object | None,
    rows_sent: int,
    total_rows: int,
    message: str | None = None,
) -> dict:
    """Build a producer progress payload for logs and dashboard/monitor visibility."""
    percent = round((rows_sent / total_rows * 100), 2) if total_rows else 0.0
    return {
        "status": status,
        "current_start_date": str(current_start_date) if current_start_date is not None else None,
        "current_end_date": str(current_end_date) if current_end_date is not None else None,
        "latest_source_timestamp": str(latest_source_timestamp) if latest_source_timestamp is not None else None,
        "rows_sent": rows_sent,
        "total_rows": total_rows,
        "percent_complete": percent,
        "last_update_time": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }


def run_producer(cfg: AppConfig) -> None:
    """Stream all weather rows to Kafka in source timestamp order by site partition."""
    wait_for_kafka(cfg)
    ensure_topics(cfg)
    weather_csv = Path(cfg.paths["weather_csv"])
    topic = cfg.topics["weather"]
    ticks_per_batch = int(cfg.streaming["producer_ticks_per_batch"])
    sleep_s = float(cfg.streaming["producer_tick_sleep_seconds"])
    partition_count = int(cfg.raw["kafka"]["partitions"])
    if ticks_per_batch < 1:
        raise ValueError("streaming.producer_ticks_per_batch must be at least 1")
    if sleep_s < 0:
        raise ValueError("streaming.producer_tick_sleep_seconds must be non-negative")

    df = pd.read_csv(weather_csv)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values(["timestamp", "site_id"]).reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    unique_timestamps = sorted(df["timestamp"].unique())
    total_rows = len(df)
    _write_state(
        cfg,
        _state(
            status="running",
            current_start_date=None,
            current_end_date=None,
            latest_source_timestamp=None,
            rows_sent=0,
            total_rows=total_rows,
            message="producer started",
        ),
    )

    producer = make_producer(cfg)
    try:
        sent_total = 0
        for start in range(0, len(unique_timestamps), ticks_per_batch):
            batch_timestamps = set(unique_timestamps[start : start + ticks_per_batch])
            batch = df[df["timestamp"].isin(batch_timestamps)].copy()
            send_ts = int(time.time())
            futures = []
            for _, row in batch.iterrows():
                payload = _jsonable(row.drop(labels=["date"]))
                payload["timestamp"] = pd.Timestamp(payload["timestamp"]).isoformat()
                payload["weather_ts"] = send_ts
                site_id = payload["site_id"]
                futures.append(
                    producer.send(
                        topic,
                        key=f"site-{site_id}",
                        value=payload,
                        partition=site_partition(site_id, partition_count),
                    )
                )
            producer.flush()
            for future in futures:
                future.get(timeout=15)
            sent_total += len(futures)
            percent = (sent_total / total_rows * 100) if total_rows else 100.0
            start_ts = pd.Timestamp(min(batch_timestamps))
            end_ts = pd.Timestamp(max(batch_timestamps))
            _write_state(
                cfg,
                _state(
                    status="running",
                    current_start_date=start_ts.date(),
                    current_end_date=end_ts.date(),
                    latest_source_timestamp=end_ts.isoformat(),
                    rows_sent=sent_total,
                    total_rows=total_rows,
                    message="sending weather rows",
                ),
            )
            print(
                f"sent {len(futures)} weather rows for source_ts {start_ts}..{end_ts}; "
                f"total={sent_total}/{total_rows} ({percent:.2f}%)",
                flush=True,
            )
            time.sleep(sleep_s)
        _write_state(
            cfg,
            _state(
                status="completed",
                current_start_date=pd.Timestamp(unique_timestamps[-1]).date() if unique_timestamps else None,
                current_end_date=pd.Timestamp(unique_timestamps[-1]).date() if unique_timestamps else None,
                latest_source_timestamp=pd.Timestamp(unique_timestamps[-1]).isoformat() if unique_timestamps else None,
                rows_sent=sent_total,
                total_rows=total_rows,
                message="all weather rows sent",
            ),
        )
        print(f"producer completed; sent {sent_total}/{total_rows} weather rows", flush=True)
    except Exception as exc:
        _write_state(
            cfg,
            _state(
                status="failed",
                current_start_date=None,
                current_end_date=None,
                latest_source_timestamp=None,
                rows_sent=locals().get("sent_total", 0),
                total_rows=total_rows,
                message=str(exc),
            ),
        )
        raise
    finally:
        producer.close()
