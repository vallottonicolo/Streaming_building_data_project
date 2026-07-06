from __future__ import annotations

from datetime import date, timedelta
import threading
import time
from collections import OrderedDict
from importlib import resources
from pathlib import Path
from typing import Any

import pandas as pd
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from .config import AppConfig, load_config
from .kafka_utils import make_consumer


class RecordStore:
    def __init__(self, max_records: int, retention_days: int = 30):
        """Create a bounded in-memory store for dashboard topic records."""
        self.max_records = max_records
        self.retention_days = max(1, retention_days)
        self.lock = threading.Lock()
        self.predictions: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.building_blocks: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.building_daily: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.site_daily: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.building_block_keys_by_date: dict[date, set[str]] = {}
        self.building_daily_keys_by_date: dict[date, set[str]] = {}
        self.site_daily_keys_by_date: dict[date, set[str]] = {}
        self.received = {"predictions": 0, "building": 0, "site_daily": 0}
        self.latest_building_date: date | None = None
        self.latest_site_daily_date: date | None = None
        self.last_building_cutoff: date | None = None
        self.last_site_daily_cutoff: date | None = None

    def upsert(self, bucket: str, record: dict[str, Any]) -> None:
        """Insert or replace a record in a topic bucket and enforce retention."""
        key = str(record.get("record_key") or f"{bucket}-{time.time_ns()}")
        with self.lock:
            if bucket == "building":
                self._upsert_building_block(key, record)
            elif bucket == "site_daily":
                self._upsert_site_daily(record)
            else:
                self._upsert_bounded(self.predictions, key, record)
            self.received[bucket] += 1

    def _upsert_bounded(self, target: OrderedDict[str, dict[str, Any]], key: str, record: dict[str, Any]) -> None:
        """Insert or replace a record and enforce the raw record count bound."""
        target[key] = record
        target.move_to_end(key)
        while len(target) > self.max_records:
            target.popitem(last=False)

    @staticmethod
    def _record_date(record: dict[str, Any]) -> date | None:
        """Parse a dashboard record's ISO date field when available."""
        value = record.get("date")
        if value is None:
            return None
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None

    @staticmethod
    def _building_daily_key(record: dict[str, Any]) -> str | None:
        """Build the aggregate key for one building/day chart row."""
        site_id = record.get("site_id")
        building_id = record.get("building_id")
        record_date = record.get("date")
        if site_id is None or building_id is None or record_date is None:
            return None
        return f"{site_id}|{building_id}|{str(record_date)[:10]}"

    @staticmethod
    def _site_daily_key(record: dict[str, Any]) -> str | None:
        """Build the aggregate key for one site/day chart row."""
        site_id = record.get("site_id")
        record_date = record.get("date")
        if site_id is None or record_date is None:
            return None
        return f"{site_id}|{str(record_date)[:10]}"

    def _retention_cutoff(self, latest_date: date | None) -> date | None:
        """Return the oldest retained date for one dashboard chart window."""
        if latest_date is None:
            return None
        return latest_date - timedelta(days=self.retention_days - 1)

    def _within_retention(self, record_date: date, latest_date: date | None) -> bool:
        """Return whether a date should be retained in one chart window."""
        cutoff = self._retention_cutoff(latest_date)
        return cutoff is None or record_date >= cutoff

    @staticmethod
    def _index_key(index: dict[date, set[str]], record_date: date, key: str) -> None:
        """Track one record key in a date bucket for cheap whole-day eviction."""
        index.setdefault(record_date, set()).add(key)

    @staticmethod
    def _discard_key(index: dict[date, set[str]], record_date: date | None, key: str) -> None:
        """Remove one key from its date bucket and drop empty buckets."""
        if record_date is None:
            return
        keys = index.get(record_date)
        if keys is None:
            return
        keys.discard(key)
        if not keys:
            index.pop(record_date, None)

    def _maybe_advance_building_window(self, record_date: date) -> None:
        """Advance the retained building window only when a newer date arrives."""
        if self.latest_building_date is None or record_date > self.latest_building_date:
            self.latest_building_date = record_date
            self._evict_old_building_dates()

    def _maybe_advance_site_daily_window(self, record_date: date) -> None:
        """Advance the retained site window only when a newer date arrives."""
        if self.latest_site_daily_date is None or record_date > self.latest_site_daily_date:
            self.latest_site_daily_date = record_date
            self._evict_old_site_daily_dates()

    def _evict_old_building_dates(self) -> None:
        """Remove building chart state outside the retained building date window."""
        cutoff = self._retention_cutoff(self.latest_building_date)
        if cutoff is None or cutoff == self.last_building_cutoff:
            return
        self.last_building_cutoff = cutoff
        for record_date in [bucket_date for bucket_date in self.building_block_keys_by_date if bucket_date < cutoff]:
            for key in self.building_block_keys_by_date.pop(record_date):
                self.building_blocks.pop(key, None)
        for record_date in [bucket_date for bucket_date in self.building_daily_keys_by_date if bucket_date < cutoff]:
            for key in self.building_daily_keys_by_date.pop(record_date):
                self.building_daily.pop(key, None)

    def _evict_old_site_daily_dates(self) -> None:
        """Remove site chart state outside the retained site date window."""
        cutoff = self._retention_cutoff(self.latest_site_daily_date)
        if cutoff is None or cutoff == self.last_site_daily_cutoff:
            return
        self.last_site_daily_cutoff = cutoff
        for record_date in [bucket_date for bucket_date in self.site_daily_keys_by_date if bucket_date < cutoff]:
            for key in self.site_daily_keys_by_date.pop(record_date):
                self.site_daily.pop(key, None)

    def _upsert_building_block(self, key: str, record: dict[str, Any]) -> None:
        """Dedupe q7b blocks while maintaining daily building chart totals."""
        record_date = self._record_date(record)
        daily_key = self._building_daily_key(record)
        if record_date is None or daily_key is None:
            return
        self._maybe_advance_building_window(record_date)
        if not self._within_retention(record_date, self.latest_building_date):
            return

        existing = self.building_blocks.get(key)
        if existing is not None:
            self._remove_building_block_from_daily(existing)
            self._discard_key(self.building_block_keys_by_date, self._record_date(existing), key)

        self.building_blocks[key] = record
        self.building_blocks.move_to_end(key)
        self._index_key(self.building_block_keys_by_date, record_date, key)
        self._add_building_block_to_daily(record)

    def _add_building_block_to_daily(self, record: dict[str, Any]) -> None:
        """Add one q7b block value into its building/day aggregate."""
        daily_key = self._building_daily_key(record)
        if daily_key is None:
            return
        current = self.building_daily.get(daily_key)
        if current is None:
            current = {
                "site_id": str(record["site_id"]),
                "building_id": str(record["building_id"]),
                "date": str(record["date"])[:10],
                "predicted_daily": 0.0,
                "block_count": 0,
            }
        current["predicted_daily"] = float(current.get("predicted_daily", 0.0)) + float(
            record.get("energy_consumption_6h", 0.0)
        )
        current["block_count"] = int(current.get("block_count", 0)) + 1
        self.building_daily[daily_key] = current
        self.building_daily.move_to_end(daily_key)
        record_date = self._record_date(record)
        if record_date is not None:
            self._index_key(self.building_daily_keys_by_date, record_date, daily_key)

    def _remove_building_block_from_daily(self, record: dict[str, Any]) -> None:
        """Subtract a previously seen q7b block from its building/day aggregate."""
        daily_key = self._building_daily_key(record)
        current = self.building_daily.get(daily_key) if daily_key else None
        if current is None:
            return
        current["predicted_daily"] = float(current.get("predicted_daily", 0.0)) - float(
            record.get("energy_consumption_6h", 0.0)
        )
        current["block_count"] = max(0, int(current.get("block_count", 0)) - 1)
        if current["block_count"] == 0:
            self.building_daily.pop(daily_key, None)
            self._discard_key(self.building_daily_keys_by_date, self._record_date(record), daily_key)
        else:
            self.building_daily[daily_key] = current

    def _upsert_site_daily(self, record: dict[str, Any]) -> None:
        """Store site daily rows by date with date-window retention."""
        record_date = self._record_date(record)
        key = self._site_daily_key(record)
        if record_date is None or key is None:
            return
        self._maybe_advance_site_daily_window(record_date)
        if self._within_retention(record_date, self.latest_site_daily_date):
            existing = self.site_daily.get(key)
            if existing is not None:
                self._discard_key(self.site_daily_keys_by_date, self._record_date(existing), key)
            self.site_daily[key] = record
            self.site_daily.move_to_end(key)
            self._index_key(self.site_daily_keys_by_date, record_date, key)

    def snapshot(self) -> dict[str, Any]:
        """Return a thread-safe copy of records and filter metadata."""
        with self.lock:
            records_by_bucket = {
                "predictions": list(self.predictions.values()),
                "building": list(self.building_daily.values()),
                "site_daily": list(self.site_daily.values()),
            }
            sites = set()
            buildings_by_site: dict[str, set[str]] = {}
            for records in records_by_bucket.values():
                for rec in records:
                    site_id = rec.get("site_id")
                    if site_id is None:
                        continue
                    site = str(site_id)
                    sites.add(site)
                    building_id = rec.get("building_id")
                    if building_id is not None:
                        buildings_by_site.setdefault(site, set()).add(str(building_id))
            return {
                **records_by_bucket,
                "received": dict(self.received),
                "unique_counts": {key: len(value) for key, value in records_by_bucket.items()},
                "available_sites": sorted(sites, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value)),
                "available_buildings_by_site": {
                    site: sorted(buildings, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value))
                    for site, buildings in buildings_by_site.items()
                },
            }


def _load_meter_references(cfg: AppConfig) -> dict[str, dict[str, float]]:
    """Load real daily metered energy keyed for site and building comparisons."""
    meters_path = Path(cfg.paths["meters_csv"])
    buildings_path = Path(cfg.paths["buildings_csv"])
    if not meters_path.exists() or not buildings_path.exists():
        return {"site": {}, "building": {}}
    meters = pd.read_csv(meters_path, usecols=["building_id", "ts", "value"])
    buildings = pd.read_csv(buildings_path, usecols=["building_id", "site_id"])
    meters["building_id"] = meters["building_id"].astype(str)
    buildings["building_id"] = buildings["building_id"].astype(str)
    buildings["site_id"] = buildings["site_id"].astype(str)
    meters["date"] = pd.to_datetime(meters["ts"], errors="coerce").dt.date.astype(str)
    merged = meters.merge(buildings, on="building_id", how="left")
    site_daily = merged.groupby(["site_id", "date"], dropna=True)["value"].sum()
    building_daily = meters.groupby(["building_id", "date"], dropna=True)["value"].sum()
    return {
        "site": {f"{site}|{date}": float(value) for (site, date), value in site_daily.items()},
        "building": {f"{building}|{date}": float(value) for (building, date), value in building_daily.items()},
    }


def build_site_daily_comparison(site_daily_records: list[dict[str, Any]], meter_reference: dict[str, float]) -> list[dict[str, Any]]:
    """Join predicted site daily records to exact site/date real meter totals."""
    comparisons = []
    for rec in site_daily_records:
        site_id = rec.get("site_id")
        date = rec.get("date")
        key = f"{site_id}|{date}"
        meter = meter_reference.get(key)
        if meter is not None:
            comparisons.append(
                {
                    "site_id": site_id,
                    "date": date,
                    "predicted_daily": float(rec.get("daily_consumption_site", 0.0)),
                    "real_daily": meter,
                }
            )
    return comparisons


def build_building_daily_comparison(building_records: list[dict[str, Any]], meter_reference: dict[str, float]) -> list[dict[str, Any]]:
    """Join retained building/day prediction rows to real building totals."""
    comparisons = []
    for rec in building_records:
        site_id = rec.get("site_id")
        building_id = rec.get("building_id")
        date = rec.get("date")
        if site_id is None or building_id is None or date is None:
            continue
        meter = meter_reference.get(f"{building_id}|{date}")
        if meter is not None:
            comparisons.append(
                {
                    "site_id": site_id,
                    "building_id": building_id,
                    "date": date,
                    "predicted_daily": float(rec.get("predicted_daily", rec.get("energy_consumption_6h", 0.0))),
                    "real_daily": meter,
                }
            )
    return comparisons


def create_app(config_path: str | None = None, cfg: AppConfig | None = None) -> FastAPI:
    """Create the FastAPI dashboard application and background Kafka consumer."""
    cfg = cfg or load_config(config_path)
    store = RecordStore(int(cfg.dashboard["max_records"]), int(cfg.dashboard.get("retention_days", 30)))
    meter_references = _load_meter_references(cfg)
    app = FastAPI(title="A2B Production Dashboard")

    def consume_topic(topic: str, bucket: str) -> None:
        """Continuously consume one dashboard topic into its store bucket."""
        while True:
            consumer = None
            try:
                consumer = make_consumer(cfg, [topic], group_id=f"a2b-dashboard-{bucket}-{int(time.time())}")
                for msg in consumer:
                    store.upsert(bucket, msg.value)
            except Exception as exc:  # pragma: no cover - long-running service recovery
                print(f"dashboard {bucket} consumer reconnecting after error: {exc}")
                time.sleep(3)
            finally:
                if consumer is not None:
                    consumer.close()

    @app.on_event("startup")
    def _startup() -> None:
        """Start independent dashboard Kafka consumer threads when FastAPI starts."""
        topic_buckets = [
            (cfg.topics["predictions_7a"], "predictions"),
            (cfg.topics["building_6h_7b"], "building"),
            (cfg.topics["site_daily_7c"], "site_daily"),
        ]
        for topic, bucket in topic_buckets:
            threading.Thread(target=consume_topic, args=(topic, bucket), daemon=True).start()

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        """Serve the static dashboard page."""
        html = resources.files("a2b_production.dashboard_static").joinpath("index.html").read_text("utf-8")
        return html

    @app.get("/api/state")
    def state() -> dict[str, Any]:
        """Return live dashboard records, counters, and comparison datasets."""
        snap = store.snapshot()
        snap["site_daily_comparison"] = build_site_daily_comparison(snap["site_daily"], meter_references["site"])
        snap["building_daily_comparison"] = build_building_daily_comparison(snap["building"], meter_references["building"])
        return snap

    return app


def run_dashboard(cfg: AppConfig) -> None:
    """Run the FastAPI dashboard service with Uvicorn."""
    app = create_app(cfg=cfg)
    uvicorn.run(app, host=str(cfg.dashboard["host"]), port=int(cfg.dashboard["port"]))
