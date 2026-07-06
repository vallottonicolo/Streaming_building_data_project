from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "local.yaml"


@dataclass(frozen=True)
class AppConfig:
    raw: dict[str, Any]

    @property
    def project_root(self) -> Path:
        """Return the resolved root directory for the mounted A2 project."""
        return Path(self.raw["project_root"]).resolve()

    @property
    def runtime_dir(self) -> Path:
        """Return the resolved directory used for generated production state."""
        return Path(self.raw["runtime_dir"]).resolve()

    @property
    def kafka_bootstrap(self) -> str:
        """Return the Kafka bootstrap address used by all services."""
        return self.raw["kafka"]["bootstrap_servers"]

    @property
    def topics(self) -> dict[str, str]:
        """Return the configured Kafka topic names keyed by logical purpose."""
        return self.raw["kafka"]["topics"]

    @property
    def paths(self) -> dict[str, str]:
        """Return filesystem paths for source data and the saved model."""
        return self.raw["paths"]

    @property
    def streaming(self) -> dict[str, Any]:
        """Return streaming and producer pacing configuration."""
        return self.raw["streaming"]

    @property
    def spark(self) -> dict[str, Any]:
        """Return Spark runtime configuration."""
        return self.raw["spark"]

    @property
    def hudi(self) -> dict[str, Any]:
        """Return Hudi table and write configuration."""
        return self.raw["hudi"]

    @property
    def lakehouse(self) -> dict[str, Any]:
        """Return asynchronous lakehouse sink configuration."""
        return self.raw.get("lakehouse", {})

    @property
    def dashboard(self) -> dict[str, Any]:
        """Return dashboard host, port, and in-memory retention settings."""
        return self.raw["dashboard"]


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load YAML configuration from an explicit path, env var, or default file."""
    cfg_path = Path(path or os.environ.get("A2B_CONFIG") or DEFAULT_CONFIG)
    with cfg_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return AppConfig(raw=raw)


def validate_config(cfg: AppConfig) -> list[str]:
    """Return human-readable configuration errors without raising."""
    errors: list[str] = []
    for key in ["weather_csv", "buildings_csv", "meters_csv", "model_path"]:
        if not Path(cfg.paths[key]).exists():
            errors.append(f"missing path {key}: {cfg.paths[key]}")
    required_topics = ["weather", "weather_6h_blocks", "predictions_7a", "building_6h_7b", "site_daily_7c"]
    for topic_key in required_topics:
        if not cfg.topics.get(topic_key):
            errors.append(f"missing kafka topic config: {topic_key}")
    if int(cfg.raw["kafka"]["partitions"]) < 1:
        errors.append("kafka.partitions must be positive")
    if int(cfg.spark["shuffle_partitions"]) < 1:
        errors.append("spark.shuffle_partitions must be positive")
    if "compaction_delta_commits" in cfg.hudi and int(cfg.hudi["compaction_delta_commits"]) < 1:
        errors.append("hudi.compaction_delta_commits must be positive")
    if cfg.lakehouse:
        if int(cfg.lakehouse.get("max_offsets_per_trigger", 1)) < 1:
            errors.append("lakehouse.max_offsets_per_trigger must be positive")
        if not str(cfg.lakehouse.get("trigger_interval", "")).strip():
            errors.append("lakehouse.trigger_interval is required")
    if cfg.dashboard and "retention_days" in cfg.dashboard and int(cfg.dashboard["retention_days"]) < 1:
        errors.append("dashboard.retention_days must be positive")
    return errors
