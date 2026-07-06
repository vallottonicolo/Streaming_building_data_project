from __future__ import annotations

from pathlib import Path

from .config import AppConfig, validate_config
from .kafka_utils import topic_offsets
from .spark import create_spark


def validate_outputs(cfg: AppConfig) -> int:
    """Validate config, Kafka offsets, and Hudi output key quality."""
    errors = validate_config(cfg)
    for err in errors:
        print(f"CONFIG ERROR: {err}")

    offsets = topic_offsets(cfg)
    for topic, partitions in offsets.items():
        total = sum(partitions.values())
        print(f"topic {topic}: offsets={partitions} total={total}")

    if cfg.hudi["enabled"] and Path(cfg.hudi["base_path"]).exists():
        spark = create_spark("a2b-production-validate", cfg)
        for table in ["gold_predictions_7a", "gold_building_6h_7b", "gold_site_daily_7c"]:
            path = Path(cfg.hudi["base_path"]) / table
            if not path.exists():
                print(f"HUDI MISSING: {table}")
                continue
            df = spark.read.format("hudi").load(str(path))
            total = df.count()
            null_keys = df.filter("record_key is null").count() if "record_key" in df.columns else total
            dupes = total - df.select("record_key").dropDuplicates().count() if "record_key" in df.columns else 0
            print(f"hudi {table}: rows={total} null_record_key={null_keys} duplicate_keys={dupes}")
            if null_keys or dupes:
                errors.append(f"{table} has null or duplicate keys")
        spark.stop()
    return 1 if errors else 0


def validate_config_command(cfg: AppConfig) -> int:
    """Print configuration validation results and return a CLI exit code."""
    errors = validate_config(cfg)
    if errors:
        for err in errors:
            print(f"CONFIG ERROR: {err}")
        return 1
    print("config ok")
    return 0
