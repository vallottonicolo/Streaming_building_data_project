from pathlib import Path

from a2b_production.config import AppConfig, validate_config
from a2b_production.transforms import hour_block_for_hour, missing_hour_blocks, q7b_record_key, q7c_record_key


def test_hour_block_for_hour():
    """Verify hour values map to the expected 6-hour block labels."""
    assert hour_block_for_hour(0) == "00-06"
    assert hour_block_for_hour(5) == "00-06"
    assert hour_block_for_hour(6) == "06-12"
    assert hour_block_for_hour(12) == "12-18"
    assert hour_block_for_hour(23) == "18-24"


def test_record_keys_are_stable():
    """Verify output record keys remain deterministic and human-readable."""
    assert q7b_record_key("101", "2022-01-01", "00-06") == "101|2022-01-01|00-06"
    assert q7c_record_key("7", "2022-01-01") == "7|2022-01-01"


def test_missing_hour_blocks():
    """Verify missing block detection preserves canonical block order."""
    assert missing_hour_blocks(["00-06", "12-18"]) == ["06-12", "18-24"]
    assert missing_hour_blocks(["00-06", "06-12", "12-18", "18-24"]) == []


def test_config_validation_reports_missing_paths(tmp_path: Path):
    """Verify config validation reports all required missing input paths."""
    cfg = AppConfig(
        raw={
            "project_root": str(tmp_path),
            "runtime_dir": str(tmp_path / "runtime"),
            "kafka": {
                "bootstrap_servers": "kafka:9092",
                "topics": {
                    "weather": "weather_stream",
                    "weather_6h_blocks": "weather_6h_blocks",
                    "predictions_7a": "predictions_7a",
                    "building_6h_7b": "agg_building6h_7b",
                    "site_daily_7c": "agg_site_daily_7c",
                },
                "partitions": 4,
                "replication_factor": 1,
            },
            "paths": {
                "weather_csv": str(tmp_path / "missing-weather.csv"),
                "buildings_csv": str(tmp_path / "missing-buildings.csv"),
                "meters_csv": str(tmp_path / "missing-meters.csv"),
                "model_path": str(tmp_path / "missing-model"),
            },
            "streaming": {},
            "spark": {"shuffle_partitions": 8},
            "hudi": {},
            "lakehouse": {"trigger_interval": "10 seconds", "max_offsets_per_trigger": 100},
            "dashboard": {},
        }
    )
    errors = validate_config(cfg)
    assert len(errors) == 4
