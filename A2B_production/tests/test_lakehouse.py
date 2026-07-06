import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from a2b_production.config import AppConfig, validate_config
from a2b_production.lakehouse_sink import LAKEHOUSE_SINKS
from a2b_production.schemas import Q7A_SCHEMA, Q7B_SCHEMA, Q7C_SCHEMA, WEATHER_BLOCK_SCHEMA
from a2b_production.spark import hudi_options


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("a2b-schema-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _parse_one(spark: SparkSession, schema, payload: dict) -> dict:
    df = spark.createDataFrame([(json.dumps(payload),)], ["value"])
    row = df.select(F.from_json("value", schema).alias("j")).select("j.*").collect()[0]
    return row.asDict(recursive=True)


def test_config_accepts_lakehouse_topics_and_settings(tmp_path: Path):
    """Verify the asynchronous lakehouse config validates when all required inputs exist."""
    weather = tmp_path / "weather.csv"
    buildings = tmp_path / "buildings.csv"
    meters = tmp_path / "meters.csv"
    model = tmp_path / "model"
    for path in [weather, buildings, meters]:
        path.write_text("header\n", encoding="utf-8")
    model.mkdir()

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
                "partitions": 16,
                "replication_factor": 1,
            },
            "paths": {
                "weather_csv": str(weather),
                "buildings_csv": str(buildings),
                "meters_csv": str(meters),
                "model_path": str(model),
            },
            "streaming": {},
            "spark": {"shuffle_partitions": 8},
            "hudi": {"enabled": True, "base_path": str(tmp_path / "hudi"), "compaction_delta_commits": 10},
            "lakehouse": {"trigger_interval": "10 seconds", "max_offsets_per_trigger": 1152},
            "dashboard": {},
        }
    )

    assert validate_config(cfg) == []


def test_hudi_options_enable_async_compaction():
    """Verify Hudi writer options keep MOR writes and compaction off the live path."""
    options = hudi_options("gold_predictions_7a", "record_key", "event_time", ["site_id"], 7)

    assert options["hoodie.datasource.write.table.type"] == "MERGE_ON_READ"
    assert options["hoodie.datasource.write.partitionpath.field"] == "site_id"
    assert options["hoodie.compact.inline"] == "false"
    assert options["hoodie.datasource.compaction.async.enable"] == "true"
    assert options["hoodie.compact.inline.max.delta.commits"] == "7"


def test_lakehouse_sinks_partition_by_site_only():
    """Verify lakehouse tables avoid one folder per site/day pair."""
    assert {sink.table: sink.partitions for sink in LAKEHOUSE_SINKS} == {
        "bronze_weather_hourly": ["site_id"],
        "silver_weather_6h": ["site_id"],
        "gold_predictions_7a": ["site_id"],
        "gold_building_6h_7b": ["site_id"],
        "gold_site_daily_7c": ["site_id"],
    }


def test_lakehouse_topic_schemas_parse_representative_json(spark):
    """Verify schemas for all derived lakehouse topics parse the Spark JSON payload shape."""
    timestamp = "2022-01-01T00:00:00.000Z"

    weather_block = _parse_one(
        spark,
        WEATHER_BLOCK_SCHEMA,
        {
            "site_id": "1",
            "date": "2022-01-01",
            "hour_block": "00-06",
            "avg_air_temperature_6h": 12.5,
            "avg_cloud_coverage_6h": 3.0,
            "avg_dew_temperature_6h": 8.5,
            "avg_sea_level_pressure_6h": 1012.0,
            "avg_wind_direction_6h": 180.0,
            "avg_wind_speed_6h": 4.5,
            "season_peak": 0.0,
            "event_time": timestamp,
            "weather_rows_in_block": 6,
        },
    )
    q7a = _parse_one(
        spark,
        Q7A_SCHEMA,
        {
            "record_key": "10|2022-01-01|00-06",
            "hour_block": "00-06",
            "prediction": 42.0,
            "date": "2022-01-01",
            "site_id": "1",
            "building_id": "10",
            "wdw_start": timestamp,
            "wdw_end": "2022-01-01T06:00:00.000Z",
            "event_time": timestamp,
            "weather_rows_in_block": 6,
        },
    )
    q7b = _parse_one(
        spark,
        Q7B_SCHEMA,
        {
            "record_key": "10|2022-01-01|00-06",
            "wdw_start": timestamp,
            "wdw_end": "2022-01-01T06:00:00.000Z",
            "date": "2022-01-01",
            "hour_block": "00-06",
            "building_id": "10",
            "site_id": "1",
            "energy_consumption_6h": 42.0,
            "event_time": timestamp,
            "weather_rows_in_block": 6,
        },
    )
    q7c = _parse_one(
        spark,
        Q7C_SCHEMA,
        {
            "site_id": "1",
            "date": "2022-01-01",
            "daily_consumption_site": 100.0,
            "event_time": timestamp,
            "wdw_start": timestamp,
            "wdw_end": "2022-01-02T00:00:00.000Z",
            "building_block_count": 4,
            "hour_block_count": 4,
            "observed_hour_blocks": ["00-06", "06-12", "12-18", "18-24"],
            "missing_hour_blocks": [],
            "has_all_hour_blocks": True,
            "site_building_count": 1,
            "expected_block_count": 4,
            "is_complete_day": True,
            "record_key": "1|2022-01-01",
        },
    )

    assert weather_block["site_id"] == "1"
    assert q7a["record_key"] == "10|2022-01-01|00-06"
    assert q7b["energy_consumption_6h"] == 42.0
    assert q7c["observed_hour_blocks"] == ["00-06", "06-12", "12-18", "18-24"]
