"""Stream Kafka topics into Hudi lakehouse tables for persisted app outputs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from .config import AppConfig
from .kafka_utils import ensure_topics, wait_for_kafka
from .schemas import (
    Q7A_SCHEMA,
    Q7B_SCHEMA,
    Q7C_SCHEMA,
    WEATHER_BLOCK_SCHEMA,
    WEATHER_JSON_SCHEMA,
)
from .spark import create_spark, hudi_options


@dataclass(frozen=True)
class LakehouseSinkSpec:
    """Configuration for one Kafka topic to Hudi table streaming sink."""

    topic_key: str
    table: str
    record_key: str
    precombine: str
    partitions: list[str]


LAKEHOUSE_SINKS = [
    LakehouseSinkSpec("weather", "bronze_weather_hourly", "weather_record_key", "event_time", ["site_id"]),
    LakehouseSinkSpec(
        "weather_6h_blocks",
        "silver_weather_6h",
        "site_id,date,hour_block",
        "event_time",
        ["site_id"],
    ),
    LakehouseSinkSpec("predictions_7a", "gold_predictions_7a", "record_key", "event_time", ["site_id"]),
    LakehouseSinkSpec("building_6h_7b", "gold_building_6h_7b", "record_key", "event_time", ["site_id"]),
    LakehouseSinkSpec("site_daily_7c", "gold_site_daily_7c", "record_key", "event_time", ["site_id"]),
]


def _read_kafka_stream(spark: SparkSession, cfg: AppConfig, topic: str) -> DataFrame:
    """Create a Kafka streaming DataFrame for one lakehouse input topic."""
    lakehouse_cfg = cfg.lakehouse
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", str(lakehouse_cfg.get("starting_offsets", "earliest")))
    )
    if "max_offsets_per_trigger" in lakehouse_cfg:
        reader = reader.option("maxOffsetsPerTrigger", int(lakehouse_cfg["max_offsets_per_trigger"]))
    if "min_partitions" in lakehouse_cfg:
        reader = reader.option("minPartitions", int(lakehouse_cfg["min_partitions"]))
    return reader.load()


def _parse_json_stream(kafka_df: DataFrame, schema: StructType, required: list[str]) -> DataFrame:
    """Parse Kafka JSON values with the supplied schema and drop unusable rows."""
    return (
        kafka_df.select(F.col("value").cast("string").alias("value"))
        .select(F.from_json("value", schema).alias("j"))
        .select("j.*")
        .dropna(subset=required)
    )


def _parse_weather_stream_for_lakehouse(kafka_df: DataFrame) -> DataFrame:
    """Parse raw weather Kafka rows into the same bronze Hudi shape used by inference."""
    parsed = (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("value"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("kafka_key", "kafka_ts", F.from_json("value", WEATHER_JSON_SCHEMA).alias("j"))
        .select("kafka_key", "kafka_ts", "j.*")
    )

    typed = parsed.select(
        F.col("month").cast("int").alias("month"),
        F.col("site_id").cast("string").alias("site_id"),
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("air_temperature").cast("double").alias("air_temperature"),
        F.col("cloud_coverage").cast("double").alias("cloud_coverage"),
        F.col("dew_temperature").cast("double").alias("dew_temperature"),
        F.col("sea_level_pressure").cast("double").alias("sea_level_pressure"),
        F.col("wind_direction").cast("double").alias("wind_direction"),
        F.col("wind_speed").cast("double").alias("wind_speed"),
        F.col("weather_ts").cast("long").alias("weather_ts"),
        F.col("season_peak").cast("double").alias("season_peak"),
        "kafka_key",
        "kafka_ts",
    )

    required = [
        "site_id",
        "timestamp",
        "weather_ts",
        "air_temperature",
        "cloud_coverage",
        "dew_temperature",
        "sea_level_pressure",
        "wind_direction",
        "wind_speed",
        "season_peak",
    ]
    return (
        typed.dropna(subset=required)
        .withColumn("event_time", F.to_timestamp(F.from_unixtime("weather_ts")))
        .dropna(subset=["event_time"])
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn(
            "hour_block",
            F.when(F.col("hour") <= 5, F.lit("00-06"))
            .when(F.col("hour") <= 11, F.lit("06-12"))
            .when(F.col("hour") <= 17, F.lit("12-18"))
            .otherwise(F.lit("18-24")),
        )
        .withColumn("timestamp_key", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("weather_record_key", F.concat_ws("|", "site_id", "timestamp_key"))
        .drop("timestamp_key")
    )


def _topic_to_hudi_df(spark: SparkSession, cfg: AppConfig, spec: LakehouseSinkSpec) -> DataFrame:
    """Build the parsed streaming DataFrame for one lakehouse sink spec."""
    kafka_df = _read_kafka_stream(spark, cfg, cfg.topics[spec.topic_key])
    if spec.topic_key == "weather":
        return _parse_weather_stream_for_lakehouse(kafka_df)
    if spec.topic_key == "weather_6h_blocks":
        return _parse_json_stream(kafka_df, WEATHER_BLOCK_SCHEMA, ["site_id", "date", "hour_block", "event_time"])
    if spec.topic_key == "predictions_7a":
        return _parse_json_stream(kafka_df, Q7A_SCHEMA, ["record_key", "site_id", "date", "event_time"])
    if spec.topic_key == "building_6h_7b":
        return _parse_json_stream(kafka_df, Q7B_SCHEMA, ["record_key", "site_id", "date", "event_time"])
    if spec.topic_key == "site_daily_7c":
        return _parse_json_stream(kafka_df, Q7C_SCHEMA, ["record_key", "site_id", "date", "event_time"])
    raise ValueError(f"unsupported lakehouse sink topic key: {spec.topic_key}")


def _start_hudi_sink(spark: SparkSession, cfg: AppConfig, spec: LakehouseSinkSpec) -> StreamingQuery:
    """Start one parsed Kafka stream as a Hudi streaming sink."""
    df = _topic_to_hudi_df(spark, cfg, spec)
    base_path = Path(cfg.hudi["base_path"]) / spec.table
    checkpoint = cfg.runtime_dir / "checkpoints" / "lakehouse" / spec.table
    compaction_delta_commits = int(cfg.hudi.get("compaction_delta_commits", 10))
    options = hudi_options(spec.table, spec.record_key, spec.precombine, spec.partitions, compaction_delta_commits)
    query = (
        df.writeStream.queryName(f"a2b_lakehouse_{spec.table}")
        .format("hudi")
        .options(**options)
        .outputMode("append")
        .trigger(processingTime=str(cfg.lakehouse.get("trigger_interval", cfg.streaming["trigger_interval"])))
        .option("checkpointLocation", str(checkpoint))
        .start(str(base_path))
    )
    print(
        f"lakehouse_sink_started table={spec.table} topic={cfg.topics[spec.topic_key]} "
        f"checkpoint={checkpoint}",
        flush=True,
    )
    return query


def run_lakehouse_sink(cfg: AppConfig) -> None:
    """Run asynchronous Kafka-to-Hudi sinks for every lakehouse table."""
    if not bool(cfg.hudi["enabled"]):
        print("lakehouse sink skipped because hudi.enabled=false", flush=True)
        return

    wait_for_kafka(cfg)
    ensure_topics(cfg)
    spark = create_spark("a2b-production-lakehouse-sink", cfg)
    queries = [_start_hudi_sink(spark, cfg, spec) for spec in LAKEHOUSE_SINKS]
    try:
        spark.streams.awaitAnyTermination()
        failures = [q for q in queries if not q.isActive and q.exception() is not None]
        if failures:
            details = "; ".join(f"{q.name}: {q.exception()}" for q in failures)
            raise RuntimeError(f"lakehouse sink terminated with failure: {details}")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
