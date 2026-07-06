from __future__ import annotations

from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from .schemas import (
    HOUR_BLOCKS,
    MODEL_NUMERIC_FEATURES,
    Q7A_COLS,
    Q7B_COLS,
    Q7C_COLS,
    WEATHER_JSON_SCHEMA,
    WEATHER_NUMERIC_COLS,
)


def hour_block_for_hour(hour: int) -> str:
    """Map an hour of day to the model's 6-hour serving block label."""
    if 0 <= hour <= 5:
        return "00-06"
    if 6 <= hour <= 11:
        return "06-12"
    if 12 <= hour <= 17:
        return "12-18"
    if 18 <= hour <= 23:
        return "18-24"
    raise ValueError(f"hour must be in 0..23, got {hour}")


def q7b_record_key(building_id: str, date: str, hour_block: str) -> str:
    """Build the stable q7b key for one building/date/hour-block prediction."""
    return f"{building_id}|{date}|{hour_block}"


def q7c_record_key(site_id: str, date: str) -> str:
    """Build the stable q7c key for one site/date daily aggregate."""
    return f"{site_id}|{date}"


def missing_hour_blocks(observed: list[str]) -> list[str]:
    """Return expected 6-hour block labels that are absent from observed blocks."""
    return [block for block in HOUR_BLOCKS if block not in set(observed)]


def parse_weather_stream(kafka_df: DataFrame, watermark_delay: str) -> DataFrame:
    """Parse, type, validate, watermark, and deduplicate weather Kafka rows."""
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
        .withWatermark("event_time", watermark_delay)
        .dropDuplicates(["site_id", "timestamp"])
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


def build_weather_blocks_microbatch(weather_df: DataFrame) -> DataFrame:
    """Aggregate typed hourly weather rows into complete 6-hour site blocks."""
    aggs = [F.avg(c).alias(f"avg_{c}_6h") for c in WEATHER_NUMERIC_COLS]
    return (
        weather_df.groupBy("site_id", "date", "hour_block")
        .agg(
            *aggs,
            F.max("season_peak").alias("season_peak"),
            F.max("event_time").alias("event_time"),
            F.countDistinct("timestamp").cast("long").alias("weather_rows_in_block"),
        )
        .filter(F.col("weather_rows_in_block") >= 6)
    )


def score_weather_blocks(weather_blocks: DataFrame, buildings: DataFrame, model: PipelineModel) -> DataFrame:
    """Join weather blocks to buildings and score them with the saved Spark ML model."""
    features = (
        weather_blocks.join(broadcast(buildings), on="site_id", how="inner")
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), F.lit(1.0)).otherwise(F.lit(0.0)))
        .dropna(subset=["building_id", "site_id", "date", "hour_block", "primary_use"] + MODEL_NUMERIC_FEATURES)
    )
    scored = model.transform(features)
    return (
        scored.withColumn("date_str", F.date_format("date", "yyyy-MM-dd"))
        .withColumn("record_key", F.concat_ws("|", "building_id", "date_str", "hour_block"))
        .withColumn(
            "wdw_start",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.col("date_str"),
                    F.when(F.col("hour_block") == "00-06", "00:00:00")
                    .when(F.col("hour_block") == "06-12", "06:00:00")
                    .when(F.col("hour_block") == "12-18", "12:00:00")
                    .otherwise("18:00:00"),
                )
            ),
        )
        .withColumn("wdw_end", F.expr("wdw_start + interval 6 hours"))
    )


def q7a_from_scored(scored: DataFrame) -> DataFrame:
    """Select the q7a prediction contract from scored building-block rows."""
    return scored.select(*Q7A_COLS).dropDuplicates(["record_key"])


def q7b_from_scored(scored: DataFrame) -> DataFrame:
    """Select the q7b building 6-hour aggregate contract from scored rows."""
    return (
        scored.select(
            "record_key",
            "wdw_start",
            "wdw_end",
            "date",
            "hour_block",
            "building_id",
            "site_id",
            F.col("prediction").alias("energy_consumption_6h"),
            "event_time",
            "weather_rows_in_block",
        )
        .select(*Q7B_COLS)
        .dropDuplicates(["record_key"])
    )


def site_building_counts(buildings: DataFrame) -> DataFrame:
    """Count distinct buildings per site for q7c completeness checks."""
    return buildings.groupBy("site_id").agg(F.countDistinct("building_id").cast("long").alias("site_building_count"))


def build_site_daily(building_blocks: DataFrame, building_counts: DataFrame) -> DataFrame:
    """Aggregate q7b building blocks into q7c site daily prediction rows."""
    block_flags = [
        F.max(F.when(F.col("hour_block") == block, F.lit(1)).otherwise(F.lit(0))).alias(f"has_{block.replace('-', '_')}")
        for block in HOUR_BLOCKS
    ]
    return (
        building_blocks.groupBy("site_id", "date")
        .agg(
            F.sum("energy_consumption_6h").alias("daily_consumption_site"),
            F.max("event_time").alias("event_time"),
            F.min("wdw_start").alias("wdw_start"),
            F.max("wdw_end").alias("wdw_end"),
            F.count("*").cast("long").alias("building_block_count"),
            *block_flags,
        )
        .withColumn(
            "observed_hour_blocks_raw",
            F.array(
                *[
                    F.when(F.col(f"has_{block.replace('-', '_')}") == 1, F.lit(block)).otherwise(F.lit(None))
                    for block in HOUR_BLOCKS
                ]
            ),
        )
        .withColumn("observed_hour_blocks", F.expr("filter(observed_hour_blocks_raw, x -> x is not null)"))
        .withColumn("hour_block_count", F.size("observed_hour_blocks").cast("long"))
        .withColumn("missing_hour_blocks", F.array_except(F.array(*[F.lit(b) for b in HOUR_BLOCKS]), F.col("observed_hour_blocks")))
        .withColumn("has_all_hour_blocks", F.size("missing_hour_blocks") == 0)
        .join(building_counts, on="site_id", how="left")
        .withColumn("expected_block_count", (F.col("site_building_count") * F.lit(4)).cast("long"))
        .withColumn(
            "is_complete_day",
            F.col("has_all_hour_blocks") & (F.col("building_block_count") >= F.col("expected_block_count")),
        )
        .withColumn("record_key", F.concat_ws("|", "site_id", F.date_format("date", "yyyy-MM-dd")))
        .select(*Q7C_COLS)
    )
