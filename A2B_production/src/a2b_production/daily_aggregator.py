"""Aggregate building-level 6-hour Kafka predictions into site daily records."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import AppConfig
from .kafka_utils import ensure_topics, wait_for_kafka
from .schemas import BUILDINGS_SCHEMA, Q7B_SCHEMA, Q7C_COLS
from .spark import add_json_kafka_columns, create_spark
from .transforms import build_site_daily, site_building_counts


def _parse_q7b_stream(kafka_df: DataFrame) -> DataFrame:
    """Parse q7b Kafka JSON messages into a deduplicated streaming DataFrame."""
    return (
        kafka_df.select(F.col("value").cast("string").alias("value"))
        .select(F.from_json("value", Q7B_SCHEMA).alias("j"))
        .select("j.*")
        .dropna(subset=["record_key", "site_id", "building_id", "date", "hour_block", "event_time"])
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["record_key"])
    )


def run_daily_aggregator(cfg: AppConfig) -> None:
    """Run the streaming job that aggregates building 6-hour predictions to site daily q7c."""
    wait_for_kafka(cfg)
    ensure_topics(cfg)
    spark = create_spark("a2b-production-daily-aggregator", cfg)

    buildings = (
        spark.read.option("header", True)
        .schema(BUILDINGS_SCHEMA)
        .csv(cfg.paths["buildings_csv"])
        .dropDuplicates(["building_id"])
    )
    building_counts = site_building_counts(buildings)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.topics["building_6h_7b"])
        .option("startingOffsets", "latest" if cfg.streaming["run_mode"] == "live" else "earliest")
        .option("maxOffsetsPerTrigger", int(cfg.streaming["max_offsets_per_trigger"]))
        .load()
    )
    q7b = _parse_q7b_stream(kafka_df)
    q7c = build_site_daily(q7b, building_counts)

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Publish one micro-batch of q7c rows to Kafka."""
        started = time.time()
        rows = batch_df.count()
        if rows == 0:
            return
        (
            add_json_kafka_columns(batch_df, "record_key", Q7C_COLS)
            .write.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
            .option("topic", cfg.topics["site_daily_7c"])
            .save()
        )
        print(
            f"daily_batch={batch_id} q7c_rows={rows} elapsed_s={time.time() - started:.2f}",
            flush=True,
        )

    query = (
        q7c.writeStream.queryName("a2b_prod_daily_aggregator")
        .outputMode("update")
        .option("checkpointLocation", str(cfg.runtime_dir / "checkpoints" / "daily_aggregator"))
        .foreachBatch(handle_batch)
        .start()
    )
    query.awaitTermination()
