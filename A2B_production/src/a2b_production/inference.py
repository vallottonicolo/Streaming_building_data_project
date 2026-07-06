from __future__ import annotations

import time
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import AppConfig
from .kafka_utils import ensure_topics, wait_for_kafka
from .schemas import BUILDINGS_SCHEMA, Q7A_COLS, Q7B_COLS, WEATHER_BLOCK_COLS
from .spark import add_json_kafka_columns, create_spark
from .transforms import (
    build_weather_blocks_microbatch,
    parse_weather_stream,
    q7a_from_scored,
    q7b_from_scored,
    score_weather_blocks,
)


def _write_kafka_batch(df: DataFrame, cfg: AppConfig, topic: str, key_col: str, value_cols: list[str]) -> int:
    """Write a batch DataFrame to Kafka as keyed JSON records and return row count."""
    rows = df.count()
    if rows == 0:
        return 0
    (
        add_json_kafka_columns(df, key_col, value_cols)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("topic", topic)
        .save()
    )
    return rows


def _with_weather_block_key(df: DataFrame) -> DataFrame:
    """Add a stable Kafka key for one site/date/hour-block weather aggregate."""
    return df.withColumn(
        "block_record_key",
        F.concat_ws("|", F.col("site_id"), F.date_format("date", "yyyy-MM-dd"), F.col("hour_block")),
    )


def run_inference(cfg: AppConfig) -> None:
    """Run the Spark streaming inference job from weather input to q7a/q7b outputs."""
    wait_for_kafka(cfg)
    ensure_topics(cfg)
    spark = create_spark("a2b-production-inference", cfg)

    buildings = (
        spark.read.option("header", True)
        .schema(BUILDINGS_SCHEMA)
        .csv(cfg.paths["buildings_csv"])
        .dropDuplicates(["building_id"])
        .cache()
    )
    model = PipelineModel.load(cfg.paths["model_path"])

    starting_offsets = "latest" if cfg.streaming["run_mode"] == "live" else "earliest"
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.topics["weather"])
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", int(cfg.streaming["max_offsets_per_trigger"]))
        .option("minPartitions", int(cfg.streaming["min_partitions"]))
        .load()
    )
    weather = parse_weather_stream(kafka_df, cfg.streaming["watermark_delay"])

    def handle_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Score one weather micro-batch and publish all live/lakehouse Kafka outputs."""
        started = time.time()
        batch_df = batch_df.cache()
        input_rows = batch_df.count()
        if input_rows == 0:
            batch_df.unpersist()
            return
        try:
            blocks = build_weather_blocks_microbatch(batch_df).cache()
            if blocks.count() == 0:
                print(f"batch={batch_id} input={input_rows} complete_blocks=0")
                return
            scored = score_weather_blocks(blocks, buildings, model).cache()
            q7a = q7a_from_scored(scored).cache()
            q7b = q7b_from_scored(scored).cache()

            q7a_rows = _write_kafka_batch(q7a, cfg, cfg.topics["predictions_7a"], "record_key", Q7A_COLS)
            q7b_rows = _write_kafka_batch(q7b, cfg, cfg.topics["building_6h_7b"], "record_key", Q7B_COLS)
            weather_block_rows = _write_kafka_batch(
                _with_weather_block_key(blocks),
                cfg,
                cfg.topics["weather_6h_blocks"],
                "block_record_key",
                WEATHER_BLOCK_COLS,
            )

            elapsed = time.time() - started
            print(
                f"batch={batch_id} input={input_rows} weather_blocks={weather_block_rows} "
                f"q7a={q7a_rows} q7b={q7b_rows} elapsed_s={elapsed:.2f}",
                flush=True,
            )
        finally:
            for obj_name in ["q7b", "q7a", "scored", "blocks"]:
                if obj_name in locals():
                    locals()[obj_name].unpersist()
            batch_df.unpersist()

    query = (
        weather.writeStream.queryName("a2b_prod_inference")
        .trigger(processingTime=str(cfg.streaming["trigger_interval"]))
        .option("checkpointLocation", str(cfg.runtime_dir / "checkpoints" / "inference"))
        .foreachBatch(handle_batch)
        .start()
    )
    query.awaitTermination()
