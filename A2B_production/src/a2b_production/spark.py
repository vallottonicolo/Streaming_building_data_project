"""Create Spark sessions and shared Kafka/Hudi DataFrame helper columns."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .config import AppConfig


HUDI_PACKAGE = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
KAFKA_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0",
]


def create_spark(app_name: str, cfg: AppConfig) -> SparkSession:
    """Create a Spark session configured for Kafka and Hudi writes."""
    packages = ",".join(KAFKA_PACKAGES + [HUDI_PACKAGE])
    builder = (
        SparkSession.builder.appName(app_name)
        .master(str(cfg.spark["master"]))
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", str(cfg.spark["timezone"]))
        .config("spark.sql.shuffle.partitions", str(cfg.spark["shuffle_partitions"]))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def hudi_options(
    table: str,
    record_key: str,
    precombine: str,
    partitions: list[str],
    compaction_delta_commits: int = 10,
) -> dict[str, str]:
    """Build common Merge-on-Read Hudi writer options for a table."""
    return {
        "hoodie.table.name": table,
        "hoodie.datasource.write.table.name": table,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": ",".join(partitions),
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.drop.partition.columns": "false",
        "hoodie.metadata.enable": "true",
        "hoodie.compact.inline": "false",
        "hoodie.datasource.compaction.async.enable": "true",
        "hoodie.compact.inline.max.delta.commits": str(compaction_delta_commits),
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
        "hoodie.cleaner.commits.retained": "10",
        "hoodie.parquet.max.file.size": str(128 * 1024 * 1024),
        "hoodie.parquet.small.file.limit": str(64 * 1024 * 1024),
    }


def write_hudi(df: DataFrame, cfg: AppConfig, table: str, record_key: str, precombine: str, partitions: list[str]) -> None:
    """Append/upsert a DataFrame into a configured Hudi table when enabled."""
    if not bool(cfg.hudi["enabled"]):
        return
    path = Path(cfg.hudi["base_path"]) / table
    compaction_delta_commits = int(cfg.hudi.get("compaction_delta_commits", 10))
    (
        df.write.format("hudi")
        .options(**hudi_options(table, record_key, precombine, partitions, compaction_delta_commits))
        .mode("append")
        .save(str(path))
    )


def empty_df(spark: SparkSession, schema: StructType) -> DataFrame:
    """Create an empty Spark DataFrame with the supplied schema."""
    return spark.createDataFrame([], schema)


def add_json_kafka_columns(df: DataFrame, key_col: str, value_cols: list[str]) -> DataFrame:
    """Project Kafka-compatible string key and JSON value columns."""
    return df.select(
        F.col(key_col).cast("string").alias("key"),
        F.to_json(F.struct(*[F.col(c) for c in value_cols])).alias("value"),
    )
