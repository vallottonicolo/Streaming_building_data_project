"""Define shared Spark schemas and output column contracts for the pipeline."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WEATHER_JSON_SCHEMA = StructType(
    [
        StructField("month", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("air_temperature", StringType(), True),
        StructField("cloud_coverage", StringType(), True),
        StructField("dew_temperature", StringType(), True),
        StructField("sea_level_pressure", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("weather_ts", IntegerType(), True),
        StructField("season_peak", StringType(), True),
    ]
)

BUILDINGS_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), False),
        StructField("building_id", StringType(), False),
        StructField("primary_use", StringType(), True),
        StructField("square_feet", DoubleType(), True),
        StructField("floor_count", DoubleType(), True),
        StructField("row_id", StringType(), True),
        StructField("year_built", DoubleType(), True),
        StructField("latent_y", DoubleType(), True),
        StructField("latent_s", DoubleType(), True),
        StructField("latent_r", DoubleType(), True),
    ]
)

Q7A_COLS = [
    "record_key",
    "hour_block",
    "prediction",
    "date",
    "site_id",
    "building_id",
    "wdw_start",
    "wdw_end",
    "event_time",
    "weather_rows_in_block",
]

Q7A_SCHEMA = StructType(
    [
        StructField("record_key", StringType(), True),
        StructField("hour_block", StringType(), True),
        StructField("prediction", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("site_id", StringType(), True),
        StructField("building_id", StringType(), True),
        StructField("wdw_start", TimestampType(), True),
        StructField("wdw_end", TimestampType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("weather_rows_in_block", LongType(), True),
    ]
)

WEATHER_BLOCK_COLS = [
    "site_id",
    "date",
    "hour_block",
    "avg_air_temperature_6h",
    "avg_cloud_coverage_6h",
    "avg_dew_temperature_6h",
    "avg_sea_level_pressure_6h",
    "avg_wind_direction_6h",
    "avg_wind_speed_6h",
    "season_peak",
    "event_time",
    "weather_rows_in_block",
]

WEATHER_BLOCK_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("hour_block", StringType(), True),
        StructField("avg_air_temperature_6h", DoubleType(), True),
        StructField("avg_cloud_coverage_6h", DoubleType(), True),
        StructField("avg_dew_temperature_6h", DoubleType(), True),
        StructField("avg_sea_level_pressure_6h", DoubleType(), True),
        StructField("avg_wind_direction_6h", DoubleType(), True),
        StructField("avg_wind_speed_6h", DoubleType(), True),
        StructField("season_peak", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("weather_rows_in_block", LongType(), True),
    ]
)

Q7B_COLS = [
    "record_key",
    "wdw_start",
    "wdw_end",
    "date",
    "hour_block",
    "building_id",
    "site_id",
    "energy_consumption_6h",
    "event_time",
    "weather_rows_in_block",
]

Q7C_COLS = [
    "site_id",
    "date",
    "daily_consumption_site",
    "event_time",
    "wdw_start",
    "wdw_end",
    "building_block_count",
    "hour_block_count",
    "observed_hour_blocks",
    "missing_hour_blocks",
    "has_all_hour_blocks",
    "site_building_count",
    "expected_block_count",
    "is_complete_day",
    "record_key",
]

Q7B_SCHEMA = StructType(
    [
        StructField("record_key", StringType(), True),
        StructField("wdw_start", TimestampType(), True),
        StructField("wdw_end", TimestampType(), True),
        StructField("date", DateType(), True),
        StructField("hour_block", StringType(), True),
        StructField("building_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("energy_consumption_6h", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("weather_rows_in_block", LongType(), True),
    ]
)

Q7C_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("daily_consumption_site", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("wdw_start", TimestampType(), True),
        StructField("wdw_end", TimestampType(), True),
        StructField("building_block_count", LongType(), True),
        StructField("hour_block_count", LongType(), True),
        StructField("observed_hour_blocks", ArrayType(StringType()), True),
        StructField("missing_hour_blocks", ArrayType(StringType()), True),
        StructField("has_all_hour_blocks", BooleanType(), True),
        StructField("site_building_count", LongType(), True),
        StructField("expected_block_count", LongType(), True),
        StructField("is_complete_day", BooleanType(), True),
        StructField("record_key", StringType(), True),
    ]
)

WEATHER_NUMERIC_COLS = [
    "air_temperature",
    "cloud_coverage",
    "dew_temperature",
    "sea_level_pressure",
    "wind_direction",
    "wind_speed",
]

MODEL_NUMERIC_FEATURES = [
    "latent_y",
    "latent_s",
    "latent_r",
    "avg_air_temperature_6h",
    "avg_cloud_coverage_6h",
    "avg_dew_temperature_6h",
    "season_peak",
    "is_weekend",
    "floor_count",
]

HOUR_BLOCKS = ["00-06", "06-12", "12-18", "18-24"]
