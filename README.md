# Building Energy Prediction and Production Streaming Pipeline

A combined machine-learning and streaming application for predicting building energy consumption from weather and building metadata. The project is split into two connected parts:

- **Part A** develops and saves a Spark ML regression model for 6-hour building energy prediction.
- **Part B** runs that saved model in a production-style streaming app using Kafka, Spark Structured Streaming, Hudi, and a FastAPI dashboard.

## Required Large Files Setup

Some required data and model artifacts are too large to keep in normal Git history. Download them from the Dropbox artifact link:

```text
https://www.dropbox.com/t/yKvnaTqGv8HmN8V6
```

After downloading, open the included `big_files` folder. It should contain:

```text
big_files/
|-- gbt_best_model/
|-- weather_transf.csv
|-- new_meters.csv
`-- meters.csv
```

Copy those files into the project as follows:

| Downloaded artifact | Destination path |
| --- | --- |
| `big_files/gbt_best_model/` | `A2A/models/gbt_best_model/` |
| `big_files/weather_transf.csv` | `A2A/weather_transf.csv` |
| `big_files/new_meters.csv` | `A2A/new_meters.csv` |
| `big_files/meters.csv` | `A2A/meters.csv` |

The Spark model must be copied as a folder. After setup, these paths should exist:

```text
A2A/models/gbt_best_model/metadata
A2A/models/gbt_best_model/stages
```

Avoid creating an extra nested folder such as:

```text
A2A/models/gbt_best_model/gbt_best_model/metadata
```

The production inference job also requires:

```text
A2A/new_building_information.csv
```

This file is small and is expected to be kept in the repository rather than downloaded from the `big_files` bundle.

## Project Summary

The end-to-end project starts with historical meter, building, and weather data. Part A explores and transforms those datasets into a supervised learning table where each row represents one building, one date, and one 6-hour block. A Spark ML pipeline is trained to predict the 6-hour energy consumption for that building block.

Part B then turns that model into a live-style streaming workflow. Transformed weather rows are produced to Kafka, Spark consumes them in micro-batches, complete 6-hour weather blocks are scored for every building at the matching site, and the outputs are published back to Kafka. A separate streaming job aggregates building-level predictions into site-level daily totals, another job persists outputs into Hudi tables, and the dashboard displays recent prediction results.

## Part A: Model Development

Part A is implemented in:

```text
A2A/A2A_nval0007.ipynb
```

The notebook builds the reusable model artifact used by the production app.

The workflow includes:

- Loading `meters.csv`, `building_information.csv`, and `weather.csv` with explicit Spark schemas.
- Aggregating raw meter readings into 6-hour energy totals per `building_id`, `date`, and `hour_block`.
- Imputing missing weather values using day-level, site-level, and global averages.
- Aggregating hourly weather rows into 6-hour site weather features.
- Engineering model features such as `hour_block`, `season_peak`, `is_weekend`, building latent features, weather averages, and categorical encodings.
- Exploring distributions, correlations, seasonal patterns, weekday effects, and outliers.
- Removing the major outlier building identified during exploration.
- Training Random Forest and Gradient Boosted Tree regression models.
- Evaluating models with RMSLE, which matches the energy-consumption prediction objective better than plain RMSE on raw values.
- Tuning the Gradient Boosted Tree model and saving the selected model for production use.

The key reusable model artifact is:

```text
A2A/models/gbt_best_model
```

This saved Spark ML pipeline is loaded directly by the Part B streaming inference service.

## Part B: Streaming, Live Inference and Visualisation

Part B is implemented in:

```text
A2B_production/
```

It is a Docker-based streaming application that runs without Jupyter. The production app uses:

- Kafka and Zookeeper for streaming message transport.
- Spark Structured Streaming for weather parsing, feature construction, model inference, and daily aggregation.
- The saved Spark ML model from Part A for prediction.
- Hudi Merge-on-Read tables for persisted lakehouse outputs.
- FastAPI and Uvicorn for the dashboard service.

The main command-line entry point is:

```text
a2b-prod
```

The currently implemented commands are:

```text
create-topics
produce-weather
run-inference
run-daily-aggregator
run-lakehouse-sink
run-dashboard
clean-runtime
```

## How The App Works

The live-style pipeline is:

```text
weather_transf.csv
  -> producer
  -> Kafka: weather_stream
  -> inference
  -> Kafka: weather_6h_blocks
  -> Kafka: predictions_7a
  -> Kafka: agg_building6h_7b
  -> daily aggregator
  -> Kafka: agg_site_daily_7c
  -> lakehouse sink
  -> Hudi tables
  -> dashboard
```

### Producer

The producer reads transformed weather data from:

```text
A2A/weather_transf.csv
```

It sends weather rows to Kafka topic `weather_stream` in source timestamp order. Records are keyed by site and assigned to deterministic site-based partitions.

### Inference Service

The inference service consumes `weather_stream`, parses JSON values into typed Spark columns, builds complete 6-hour weather blocks, and joins each block with the buildings at that site.

It loads the saved model from:

```text
A2A/models/gbt_best_model
```

It then emits:

- `weather_6h_blocks`: complete 6-hour weather aggregates.
- `predictions_7a`: scored building/date/hour-block predictions.
- `agg_building6h_7b`: building-level 6-hour energy consumption output.

### Daily Aggregator

The daily aggregator consumes `agg_building6h_7b` and groups predictions by `site_id` and `date`.

It calculates:

- total daily predicted consumption per site;
- observed 6-hour blocks;
- missing 6-hour blocks;
- expected building-block counts;
- whether the site/day appears complete.

It publishes the result to:

```text
agg_site_daily_7c
```

### Lakehouse Sink

The lakehouse sink consumes the Kafka topics and writes them into Hudi tables under:

```text
A2B_production/runtime/hudi
```

The configured Hudi tables are:

| Table | Purpose |
| --- | --- |
| `bronze_weather_hourly` | Parsed hourly weather records |
| `silver_weather_6h` | 6-hour weather feature blocks |
| `gold_predictions_7a` | Building prediction rows |
| `gold_building_6h_7b` | Building-level 6-hour aggregate outputs |
| `gold_site_daily_7c` | Site-level daily aggregate outputs |

### Dashboard

The dashboard service consumes recent Kafka outputs and serves a FastAPI web app at:

```text
http://localhost:8080
```

It provides a lightweight view of prediction and aggregation outputs without requiring direct Hudi inspection.

## Software And Dependencies

### Required Host Software

- Docker Desktop or Docker Engine with Docker Compose.
- Python 3.10+ for local package context.
- Jupyter with PySpark support if rerunning the Part A notebook.

### Python App Dependencies

The production app dependencies are defined in `A2B_production/requirements.txt` and `A2B_production/pyproject.toml`.

Key dependencies are:

| Dependency | Role |
| --- | --- |
| `pyspark==3.5.0` | Spark batch and streaming jobs |
| `fastapi` | Dashboard web application |
| `uvicorn[standard]` | Dashboard ASGI server |
| `kafka-python` | Kafka producer, consumer, and topic helpers |
| `pandas` | Producer CSV loading and row conversion |
| `pyyaml` | YAML configuration loading |
| `pytest` | Developer tests |

### Service Dependencies

The Docker Compose stack provides or configures:

- Zookeeper
- Kafka
- Spark Kafka connector packages
- Hudi Spark bundle
- The production app image built from `A2B_production/docker/Dockerfile`

## Required Data And Model Artifacts

The current production configuration is:

```text
A2B_production/configs/local.yaml
```

It expects these paths inside the Docker-mounted workspace:

| Config Key | Expected Path | Purpose |
| --- | --- | --- |
| `weather_csv` | `/workspace/A2/A2A/weather_transf.csv` | Transformed weather input for the producer |
| `model_path` | `/workspace/A2/A2A/models/gbt_best_model` | Saved Spark ML model from Part A |
| `buildings_csv` | `/workspace/A2/A2A/new_building_information.csv` | Building metadata for inference |
| `meters_csv` | `/workspace/A2/A2A/new_meters.csv` | Meter data for dashboard real-vs-predicted comparisons |

The core streaming inference path requires `weather_transf.csv`, `new_building_information.csv`, and `gbt_best_model`. The dashboard can still run without `new_meters.csv`, but it will not show real meter vs predicted energy comparison data unless `A2A/new_meters.csv` is present.

## Running The Production App

Run the production commands from the `A2B_production` folder:

```bash
cd A2B_production
```

Build the app image:

```bash
docker compose build app
```

Start Kafka and Zookeeper:

```bash
docker compose up -d kafka zookeeper
```

Create the required Kafka topics:

```bash
docker compose run --rm app a2b-prod create-topics
```

For a fresh run, clean the app runtime state before starting the pipeline:

```bash
docker compose run --rm app a2b-prod clean-runtime
```

Start the streaming pipeline and dashboard:

```bash
docker compose --profile pipeline --profile dashboard up -d
```

Open the dashboard:

```text
http://localhost:8080
```

Stop the stack:

```bash
docker compose down
```

For a full Kafka reset, including Kafka volumes, run this separately:

```bash
docker compose down -v
```

Use `down -v` carefully because it removes persisted Docker volumes for the stack.

## Running Tests

The developer test command is:

```bash
cd A2B_production
docker compose run --rm app pytest -q
```

Before relying on the test suite, check that tests match the current source code. At the time this guide was written, one test imports `validate_config` from `a2b_production.config`, but that function is not currently present in `config.py`.

## Important Files

| File or Folder | Purpose |
| --- | --- |
| `A2A/A2A_nval0007.ipynb` | Part A notebook for data preparation, exploration, training, tuning, and model export |
| `A2A/weather_transf.csv` | Transformed weather data consumed by the Part B producer |
| `A2A/models/gbt_best_model` | Saved Spark ML model used by streaming inference |
| `A2B_production/configs/local.yaml` | Runtime configuration for paths, Kafka topics, Spark, Hudi, and dashboard settings |
| `A2B_production/docker-compose.yml` | Service definitions for Kafka, Zookeeper, app jobs, and dashboard |
| `A2B_production/docker/Dockerfile` | Production app image definition |
| `A2B_production/src/a2b_production/producer.py` | Weather CSV to Kafka producer |
| `A2B_production/src/a2b_production/inference.py` | Spark streaming inference job |
| `A2B_production/src/a2b_production/daily_aggregator.py` | Site/day aggregation job |
| `A2B_production/src/a2b_production/lakehouse_sink.py` | Kafka to Hudi streaming sink |
| `A2B_production/src/a2b_production/dashboard.py` | FastAPI dashboard service |
| `A2B_production/src/a2b_production/transforms.py` | Shared feature, scoring, and aggregation transformations |
| `A2B_production/src/a2b_production/schemas.py` | Spark schemas and output column contracts |

## Kafka Topics

| Topic | Produced By | Consumed By | Purpose |
| --- | --- | --- | --- |
| `weather_stream` | Producer | Inference, lakehouse sink | Hourly weather JSON records |
| `weather_6h_blocks` | Inference | Lakehouse sink | Complete 6-hour weather aggregates |
| `predictions_7a` | Inference | Dashboard, lakehouse sink | Building-level prediction records |
| `agg_building6h_7b` | Inference | Daily aggregator, dashboard, lakehouse sink | Building 6-hour energy output |
| `agg_site_daily_7c` | Daily aggregator | Dashboard, lakehouse sink | Site daily energy output |

## Notes And Troubleshooting

- If the model fails to load, rerun the Part A notebook and confirm `A2A/models/gbt_best_model` exists.
- If the producer fails to start, confirm `A2A/weather_transf.csv` exists and matches the expected transformed weather schema.
- If inference fails while reading buildings, confirm `A2A/new_building_information.csv` exists or update `configs/local.yaml`.
- If no dashboard data appears, confirm the producer, inference, and daily aggregator services are running.
- If Kafka topics already exist with the wrong partition count, use `docker compose down -v` and recreate the topics.
- Run `clean-runtime` before a fresh pipeline run, not after starting the pipeline, because it removes runtime state such as checkpoints and Hudi data.
- The existing `A2B_production/README.md` may mention commands such as `monitor` or `validate-outputs`; those commands are not currently implemented in `a2b-prod`.
