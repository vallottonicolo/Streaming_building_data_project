# Energy Consumption Forecasting Pipeline

## Project Overview

This project trains a machine learning model to predict building energy consumption, then uses that model in a streaming pipeline.

The project has two main parts:

- `A2A` trains and saves a Spark ML model using historical building, weather, and meter data.
- `A2B` simulates live weather data, sends it through Kafka, scores it with Spark Structured Streaming, and visualises the prediction outputs.

The local stack uses:

- Docker Compose
- Jupyter Notebook
- Apache Spark / PySpark
- Apache Kafka
- Spark MLlib
- pandas, NumPy, Matplotlib, and Seaborn

## Folder Structure

```text
.
|-- A2A/
|   |-- A2A_nval0007.ipynb
|   |-- building_information.csv
|   |-- meters.csv
|   |-- weather.csv
|   |-- weather_transf.csv
|   `-- models/
|       `-- gbt_best_model/
|
|-- A2B/
|   |-- Assignment-2B-Task1_producer_nval0007.ipynb
|   |-- Assignment-2B-Task2_spark_streaming_nval0007.ipynb
|   |-- Assignment-2B-Task3_consumer_nval0007.ipynb
|   |-- new_building_information.csv
|   |-- new_meters.csv
|   |-- checkpoints/
|   `-- parquet/
|
|-- docker/
|   `-- jupyter-spark/
|       `-- Dockerfile
|
|-- docker-compose.yml
|-- .env.example
|-- .dockerignore
`-- Readme.md
```

Generated folders such as `A2B/checkpoints/`, `A2B/parquet/`, pointer folders, and Spark warehouse folders are runtime state. They are created or updated while the streaming notebooks run.

## How To Run The Project

### 1. Install Docker Desktop

Install Docker Desktop and make sure it is running.

Check Docker is available:

```bash
docker --version
docker compose version
```

If your Docker installation uses the older Compose command, use this instead:

```bash
docker-compose --version
```

### 2. Open the Project Folder

Open a terminal in the project root:

```bash
cd C:\Users\vallo\fit5202\labs\A2
```

On another machine, use the folder where the project was cloned.

### 3. Create the Local Environment File

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

On macOS or Linux:

```bash
cp .env.example .env
```

The default Jupyter token is:

```text
a2-demo
```

### 4. Start Docker Containers

Run:

```bash
docker compose up -d --build
```

If `docker compose` is not available on your machine, use:

```bash
docker-compose up -d --build
```

This creates:

- `zookeeper`
- `kafka`
- `jupyter-spark`

Kafka topics are created or used by the notebooks during the normal workflow. Run the notebooks in the order below so the producer, streaming inference, and consumers start at the right time.

### 5. Open Jupyter

Open this URL in your browser:

```text
http://localhost:8888/?token=a2-demo
```

Inside Jupyter, open:

```text
/home/student/A2
```

Select the kernel:

```text
A2 Energy Pipeline
```

### 6. Train or Load the Model

Open:

```text
A2A/A2A_nval0007.ipynb
```

Run all cells if the saved model does not already exist at:

```text
A2A/models/gbt_best_model
```

If the model folder already exists, you can skip retraining and move to the streaming notebooks.

### 7. Start Streaming Inference

Open:

```text
A2B/Assignment-2B-Task2_spark_streaming_nval0007.ipynb
```

Run the setup and streaming inference cells before starting the producer.

The notebook should connect to Kafka using:

```text
kafka:9092
```

This is the internal Docker network address for Kafka.

### 8. Start the Weather Producer

Open:

```text
A2B/Assignment-2B-Task1_producer_nval0007.ipynb
```

Run the producer cells. This sends simulated weather events to:

```text
weather_stream
```

### 9. Start the Consumer Visualisations

Open:

```text
A2B/Assignment-2B-Task3_consumer_nval0007.ipynb
```

Run the consumer cells after Task 2 has started producing prediction outputs.

This notebook visualises:

- building-level 6-hour predictions
- site-level daily predictions
- prediction error against metered energy

### 10. Stop the Project

When finished, stop the containers:

```bash
docker compose down
```

Or, with older Docker Compose:

```bash
docker-compose down
```

To also remove Docker volumes:

```bash
docker compose down -v
```

## Useful Docker Commands

List running containers:

```bash
docker compose ps
```

If needed, replace `docker compose` with `docker-compose` in the commands below.

View Jupyter logs:

```bash
docker compose logs -f jupyter-spark
```

View Kafka logs:

```bash
docker compose logs -f kafka
```

List Kafka topics:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Open a shell inside the Jupyter/Spark container:

```bash
docker compose exec jupyter-spark bash
```

## Troubleshooting

### Jupyter asks for a password or token

Use:

```text
http://localhost:8888/?token=a2-demo
```

If you changed `.env`, use the token from that file.

### The notebook asks for a kernel

Choose:

```text
A2 Energy Pipeline
```

### Kafka connection fails

Inside Docker, notebooks should use:

```text
kafka:9092
```

From your host machine, Kafka is exposed at:

```text
localhost:29092
```

### Spark shows `ConnectionRefusedError`

Restart the notebook kernel and rerun the setup cells. This usually means the Spark JVM for that notebook kernel stopped.

### You want a clean streaming rerun

Stop the notebooks and containers first. Then remove generated runtime state only if you intentionally want to replay from scratch:

```text
A2B/checkpoints/
A2B/chk/
A2B/parquet/
A2B/.weather_pointer.json
A2B/.site_pointers/
A2B/site_seq_pointers/
```

Do not remove these folders during an active streaming run.
