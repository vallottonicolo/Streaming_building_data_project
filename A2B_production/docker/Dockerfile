# Image can be changed to any Spark image with Python 3.9 and Java 11 installed.
FROM monashfit/fit5202-pyspark:latest 

USER root

WORKDIR /workspace/A2/A2B_production

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar pyspark-shell"

COPY A2B_production /workspace/A2/A2B_production
RUN python -m pip install --no-cache-dir -r /workspace/A2/A2B_production/requirements.txt

# Resolve Spark Kafka/Hudi packages at image build time so service startup does
# not spend the first minute downloading Maven artifacts.
RUN python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('a2b-prod-package-warmup').master('local[1]').getOrCreate(); spark.stop()"

CMD ["a2b-prod", "--help"]
