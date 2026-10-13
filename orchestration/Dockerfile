# Airflow plus a local Spark runtime (via the pyspark wheel) so BashOperator tasks
# can run spark-submit against the assembled Scala jar.
FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
# pyspark ships spark-submit and the Spark 3.5.1 jars, matching build.sbt.
RUN pip install --no-cache-dir "pyspark==3.5.1"
