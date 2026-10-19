# same Airflow version as the course server, plus Java and pyspark for spark-submit
FROM apache/airflow:2.8.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow==2.8.1 \
    apache-airflow-providers-apache-spark==4.7.0 \
    pyspark==3.5.0 \
    pytest==7.4.4
