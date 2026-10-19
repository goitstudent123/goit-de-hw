import os

import requests
from pyspark.sql import SparkSession

FTP_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]


def download(table):
    os.makedirs("landing", exist_ok=True)
    path = os.path.abspath(f"landing/{table}.csv")
    response = requests.get(f"{FTP_URL}{table}.csv", timeout=120)
    response.raise_for_status()
    with open(path, "wb") as file:
        file.write(response.content)
    print(f"Downloaded {FTP_URL}{table}.csv to {path}")
    return path


def landing_to_bronze(spark, table):
    df = spark.read.csv(download(table), header=True, inferSchema=True)
    # absolute path, the Spark JVM may have another working directory than Python
    df.write.mode("overwrite").parquet(os.path.abspath(f"bronze/{table}"))
    print(f"Saved bronze/{table}")
    return df


def main():
    spark = SparkSession.builder.appName("landing_to_bronze").getOrCreate()
    for table in TABLES:
        landing_to_bronze(spark, table).show()
    spark.stop()


if __name__ == "__main__":
    main()
