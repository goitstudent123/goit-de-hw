import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]


def clean_text(column):
    # keep letters, digits, spaces and basic punctuation, then squeeze the spaces
    cleaned = F.regexp_replace(column, r"[^\p{L}\p{N}\s,.'\"()-]", "")
    return F.trim(F.regexp_replace(cleaned, r"\s+", " "))


def clean_table(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, clean_text(F.col(field.name)))
    return df.dropDuplicates()


def bronze_to_silver(spark, table):
    # absolute paths, the Spark JVM may have another working directory than Python
    df = clean_table(spark.read.parquet(os.path.abspath(f"bronze/{table}")))
    df.write.mode("overwrite").parquet(os.path.abspath(f"silver/{table}"))
    print(f"Saved silver/{table}")
    return df


def main():
    spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()
    for table in TABLES:
        bronze_to_silver(spark, table).show()
    spark.stop()


if __name__ == "__main__":
    main()
