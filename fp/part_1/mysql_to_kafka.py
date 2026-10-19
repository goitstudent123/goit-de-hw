from pyspark.sql import functions as F

from config import (
    RESULTS_TABLE,
    RESULTS_TOPIC,
    create_spark,
    jdbc_options,
    spark_kafka_options,
)


def to_kafka_messages(results):
    return results.select(
        F.col("athlete_id").cast("string").alias("key"),
        F.to_json(F.struct(*results.columns)).alias("value"),
    )


def main():
    spark = create_spark("mysql_to_kafka")

    # Stage 3: read athlete_event_results from MySQL and write it to the Kafka topic
    results = (
        spark.read.format("jdbc").options(**jdbc_options(RESULTS_TABLE)).load().cache()
    )
    to_kafka_messages(results).write.format("kafka").options(
        **spark_kafka_options()
    ).option("topic", RESULTS_TOPIC).save()

    print(
        f"[mysql_to_kafka] sent {results.count()} rows from {RESULTS_TABLE} to {RESULTS_TOPIC}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
