import argparse
import tempfile

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    BIO_TABLE,
    OUTPUT_TABLE,
    OUTPUT_TOPIC,
    RESULTS_TOPIC,
    create_spark,
    jdbc_options,
    spark_kafka_options,
)

RESULT_SCHEMA = StructType(
    [
        StructField("edition", StringType()),
        StructField("edition_id", IntegerType()),
        StructField("country_noc", StringType()),
        StructField("sport", StringType()),
        StructField("event", StringType()),
        StructField("result_id", LongType()),
        StructField("athlete", StringType()),
        StructField("athlete_id", IntegerType()),
        StructField("pos", StringType()),
        StructField("medal", StringType()),
        StructField("isTeamSport", StringType()),
    ]
)


def read_bio(spark):
    return spark.read.format("jdbc").options(**jdbc_options(BIO_TABLE)).load()


def filter_bio(bio):
    # try_cast gives NULL for empty strings and values like "80-82"
    return (
        bio.withColumn("height", F.col("height").try_cast("double"))
        .withColumn("weight", F.col("weight").try_cast("double"))
        .where(F.col("height").isNotNull() & F.col("weight").isNotNull())
    )


def parse_results(raw):
    return raw.select(
        F.from_json(F.col("value").cast("string"), RESULT_SCHEMA).alias("r")
    ).select("r.*")


def enrich(results, bio):
    # country_noc comes from the results: the country the athlete competed for
    return results.join(
        bio.select("athlete_id", "sex", "height", "weight"), "athlete_id"
    )


def average_stats(enriched):
    # no medal is NULL in the local database and "nan" in the course one
    medal = F.when(
        F.col("medal").isNull() | (F.col("medal") == "nan"), "No medal"
    ).otherwise(F.col("medal"))
    return (
        enriched.withColumn("medal", medal)
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            F.avg("height").alias("avg_height"),
            F.avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", F.current_timestamp())
    )


def to_kafka_messages(stats):
    return stats.select(F.to_json(F.struct(*stats.columns)).alias("value"))


def write_batch(batch, batch_id):
    batch.persist()

    # Stage 6a: write to the output Kafka topic
    to_kafka_messages(batch).write.format("kafka").options(
        **spark_kafka_options()
    ).option("topic", OUTPUT_TOPIC).save()

    # Stage 6b: write to the database
    batch.write.format("jdbc").options(**jdbc_options(OUTPUT_TABLE)).mode(
        "append"
    ).save()

    print(
        f"[pipeline] batch {batch_id}: {batch.count()} rows to {OUTPUT_TOPIC} and {OUTPUT_TABLE}",
        flush=True,
    )
    batch.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-offsets",
        type=int,
        default=100000,
        help="messages per micro-batch",
    )
    args = parser.parse_args()

    spark = create_spark("streaming_pipeline")

    # Stage 1: read athlete_bio from MySQL
    bio = read_bio(spark)

    # Stage 2: drop rows where height or weight is empty or not a number
    bio = filter_bio(bio).cache()

    # Stage 3: read the results from the Kafka topic, one column per json field
    raw = (
        spark.readStream.format("kafka")
        .options(**spark_kafka_options())
        .option("subscribe", RESULTS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    results = parse_results(raw)

    # Stage 4: join the results with athlete_bio by athlete_id
    enriched = enrich(results, bio)

    # Stage 5: average height and weight per sport, medal, sex and country_noc, with a timestamp
    stats = average_stats(enriched)

    # Stage 6: stream every micro-batch to Kafka and MySQL with forEachBatch
    print(f"[pipeline] reading {RESULTS_TOPIC}", flush=True)
    query = (
        stats.writeStream.outputMode("complete")
        .foreachBatch(write_batch)
        .option("checkpointLocation", tempfile.mkdtemp(prefix="pipeline_checkpoint_"))
        # process everything in the topic, then stop
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
