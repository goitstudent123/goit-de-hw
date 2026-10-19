import argparse
import tempfile

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import ALERTS, BUILDING_SENSORS, spark_kafka_options

CONDITIONS_FILE = "alerts_conditions.csv"
NOT_USED = -999

READING_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("timestamp", TimestampType()),
    ]
)

CONDITIONS_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("humidity_min", DoubleType()),
        StructField("humidity_max", DoubleType()),
        StructField("temperature_min", DoubleType()),
        StructField("temperature_max", DoubleType()),
        StructField("code", StringType()),
        StructField("message", StringType()),
    ]
)


def create_spark():
    kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    return (
        SparkSession.builder.appName("alerts_processor")
        .master("local[*]")
        .config("spark.jars.packages", kafka_package)
        # on the LAN address the driver sometimes reads its own jar download as an RPC message
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_conditions(spark, path=CONDITIONS_FILE):
    return spark.read.csv(path, header=True, schema=CONDITIONS_SCHEMA)


def parse_readings(raw):
    return raw.select(
        F.from_json(F.col("value").cast("string"), READING_SCHEMA).alias("r")
    ).select("r.*")


def average_by_window(readings):
    return (
        readings.withWatermark("timestamp", "10 seconds")
        .groupBy(F.window("timestamp", "1 minute", "30 seconds"))
        .agg(
            F.avg("temperature").alias("t_avg"),
            F.avg("humidity").alias("h_avg"),
        )
    )


def within(avg_column, measure):
    low = F.col(f"{measure}_min")
    high = F.col(f"{measure}_max")
    value = F.col(avg_column)
    return ((low == NOT_USED) | (value >= low)) & ((high == NOT_USED) | (value <= high))


def find_alerts(averages, conditions):
    return (
        averages.crossJoin(conditions)
        .where(within("t_avg", "temperature") & within("h_avg", "humidity"))
        .select(
            "window",
            "t_avg",
            "h_avg",
            "code",
            "message",
            F.current_timestamp().cast("string").alias("timestamp"),
        )
    )


def to_kafka_messages(alerts):
    return alerts.select(
        F.col("code").alias("key"),
        F.to_json(F.struct(*alerts.columns)).alias("value"),
    )


def print_averages(batch, batch_id):
    for row in batch.orderBy("window").collect():
        print(
            f"[processor] window {row.window.start:%H:%M:%S}-{row.window.end:%H:%M:%S}: "
            f"t_avg {row.t_avg:.2f}, h_avg {row.h_avg:.2f}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--timeout", type=int, default=0, help="stop after N seconds, 0 means never"
    )
    parser.add_argument(
        "--starting-offsets", default="latest", choices=["latest", "earliest"]
    )
    args = parser.parse_args()

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .options(**spark_kafka_options())
        .option("subscribe", BUILDING_SENSORS)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    averages = average_by_window(parse_readings(raw))
    alerts = find_alerts(averages, load_conditions(spark))

    averages.writeStream.outputMode("append").foreachBatch(print_averages).start()
    to_kafka_messages(alerts).writeStream.outputMode("append").format("kafka").options(
        **spark_kafka_options()
    ).option("topic", ALERTS).option(
        "checkpointLocation", tempfile.mkdtemp(prefix="alerts_checkpoint_")
    ).start()
    print(
        f"[processor] reading {BUILDING_SENSORS}, writing alerts to {ALERTS}",
        flush=True,
    )

    try:
        if args.timeout:
            spark.streams.awaitAnyTermination(args.timeout)
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        pass
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
