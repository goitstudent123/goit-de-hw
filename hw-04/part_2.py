from pyspark.sql import SparkSession

DATA_PATH = "./data/nuek-vuh3.csv"


def run(spark: SparkSession) -> list:
    nuek_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(DATA_PATH)
    )

    nuek_repart = nuek_df.repartition(2)

    nuek_processed = (
        nuek_repart.where("final_priority < 3")
        .select("unit_id", "final_priority")
        .groupBy("unit_id")
        .count()
    )

    # intermediate action
    nuek_processed.collect()

    nuek_processed = nuek_processed.where("count>2")

    return nuek_processed.collect()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .appName("MyGoitSparkSandbox")
        .getOrCreate()
    )

    rows = run(spark)
    jobs = spark.sparkContext.statusTracker().getJobIdsForGroup()
    print(f"Part 2: {len(rows)} rows, {len(jobs)} jobs")

    input("Press Enter to continue...5")

    spark.stop()
