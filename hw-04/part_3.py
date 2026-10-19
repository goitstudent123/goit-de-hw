from pyspark.sql import SparkSession

DATA_PATH = "./data/nuek-vuh3.csv"


def run(spark: SparkSession) -> list:
    nuek_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(DATA_PATH)
    )

    nuek_repart = nuek_df.repartition(2)

    nuek_processed_cached = (
        nuek_repart.where("final_priority < 3")
        .select("unit_id", "final_priority")
        .groupBy("unit_id")
        .count()
        .cache()
    )

    # intermediate action, also fills the cache
    nuek_processed_cached.collect()

    nuek_processed = nuek_processed_cached.where("count>2")

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
    print(f"Part 3: {len(rows)} rows, {len(jobs)} jobs")

    input("Press Enter to continue...5")

    # free the cached DataFrame
    spark.catalog.clearCache()

    spark.stop()
