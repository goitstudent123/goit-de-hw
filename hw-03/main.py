from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(__file__).parent / "data"


def load_data(
    spark: SparkSession, data_dir: str
) -> tuple[DataFrame, DataFrame, DataFrame]:
    def read(name):
        return spark.read.csv(f"{data_dir}/{name}.csv", header=True, inferSchema=True)

    return read("users"), read("purchases"), read("products")


def clean(df: DataFrame) -> DataFrame:
    return df.dropna()


def total_by_category(purchases: DataFrame, products: DataFrame) -> DataFrame:
    return (
        purchases.join(products, "product_id")
        .groupBy("category")
        .agg(F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("total"))
    )


def total_by_category_for_age(
    purchases: DataFrame,
    products: DataFrame,
    users: DataFrame,
    min_age: int,
    max_age: int,
) -> DataFrame:
    buyers = users.filter(F.col("age").between(min_age, max_age))
    return total_by_category(purchases.join(buyers, "user_id"), products)


def share_by_category(totals: DataFrame) -> DataFrame:
    grand_total = totals.agg(F.sum("total")).first()[0]
    return totals.withColumn(
        "percentage", F.round(F.col("total") / grand_total * 100, 2)
    )


def top_categories(shares: DataFrame, n: int) -> DataFrame:
    return shares.orderBy(F.col("percentage").desc()).limit(n)


def main():
    spark = SparkSession.builder.appName("hw-03").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    users, purchases, products = load_data(spark, str(DATA_DIR))
    print("1. Raw data")
    for df in (users, purchases, products):
        df.show(5)

    users, purchases, products = clean(users), clean(purchases), clean(products)
    print(
        f"2. After cleaning: users={users.count()}, purchases={purchases.count()}, products={products.count()}"
    )

    print("3. Total by category")
    total_by_category(purchases, products).orderBy("category").show()

    young_totals = total_by_category_for_age(purchases, products, users, 18, 25)
    print("4. Total by category, age 18-25")
    young_totals.orderBy("category").show()

    shares = share_by_category(young_totals)
    print("5. Share by category, age 18-25")
    shares.orderBy("category").show()

    print("6. Top 3 categories, age 18-25")
    top_categories(shares, 3).show()

    spark.stop()


if __name__ == "__main__":
    main()
