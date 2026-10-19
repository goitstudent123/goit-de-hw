import os
import sys

import pytest
from pyspark.sql import SparkSession

from main import (
    clean,
    load_data,
    share_by_category,
    top_categories,
    total_by_category,
    total_by_category_for_age,
)


@pytest.fixture(scope="session")
def spark():
    # workers default to system python3, which may not match the venv
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[1]").appName("hw-03-tests").getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def users(spark):
    return spark.createDataFrame(
        [
            (1, "Ann", 18, "a@x.com"),
            (2, "Bob", 25, "b@x.com"),
            (3, "Cid", 40, "c@x.com"),
            (4, "Dan", 17, "d@x.com"),
        ],
        "user_id int, name string, age int, email string",
    )


@pytest.fixture
def products(spark):
    return spark.createDataFrame(
        [
            (1, "Lipstick", "Beauty", 10.0),
            (2, "Ball", "Sports", 5.0),
            (3, "Lamp", "Home", 20.0),
            (4, "Phone", "Electronics", 100.0),
        ],
        "product_id int, product_name string, category string, price double",
    )


@pytest.fixture
def purchases(spark):
    return spark.createDataFrame(
        [
            (1, 1, 1, "2022-01-01", 2),
            (2, 2, 2, "2022-01-02", 4),
            (3, 2, 3, "2022-01-03", 1),
            (4, 3, 3, "2022-01-04", 3),
            (5, 4, 4, "2022-01-05", 1),
            (6, 1, 4, "2022-01-06", 1),
        ],
        "purchase_id int, user_id int, product_id int, date string, quantity int",
    )


def as_dict(df, value_column):
    return {row["category"]: row[value_column] for row in df.collect()}


def test_load_data_reads_csv_with_header_and_types(spark, tmp_path):
    (tmp_path / "users.csv").write_text("user_id,name,age,email\n1,Ann,18,a@x.com\n")
    (tmp_path / "purchases.csv").write_text(
        "purchase_id,user_id,product_id,date,quantity\n1,1,1,2022-01-01,2\n"
    )
    (tmp_path / "products.csv").write_text(
        "product_id,product_name,category,price\n1,Lipstick,Beauty,8.3\n"
    )

    users, purchases, products = load_data(spark, str(tmp_path))

    assert users.columns == ["user_id", "name", "age", "email"]
    assert purchases.columns == [
        "purchase_id",
        "user_id",
        "product_id",
        "date",
        "quantity",
    ]
    assert products.columns == ["product_id", "product_name", "category", "price"]
    assert users.first()["age"] == 18
    assert products.first()["price"] == 8.3


def test_clean_drops_rows_with_any_missing_value(spark):
    df = spark.createDataFrame(
        [(1, "Ann", 18), (2, None, 30), (3, "Cid", None), (None, "Dan", 20)],
        "user_id int, name string, age int",
    )

    assert [row["user_id"] for row in clean(df).collect()] == [1]


def test_total_by_category(purchases, products):
    result = as_dict(total_by_category(purchases, products), "total")

    assert result == {
        "Beauty": 20.0,
        "Sports": 20.0,
        "Home": 80.0,
        "Electronics": 200.0,
    }


def test_total_by_category_for_age_includes_18_and_25(purchases, products, users):
    result = as_dict(
        total_by_category_for_age(purchases, products, users, 18, 25), "total"
    )

    assert result == {
        "Beauty": 20.0,
        "Sports": 20.0,
        "Home": 20.0,
        "Electronics": 100.0,
    }


def test_share_by_category_is_percent_of_total(purchases, products, users):
    totals = total_by_category_for_age(purchases, products, users, 18, 25)

    result = as_dict(share_by_category(totals), "percentage")

    assert result == {"Beauty": 12.5, "Sports": 12.5, "Home": 12.5, "Electronics": 62.5}
    assert sum(result.values()) == 100.0


def test_top_categories_returns_highest_percentages(spark):
    shares = spark.createDataFrame(
        [("Beauty", 10.0), ("Sports", 40.0), ("Home", 20.0), ("Electronics", 30.0)],
        "category string, percentage double",
    )

    result = [row["category"] for row in top_categories(shares, 3).collect()]

    assert result == ["Sports", "Electronics", "Home"]
