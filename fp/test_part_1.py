import json
import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "part_1"))

from mysql_to_kafka import to_kafka_messages as results_to_kafka
from streaming_pipeline import (
    RESULT_SCHEMA,
    average_stats,
    enrich,
    filter_bio,
    parse_results,
    to_kafka_messages,
)

os.environ["PYSPARK_PYTHON"] = sys.executable

BIO_COLUMNS = ["athlete_id", "name", "sex", "height", "weight", "country_noc"]
RESULT_COLUMNS = [field.name for field in RESULT_SCHEMA.fields]


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test_part_1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def make_result(
    athlete_id, sport="Swimming", medal=None, country_noc="UKR", result_id=1
):
    return (
        "2004 Summer Olympics",
        26,
        country_noc,
        sport,
        "100 metres Freestyle, Men",
        result_id,
        f"Athlete {athlete_id}",
        athlete_id,
        "1",
        medal,
        "False",
    )


def results_df(spark, rows):
    return spark.createDataFrame(rows, RESULT_SCHEMA)


def bio_df(spark, rows):
    return spark.createDataFrame(rows, BIO_COLUMNS)


def test_filter_bio_keeps_only_numeric_height_and_weight(spark):
    bio = bio_df(
        spark,
        [
            (1, "A", "Male", "180", "75", "UKR"),
            (2, "B", "Male", "", "80", "UKR"),
            (3, "C", "Female", "170", "", "UKR"),
            (4, "D", "Female", "165", "60-62", "UKR"),
            (5, "E", "Male", None, "70", "UKR"),
            (6, "F", "Female", "abc", "55", "UKR"),
            (7, "G", "Female", "168.5", "57.5", "UKR"),
        ],
    )
    rows = filter_bio(bio).orderBy("athlete_id").collect()
    assert [(r.athlete_id, r.height, r.weight) for r in rows] == [
        (1, 180.0, 75.0),
        (7, 168.5, 57.5),
    ]


def test_results_go_to_kafka_as_json_with_athlete_id_key(spark):
    results = results_df(spark, [make_result(10, medal="Gold")])
    message = results_to_kafka(results).collect()[0]
    assert message.key == "10"
    assert json.loads(message.value)["medal"] == "Gold"


def test_parse_results_gives_one_column_per_json_field(spark):
    rows = [make_result(10, medal="Gold"), make_result(11, result_id=2)]
    messages = results_to_kafka(results_df(spark, rows))
    parsed = parse_results(messages)
    assert parsed.columns == RESULT_COLUMNS
    assert sorted(tuple(r) for r in parsed.collect()) == sorted(rows)


def test_enrich_joins_by_athlete_id(spark):
    results = results_df(spark, [make_result(1), make_result(2, result_id=2)])
    bio = filter_bio(
        bio_df(
            spark,
            [
                (1, "A", "Male", "180", "75", "USA"),
                (3, "C", "Male", "170", "70", "UKR"),
            ],
        )
    )
    rows = enrich(results, bio).collect()
    assert len(rows) == 1
    assert rows[0].athlete_id == 1
    assert (rows[0].sex, rows[0].height, rows[0].weight) == ("Male", 180.0, 75.0)
    # the country from the results, not from the bio
    assert rows[0].country_noc == "UKR"


def test_average_stats_per_sport_medal_sex_and_country(spark):
    bio = filter_bio(
        bio_df(
            spark,
            [
                (1, "A", "Male", "180", "80", "UKR"),
                (2, "B", "Male", "190", "90", "UKR"),
                (3, "C", "Female", "170", "60", "UKR"),
                (4, "D", "Male", "200", "100", "UKR"),
            ],
        )
    )
    results = results_df(
        spark,
        [
            make_result(1, medal="Gold", result_id=1),
            make_result(2, medal="Gold", result_id=2),
            make_result(3, medal="Gold", result_id=3),
            make_result(4, medal=None, result_id=4),
            make_result(4, medal="nan", result_id=5),
            make_result(1, sport="Rowing", medal="Gold", result_id=6),
        ],
    )
    before = datetime.now()
    stats = average_stats(enrich(results, bio))
    assert stats.columns == [
        "sport",
        "medal",
        "sex",
        "country_noc",
        "avg_height",
        "avg_weight",
        "timestamp",
    ]

    rows = stats.collect()
    by_group = {(r.sport, r.medal, r.sex, r.country_noc): r for r in rows}
    assert len(rows) == 4
    assert (by_group["Swimming", "Gold", "Male", "UKR"].avg_height) == 185.0
    assert (by_group["Swimming", "Gold", "Male", "UKR"].avg_weight) == 85.0
    assert (by_group["Swimming", "Gold", "Female", "UKR"].avg_height) == 170.0
    assert (by_group["Rowing", "Gold", "Male", "UKR"].avg_weight) == 80.0
    # NULL and "nan" both mean no medal
    assert by_group["Swimming", "No medal", "Male", "UKR"].avg_height == 200.0
    assert all(r.timestamp >= before.replace(microsecond=0) for r in rows)


def test_output_messages_carry_all_columns(spark):
    bio = filter_bio(bio_df(spark, [(1, "A", "Male", "180", "80", "UKR")]))
    results = results_df(spark, [make_result(1, medal="Silver")])
    message = json.loads(
        to_kafka_messages(average_stats(enrich(results, bio))).collect()[0].value
    )
    assert set(message) == {
        "sport",
        "medal",
        "sex",
        "country_noc",
        "avg_height",
        "avg_weight",
        "timestamp",
    }
    assert message["medal"] == "Silver"
    assert message["avg_height"] == 180.0
