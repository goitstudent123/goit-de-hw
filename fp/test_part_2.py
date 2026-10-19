import os
import sys
from datetime import datetime

import pytest
import requests
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "part_2"))

import landing_to_bronze as l2b
from bronze_to_silver import bronze_to_silver, clean_table, clean_text
from silver_to_gold import average_stats, silver_to_gold

os.environ["PYSPARK_PYTHON"] = sys.executable

RESULTS_CSV = (
    "﻿edition,edition_id,country_noc,sport,event,result_id,athlete,athlete_id,pos,medal,isTeamSport\n"
    '1908 Summer Olympics,5,ANZ,Athletics,"100 metres, Men",56265,Ernest Hutcheon,64710,DNS,,False\n'
    '1908 Summer Olympics,5,ANZ,Athletics,"400 metres, Men",56313,Henry Murray,64756,1,Gold,False\n'
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test_part_2")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def in_tmp_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


class FakeResponse:
    def __init__(self, content, status_code=200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError(f"{self.status_code}")


def test_download_saves_csv_to_landing(monkeypatch):
    urls = []

    def fake_get(url, timeout):
        urls.append(url)
        return FakeResponse(RESULTS_CSV.encode("utf-8"))

    monkeypatch.setattr(l2b.requests, "get", fake_get)
    path = l2b.download("athlete_event_results")

    assert urls == ["https://ftp.goit.study/neoversity/athlete_event_results.csv"]
    assert path == os.path.abspath("landing/athlete_event_results.csv")
    with open(path, encoding="utf-8") as file:
        assert file.read() == RESULTS_CSV


def test_download_fails_on_http_error(monkeypatch):
    monkeypatch.setattr(
        l2b.requests, "get", lambda url, timeout: FakeResponse(b"", 404)
    )
    with pytest.raises(requests.HTTPError):
        l2b.download("athlete_bio")


def test_landing_to_bronze_writes_parquet(spark, monkeypatch):
    monkeypatch.setattr(
        l2b.requests,
        "get",
        lambda url, timeout: FakeResponse(RESULTS_CSV.encode("utf-8")),
    )
    l2b.landing_to_bronze(spark, "athlete_event_results")

    bronze = spark.read.parquet(os.path.abspath("bronze/athlete_event_results"))
    # the byte order mark of the real file must not end up in the column name
    assert bronze.columns[0] == "edition"
    rows = bronze.orderBy("result_id").collect()
    assert [r.event for r in rows] == ["100 metres, Men", "400 metres, Men"]
    assert [r.medal for r in rows] == [None, "Gold"]
    assert rows[0].athlete_id == 64710


@pytest.mark.parametrize(
    "text, expected",
    [
        ("  Ivanka   Bonova ", "Ivanka Bonova"),
        ("Personal Best: 400 – 53.54 (1980).", "Personal Best 400 53.54 (1980)."),
        ("Jürgen O'Brien-Müller", "Jürgen O'Brien-Müller"),
        ("80-82", "80-82"),
        ("a#b$c%d*e", "abcde"),
        (None, None),
    ],
)
def test_clean_text(spark, text, expected):
    df = spark.createDataFrame([(text,)], "value string")
    assert df.select(clean_text(df.value).alias("v")).collect()[0].v == expected


def test_clean_table_cleans_only_text_columns_and_drops_duplicates(spark):
    df = spark.createDataFrame(
        [(1, " Bob  Smith", 1.5), (1, "Bob Smith#", 1.5), (2, "Ann", 2.5)],
        "id int, name string, score double",
    )
    cleaned = clean_table(df)
    assert dict(cleaned.dtypes) == {"id": "int", "name": "string", "score": "double"}
    assert sorted(tuple(r) for r in cleaned.collect()) == [
        (1, "Bob Smith", 1.5),
        (2, "Ann", 2.5),
    ]


def test_bronze_to_silver_reads_bronze_and_writes_silver(spark):
    spark.createDataFrame(
        [(1, "Ann!"), (1, "Ann"), (2, "Bob")], "athlete_id int, name string"
    ).write.parquet(os.path.abspath("bronze/athlete_bio"))
    bronze_to_silver(spark, "athlete_bio")
    silver = spark.read.parquet(os.path.abspath("silver/athlete_bio"))
    assert sorted(tuple(r) for r in silver.collect()) == [(1, "Ann"), (2, "Bob")]


def make_bio(spark, rows):
    return spark.createDataFrame(
        rows,
        "athlete_id int, sex string, height double, weight string, country_noc string",
    )


def make_results(spark, rows):
    return spark.createDataFrame(
        rows, "athlete_id int, sport string, medal string, country_noc string"
    )


def test_average_stats(spark):
    bio = make_bio(
        spark,
        [
            (1, "Male", 180.0, "80", "USA"),
            (2, "Male", 190.0, "90", "USA"),
            (3, "Female", 170.0, "60", "USA"),
            (4, "Male", 200.0, "80-82", "USA"),
            (5, "Male", None, "70", "USA"),
            (6, "Female", 160.0, "", "USA"),
        ],
    )
    results = make_results(
        spark,
        [
            (1, "Rowing", "Gold", "UKR"),
            (2, "Rowing", "Gold", "UKR"),
            (3, "Rowing", None, "UKR"),
            (4, "Rowing", "Gold", "UKR"),
            (5, "Rowing", "Gold", "UKR"),
            (6, "Rowing", "Gold", "UKR"),
        ],
    )
    before = datetime.now().replace(microsecond=0)
    stats = average_stats(bio, results)

    assert stats.columns == [
        "sport",
        "medal",
        "sex",
        "country_noc",
        "avg_height",
        "avg_weight",
        "timestamp",
    ]
    rows = {(r.sport, r.medal, r.sex, r.country_noc): r for r in stats.collect()}
    # athletes 4, 5 and 6 have no numeric height or weight
    assert set(rows) == {
        ("Rowing", "Gold", "Male", "UKR"),
        ("Rowing", "No medal", "Female", "UKR"),
    }
    assert rows["Rowing", "Gold", "Male", "UKR"].avg_height == 185.0
    assert rows["Rowing", "Gold", "Male", "UKR"].avg_weight == 85.0
    assert rows["Rowing", "No medal", "Female", "UKR"].avg_weight == 60.0
    assert all(r.timestamp >= before for r in rows.values())


def test_silver_to_gold_writes_avg_stats(spark):
    bio = make_bio(spark, [(1, "Male", 180.0, "80", "UKR")])
    bio.write.parquet(os.path.abspath("silver/athlete_bio"))
    results = make_results(spark, [(1, "Judo", "Bronze", "UKR")])
    results.write.parquet(os.path.abspath("silver/athlete_event_results"))
    silver_to_gold(spark)
    gold = spark.read.parquet(os.path.abspath("gold/avg_stats")).collect()
    assert [(r.sport, r.medal, r.avg_height, r.avg_weight) for r in gold] == [
        ("Judo", "Bronze", 180.0, 80.0)
    ]
