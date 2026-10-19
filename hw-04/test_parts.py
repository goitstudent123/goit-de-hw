import os
import sys

import pytest
from pyspark.sql import SparkSession

import part_1
import part_2
import part_3


@pytest.fixture(scope="session")
def spark():
    # workers default to system python3, which may not match the venv
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .appName("hw-04-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def run_and_count_jobs(spark, part):
    sc = spark.sparkContext
    sc.setJobGroup(part.__name__, part.__name__)
    try:
        rows = part.run(spark)
    finally:
        # part 3 leaves its cache, which would shorten the other parts' plans
        spark.catalog.clearCache()
    return rows, len(sc.statusTracker().getJobIdsForGroup(part.__name__))


@pytest.mark.parametrize(
    "part, expected_jobs",
    [(part_1, 5), (part_2, 8), (part_3, 7)],
)
def test_job_count(spark, part, expected_jobs):
    _, jobs = run_and_count_jobs(spark, part)
    assert jobs == expected_jobs


def test_parts_return_same_rows(spark):
    results = [sorted(run_and_count_jobs(spark, p)[0]) for p in (part_1, part_2, part_3)]
    assert results[0] == results[1] == results[2]
    assert results[0]
    assert all(row["count"] > 2 for row in results[0])
