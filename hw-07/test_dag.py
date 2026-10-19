import os
import sys
import tempfile

# keep airflow from creating ~/airflow when the dag gets imported
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp())
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "dags"))

import pymysql
import pytest

import medals_dag
from medals_dag import MEDALS, RESULTS_TABLE, SOURCE_TABLE, dag

TEST_TABLE = "anton.test_dag_results"


def test_dag_has_one_task_per_step():
    assert set(dag.task_ids) == {
        "create_table",
        "pick_medal",
        "pick_medal_task",
        "calc_Bronze",
        "calc_Silver",
        "calc_Gold",
        "generate_delay",
        "check_for_correctness",
    }


def test_task_order():
    assert dag.get_task("create_table").downstream_task_ids == {"pick_medal"}
    assert dag.get_task("pick_medal").downstream_task_ids == {"pick_medal_task"}
    assert dag.get_task("pick_medal_task").downstream_task_ids == {
        "calc_Bronze",
        "calc_Silver",
        "calc_Gold",
    }
    assert dag.get_task("generate_delay").upstream_task_ids == {
        "calc_Bronze",
        "calc_Silver",
        "calc_Gold",
    }
    assert dag.get_task("check_for_correctness").upstream_task_ids == {"generate_delay"}


def test_runs_do_not_overlap():
    assert dag.max_active_runs == 1


def test_create_table_sql():
    sql = " ".join(dag.get_task("create_table").sql)
    assert f"CREATE TABLE IF NOT EXISTS {RESULTS_TABLE}" in sql
    for column in [
        "id INT AUTO_INCREMENT PRIMARY KEY",
        "medal_type",
        "count",
        "created_at",
    ]:
        assert column in sql


def test_pick_medal_returns_each_medal():
    picked = {dag.get_task("pick_medal").python_callable() for _ in range(200)}
    assert picked == set(MEDALS)


class FakeTaskInstance:
    def __init__(self, medal):
        self.medal = medal

    def xcom_pull(self, task_ids):
        assert task_ids == "pick_medal"
        return self.medal


@pytest.mark.parametrize("medal", MEDALS)
def test_branch_runs_calc_task_for_picked_medal(medal):
    task_id = dag.get_task("pick_medal_task").python_callable(
        ti=FakeTaskInstance(medal)
    )
    assert task_id == f"calc_{medal}"
    assert task_id in dag.task_ids


def test_delay_runs_after_any_successful_calc_task():
    assert dag.get_task("generate_delay").trigger_rule == "one_success"


def test_delay_sleeps_given_seconds(monkeypatch):
    slept = []
    monkeypatch.setattr(medals_dag.time, "sleep", slept.append)
    dag.get_task("generate_delay").python_callable(params={"delay": 35})
    assert slept == [35]


def test_default_delay_is_shorter_than_max_age():
    assert dag.params["delay"] < medals_dag.MAX_AGE_SECONDS


def test_sensor_gives_up_on_old_record():
    sensor = dag.get_task("check_for_correctness")
    assert sensor.mode == "poke"
    assert sensor.timeout == 30


def connect():
    # same server as the dag: .env one if set, otherwise the local container
    if os.getenv("MYSQL_HOST"):
        return pymysql.connect(
            host=os.environ["MYSQL_HOST"],
            port=int(os.getenv("MYSQL_PORT") or 3306),
            user=os.environ["MYSQL_USER"],
            password=os.environ["MYSQL_PASSWORD"],
        )
    return pymysql.connect(host="127.0.0.1", port=3307, user="root", password="root")


def to_test_table(sql):
    return sql.replace(RESULTS_TABLE, TEST_TABLE)


@pytest.fixture
def cursor():
    try:
        connection = connect()
    except pymysql.err.OperationalError:
        pytest.skip("MySQL is not running")
    with connection, connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
        for statement in dag.get_task("create_table").sql:
            cursor.execute(to_test_table(statement))
        yield cursor
        cursor.execute(f"DROP TABLE {TEST_TABLE}")


def check_newest_record(cursor):
    cursor.execute(to_test_table(dag.get_task("check_for_correctness").sql))
    return cursor.fetchone()[0]


@pytest.mark.parametrize("medal", MEDALS)
def test_calc_task_saves_medal_count(cursor, medal):
    cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE} WHERE medal = %s", medal)
    expected = cursor.fetchone()[0]

    cursor.execute(to_test_table(dag.get_task(f"calc_{medal}").sql))
    cursor.execute(
        f"SELECT medal_type, count, TIMESTAMPDIFF(SECOND, created_at, NOW()) FROM {TEST_TABLE}"
    )
    rows = cursor.fetchall()

    assert expected > 0
    assert len(rows) == 1
    medal_type, count, age = rows[0]
    assert (medal_type, count) == (medal, expected)
    assert age <= 1


def test_check_passes_for_fresh_record(cursor):
    cursor.execute(to_test_table(dag.get_task("calc_Gold").sql))
    assert check_newest_record(cursor) == 1


def test_check_fails_for_record_older_than_30_seconds(cursor):
    cursor.execute(
        f"INSERT INTO {TEST_TABLE} (medal_type, count, created_at) "
        "VALUES ('Gold', 1, NOW() - INTERVAL 35 SECOND)"
    )
    assert check_newest_record(cursor) == 0


def test_check_fails_for_empty_table(cursor):
    assert not check_newest_record(cursor)
