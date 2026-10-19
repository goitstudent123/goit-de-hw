import random
import time
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG, Param, TriggerRule

CONNECTION_ID = "goit_mysql_db"
SCHEMA = "anton"
RESULTS_TABLE = f"{SCHEMA}.hw_dag_results"
SOURCE_TABLE = "olympic_dataset.athlete_event_results"
MEDALS = ["Bronze", "Silver", "Gold"]
MAX_AGE_SECONDS = 30

CREATE_TABLE_SQL = [
    f"CREATE DATABASE IF NOT EXISTS {SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at DATETIME
    )
    """,
]

# 1 if the newest record is at most 30 seconds old, 0 (or NULL for an empty table) otherwise
CHECK_SQL = f"""
SELECT TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {MAX_AGE_SECONDS}
FROM {RESULTS_TABLE}
"""


def count_medals_sql(medal):
    return f"""
    INSERT INTO {RESULTS_TABLE} (medal_type, count, created_at)
    SELECT '{medal}', COUNT(*), NOW()
    FROM {SOURCE_TABLE}
    WHERE medal = '{medal}'
    """


def pick_medal():
    return random.choice(MEDALS)


def choose_calc_task(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay(params):
    time.sleep(params["delay"])


with DAG(
    dag_id="anton_medals",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    # runs share one table, so a parallel run could make the sensor pass for another one
    max_active_runs=1,
    params={"delay": Param(10, type="integer", minimum=0)},
    tags=["anton"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONNECTION_ID,
        sql=CREATE_TABLE_SQL,
    )

    pick_medal_value = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=choose_calc_task,
    )

    calc_tasks = [
        SQLExecuteQueryOperator(
            task_id=f"calc_{medal}",
            conn_id=CONNECTION_ID,
            sql=count_medals_sql(medal),
        )
        for medal in MEDALS
    ]

    delay = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONNECTION_ID,
        sql=CHECK_SQL,
        mode="poke",
        poke_interval=5,
        timeout=30,
    )

    create_table >> pick_medal_value >> pick_medal_task >> calc_tasks
    calc_tasks >> delay >> check_for_correctness
