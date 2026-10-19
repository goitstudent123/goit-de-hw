import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# the jobs read and write landing/, bronze/, silver/ and gold/ next to this file
DAG_DIR = os.path.dirname(os.path.abspath(__file__))


def spark_job(name):
    return BashOperator(
        task_id=name,
        bash_command=f"spark-submit --master 'local[*]' {name}.py",
        cwd=DAG_DIR,
    )


with DAG(
    dag_id="anton_project_solution",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["anton"],
) as dag:
    landing_to_bronze = spark_job("landing_to_bronze")
    bronze_to_silver = spark_job("bronze_to_silver")
    silver_to_gold = spark_job("silver_to_gold")

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
