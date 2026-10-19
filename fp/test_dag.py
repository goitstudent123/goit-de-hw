import os

import pytest
from airflow.configuration import conf
from airflow.models import DagBag

DAG_ID = "anton_project_solution"
DAGS_FOLDER = conf.get("core", "dags_folder")
JOBS = ["landing_to_bronze", "bronze_to_silver", "silver_to_gold"]


@pytest.fixture(scope="module")
def dag():
    dag_bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert dag_bag.import_errors == {}
    return dag_bag.get_dag(DAG_ID)


def test_dag_is_in_project_solution(dag):
    assert os.path.basename(dag.fileloc) == "project_solution.py"


def test_dag_runs_three_jobs_one_after_another(dag):
    assert dag.task_ids == JOBS
    assert dag.get_task("landing_to_bronze").downstream_task_ids == {"bronze_to_silver"}
    assert dag.get_task("bronze_to_silver").downstream_task_ids == {"silver_to_gold"}
    assert dag.get_task("silver_to_gold").downstream_task_ids == set()


@pytest.mark.parametrize("job", JOBS)
def test_each_task_submits_its_script(dag, job):
    task = dag.get_task(job)
    assert task.bash_command.startswith("spark-submit ")
    assert task.bash_command.endswith(f" {job}.py")
    assert task.cwd == os.path.dirname(dag.fileloc)
    assert os.path.isfile(os.path.join(task.cwd, f"{job}.py"))


def test_runs_only_on_demand(dag):
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
