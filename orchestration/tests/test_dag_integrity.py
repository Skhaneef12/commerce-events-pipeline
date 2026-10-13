"""DAG integrity tests: import errors, structure, and the retry/idempotency settings
we rely on. Run in CI with Airflow installed (see .github/workflows/ci.yml)."""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_daily_metrics_structure(dagbag):
    dag = dagbag.get_dag("daily_customer_metrics")
    assert dag is not None
    assert set(dag.task_ids) == {"build_daily_metrics", "quality_gate", "latest_only", "freshness_slo"}
    assert dag.get_task("build_daily_metrics").downstream_task_ids == {"quality_gate"}
    assert dag.get_task("latest_only").downstream_task_ids == {"freshness_slo"}


def test_safe_scheduling_settings(dagbag):
    dag = dagbag.get_dag("daily_customer_metrics")
    assert dag.max_active_runs == 1
    assert dag.catchup is True
    assert dag.get_task("build_daily_metrics").retries == 2
    assert dag.get_task("quality_gate").retries == 0


def test_jobs_are_parameterised_by_logical_date(dagbag):
    dag = dagbag.get_dag("daily_customer_metrics")
    for task_id in ("build_daily_metrics", "quality_gate"):
        assert "{{ ds }}" in dag.get_task(task_id).bash_command
