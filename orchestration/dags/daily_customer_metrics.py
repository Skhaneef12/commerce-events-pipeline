"""Daily customer metrics: silver -> gold, then a quality gate and a freshness SLO check.

Schedule: 02:00 UTC daily. A run with logical date `ds` processes that full UTC day,
and starts after the day ends, so late events have time to arrive.

Idempotency: every task takes `ds` as its only input, and DailyMetricsJob
overwrites just that date's partition. Retries and backfills
(`airflow dags backfill -s 2026-10-01 -e 2026-10-07 daily_customer_metrics`)
are therefore safe.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator

JAR = os.environ.get("PIPELINE_JAR", "/opt/pipeline/jars/commerce-events-pipeline.jar")
SPARK_SUBMIT = "spark-submit --master local[*] --driver-memory 2g"


def spark_job(main_class: str, *args: str) -> str:
    return " ".join([SPARK_SUBMIT, "--class", f"commerce.pipeline.jobs.{main_class}", JAR, *args])


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="daily_customer_metrics",
    description="Silver sessions + bookings -> gold daily customer metrics, with Deequ gate and freshness SLO",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=True,          # missed days are filled in automatically
    max_active_runs=1,     # one day at a time: no concurrent writes to the gold table
    default_args=default_args,
    tags=["commerce", "spark", "scala", "gold"],
    doc_md=__doc__,
) as dag:

    build_daily_metrics = BashOperator(
        task_id="build_daily_metrics",
        bash_command=spark_job("DailyMetricsJob", "{{ ds }}"),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=spark_job("QualityJob", "{{ ds }}"),
        retries=0,  # a failed data check won't fix itself; alert instead of retrying
    )

    # Freshness describes the live stream, so check it only on the newest run, not during backfills.
    latest_only = LatestOnlyOperator(task_id="latest_only")

    freshness_slo = BashOperator(
        task_id="freshness_slo",
        bash_command=spark_job("FreshnessCheckJob"),
        retries=1,
    )

    build_daily_metrics >> quality_gate
    latest_only >> freshness_slo
