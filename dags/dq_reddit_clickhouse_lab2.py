from __future__ import annotations

from datetime import datetime
import time
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASS = "airflow"
CH = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@clickhouse:8123/"


def _ch_query_scalar(sql: str) -> int:
    resp = requests.get(CH, params={"query": sql})
    resp.raise_for_status()
    data = resp.json()["data"]
    if not data:
        return 0
    row = data[0]
    return int(next(iter(row.values())))


def wait_for_dds():

    timeout_sec = 600
    interval_sec = 10
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        cnt_fact = _ch_query_scalar(
            "SELECT count() AS cnt FROM fact_comment FORMAT JSON"
        )
        if cnt_fact > 0:
            return
        time.sleep(interval_sec)

    raise TimeoutError(
        "DQ WAIT FAIL: fact_comment is still empty, DDS process probably has not finished"
    )


def check_counts():
    """
    Data Quality сравнение объёмов данных.
    - факт не пустой
    - кол-во разных пользователей в fact_comment == кол-ву current-пользователей в dim_user
    - кол-во разных сабреддитов в fact_comment == кол-ву строк в dim_subreddit
    """
    cnt_fact = _ch_query_scalar("SELECT count() AS cnt FROM fact_comment FORMAT JSON")
    if cnt_fact == 0:
        raise ValueError("DQ FAIL: fact_comment is empty")

    cnt_users_fact = _ch_query_scalar(
        "SELECT count(DISTINCT username) AS cnt "
        "FROM fact_comment WHERE username != '' FORMAT JSON"
    )
    cnt_users_dim = _ch_query_scalar(
        "SELECT count() AS cnt FROM dim_user WHERE is_current = 1 FORMAT JSON"
    )
    if cnt_users_fact != cnt_users_dim:
        raise ValueError(
            f"DQ FAIL: users count mismatch (fact={cnt_users_fact}, dim={cnt_users_dim})"
        )

    cnt_sub_fact = _ch_query_scalar(
        "SELECT count(DISTINCT subreddit) AS cnt FROM fact_comment FORMAT JSON"
    )
    cnt_sub_dim = _ch_query_scalar(
        "SELECT count() AS cnt FROM dim_subreddit FORMAT JSON"
    )
    if cnt_sub_fact != cnt_sub_dim:
        raise ValueError(
            f"DQ FAIL: subreddit count mismatch (fact={cnt_sub_fact}, dim={cnt_sub_dim})"
        )


def check_anomalies():
    """
    Data Quality: проверка аномальных значений.
    - score >= 0
    - body_length > 0
    - is_op_comment ∈ {0,1}
    """
    neg_scores = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment WHERE score < 0 FORMAT JSON"
    )
    if neg_scores > 0:
        raise ValueError(f"DQ FAIL: found {neg_scores} rows with negative score")

    bad_body_len = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment WHERE body_length <= 0 FORMAT JSON"
    )
    if bad_body_len > 0:
        raise ValueError(f"DQ FAIL: found {bad_body_len} rows with body_length <= 0")

    bad_op_flag = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment "
        "WHERE is_op_comment NOT IN (0,1) FORMAT JSON"
    )
    if bad_op_flag > 0:
        raise ValueError(
            f"DQ FAIL: found {bad_op_flag} rows with invalid is_op_comment flag"
        )


with DAG(
    dag_id="dq_reddit_clickhouse_lab2",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["reddit", "clickhouse", "dq"],
) as dag:
    wait = PythonOperator(
        task_id="wait_for_dds",
        python_callable=wait_for_dds,
    )

    dq_counts = PythonOperator(
        task_id="dq_check_counts",
        python_callable=check_counts,
    )

    dq_anomalies = PythonOperator(
        task_id="dq_check_anomalies",
        python_callable=check_anomalies,
    )

    wait >> dq_counts >> dq_anomalies
