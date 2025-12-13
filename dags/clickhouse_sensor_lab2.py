from __future__ import annotations

from datetime import datetime
import requests

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator

CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASS = "airflow"
CH = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@clickhouse:8123/"


def _ch_query_scalar(sql: str) -> int:
    resp = requests.get(CH, params={"query": sql})
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        return 0
    row = data[0]
    return int(next(iter(row.values())))


def fresh_fact_for_ds(**context) -> bool:
    ds = context["ds"]
    sql = (
        "SELECT count() AS cnt "
        f"FROM fact_comment WHERE date = toDate('{ds}') FORMAT JSON"
    )
    cnt = _ch_query_scalar(sql)
    print(f"[sensor] fact_comment rows for {ds} = {cnt}")
    return cnt > 0


def check_counts():

    cnt_fact = _ch_query_scalar("SELECT count() AS cnt FROM fact_comment FORMAT JSON")
    if cnt_fact == 0:
        raise ValueError("DQ FAIL: fact_comment is empty")

    # количество пользователей в факте vs dim_user
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

    # количество сабреддитов в факте vs dim_subreddit
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

    # отрицательные score
    neg_scores = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment WHERE score < 0 FORMAT JSON"
    )
    if neg_scores > 0:
        raise ValueError(f"DQ FAIL: found {neg_scores} rows with negative score")

    # некорректная длина текста
    bad_body_len = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment WHERE body_length <= 0 FORMAT JSON"
    )
    if bad_body_len > 0:
        raise ValueError(f"DQ FAIL: found {bad_body_len} rows with body_length <= 0")

    # is_op_comment не 0/1
    bad_op_flag = _ch_query_scalar(
        "SELECT count() AS cnt FROM fact_comment "
        "WHERE is_op_comment NOT IN (0,1) FORMAT JSON"
    )
    if bad_op_flag > 0:
        raise ValueError(
            f"DQ FAIL: found {bad_op_flag} rows with invalid is_op_comment flag"
        )


with DAG(
        dag_id="dq_reddit_clickhouse_with_sensor",
        start_date=datetime(2025, 11, 29),
        schedule="@daily",
        catchup=False,
        tags=["reddit", "clickhouse", "dq", "sensor"],
) as dag:

    wait_for_fresh_batch = PythonSensor(
        task_id="wait_for_fresh_fact_for_ds",
        python_callable=fresh_fact_for_ds,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
    )

    dq_counts = PythonOperator(
        task_id="dq_check_counts",
        python_callable=check_counts,
    )

    dq_anomalies = PythonOperator(
        task_id="dq_check_anomalies",
        python_callable=check_anomalies,
    )

    wait_for_fresh_batch >> dq_counts >> dq_anomalies
