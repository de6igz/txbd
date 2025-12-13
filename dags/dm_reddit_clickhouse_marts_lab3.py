from __future__ import annotations

from datetime import datetime
import requests

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.standard.operators.python import PythonOperator

CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASS = "airflow"
CH = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@clickhouse:8123/"


def _ch_execute(sql: str):
    resp = requests.post(CH, data=sql.encode("utf-8"))
    resp.raise_for_status()


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
    print(f"[dm sensor] fact_comment rows for {ds} = {cnt}")
    return cnt > 0


def build_data_marts(**context):
    """
    витрины данных на основе детального слоя fact_comment.
    Витрины:
      - dm_daily_subreddit_activity (upsert по date)
      - dm_user_activity           (полный пересчёт)
      - dm_hourly_activity         (upsert по date)
    """
    ds = context["ds"]

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dm_daily_subreddit_activity (
            date Date,
            subreddit String,
            comments_count UInt32,
            unique_users UInt32,
            avg_score Float32,
            op_comments UInt32,
            non_op_comments UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (date, subreddit)
        """,
        """
        CREATE TABLE IF NOT EXISTS dm_user_activity (
            username String,
            total_comments UInt32,
            first_comment_date Date,
            last_comment_date Date,
            active_days UInt32,
            avg_score Float32,
            op_comments UInt32,
            non_op_comments UInt32
        )
        ENGINE = MergeTree()
        ORDER BY username
        """,
        """
        CREATE TABLE IF NOT EXISTS dm_hourly_activity (
            date Date,
            hour UInt8,
            comments_count UInt32
        )
        ENGINE = MergeTree()
        ORDER BY (date, hour)
        """,
    ]

    for stmt in ddl_statements:
        _ch_execute(stmt)

    sql_delete_daily = f"""
    ALTER TABLE dm_daily_subreddit_activity
    DELETE WHERE date = toDate('{ds}')
    """
    _ch_execute(sql_delete_daily)

    sql_insert_daily = f"""
    INSERT INTO dm_daily_subreddit_activity
    SELECT
        date,
        subreddit,
        count()                       AS comments_count,
        uniqExact(username)           AS unique_users,
        avg(score)                    AS avg_score,
        sum(is_op_comment)            AS op_comments,
        count() - sum(is_op_comment)  AS non_op_comments
    FROM fact_comment
    WHERE date = toDate('{ds}')
    GROUP BY date, subreddit
    """
    _ch_execute(sql_insert_daily)

    sql_delete_hourly = f"""
    ALTER TABLE dm_hourly_activity
    DELETE WHERE date = toDate('{ds}')
    """
    _ch_execute(sql_delete_hourly)

    sql_insert_hourly = f"""
    INSERT INTO dm_hourly_activity
    SELECT
        toDate(comment_created_at)    AS date,
        toHour(comment_created_at)    AS hour,
        count()                       AS comments_count
    FROM fact_comment
--     WHERE toDate(comment_created_at) = toDate('{ds}')
    GROUP BY date, hour
    """
    _ch_execute(sql_insert_hourly)

    sql_truncate_user = "TRUNCATE TABLE dm_user_activity"
    _ch_execute(sql_truncate_user)

    sql_insert_user = """
    INSERT INTO dm_user_activity
    SELECT
        username,
        count()                                        AS total_comments,
        toDate(min(comment_created_at))               AS first_comment_date,
        toDate(max(comment_created_at))               AS last_comment_date,
        uniqExact(date)                               AS active_days,
        avg(score)                                    AS avg_score,
        sum(is_op_comment)                            AS op_comments,
        count() - sum(is_op_comment)                  AS non_op_comments
    FROM fact_comment
    WHERE username != ''
    GROUP BY username
    """
    _ch_execute(sql_insert_user)

    print("[dm] data marts successfully rebuilt for ds =", ds)


with DAG(
        dag_id="dm_reddit_clickhouse_marts",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["reddit", "clickhouse", "dm", "marts"],
) as dag:
    wait_for_fresh_fact = PythonSensor(
        task_id="wait_for_fresh_fact_for_ds",
        python_callable=fresh_fact_for_ds,
        poke_interval=30,
        timeout=60 * 20,
        mode="reschedule",
    )

    build_marts = PythonOperator(
        task_id="build_data_marts",
        python_callable=build_data_marts,
    )

    wait_for_fresh_fact >> build_marts
