from __future__ import annotations

from datetime import datetime, timezone

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

HEADERS = {"User-Agent": "airflow-reddit-lab/1.0"}
REDDIT_URL = "https://www.reddit.com/r/technology/top.json?t=day&limit=50"

CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASS = "airflow"
CH = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@clickhouse:8123/"


def fetch_posts():
    resp = requests.get(REDDIT_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def load_to_clickhouse(**context):
    data = context["ti"].xcom_pull(task_ids="fetch_posts")
    posts = data.get("data", {}).get("children", [])

    create_sql = """
    CREATE TABLE IF NOT EXISTS reddit_posts_analytics (
        id String,
        title String,
        author String,
        created_utc DateTime,
        score Int32,
        num_comments Int32
    )
    ENGINE = MergeTree()
    ORDER BY id
    """

    requests.post(CH, data=create_sql.encode("utf-8")).raise_for_status()

    rows = []
    for p in posts:
        d = p.get("data", {})
        dt = datetime.fromtimestamp(d.get("created_utc", 0), tz=timezone.utc)
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")

        rows.append("\t".join([
            d.get("id", ""),
            (d.get("title") or "").replace("\t", " ").replace("\n", " "),
            d.get("author", ""),
            dt_str,
            str(d.get("score", 0)),
            str(d.get("num_comments", 0)),
        ]))

    if not rows:
        return

    insert_sql = "INSERT INTO reddit_posts_analytics FORMAT TabSeparated\n" + "\n".join(rows)
    requests.post(CH, data=insert_sql.encode("utf-8")).raise_for_status()


with DAG(
    dag_id="reddit_posts_analytics_to_clickhouse",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["reddit", "analytics"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_posts",
        python_callable=fetch_posts,
    )
    to_ch = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
    )

    fetch >> to_ch
