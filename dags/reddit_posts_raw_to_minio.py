from __future__ import annotations
import io
import json
from datetime import datetime, timezone

import requests
from minio import Minio
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

REDDIT_URL = "https://www.reddit.com/r/technology/top.json?t=day&limit=50"
HEADERS = {"User-Agent": "airflow-reddit-lab/1.0"}


def fetch_posts():
    resp = requests.get(REDDIT_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def write_to_minio(**context):
    data = context["ti"].xcom_pull(task_ids="fetch_posts")
    if not data:
        raise ValueError("No data received from fetch_posts")

    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False,
    )

    bucket = "reddit-posts"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    filename = f"posts_{now}.json"

    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=filename,
        data=io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )


with DAG(
        dag_id="reddit_posts_raw_to_minio",
        start_date=datetime(2024, 1, 1),
        schedule="@hourly",
        catchup=False,
        tags=["reddit", "posts", "raw"],
) as dag:
    fetch = PythonOperator(
        task_id="fetch_posts",
        python_callable=fetch_posts,
    )

    to_minio = PythonOperator(
        task_id="write_to_minio",
        python_callable=write_to_minio,
    )

    fetch >> to_minio
