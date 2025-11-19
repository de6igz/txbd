from __future__ import annotations

from datetime import datetime, timezone
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

HEADERS = {"User-Agent": "airflow-reddit-lab/1.0"}

POSTS_URL = "https://www.reddit.com/r/technology/top.json?t=day&limit=10"


def load_comments_to_mongo():
    resp = requests.get(POSTS_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    posts_data = resp.json()

    posts = posts_data.get("data", {}).get("children", [])
    if not posts:
        return

    # чтобы не спамить
    max_posts = 5
    selected_posts = posts[:max_posts]

    all_comments = []

    for p in selected_posts:
        pd = p.get("data", {})
        post_id = pd.get("id")
        post_title = pd.get("title")
        if not post_id:
            continue

        comments_url = f"https://www.reddit.com/comments/{post_id}.json"
        try:
            c_resp = requests.get(comments_url, headers=HEADERS, timeout=20)
            c_resp.raise_for_status()
        except Exception:
            continue

        c_json = c_resp.json()
        if len(c_json) < 2:
            continue

        comments = c_json[1].get("data", {}).get("children", [])
        # ограничить количество комментов на пост
        for c in comments[:50]:
            cd = c.get("data", {})
            if "body" not in cd:
                continue

            created_ts = cd.get("created_utc", 0) or 0
            created_dt = datetime.fromtimestamp(created_ts, tz=timezone.utc)

            all_comments.append(
                {
                    "comment_id": cd.get("id"),
                    "post_id": post_id,
                    "post_title": post_title,
                    "author": cd.get("author"),
                    "body": cd.get("body"),
                    "score": cd.get("score", 0),
                    "created_utc": created_ts,
                    "created_dt": created_dt,
                    "subreddit": cd.get("subreddit"),
                }
            )

    if not all_comments:
        return

    client = MongoClient("mongodb://mongo:27017")
    db = client.reddit
    coll = db.comments_multi  # отдельная коллекция для много-постовых комментов

    coll.insert_many(all_comments)


with DAG(
        dag_id="reddit_comments_raw_to_mongo",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["reddit", "comments", "raw"],
) as dag:
    load = PythonOperator(
        task_id="load_comments_to_mongo",
        python_callable=load_comments_to_mongo,
    )
