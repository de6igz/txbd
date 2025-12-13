from __future__ import annotations

from datetime import datetime, timezone
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

HEADERS = {"User-Agent": "airflow-reddit-lab/1.0"}
POSTS_URL = "https://www.reddit.com/r/technology/top.json?t=day&limit=10"

CLICKHOUSE_USER = "airflow"
CLICKHOUSE_PASS = "airflow"
CH = f"http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASS}@clickhouse:8123/"


def extract_reddit():
    """
    """
    resp = requests.get(POSTS_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    posts_json = resp.json()

    posts_raw = posts_json.get("data", {}).get("children", [])
    posts_raw = posts_raw[:5]  # ограничимся 5 постами, чтобы не спамить

    result = {"posts": [], "comments": []}

    for p in posts_raw:
        pd = p.get("data", {})
        post_id = pd.get("id")
        if not post_id:
            continue

        post_created_ts = pd.get("created_utc", 0) or 0
        post_created_dt = datetime.fromtimestamp(post_created_ts, tz=timezone.utc)

        result["posts"].append(
            {
                "post_id": post_id,
                "title": pd.get("title"),
                "subreddit": pd.get("subreddit"),
                "author": pd.get("author"),
                "created_at": post_created_dt,
                "url": pd.get("url"),
                "score": pd.get("score", 0),
                "num_comments": pd.get("num_comments", 0),
            }
        )

        comments_url = f"https://www.reddit.com/comments/{post_id}.json"
        c_resp = requests.get(comments_url, headers=HEADERS, timeout=20)
        if not c_resp.ok:
            continue
        c_json = c_resp.json()
        if len(c_json) < 2:
            continue

        comments = c_json[1].get("data", {}).get("children", [])
        for c in comments[:100]:
            cd = c.get("data", {})
            if "body" not in cd:
                continue

            c_ts = cd.get("created_utc", 0) or 0
            c_dt = datetime.fromtimestamp(c_ts, tz=timezone.utc)
            c_date = c_dt.date()

            result["comments"].append(
                {
                    "comment_id": cd.get("id"),
                    "post_id": post_id,
                    "username": cd.get("author"),
                    "subreddit": cd.get("subreddit"),
                    "created_at": c_dt,
                    "date": c_date,
                    "score": cd.get("score", 0),
                    "body_length": len(cd.get("body", "")),
                    "is_op_comment": 1 if cd.get("is_submitter") else 0,
                }
            )

    return result


def load_clickhouse(**context):

    data = context["ti"].xcom_pull(task_ids="extract_reddit")
    if not data:
        raise ValueError("No data received from extract_reddit")

    posts = data.get("posts", [])
    comments = data.get("comments", [])


    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date Date,
            year UInt16,
            month UInt8,
            day UInt8,
            day_of_week UInt8,
            is_weekend UInt8
        )
        ENGINE = MergeTree()
        ORDER BY date
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_subreddit (
            subreddit String,
            created_at DateTime
        )
        ENGINE = MergeTree()
        ORDER BY subreddit
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_user (
            username String,
            total_comments UInt32,
            first_comment_at DateTime,
            last_comment_at DateTime,
            effective_from DateTime,
            effective_to   DateTime,
            is_current UInt8
        )
        ENGINE = MergeTree()
        ORDER BY (username, effective_from)
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_post (
            post_id String,
            title String,
            subreddit String,
            author String,
            created_at DateTime,
            url String,
            score Int32,
            num_comments Int32
        )
        ENGINE = MergeTree()
        ORDER BY post_id
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_comment (
            comment_id String,
            post_id String,
            username String,
            subreddit String,
            comment_created_at DateTime,
            date Date,
            score Int32,
            body_length UInt32,
            is_op_comment UInt8
        )
        ENGINE = MergeTree()
        ORDER BY comment_id
        """,
    ]

    for stmt in ddl_statements:
        resp = requests.post(CH, data=stmt.encode("utf-8"))
        resp.raise_for_status()


    dates = {}
    for c in comments:
        d = c["date"]
        if d not in dates:
            dates[d] = {
                "date": d,
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "day_of_week": d.isoweekday(),
                "is_weekend": 1 if d.isoweekday() >= 6 else 0,
            }


    resp = requests.get(
        CH,
        params={"query": "SELECT DISTINCT subreddit FROM dim_subreddit FORMAT JSON"},
    )
    resp.raise_for_status()
    existing_rows = resp.json().get("data", [])
    existing = {row["subreddit"] for row in existing_rows}

    subreddits = {}
    now = datetime.now(timezone.utc)
    for p in posts:
        s = p.get("subreddit")
        if not s:
            continue

        if s not in existing and s not in subreddits:
            subreddits[s] = {"subreddit": s, "created_at": now}


    users = {}
    for c in comments:
        u = c.get("username") or ""
        if not u:
            continue
        created_at = c["created_at"]
        cur = users.get(u)
        if not cur:
            users[u] = {
                "username": u,
                "first_comment_at": created_at,
                "last_comment_at": created_at,
                "total_comments": 1,
            }
        else:
            if created_at < cur["first_comment_at"]:
                cur["first_comment_at"] = created_at
            if created_at > cur["last_comment_at"]:
                cur["last_comment_at"] = created_at
            cur["total_comments"] += 1


    def insert_tsv(table: str, rows: list[str]):
        if not rows:
            return
        sql = f"INSERT INTO {table} FORMAT TabSeparated\n" + "\n".join(rows)
        r = requests.post(CH, data=sql.encode("utf-8"))
        r.raise_for_status()

    # dim_date
    date_rows = [
        "\t".join(
            [
                d["date"].strftime("%Y-%m-%d"),
                str(d["year"]),
                str(d["month"]),
                str(d["day"]),
                str(d["day_of_week"]),
                str(d["is_weekend"]),
            ]
        )
        for d in dates.values()
    ]
    insert_tsv("dim_date", date_rows)


    sub_rows = [
        "\t".join(
            [
                s["subreddit"],
                s["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
            ]
        )
        for s in subreddits.values()
    ]
    insert_tsv("dim_subreddit", sub_rows)


    post_rows = [
        "\t".join(
            [
                p["post_id"],
                (p["title"] or "").replace("\t", " ").replace("\n", " "),
                p.get("subreddit") or "",
                p.get("author") or "",
                p["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                p.get("url") or "",
                str(p.get("score", 0)),
                str(p.get("num_comments", 0)),
            ]
        )
        for p in posts
    ]
    insert_tsv("dim_post", post_rows)


    fact_rows = [
        "\t".join(
            [
                c["comment_id"],
                c["post_id"],
                c.get("username") or "",
                c.get("subreddit") or "",
                c["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                c["date"].strftime("%Y-%m-%d"),
                str(c.get("score", 0)),
                str(c["body_length"]),
                str(c["is_op_comment"]),
            ]
        )
        for c in comments
    ]
    insert_tsv("fact_comment", fact_rows)


    run_ts = datetime.now(timezone.utc)
    run_ts_str = run_ts.strftime("%Y-%m-%d %H:%M:%S")

    for u in users.values():
        username = u["username"]
        username_escaped = username.replace("'", "\\'")
        total_comments = u["total_comments"]
        first_comment_at = u["first_comment_at"]
        last_comment_at = u["last_comment_at"]

        first_str = first_comment_at.strftime("%Y-%m-%d %H:%M:%S")
        last_str = last_comment_at.strftime("%Y-%m-%d %H:%M:%S")


        select_sql = f"""
        SELECT
            username,
            total_comments,
            first_comment_at,
            last_comment_at,
            effective_from,
            effective_to,
            is_current
        FROM dim_user
        WHERE username = '{username_escaped}' AND is_current = 1
        FORMAT JSON
        """

        resp = requests.get(CH, params={"query": select_sql})
        resp.raise_for_status()
        data_json = resp.json()
        rows = data_json.get("data", [])

        if not rows:
            # нет пользователя - первая версия
            insert_sql = f"""
            INSERT INTO dim_user
            FORMAT Values
            ('{username_escaped}', {total_comments}, '{first_str}', '{last_str}',
             '{run_ts_str}', '9999-12-31 00:00:00', 1)
            """
            r_ins = requests.post(CH, data=insert_sql.encode("utf-8"))
            r_ins.raise_for_status()
            continue

        cur = rows[0]
        cur_total = int(cur["total_comments"])

        # если total_comments не поменялся - версию не создаём
        if cur_total == total_comments:
            continue

        # 2. закрываем старую версию
        alter_sql = f"""
        ALTER TABLE dim_user
        UPDATE effective_to = toDateTime('{run_ts_str}'), is_current = 0
        WHERE username = '{username_escaped}' AND is_current = 1
        """
        r_alt = requests.post(CH, data=alter_sql.encode("utf-8"))
        r_alt.raise_for_status()

        # 3. вставляем новую версию
        insert_sql = f"""
        INSERT INTO dim_user
        FORMAT Values
        ('{username_escaped}', {total_comments}, '{first_str}', '{last_str}',
         '{run_ts_str}', '9999-12-31 00:00:00', 1)
        """
        r_ins = requests.post(CH, data=insert_sql.encode("utf-8"))
        r_ins.raise_for_status()


with DAG(
    dag_id="dds_reddit_to_clickhouse_star",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["reddit", "dds", "clickhouse", "star", "scd2"],
) as dag:
    extract = PythonOperator(
        task_id="extract_reddit",
        python_callable=extract_reddit,
    )

    load = PythonOperator(
        task_id="load_clickhouse",
        python_callable=load_clickhouse,
    )

    extract >> load
