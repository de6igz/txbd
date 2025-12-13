CREATE TABLE IF NOT EXISTS dim_date (
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    day_of_week UInt8,
    is_weekend UInt8
)
ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS dim_subreddit (
    subreddit String,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY subreddit;

CREATE TABLE IF NOT EXISTS dim_user (
    username String,
    first_seen DateTime,
    last_seen DateTime,
    is_active UInt8
)
ENGINE = ReplacingMergeTree(last_seen)
ORDER BY username;

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
ORDER BY post_id;

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
ORDER BY comment_id;
