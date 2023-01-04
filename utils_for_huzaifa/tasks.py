from airflow.decorators import task
from .utils import *
from .config import client_id, client_secret
from datetime import datetime


@task
def fetch_data_from_reddit(stocks):
    import praw
    import hashlib

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="praw_scraper_1.0",  # noqa: E501
    )
    for stock in stocks:
        ticker = stock["ticker"]
        company = stock["company_name"]

        query = ticker + "|" + company
        print(query)

        all_posts = reddit.subreddit("all")
        reddit_args = {"limit": 5, "time_filter": "week", "sort": "new"}
        reddit_results = []
        for post in all_posts.search(query, **reddit_args):
            comments = []
            post.comments.replace_more(limit=0)
            for comment in post.comments.list()[:2]:
                comments.append(
                    {
                        "hash": hashlib.sha256(
                            comment.body.encode("utf-8")
                        ).hexdigest(),
                        "author": comment.author.name,
                        "likes": comment.score,
                        "content": comment.body,
                        "publish_time": datetime.fromtimestamp(
                            comment.created_utc
                        ).strftime(  # noqa: E501
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }
                )
            reddit_results.append(
                {
                    "hash": hashlib.sha256(post.title.encode("utf-8")).hexdigest(),
                    "ticker": ticker,
                    "content": post.title,
                    "likes": post.score,
                    "publish_time": datetime.fromtimestamp(
                        post.created_utc
                    ).strftime(  # noqa: E501
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "image": post.url,
                    "author": post.author.name,
                    "comments": comments,
                }
            )
    return {"reddit_results": reddit_results}


@task
def insert_data_to_database(result):
    import psycopg2

    conn = psycopg2.connect(
        user="huzaifa",
        password="Django.123",
        host="127.0.0.1",
        port="5432",
        database="reddit_huzaifa",
    )
    query_post = "INSERT INTO news_news (hash, ticker, content, likes, author, publish_time, image, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW()) ON CONFLICT DO NOTHING;"  # noqa: E501
    query_comment = "INSERT INTO comments_comment (hash, news_id, author, content, likes, publish_time, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) ON CONFLICT DO NOTHING;"  # noqa: E501
    posts = result["reddit_results"]

    values_posts = [
        (
            post["hash"],
            post["ticker"],
            post["content"],
            post["likes"],
            post["author"],
            post["publish_time"],
            post["image"],
        )
        for post in posts
    ]
    cursor = conn.cursor()
    cursor.executemany(query_post, values_posts)

    for post in posts:
        comments = post["comments"]
        values_comments = [
            (
                comment["hash"],
                post["hash"],
                comment["author"],
                comment["content"],
                comment["likes"],
                comment["publish_time"],
            )
            for comment in comments  # noqa: E501
        ]
        cursor.executemany(query_comment, values_comments)
    conn.commit()
    cursor.close()
    conn.close()