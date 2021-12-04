import os
import tweepy
import logging
import sys
import psycopg2
import csv
import functools
import operator

from time import sleep
from psycopg2.extras import execute_values
from typing import Optional, List
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-30s %(message)s")
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(fmt)
logger.addHandler(sh)


@dataclass
class Tweet:
    status_id: int
    text: str
    url: str
    date_label: datetime
    hashtags: List[str]
    tagged_persons: List[str]
    author: str
    language_code: str
    favorite_count: int
    retweet_count: int
    time_collected: datetime

    def parse_to_file_format(self):
        return {
            "status_id": self.status_id,
            "text": f'"{self.text}"',
            "url": self.url,
            "date_label": self.date_label.strftime("%Y-%m-%d"),
            "hashtags": str([hashtag for hashtag in self.hashtags]),
            "tagged_persons": str([user.lower() for user in self.tagged_persons]),
            "author": self.author,
            "language_code": self.language_code,
            "favorite_count": self.favorite_count,
            "retweet_count": self.retweet_count,
            "time_collected": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        }


@dataclass
class Job:
    q: str
    type: str
    execution_intervall: int
    last_time_executed: datetime
    next_execution_time: datetime


class Settings:
    BASE_DIR = os.path.dirname(os.path.realpath(__file__))

    @staticmethod
    def get_twitter_oauth_handler_kwargs():
        return {
            "consumer_key": os.getenv("TWITTER_API_KEY"),
            "consumer_secret": os.getenv("TWITTER_API_KEY_SECRET")
        }

    @staticmethod
    def get_twitter_access_token_kwargs():
        return {
            "key": os.getenv("TWITTER_ACCESS_TOKEN"),
            "secret": os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
        }

    @staticmethod
    def get_db_kwargs() -> dict:
        return {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME")
        }


class DB:
    DB: Optional["DB"] = None

    @classmethod
    def get_instance(cls) -> "DB":
        if not cls.DB:
            cls.DB = cls()
        return cls.DB

    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**Settings.get_db_kwargs())
        self.cur = self.conn.cursor()
        self.conn.autocommit = True


class ORM:
    db: DB = DB.get_instance().DB

    @classmethod
    def get_jobs_to_execute(cls):
        logger.info("Start Fetching Jobs")
        sql = f"""
            SELECT {",".join(Job.__annotations__.keys())} FROM job
            WHERE next_execution_time < %s
        """
        cls.db.cur.execute(sql, (datetime.now(timezone.utc),))
        jobs = [Job(*job) for job in cls.db.cur.fetchall()]
        logger.info(f"Retrieved jobs to execute: {len(jobs)}")
        return jobs

    @classmethod
    def update_job(cls, job: Job):
        sql = """
            UPDATE job
            SET last_time_executed = %s, next_execution_time = %s
            WHERE q = %s AND type = %s
        """
        now = datetime.now(timezone.utc)
        next_execution = now + timedelta(minutes=job.execution_intervall)
        cls.db.cur.execute(sql, (datetime.now(timezone.utc), next_execution, job.q, job.type))

    @classmethod
    def insert_sync(cls, job: Job):
        sql = "INSERT INTO sync (q, type) VALUES (%s, %s)"
        cls.db.cur.execute(sql, (job.q, job.type, ))

    @classmethod
    def write_tweets_to_postgres(cls, tweets: List[Tweet]):
        logger.info(f"Start Loading {len(tweets)} Tweet to Postgres")
        sql = f"""
                INSERT INTO tweet ({','.join(Tweet.__annotations__.keys())})  VALUES %s
                ON CONFLICT (status_id) DO NOTHING
            """
        execute_values(cls.db.cur, sql, [tuple(tweet.__dict__.values()) for tweet in tweets])
        logger.info(f"Loading tweets to postgres done.")

    @classmethod
    def write_users_to_postgres(cls, users: List[str]):
        logger.info(f"Start Loading {len(users)} Users to Postgres")
        users = [(user, datetime.now()) for user in users]
        sql = f"""
                INSERT INTO twitter_user (username, timestamp) VALUES %s
                ON CONFLICT (username) DO NOTHING
            """
        execute_values(cls.db.cur, sql, users)

    @classmethod
    def write_hashtags_to_postgres(cls, hashtags: List[str]):
        logger.info(f"Start Loading {len(hashtags)} Hashtags to Postgres")
        hashtags = [(hashtag, datetime.now()) for hashtag in hashtags]
        sql = f"""
                INSERT INTO hashtag (hashtag, timestamp)  VALUES %s
                ON CONFLICT (hashtag) DO NOTHING
            """
        execute_values(cls.db.cur, sql, hashtags, )


class TwitterRunner:
    def __init__(self):
        auth = tweepy.OAuthHandler(**Settings.get_twitter_oauth_handler_kwargs())
        auth.set_access_token(**Settings.get_twitter_access_token_kwargs())
        self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def get_tweets_by_hashtag(self, hashtag: str) -> List[Tweet]:
        logger.info(f"start retrieving tweets for hashtag {hashtag}")
        tweets = []
        for tweet in self.api.search_tweets(q=f"#{hashtag}", tweet_mode="extended", count=100):
            if tweet.full_text.startswith("RT"):
                # Replace tweet with original
                # tweet.full_text = tweet.retweeted_status.full_text

                # continue - no retweets wanted
                continue
            if len(tweet.entities["hashtags"]) > 7:
                continue
            tweets.append(
                Tweet(
                    status_id=tweet.id,
                    url=f"https://twitter.com/twitter/statuses/{tweet.id}",
                    text=tweet.full_text,
                    date_label=tweet.created_at,
                    hashtags=[hashtag["text"].lower() for hashtag in tweet.entities["hashtags"]],
                    tagged_persons=[user["screen_name"].lower() for user in tweet.entities["user_mentions"]],
                    author=tweet.author.screen_name.lower(),
                    language_code=tweet.lang,
                    favorite_count=tweet.favorite_count,
                    retweet_count=tweet.retweet_count,
                    time_collected=datetime.now(timezone.utc)
                )
            )
        logger.info(f"Fetched {len(tweets)} Tweets.")
        return tweets


def write_tweets_to_file(hashtag: str, tweets: List[Tweet]):
    filename = f'{hashtag}_{datetime.now(timezone.utc).strftime("%Y_%m_%dT%H_%M_%SZ")}.csv'
    path = os.path.join(Settings.BASE_DIR, "output", filename)

    with open(path, "w") as file:
        writer = csv.writer(file, delimiter=',', quotechar='"')
        header = list(Tweet.__annotations__.keys())
        writer.writerow(header)
        for tweet in tweets:
            writer.writerow(tweet.parse_to_file_format().values())


def get_users_from_tweets(tweets: List[Tweet]) -> List[str]:
    author_list = [entity.author for entity in tweets]

    linked_users_list = [entity.tagged_persons for entity in tweets if entity.tagged_persons != []]

    # flatten list
    linked_users_list = list(functools.reduce(operator.concat, linked_users_list))

    # create unique lists of users
    user_list = list(set(author_list + linked_users_list))

    return user_list


def get_hashtags_from_tweets(tweets: List[Tweet]) -> List[str]:
    hashtag_list = [entity.hashtags for entity in tweets if entity.hashtags != []]

    # flatten list
    hashtag_list = list(functools.reduce(operator.concat, hashtag_list))

    # create unique lists of users
    hashtag_list = list(set(hashtag_list))

    return hashtag_list


def run_hashtag_job(job: Job):
    tweet_runner = TwitterRunner()
    tweets = list(tweet_runner.get_tweets_by_hashtag(job.q))
    users = get_users_from_tweets(tweets)
    hashtags = get_hashtags_from_tweets(tweets)
    ORM.write_users_to_postgres(users)
    ORM.write_hashtags_to_postgres(hashtags)
    ORM.write_tweets_to_postgres(tweets)
    write_tweets_to_file(job.q, tweets)
    ORM.insert_sync(job)
    ORM.update_job(job)


def run_bubble_job(job: Job):
    tweet_runner = TwitterRunner()
    hashtags = job.q.split(",")
    for hashtag in hashtags:
        logger.info(f"Run Hashtagjob {hashtag} for bubble.")
        tweets = list(tweet_runner.get_tweets_by_hashtag(hashtag))
        users = get_users_from_tweets(tweets)
        fetched_hashtags = get_hashtags_from_tweets(tweets)
        ORM.write_users_to_postgres(users)
        ORM.write_hashtags_to_postgres(fetched_hashtags)
        ORM.write_tweets_to_postgres(tweets)
        write_tweets_to_file(hashtag, tweets)

    ORM.insert_sync(job)
    ORM.update_job(job)


def runner():
    jobs = ORM.get_jobs_to_execute()
    for job in jobs:
        if job.type == "hashtag":
            run_hashtag_job(job)
        elif job.type == "bubble":
            run_bubble_job(job)
        else:
            continue


if __name__ == "__main__":
    if not os.path.exists(os.path.join(Settings.BASE_DIR, "output")):
        os.mkdir(os.path.join(Settings.BASE_DIR, "output"))

    while True:
        try:
            runner()
        except Exception as e:
            logger.error(str(e))
        logger.info("Sleep 15 Seconds.")
        sleep(15)
