import os
import tweepy
import logging
import sys

from typing import List
from dataclasses import dataclass
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-30s %(message)s")
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(fmt)
logger.addHandler(sh)


@dataclass
class Tweet:
    id: int
    url: str
    created_at: str  # datetime format as string
    hashtags: str  # List[str] format as string
    tagged_persons: str  # List[str] format as string
    author: str
    language_code: str
    favorite_count: int
    retweet_count: int
    time_collected: str  # datetime format as string


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


class TwitterRunner:
    def __init__(self):
        auth = tweepy.OAuthHandler(**Settings.get_twitter_oauth_handler_kwargs())
        auth.set_access_token(**Settings.get_twitter_access_token_kwargs())
        self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def get_tweets_by_hashtag(self, hashtag: str):
        for tweet in self.api.search_tweets(q=f"#{hashtag}", tweet_mode="extended", count=100):
            logger.info("Tweet retrieved")
            yield Tweet(
                id=tweet.id,
                url=f"https://twitter.com/twitter/statuses/{tweet.id}",
                created_at=tweet.created_at.strftime("%Y-%m-%dT%H-%M-%SZ"),
                hashtags=str([hashtag["text"].lower() for hashtag in tweet.entities["hashtags"]]),
                tagged_persons=str([user["screen_name"].lower() for user in tweet.entities["user_mentions"]]),
                author=tweet.author.screen_name.lower(),
                language_code=tweet.lang,
                favorite_count=tweet.favorite_count,
                retweet_count=tweet.retweet_count,
                time_collected=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
            )


def main():
    hashtag = "opec"
    runner = TwitterRunner()
    logger.info("start retrieving tweets")
    tweets = list(runner.get_tweets_by_hashtag(hashtag))
    logger.info(f"retrieving stop. Retrieved tweets: {len(tweets)}")
    filename = f'{hashtag}_{datetime.now(timezone.utc).strftime("%Y_%m_%dT%H_%M_%SZ")}.csv'
    path = os.path.join(Settings.BASE_DIR, "output", filename)

    if not os.path.exists(os.path.join(Settings.BASE_DIR, "output")):
        os.mkdir(os.path.join(Settings.BASE_DIR, "output"))

    with open(path, "w") as file:
        header = ",".join(Tweet.__annotations__.keys())
        format_tweets = [",".join(map(str, tweet.__dict__.values())) for tweet in tweets]
        rows = "\n".join(format_tweets)
        file.write(header + "\n" + rows)


if __name__ == "__main__":
    main()

