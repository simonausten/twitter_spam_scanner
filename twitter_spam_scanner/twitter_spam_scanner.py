import re, random, pendulum, sys, os
import pprint

pp = pprint.PrettyPrinter(indent=4).pprint

import tweepy

bearer_token = "AAAAAAAAAAAAAAAAAAAAALOViQEAAAAAHobY6LL5SnWB8omguxJhdIppwZs%3D9yqZ4bgfm3lBxu1GHaDKpleKHjDByw1EFcdXO476n1Myv66Bhn"
twitter_client = tweepy.Client(bearer_token)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "twitter-spam-filter-c955582f2872.json"
from google.cloud import datastore

datastore_client = datastore.Client()
key = datastore_client.key("Org")

from google.cloud import pubsub

users = [
    {"username": "POTUS", "name": "Joe Biden"},
]

new_tweet_scan_start_offset = pendulum.duration(hours=12)
new_reply_scan_start_offset = pendulum.duration(minutes=30)


def tweet_exists_in_db(tweet):

    query = datastore_client.query(kind="tweet")
    query.add_filter("id", "=", tweet.id)
    query = datastore_client.query()
    results = list(query.fetch())

    return True if len(results) else False


def upsert_tweet(kind, tweet, params, skip_if_exists=False):

    if skip_if_exists and tweet_exists_in_db(tweet):
        return

    complete_key = datastore_client.key(kind, tweet.id)
    entity = datastore.Entity(key=complete_key)
    entity.update(
        {
            "id": tweet.id,
            "author_id": tweet.author_id,
            "created_at": tweet.created_at,
            **params,
        }
    )
    # TODO: this should be batched using client.put_multi([*entities])
    datastore_client.put(entity)


def scan_for_new_tweets():

    for user in users:
        user = twitter_client.get_user(username=user["username"])
        tweets = twitter_client.get_users_tweets(
            user.data.id,
            start_time=pendulum.now("UTC") - new_tweet_scan_start_offset,
            tweet_fields=["context_annotations", "created_at"],
            expansions="author_id",
        )
        for tweet in tweets.data:
            upsert_tweet(
                "Tweet", tweet, {"checked_for_replies": False}, skip_if_exists=False
            )


def get_not_checked_for_replies():

    query = datastore_client.query(kind="Tweet")
    # query.add_filter("checked_for_replies", "=", False)
    # query.add_filter("created", "<", pendulum.now("UTC") + new_reply_scan_start_offset)
    # query = datastore_client.query()
    return list(query.fetch())


def scan_for_new_replies():

    not_checked = get_not_checked_for_replies()

    for tweet in not_checked:

        tweet_id, author_id = tweet.id, tweet["author_id"]

        for reply in twitter_client.search_recent_tweets(
            f"to: {author_id}", max_results=100, expansions="referenced_tweets.id"
        ):
            # Using getattr to avoid AttributeError when not found
            # then filtering out empty lists
            pp(getattr(reply, "referenced_tweets", []))
            tweets = filter(lambda e: e != [], getattr(reply, "referenced_tweets", []))

            pp(list(tweets))
            for referenced_tweet in referenced_tweets:
                if referenced_tweet.id == tweet_id:
                    pp(tweet_id, referenced_tweet.id)
                    upsert_tweet("Reply", referenced_tweet, {"spam": -1})


def classify_replies():
    pp("Classifying!")
    return True


if __name__ == "__main__":
    scan_for_new_tweets()
    scan_for_new_replies()
    classify_replies()
