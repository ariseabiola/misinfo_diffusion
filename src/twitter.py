import logging

import progressbar
import tweepy
from pymongo.errors import DuplicateKeyError
from twitterscraper import query_tweets


def auth(consumer_key, consumer_secret, access_token, access_token_secret):
    auth_ = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_.set_access_token(access_token, access_token_secret)
    return tweepy.API(
        auth_,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True,
        retry_count=3,
        retry_delay=5,
        retry_errors=set([401, 404, 500, 503]))


def scrap(query=None, limit=-1):
    if not limit:
        tweets = query_tweets(query)
    else:
        tweets = query_tweets(query, limit)
    return tweets


def get_retweets(api=None, tweet_id=None, count=None):
    if count is None:
        retweets = api.retweets(tweet_id)
    else:
        retweets = api.retweets(tweet_id, count)

    return retweets


def get_tweet(api=None, tweet_id=None):
    status = api.get_status(tweet_id)
    return status._json


def search(api=None, query=None, db=None, n_tweets=1000):
    """Returns a collection of relevant Tweets matching a specified query.

    Keyword Arguments:
        api {[type]} -- [description] (default: {None})
        query {[type]} -- [description] (default: {None})
        collection {[type]} -- [description] (default: {None})

    Raises:
        tweepy.TweepError: [description]
    """
    logging.info(f"Fetching Tweet Result for {query}.")

    topic_collection = query.split()[0]
    collection = db[topic_collection]

    bar = progressbar.ProgressBar()
    cursor = tweepy.Cursor(api.search, q=query, count=100, lang='en',
                           tweet_mode='extended').items(n_tweets)
    for status in bar(cursor):
        tweet = status._json

        id_ = {"_id": tweet['id_str']}
        new_document = {**id_, **tweet}
        try:
            collection.insert_one(new_document)
        except DuplicateKeyError:
            logging.info(f"found duplicate key: {tweet['id_str']}")
            continue
