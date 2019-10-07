import logging

import progressbar
import tweepy
from pymongo.errors import DuplicateKeyError


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


def search(api=None, query=None, collection=None):
    """Returns a collection of relevant Tweets matching a specified query.

    Keyword Arguments:
        api {[type]} -- [description] (default: {None})
        query {[type]} -- [description] (default: {None})
        collection {[type]} -- [description] (default: {None})

    Raises:
        tweepy.TweepError: [description]
    """

    logging.info(f"Fetching Tweet Result for {query}.")

    bar = progressbar.ProgressBar()
    cursor = tweepy.Cursor(api.search, q=query, count=20, lang='en',
                           tweet_mode='extended').items()
    for status in bar(cursor):
        tweet = status._json

        id_ = {"_id": tweet['id_str']}
        new_document = {**id_, **tweet}
        try:
            collection.insert_one(new_document)
        except DuplicateKeyError:
            logging.info(f"found duplicate key: {tweet['id_str']}")
            continue
