# -*- coding: utf-8 -*-
import logging
import os

import click
import progressbar
import pymongo
import requests
from dotenv import find_dotenv, load_dotenv
from pymongo.errors import DuplicateKeyError

from src.twitter import auth, get_retweets, get_tweet, scrap
from src.utils import save_tweet_to_db


def process_retweets(api=None, tweets=[], collection=None):
    bar = progressbar.ProgressBar(max_len=len(tweets))
    for tweet in bar(tweets):
        if not tweet.is_retweet:
            retweets = get_retweets(api=api,
                                    tweet_id=tweet.tweet_id)
            if retweets:
                for retweet in retweets:
                    try:
                        save_tweet_to_db(tweet=retweet._json,
                                         collection=collection)
                    except DuplicateKeyError:
                        continue
        else:
            tweet_ = get_tweet(api=api, tweet_id=tweet.tweet_id)
            try:
                save_tweet_to_db(tweet=tweet_._json, collection=collection)
            except DuplicateKeyError:
                continue


@click.command()
@click.argument('topic')
@click.argument('query', nargs=-1)
@click.option('--limit', type=click.INT, default=-1)
def main(topic, query, limit):
    """ Downloads Users' Tweets
    """
    logger = logging.getLogger(__name__)

    url = "http://example.com/"
    timeout = 5

    client = None
    db = None

    tweets = []
    try:
        # test internet conncetivity is active
        req = requests.get(url, timeout=timeout)
        req.raise_for_status()

        # prepare credentials for accessing twitter API
        consumer_key = os.environ.get('CONSUMER_KEY')
        consumer_secret = os.environ.get('CONSUMER_SECRET')
        access_token = os.environ.get('ACCESS_TOKEN')
        access_token_secret = os.environ.get('ACCESS_TOKEN_SECRET')

        api = auth(consumer_key=consumer_key,
                   consumer_secret=consumer_secret,
                   access_token=access_token,
                   access_token_secret=access_token_secret)

        db_name = os.environ.get('DB_NAME')
        client = pymongo.MongoClient(host='localhost', port=27017,
                                     appname=__file__)

        db = client[db_name]
        topic_collection = db[topic]
        query = ' '.join(query)

        logger.info(f'scrapping tweets for "{query}"')
        tweets = scrap(query=query, limit=limit)
    except requests.exceptions.HTTPError as e:
        logger.error("Checking internet connection failed, "
                     f"status code {e.response.status_code}")
    except requests.exceptions.ConnectionError:
        logger.error("No internet connection available.")
    finally:
        if tweets:
            logger.info(f'processing retweets')
            process_retweets(api=api, tweets=tweets,
                             collection=topic_collection)

        if client is not None:
            logger.info('ending all server sessions')
            client.close()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
