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


@click.command()
@click.argument('query', nargs=-1)
@click.argument('topic')
def main(query, topic):
    """ Downloads Users' Tweets
    """
    logger = logging.getLogger(__name__)

    url = "http://example.com/"
    timeout = 5

    client = None
    db = None

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
    except (ValueError, FileNotFoundError, FileExistsError) as error:
        logger.error(error)
    except requests.HTTPError as e:
        logger.error("Checking internet connection failed, "
                     f"status code {e.response.status_code}")
    except requests.ConnectionError:
        logger.error("No internet connection available.")
    else:
        db = client[db_name]
        topic_collection = db[topic]

        query = ' '.join(query)
        logger.info(f'scrapping twitter for "{query}"')
        tweets = scrap(query)

        logger.info('processing retweets...')
        bar = progressbar.ProgressBar(max_value=len(tweets))
        for tweet in bar(tweets):
            if not tweet.is_retweet:
                retweets = get_retweets(api=api, tweet_id=tweet.tweet_id)
                if retweets:
                    for retweet in retweets:
                        try:
                            save_tweet_to_db(tweet=retweet._json,
                                             collection=topic_collection)
                        except DuplicateKeyError:
                            continue
            else:
                tweet_ = get_tweet(api=api, tweet_id=tweet.tweet_id)
                try:
                    save_tweet_to_db(tweet=tweet_._json,
                                     collection=topic_collection)
                except DuplicateKeyError:
                    continue

    finally:
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
