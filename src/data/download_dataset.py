# -*- coding: utf-8 -*-
import logging
import os
from collections import deque

import click
import progressbar
import pymongo
import requests
from dotenv import find_dotenv, load_dotenv
from pymongo.errors import DuplicateKeyError
from twitterscraper.tweet import Tweet
from tweepy.error import TweepError

from src.twitter import auth, get_retweets, scrap
from src.utils import save_tweet_to_db


def fetch_retweets(api=None, tweet_id=None, retweet_collection=None):
    try:
        retweets = get_retweets(api=api, tweet_id=tweet_id)
        if retweets is not None:
            for retweet in retweets:
                try:
                    save_tweet_to_db(tweet=retweet._json,
                                     collection=retweet_collection)
                except DuplicateKeyError:
                    continue
    except TweepError as e:
        logging.info(e)
        raise


def fresh_scrap(query=None, tweets=None, limit=None):
    if tweets is None or not isinstance(tweets, deque):
        raise TypeError('Expected Type "collections.deque", '
                        f'got {type(tweets)}.')

    tweets_ = scrap(query=query, limit=limit)
    tweets.extend(tweets_)


def resume_scrap(query=None, tweets=None, tweet_collection=None):
    if tweets is None or not isinstance(tweets, deque):
        raise TypeError('Expected Type "collections.deque", '
                        f'got {type(tweets)}.')

    resume_query = {'is_processed': False}
    tweets_ = tweet_collection.find(resume_query, {'_id': 0})
    tweets_ = [Tweet(username=tweet['username'],
                     fullname=tweet['fullname'],
                     user_id=tweet['user_id'],
                     tweet_id=tweet['tweet_id'],
                     tweet_url=tweet['tweet_url'],
                     timestamp=tweet['timestamp'],
                     timestamp_epochs=tweet['timestamp_epochs'],
                     replies=tweet['replies'],
                     retweets=tweet['retweets'],
                     likes=tweet['likes'],
                     is_retweet=tweet['is_retweet'],
                     retweeter_username=tweet['retweeter_username'],
                     retweeter_userid=tweet['retweeter_userid'],
                     retweet_id=tweet['retweet_id'],
                     text=tweet['text'],
                     html=tweet['html']) for tweet in tweets_]

    logging.info(f'{len(tweets_)} unprocessed tweets have been queued for '
                 'processing.')
    tweets.extend(tweets_)


def to_fetch_retweets(tweet_id, tweet_collection):
    id_query = {'_id': tweet_id}
    result = tweet_collection.find_one(id_query)
    return not result or not result['is_processed']


def process_retweets(api=None, tweets=None, tweet_collection=None,
                     retweet_collection=None):
    bar = progressbar.ProgressBar(max_value=len(tweets))
    bar.start()
    while tweets:
        tweet = tweets[-1]
        if not tweet.is_retweet:
            if to_fetch_retweets(tweet_id=tweet.tweet_id,
                                 tweet_collection=tweet_collection):
                try:
                    fetch_retweets(api=api, tweet_id=tweet.tweet_id,
                                   retweet_collection=retweet_collection)
                    new_document = {'_id': tweet.tweet_id,
                                    'is_processed': True,
                                    'username': tweet.username,
                                    'fullname': tweet.fullname,
                                    'user_id': tweet.user_id,
                                    'tweet_id': tweet.tweet_id,
                                    'tweet_url': tweet.tweet_url,
                                    'timestamp': tweet.timestamp,
                                    'timestamp_epochs': tweet.timestamp_epochs,
                                    'replies': tweet.replies,
                                    'retweets': tweet.retweets,
                                    'likes': tweet.likes,
                                    'is_retweet': tweet.is_retweet,
                                    'retweeter_username': tweet.retweeter_username,
                                    'retweeter_userid': tweet.retweeter_userid,
                                    'retweet_id': tweet.retweet_id,
                                    'text': tweet.text,
                                    'html': tweet.html,
                                    }
                    tweet_collection.insert_one(new_document)
                except DuplicateKeyError:
                    tweet_query = {'_id': tweet.tweet_id}
                    new_values = {"$set": {"is_processed": True}}
                    tweet_collection.update_one(tweet_query, new_values)
                except TweepError:
                    _ = tweets.pop()
                    bar += 1
                    continue
        _ = tweets.pop()
        bar += 1
    bar.finish()


def process_left_over_tweets(tweets=None, tweet_collection=None):
    bar = progressbar.ProgressBar(max_value=len(tweets))
    bar.start()
    while tweets:
        tweet = tweets.pop()
        try:
            new_document = {'_id': tweet.tweet_id,
                            'is_processed': False,
                            'username': tweet.username,
                            'fullname': tweet.fullname,
                            'user_id': tweet.user_id,
                            'tweet_id': tweet.tweet_id,
                            'tweet_url': tweet.tweet_url,
                            'timestamp': tweet.timestamp,
                            'timestamp_epochs': tweet.timestamp_epochs,
                            'replies': tweet.replies,
                            'retweets': tweet.retweets,
                            'likes': tweet.likes,
                            'is_retweet': tweet.is_retweet,
                            'retweeter_username': tweet.retweeter_username,
                            'retweeter_userid': tweet.retweeter_userid,
                            'retweet_id': tweet.retweet_id,
                            'text': tweet.text,
                            'html': tweet.html,
                            }
            tweet_collection.insert_one(new_document)
        except DuplicateKeyError:
            bar += 1
            continue
        bar += 1
    bar.finish()


@click.command()
@click.argument('topic')
@click.argument('query', nargs=-1)
@click.option('--limit', type=click.INT, default=-1)
@click.option('--resume', is_flag=True)
def main(topic, query, limit, resume):
    """ Downloads Users' Tweetsd
    """
    logger = logging.getLogger(__name__)

    client = None
    db = None

    # ensure topic is in lowercase
    topic = topic.lower()

    tweets = deque()
    try:
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
        tweet_collection_name = topic + '-tweets'
        tweet_collection = db[tweet_collection_name]
        retweet_collection = db[topic + '-retweets']

        query = ' '.join(query)

        collections = db.list_collection_names()
        if tweet_collection_name not in collections:
            logger.info(f'scrapping tweets for "{query}"')
            fresh_scrap(query=query, tweets=tweets, limit=limit)

        if tweet_collection_name in collections and not resume:
            logger.info(f'scrapping tweets for "{query}"')
            fresh_scrap(query=query, tweets=tweets, limit=limit)

            logger.info(f'fetching previously unprocessed tweets for {query}')
            resume_scrap(query=query, tweets=tweets,
                         tweet_collection=tweet_collection)

        if tweet_collection_name in collections and resume:
            logger.info(f'resuming scrapping for {query}')
            resume(query=query, tweets=tweets,
                   tweet_collection=tweet_collection)

        logger.info(f'processing retweets')
        process_retweets(api=api, tweets=tweets,
                         tweet_collection=tweet_collection,
                         retweet_collection=retweet_collection)

    except requests.exceptions.HTTPError as e:
        logger.error("Checking internet connection failed, "
                     f"status code {e.response.status_code}")
    except requests.exceptions.ConnectionError:
        logger.error("Could not establish a connection.")
    except (ValueError, TypeError) as e:
        logger.error(e)
    except TweepError as e:
        logger.error(e)
    finally:
        logger.info('saving unprocessed tweets')
        process_left_over_tweets(tweets=tweets,
                                 tweet_collection=tweet_collection)

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
