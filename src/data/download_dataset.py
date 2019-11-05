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
from tweepy.error import TweepError

from src.twitter import auth, get_retweets, scrap
from src.utils import (generate_collection_name, get_topic_collection_names,
                       save_tweet_to_db)


def fetch_retweets(api=None, tweet_id=None, retweet_collection=None, depth=1):
    """
    Fetches retweets of a tweet and save to retweet collection.
    """
    try:
        retweets = get_retweets(api=api, tweet_id=tweet_id)
        if retweets:
            for retweet in retweets:
                try:
                    tweet = retweet._json
                    tweet['is_processed'] = {'depth': depth + 1,
                                             'status': False}
                    save_tweet_to_db(tweet=tweet,
                                     collection=retweet_collection)
                except DuplicateKeyError:
                    continue
    except TweepError as e:
        logging.info(e)
        raise


def start_fresh_scrap(query=None, tweets=None, limit=None):
    """
    Search Twitter for query q, extend tweets with search result.

    Raises:
        TypeError: if tweets object is not a collections.deque
    """
    if tweets is None or not isinstance(tweets, deque):
        raise TypeError('Expected Type "collections.deque", '
                        f'got {type(tweets)}.')

    tweets_ = scrap(query=query, limit=limit)
    for tweet in tweets_:
        new_document = {'_id': tweet.tweet_id,
                        'is_processed': {'depth': 0, 'status': False},
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
        tweets.append(new_document)


def enqueue_backlogs(tweets=None, tweet_collection=None, topic=None,
                     collection_names=None, db=None):
    """
    Fetch tweets yet to processed in tweet collection.

    Raises:
        TypeError: if tweets object is not a collections.deque
    """
    if tweets is None or not isinstance(tweets, deque):
        raise TypeError('Expected Type "collections.deque", '
                        f'got {type(tweets)}.')

    topic_collections = get_topic_collection_names(
        topic=topic, collection_names=collection_names)

    topic_collections = sorted(topic_collections)
    tweet_collection = db[topic_collections[-1]]

    if len(topic_collections) == 1:
        retweet_collections = f'{topic}-retweets'
        retweet_collections = [db[retweet_collections]]
    else:
        retweet_collections = topic_collections[:-1]
        retweet_collections = [db[retweet_collection]
                               for retweet_collection in retweet_collections]

    # enqueue tweet (scrap) backlog
    tweet_resume_query = {'is_processed.status': False}
    n_tweets_ = tweet_collection.count_documents(tweet_resume_query)

    if n_tweets_:
        tweets_ = tweet_collection.find(tweet_resume_query, {'_id': 0})
        logging.info(f'{n_tweets_} unprocessed tweets have been queued for '
                     'processing.')
        tweets.extend(tweets_)

    # process retweets backlog
    for depth, retweet_collection in enumerate(retweet_collections, 1):
        retweet_resume_query = {'is_processed.status': False}
        n_retweets = retweet_collection.count_documents(retweet_resume_query)
        if not n_retweets:
            continue
        retweets = retweet_collection.find(retweet_resume_query, {'_id': 0})
        logging.info(f'{n_retweets} unprocessed retweets found at depth '
                     f'{depth} have been queued for processing.')
        tweets.extend(retweets)


def is_fetch_retweet(tweet_id, tweet_collection):
    """
    Tests a tweet on whether to fetch its retweet from a tweet collection.

    Returns:
        boolean -- Returns True if a tweet's retweet has not been previously
        fetched.
    """
    id_query = {'_id': tweet_id}
    result = tweet_collection.find_one(id_query, {'_id': 0, 'is_processed': 1})
    return result is None or not result['is_processed']['status']


def process_retweets(api=None, tweets=None, tweet_collection=None,
                     retweet_collection=None, depth=None,
                     db=None, topic=None, level_0=True, max_depth=None):
    bar = progressbar.ProgressBar(max_value=len(tweets))
    bar.start()
    while tweets:
        tweet = tweets[-1]
        if level_0:
            depth = tweet['is_processed']['depth']

            tweet_collection_name = generate_collection_name(topic=topic,
                                                             depth=depth)
            tweet_collection = db[tweet_collection_name]

            retweet_collection_name = generate_collection_name(
                topic=topic, depth=depth + 1
                )
            retweet_collection = db[retweet_collection_name]

        if depth == 0 and not tweet['is_retweet']:
            tweet_id = tweet['tweet_id']

        if depth >= 1:
            tweet_id = tweet['id_str']

        if is_fetch_retweet(tweet_id=tweet_id,
                            tweet_collection=tweet_collection):
            try:
                if depth >= max_depth:
                    raise ValueError
                fetch_retweets(api=api, tweet_id=tweet_id,
                               retweet_collection=retweet_collection,
                               depth=depth)

                tweet['_id'] = tweet_id
                tweet['is_processed']['status'] = True
                tweet_collection.insert_one(tweet)
            except DuplicateKeyError:
                tweet_query = {'_id': tweet_id}
                new_values = {"$set": {"is_processed.status": True}}
                tweet_collection.update_one(tweet_query, new_values)
                _ = tweets.pop()
                bar += 1
                continue
            except TweepError:
                _ = tweets.pop()
                bar += 1
                continue
            except ValueError:
                _ = tweets.pop()
                bar += 1
                continue
        _ = tweets.pop()
        bar += 1
    bar.finish()


def process_left_over_tweets(tweets=None, topic=None, db=None):
    bar = progressbar.ProgressBar(max_value=len(tweets))
    bar.start()
    while tweets:
        tweet = tweets.pop()
        depth = tweet['is_processed']['depth']
        tweet_collection_name = generate_collection_name(topic=topic,
                                                         depth=depth)

        if depth == 0:
            tweet['_id'] = tweet['tweet_id']

        if depth >= 1:
            tweet['_id'] = tweet['id_str']

        try:
            tweet_collection = db[tweet_collection_name]
            tweet_collection.insert_one(tweet)
        except DuplicateKeyError:
            bar += 1
            continue
        bar += 1
    bar.finish()


@click.command()
@click.argument('topic')
@click.argument('query', nargs=-1)
@click.option('--limit', default=-1)
@click.option('--resume', is_flag=True)
@click.option('--max_depth', default=1, required=True)
def main(topic, query, limit, resume, max_depth):
    """ Downloads Users' Tweetsd
    """
    logger = logging.getLogger(__name__)

    client = None
    db = None

    # ensure topic is in lowercase
    topic = topic.lower()

    tweets = deque()
    try:
        if topic in ['tweets', 'retweets']:
            raise ValueError(f'Topic cannot be {topic}.')

        if max_depth < 1:
            raise ValueError(f'Expected MAX_DEPTH >= 1, got {max_depth}.')

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

        # generate collection names
        collection_names = {}
        for i in range(max_depth):
            if i == 0:
                collection_names[i] = tweet_collection_name
            if i == 1:
                collection_names[i] = topic + '-retweets'
            if i >= 2:
                collection_names[i] = topic + f'-retweets-{i}'

        query = ' '.join(query)

        collections = db.list_collection_names()

        # if tweet collections does not exist in collections, it means the
        # topic has never been processed. Thus, start a fresh crawl.
        if tweet_collection_name not in collections:
            logger.info(f'scrapping tweets for "{query}"')
            start_fresh_scrap(query=query, tweets=tweets, limit=limit)

        # if tweet collection exists in collections but resume flag is turned
        # off,start a fresh crawl and check if there were backlogs from
        # previous trials. If there happens to be any backlog, enqueue their
        # tweet IDs.
        if tweet_collection_name in collections and not resume:
            logger.info(f'scrapping tweets for "{query}"')
            start_fresh_scrap(query=query, tweets=tweets, limit=limit)

            logger.info(f'fetching previously unprocessed tweets for {query}')
            enqueue_backlogs(tweets=tweets,
                             tweet_collection=db[generate_collection_name(
                                 topic, depth=0
                                 )],
                             topic=topic, collection_names=collections, db=db)

        # if tweet collection exists and resume flag is turned on, then only
        # check if there are backlogs. If there are any, enqueue them for
        # processing.
        if tweet_collection_name in collections and resume:
            logger.info(f'resuming scrapping for {query}')
            enqueue_backlogs(tweets=tweets,
                             tweet_collection=db[generate_collection_name(
                                 topic, depth=0
                                 )],
                             topic=topic, collection_names=collections, db=db)

        # process retweets for all tweets (or backlogs)
        logger.info('processing retweet of backlogs.')
        process_retweets(api=api, tweets=tweets, depth=None, topic=topic,
                         db=db, tweet_collection=None, retweet_collection=None,
                         max_depth=max_depth)

        # debug: make sure that tweets is completely empty as this point
        n_topic_collections = len([collection_name
                                   for collection_name in collections
                                   if topic + '-retweets' in collection_name or
                                   topic + '-tweets' in collection_name])

        if n_topic_collections - 1 < max_depth:
            for i in range(1, max_depth):
                logger.info(f'processing retweets for depth {i}')
                tweet_collection = db[generate_collection_name(topic=topic,
                                                               depth=i)]
                retweet_collection = db[generate_collection_name(
                    topic=topic, depth=i + 1
                    )]

                enqueue_backlogs(tweets=tweets,
                                 tweet_collection=retweet_collection,
                                 topic=topic, collection_names=collections,
                                 db=db)

                process_retweets(api=api, tweets=tweets, depth=i,
                                 tweet_collection=tweet_collection,
                                 retweet_collection=retweet_collection,
                                 level_0=False, max_depth=max_depth)

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
        if tweets:
            logger.info('saving unprocessed tweets')
            process_left_over_tweets(tweets=tweets, topic=topic, db=db)

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
