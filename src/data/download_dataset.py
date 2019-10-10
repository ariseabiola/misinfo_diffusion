# -*- coding: utf-8 -*-
import logging
import os

import click
import pymongo
import requests
from dotenv import find_dotenv, load_dotenv

from src.twitter import auth, search
from src.utils import read_keywords_from_file


@click.command()
@click.argument('topic_filepath', type=click.Path(exists=True))
@click.argument('n_tweets', type=click.INT, default=1000)
def main(topic_filepath, n_tweets):
    """ Downloads Users' Tweets
    """
    logger = logging.getLogger(__name__)

    topic, _ = os.path.splitext(os.path.basename(topic_filepath))

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

        logger.info('downloading data set from raw data')
        queries = read_keywords_from_file(topic_filepath)

        for query in queries:
            search(api=api, query=query, db=db, n_tweets=n_tweets)

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
