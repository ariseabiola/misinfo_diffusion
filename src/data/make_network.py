# -*- coding: utf-8 -*-
import logging
import os
from pathlib import Path

import click
import networkx as nx
import progressbar
import pymongo
from dotenv import find_dotenv, load_dotenv

import src.utils as utils
from src.logger import get_logger

logger = get_logger('src.data.make_network')


@click.command()
@click.argument('topics', nargs=-1)
@click.option('--using', type=click.Choice(['simple', 'directed', 'multi',
                                            'multi_directed'],
                                           case_sensitive=True), nargs=1)
def main(topics, using):
    """ Runs network creation task scripts.
    """
    logger.info('creating network of tweets and retweets')

    project_dir = Path(__file__).resolve().parents[2]

    client = None
    db = None

    try:
        db_name = os.environ.get('DB_NAME')
        client = pymongo.MongoClient(host='localhost', port=27017,
                                     appname=__file__)

        db = client[db_name]

        topics_collection = []
        logger.info('fetching documents from topic collections')
        topics = set(topics)
        bar = progressbar.ProgressBar(max_value=len(topics))
        for topic in bar(topics):
            topic_collections = utils.get_topic_collection_names(
                topic=topic, db=db)
            topics_collection.extend(topic_collections[:-1])

        retweet_collections = [db[retweet_collection]
                               for retweet_collection in topics_collection]

        logger.info('creating tweet-retweet network')
        graph = utils.create_tweet_retweet_network(*retweet_collections,
                                                   create_using=using)

        if retweet_collections:
            filename = '-'.join(topics) + f'-{using}-network.gexf'
            save_to = os.path.join(project_dir, 'data', 'external')
            if not os.path.exists(save_to):
                os.makedirs(save_to)
            save_as = os.path.join(save_to, filename)
            logger.info(f'writing network to {save_as}')
            nx.write_gexf(G=graph, path=save_as)

    except ValueError as e:
        logger.error(e)
    finally:
        if client is not None:
            logger.info('ending all server sessions')
            client.close()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
