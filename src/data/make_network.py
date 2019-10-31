# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import os
import pymongo
import networkx as nx
import progressbar

import src.utils as utils


@click.command()
@click.argument('topics', nargs=-1)
@click.option('--using', type=click.Choice(['simple', 'directed', 'multi',
                                            'multi_directed'],
                                           case_sensitive=True), nargs=1)
def main(topics, using):
    """ Runs data processing scripts to turn raw data from (data/raw) into
        cleaned data ready to be analyzed (saved in data/processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('creating network of tweets and retweets')

    project_dir = Path(__file__).resolve().parents[2]

    client = None
    db = None

    try:
        db_name = os.environ.get('DB_NAME')
        client = pymongo.MongoClient(host='localhost', port=27017,
                                     appname=__file__)

        db = client[db_name]
        collections = db.list_collection_names()

        retweet_collections = []
        topics = set(topics)
        logger.info('fetching documents from topic collection')
        bar = progressbar.ProgressBar(max_value=len(topics))
        for topic in bar(topics):
            topic_name = topic + '-retweets'
            if topic_name in collections:
                retweet_collection = db[topic_name]
                retweet_collections.append(retweet_collection)

        logger.info('creating tweet-retweet network')
        graph = utils.create_tweet_retweet_network(*retweet_collections,
                                                   create_using=using)

        if retweet_collections:
            filename = '-'.join(topics) + f'-{using}-network.gexf'
            save_as = os.path.join(project_dir, 'data', 'external', filename)
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
