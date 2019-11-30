# -*- coding: utf-8 -*-
import os
from itertools import permutations
from pathlib import Path

import click
import pandas as pd
import progressbar
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient

import src.utils as utils
from src.features import build_features as bfs
from src.logger import get_logger

logger = get_logger('src.data.make_data')


def compute_instance_features(**kwargs):
    message = kwargs['message']
    src_user_messages = kwargs['src_user_message']
    dest_user_messages = kwargs['dest_user_messages']
    src_user_profile = kwargs['src_user_profile']
    dest_user_profile = kwargs['dest_user_profile']
    dest_user_id = kwargs['dest_user_id']

    info_features = bfs.compute_info_features(src_user_messages,
                                              dest_user_messages,
                                              src_user_profile,
                                              dest_user_id)

    src_user_features = bfs.compute_user_features(src_user_profile)
    info_features.update(src_user_features)

    dest_user_features = bfs.compute_user_features(dest_user_profile,
                                                   label='dest')
    info_features.update(dest_user_features)

    message_features = bfs.compute_message_features(message)
    info_features.update(message_features)

    dots_features = bfs.compute_dots_features(message)
    info_features.update(dots_features)

    y = bfs.compute_y(src_user_profile, message)
    y = {'y': y}
    info_features.update(y)

    return info_features


def process_features(edges=None, messages_graph=None, users_graph=None,
                     is_extended=False):
    cache = {
        'user': None,
        'messages': None,
        'user_profile': None,
    }

    info = {}
    logger.info('processing edges')
    bar = progressbar.ProgressBar()
    for edge in bar(edges):
        src_user_id, dest_user_id = edge

        if cache['user'] is None or cache['user'] != src_user_id:
            user_tweets = utils.get_user_messages_from_network(messages_graph,
                                                               src_user_id)

            # update cache user and cache message
            cache['user'] = src_user_id
            cache['messages'] = user_tweets
            cache['user_profile'] = users_graph.nodes[src_user_id]

        info['src_user_message'] = cache['messages']
        info['src_user_profile'] = cache['user_profile']
        user_tweets = utils.get_user_messages_from_network(messages_graph,
                                                           dest_user_id)
        info['dest_user_id'] = dest_user_id
        info['dest_user_messages'] = user_tweets
        info['dest_user_profile'] = users_graph.nodes[dest_user_id]

        for _, message in cache['messages'].items():
            info['message'] = message
            yield compute_instance_features(**info)


@click.command()
@click.argument('topics', nargs=-1)
@click.option('--extended', is_flag=True)
def main(topics, extended):
    """ Create Data for Training
    """
    logger.info('making data for training')
    project_dir = os.path.basename(Path(__file__).resolve().parents[2])
    src_dir = os.path.basename(Path(__file__).resolve().parents[1])
    data_dir = os.path.basename(Path(__file__).resolve().parents[0])
    file_, ext = os.path.split(__file__)
    appname = (f'{project_dir}.{src_dir}.{data_dir}.'
               f'{os.path.basename(__file__)}')

    client = None
    db = None
    documents = None
    try:
        db_name = os.environ.get('DB_NAME')

        client = MongoClient(host='localhost', port=27017, appname=appname)
        db = client[db_name]

        topics = utils.prune_topics(topics=topics, db=db)

        logger.info(f'Creating Data for Topic(s): {", ".join(topics)}')
        # get the full depths and corresponding collection of each topic
        topics_collections_names = []
        for topic in topics:
            topic_collection_names = utils.get_topic_collection_names(
                topic=topic, db=db, ignore_depth_0=True
                )
            topics_collections_names.extend(topic_collection_names)

        collections = [db[topic] for topic in topics_collections_names]

        # create a list of users in the given collections
        users_graph = utils.create_user_network(*collections)
        list_of_users = users_graph.nodes()

        messages_graph = utils.create_tweet_retweet_network(*collections)

        # generate all possible edges of users in the network
        edges = permutations(list_of_users, 2)

        # process features
        data = process_features(edges=edges, users_graph=users_graph,
                                messages_graph=messages_graph,
                                is_extended=extended)

        df = pd.DataFrame(data)

        # save features to a centralised raw directory
        project_dir = Path(__file__).resolve().parents[2]
        processed_dir = os.path.join(project_dir, 'data', 'processed')
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
        logger.info(f'saving computed features to "{processed_dir}"')
        df.to_parquet(os.path.join(processed_dir, f'{"_".join(topics)}.pqt'),
                      engine="pyarrow")

    finally:
        if documents is not None and documents.alive:
            logger.debug('found the documents cursor is still alive')
            documents.close()

        if client is not None:
            logger.info('ending all server sessions')
            client.close()


if __name__ == '__main__':
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
