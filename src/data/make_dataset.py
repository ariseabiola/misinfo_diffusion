# -*- coding: utf-8 -*-
import os
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


def process_features(edges=None, messages_graph=None, users_graph=None):
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


def process_topics(topics=None, db=None, save_as=None):
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

    messages_graph = utils.create_tweet_retweet_network(*collections)

    # get all user edges in user graph
    edges = users_graph.edges()

    # process features
    data = process_features(edges=edges, users_graph=users_graph,
                            messages_graph=messages_graph)

    df = pd.DataFrame(data)

    return df


@click.command()
@click.argument('topics', nargs=-1)
@click.option('--extended', type=click.Choice(['True', 'False'],
                                              case_sensitive=False)
              )
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

    # normalise extended values
    extended = extended.lower()
    if extended == 'false':
        extended = False
    if extended == 'true':
        extended = True

    # get topics from input if extended is true
    if extended:
        true_topics = input("Please supply TRUE topic(s) "
                            "seperated by a comma and without quotes: ")
        false_topics = input("Please supply FALSE topic(s) "
                             "seperated by a comma and without quotes: ")

        # split topics string
        true_topics = true_topics.split(',')
        false_topics = false_topics.split(',')

        # ensure that there are no leader or trailing white spaces
        true_topics = [topic.strip() for topic in true_topics]
        false_topics = [topic.strip() for topic in false_topics]

    client = None
    db = None
    documents = None
    df = None

    try:
        db_name = os.environ.get('DB_NAME')

        client = MongoClient(host='localhost', port=27017, appname=appname)
        db = client[db_name]

        if extended:
            # processed true_topics as topics
            logger.info('Processing TRUE topics.')
            true_topics_df = process_topics(topics=true_topics, db=db,
                                            save_as='TRUE')
            true_topics_df['topic'] = 1

            logger.info('Processing FALSE topics.')
            false_topics_df = process_topics(topics=false_topics, db=db,
                                             save_as='FALSE')
            false_topics_df['topic'] = 0

            df = pd.concat([true_topics_df, false_topics_df], sort=False,
                           ignore_index=True)

            topics = true_topics + false_topics
        else:
            # process topics
            df = process_topics(topics=topics, db=db)

        # save features to a centralised raw directory
        project_dir = Path(__file__).resolve().parents[2]
        processed_dir = os.path.join(project_dir, 'data', 'processed')
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
        logger.info(f'saving computed features to "{processed_dir}"')
        save_as_filename = os.path.join(
            processed_dir, f'{"_".join(sorted(topics))}_TRUE_FALSE.pqt')

        df.to_parquet(save_as_filename, engine="pyarrow")

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
