# -*- coding: utf-8 -*-
import configparser
import datetime
import os
import time
from pathlib import Path

import click
import paralleldots as pds
import progressbar
from dateutil.parser import parse
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient, ReturnDocument

from src.logger import get_logger
from src.utils import get_topics_in_db, generate_collection_name

logger = get_logger('src.data.make_content_analysis')


def get_sleep_time(wait_time=88200, config_filename='configs.cfg'):
    config = configparser.ConfigParser()
    _ = config.read(config_filename)

    last_sleep_time = config.get('rate_limit', 'last_sleep_time',
                                 fallback=None)
    if last_sleep_time is not None:
        last_sleep_time = parse(last_sleep_time)

    expected_restart_time = config.get('rate_limit', 'expected_restart_time',
                                       fallback=None)
    if expected_restart_time is not None:
        expected_restart_time = parse(expected_restart_time)

    current_time = datetime.datetime.now()

    if ((last_sleep_time is None or expected_restart_time is None) or
            current_time > expected_restart_time):
        last_sleep_time = current_time
        expected_restart_time = last_sleep_time + datetime.timedelta(
            seconds=wait_time)

        # update rate_limit in file
        config['rate_limit'] = {}
        config['rate_limit']['last_sleep_time'] = str(last_sleep_time)
        config['rate_limit']['expected_restart_time'] = str(
            expected_restart_time)

        with open(config_filename, 'w') as configfile:
            config.write(configfile)
            logger.debug('config file was updated with new values.')

    time_left_to_restart = expected_restart_time - current_time

    return time_left_to_restart.total_seconds()


def compute_content_analysis(document, collection, time_out=5):
    text = document['text']
    document_id = document['_id']

    logger.debug(f'Tweet ID: {document_id} - Text: {text}')
    if 'abuse' not in document:
        logger.debug('Accessing paralleldots API for `abuse`')
        abuse = pds.abuse(text)
        if 'code' in abuse:
            sleep_time = get_sleep_time()
            expected_restart_time = (datetime.datetime.now() +
                                     datetime.timedelta(seconds=sleep_time))
            logger.warning('Rate Limit Reached. '
                           f'Restart Time: {expected_restart_time}')
            time.sleep(sleep_time)
            abuse = pds.abuse(text)

        abuse = {'abuse': abuse}
        logger.debug(f'ID: {document_id} - (abuse): {abuse}')
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': abuse},
            return_document=ReturnDocument.AFTER
            )
        logger.debug('`abuse` was successfully added to Tweet ID: '
                     f'{document_id}')

        # ensure that there's a rest time betweet API calls
        time.sleep(time_out)

    if 'intent' not in document:
        logger.debug('Accessing paralleldots API for `intent`')
        intent = pds.intent(text)
        if 'code' in intent:
            sleep_time = get_sleep_time()
            expected_restart_time = (datetime.datetime.now() +
                                     datetime.timedelta(seconds=sleep_time))
            logger.warning('Rate Limit Reached. '
                           f'Restart Time: {expected_restart_time}')
            time.sleep(sleep_time)
            intent = pds.intent(text)

        logger.debug(f'ID: {document_id} - (intent): {intent}')
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': intent},
            return_document=ReturnDocument.AFTER
            )
        logger.debug('`intent` was successfully added to '
                     f'Tweet ID: {document_id}')

        # ensure that there's a rest time betweet API calls
        time.sleep(time_out)

    if 'emotion' not in document:
        logger.debug('Accessing paralleldots API for `emotion`')
        emotion = pds.emotion(text)
        if 'code' in emotion:
            sleep_time = get_sleep_time()
            expected_restart_time = (datetime.datetime.now() +
                                     datetime.timedelta(seconds=sleep_time))
            logger.warning('Rate Limit Reached. '
                           f'Restart Time: {expected_restart_time}')
            time.sleep(sleep_time)
            emotion = pds.emotion(text)

        logger.debug(f'ID: {document_id} - (emotion): {emotion}')
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': emotion},
            return_document=ReturnDocument.AFTER
            )
        logger.debug('`emotion` was successfully added to '
                     f'Tweet ID: {document_id}')

        # ensure that there's a rest time betweet API calls
        time.sleep(time_out)

    if 'sentiment' not in document:
        logger.debug('Accessing paralleldots API for `sentiment`')
        sentiment = pds.sentiment(text)
        if 'code' in sentiment:
            sleep_time = get_sleep_time()
            expected_restart_time = (datetime.datetime.now() +
                                     datetime.timedelta(seconds=sleep_time))
            logger.warning('Rate Limit Reached. '
                           f'Restart Time: {expected_restart_time}')
            time.sleep(sleep_time)
            sentiment = pds.sentiment(text)

        logger.debug(f'ID: {document_id} - (sentiment): {sentiment}')
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': sentiment},
            return_document=ReturnDocument.AFTER
            )
        logger.debug('`sentiment` was successfully added to '
                     f'Tweet ID: {document_id}')

        # ensure that there's a rest time betweet API calls
        time.sleep(time_out)


def infer_content_analysis(db=None, **kwargs):
    collection_topic = kwargs['topic']
    collection_depth = kwargs['depth']
    collection_name = kwargs['name']

    # get all documents at depth
    collection = db[collection_name]
    documents = collection.find({}, {'_id': True,
                                     'retweeted_status.id_str': True})
    n_documents = collection.count_documents({})
    logger.info(f'inferring content analysis for `{collection_topic}` '
                f'at depth {collection_depth}')
    bar = progressbar.ProgressBar(max_value=n_documents)
    for document in bar(documents):
        document_id = document['_id']
        parent_document_id = document['retweeted_status']['id_str']

        # get parent document
        parent_collection_name = generate_collection_name(
            topic=collection_topic, depth=collection_depth - 1)
        parent_collection = db[parent_collection_name]
        parent_document = parent_collection.find_one(
            {'_id': parent_document_id}, {'abuse': True, 'intent': True,
                                          'emotion': True, 'sentiment': True}
            )

        abuse = parent_document['abuse']
        abuse = {'abuse': abuse}
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': abuse},
            return_document=ReturnDocument.AFTER)
        logger.debug('`abuse` was successfully added to '
                     f'Tweet ID: {document_id}')

        intent = parent_document['intent']
        intent = {'intent': intent}
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': intent},
            return_document=ReturnDocument.AFTER)
        logger.debug('`intent` was successfully added to '
                     f'Tweet ID: {document_id}')

        emotion = parent_document['emotion']
        emotion = {'emotion': emotion}
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': emotion},
            return_document=ReturnDocument.AFTER)
        logger.debug('`emotion` was successfully added to '
                     f'Tweet ID: {document_id}')

        sentiment = parent_document['sentiment']
        _ = collection.find_one_and_update(
            {'_id': document_id}, {'$set': sentiment},
            return_document=ReturnDocument.AFTER)
        logger.debug('`sentiment` was successfully added to '
                     f'Tweet ID: {document_id}')


@click.command()
@click.argument('topics', nargs=-1)
def main(topics):
    """ Run content analysis script on tweet collections.
    """
    logger.info('making content analysis of topics')
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
        paralleldots_api_key = os.environ.get('PARALLEL_DOTS_API')

        client = MongoClient(host='localhost', port=27017, appname=appname)
        pds.set_api_key(paralleldots_api_key)

        db = client[db_name]
        available_topics = get_topics_in_db(db=db)

        # get collection names of all topics at depth 0
        collection_names_to_process = []
        for topic in topics:
            if topic in available_topics:
                depth_names = available_topics[topic]
                collection_name = topic + '-' + depth_names[0]
                collection_names_to_process.append(collection_name)

        # process content anaylsis on all collection at depth 0
        for collection_name in collection_names_to_process:
            collection = db[collection_name]
            documents = collection.find({}, {'_id': True,
                                             'text': True,
                                             'abuse': True,
                                             'intent': True,
                                             'emotion': True,
                                             'sentiment': True},
                                        no_cursor_timeout=True)

            logger.info(f'processing collection, `{collection_name}`')
            n_documents = collection.count_documents({})
            bar = progressbar.ProgressBar(max_value=n_documents)
            for document in bar(documents):
                compute_content_analysis(document=document,
                                         collection=collection)
            documents.close()

        # infer content analysis result from previous depth
        collection_names_to_process = []
        for topic in topics:
            if topic in available_topics:
                depth_names = available_topics[topic]
                max_depth = len(depth_names)
                collection_info = {}
                for depth in range(1, max_depth):
                    collection_info['name'] = topic + '-' + depth_names[depth]
                    collection_info['depth'] = depth
                    collection_info['topic'] = topic
                    collection_names_to_process.append(collection_info)

        for collection_info in collection_names_to_process:
            infer_content_analysis(db=db, **collection_info)

    finally:
        if documents is not None and documents.alive:
            logger.debug('found the documents cursor is still alive')
            documents.close()

        if client is not None:
            logger.info('ending all server sessions')
            client.close()


if __name__ == '__main__':

    load_dotenv(find_dotenv())

    main()
