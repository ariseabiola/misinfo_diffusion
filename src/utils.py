import logging

from pymongo.errors import DuplicateKeyError


def format_query(s):
    pass


def read_keywords_from_file(filename=None):
    queries = ''
    with open(filename) as f:
        queries = f.readlines()
    return queries


def save_tweet_to_db(tweet=None, collection=None):
    id_ = {"_id": tweet['id_str']}
    new_document = {**id_, **tweet}
    try:
        collection.insert_one(new_document)
    except DuplicateKeyError:
        logging.info(f"found duplicate key: {tweet['id_str']}")
        raise
