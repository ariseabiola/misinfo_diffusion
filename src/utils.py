import logging

from pymongo.errors import DuplicateKeyError


def save_tweet_to_db(tweet=None, collection=None):
    """Save Tweet object to collection.

    Keyword Arguments:
        tweet {dict} -- JSON representation of a Status object
        (default: {None})
        collection {pymongo.collection.Collection} -- A Mongo collection.
        (default: {None})
    """
    id_ = {"_id": tweet['id_str']}
    new_document = {**id_, **tweet}
    try:
        collection.insert_one(new_document)
    except DuplicateKeyError:
        logging.info(f"found duplicate key: {tweet['id_str']}")
        raise
