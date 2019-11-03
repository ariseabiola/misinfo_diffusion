import logging
from collections import defaultdict
from itertools import chain

import networkx as nx
from dateutil.parser import parse
from pymongo.collection import Collection
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


def generate_tweet_retweet_dict_from_collection(collection=None):
    """Return a dict of tweet IDs and their respective retweet IDs

    Keyword Arguments:
        collection {pymongo.collection.Collection} -- a mongo collection
        (default: {None})

    Raises:
        TypeError: if collection is not an instance of
        pymongo.collection.Collection

    Returns:
        dict -- a dict of tweets IDs and retweet IDs
    """
    if not isinstance(collection, Collection):
        raise TypeError('Expected `pymongo.collection.Collection`, '
                        f'got {type(collection)}.')

    network = defaultdict(list)

    retweets = collection.find({})

    for retweet in retweets:
        retweet_id = retweet['id_str']
        original_tweet_id = retweet['retweeted_status']['id_str']
        network[original_tweet_id].append(retweet_id)

    return network


def merge_multiple_tweet_retweet_dicts(*dicts):
    """Merge multiple dicts into a single dict.

    Returns:
        dict -- a dict of tweets IDs and retweet IDs
    """
    if len(dicts) == 1:
        return dicts[0]

    network = defaultdict(list)
    args = (dict_.items() for dict_ in dicts)
    for k, v in chain(*args):
        network[k].extend(v)

    return network


def create_tweet_retweet_network(*collections, create_using='simple'):
    """Returns a graph representation of one or more collections.

    Keyword Arguments:
        create_using {str} -- Graph type to create. (default: {'simple'})

    Raises:
        ValueError: Raised if argument is not a valid graph type

    Returns:
        Graph -- a graph
    """
    graph_types = {'simple': nx.Graph,
                   'directed': nx.DiGraph,
                   'multi': nx.MultiGraph,
                   'multi_directed': nx.MultiDiGraph
                   }

    create_using = create_using.lower()
    if create_using not in graph_types:
        raise ValueError(f'Expected any of {str(graph_types.keys())}, '
                         f'got `{create_using}` for create_using.')

    networks = []
    for collection in collections:
        network = generate_tweet_retweet_dict_from_collection(collection)
        networks.append(network)

    combined_network = merge_multiple_tweet_retweet_dicts(*networks)
    graph = nx.from_dict_of_lists(combined_network,
                                  create_using=graph_types[create_using])

    return graph


def generate_collection_name(topic, depth):
    if depth == 0:
        return topic + '-tweets'

    if depth == 1:
        return topic + '-retweets'

    if depth >= 2:
        return topic + f'-retweets-{depth-1}'


def get_topic_collections(topic, collection_names):
    """Given a list of collection names and a topic, return all collection that
    matches topic.
    """
    topic_collections = [collection_name
                         for collection_name in collection_names
                         if topic + '-retweets' in collection_name or
                         topic + '-tweets' in collection_name]

    return sorted(topic_collections)


def collection_dates(*collections):
    for collection in collections:
        date_return = {'_id': 0, 'created_at': 1, 'timestamp': 1}
        timestamps = collection.find({}, date_return)

        for timestamp in timestamps:
            if 'timestamp' in timestamp:
                year = timestamp['timestamp'].year

            if 'created_at' in timestamp:
                date_ = parse(timestamp['created_at'])
                year = date_.year

            yield year
