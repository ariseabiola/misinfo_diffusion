import logging
import random
import re
import string
from collections import Counter, defaultdict
from itertools import chain

import networkx as nx
import pandas as pd
from dateutil.parser import parse
from nltk.corpus import stopwords
from nltk.tokenize import TweetTokenizer
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
    attribs = {}

    retweets = collection.find({})

    for retweet in retweets:
        retweet_id = retweet['id_str']
        original_tweet_id = retweet['retweeted_status']['id_str']
        network[original_tweet_id].append(retweet_id)

        # update node attributes
        original_tweet = retweet['retweeted_status']
        original_tweet_date = parse(original_tweet['created_at'])
        retweet_date = parse(retweet['created_at'])

        # only update node attributes if user account's date is the latest
        if (original_tweet_id not in attribs or
                parse(
                    attribs[original_tweet_id]['created_at']
                    ) < original_tweet_date):
            attribs[original_tweet_id] = original_tweet

        if (retweet_id not in attribs or
                parse(
                    attribs[retweet_id]['created_at']
                    ) < retweet_date):
            attribs[retweet_id] = retweet

    return network, attribs


def merge_multiple_dicts_of_list(*dicts):
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


def merge_multiple_dicts_of_attribs(*args):
    """Merge multiple dicts into a single dict.

    Returns:
        dict -- a dict of tweets IDs and retweet IDs
    """
    if len(args) == 1:
        return args[0]

    attribs = {}
    args = (dict_.items() for dict_ in args)

    for k, v in chain(*args):
        created_date = v['created_at']
        if (k not in attribs or
                parse(attribs[k]['created_at']) < parse(created_date)):
            attribs[k] = v

    return attribs


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
    message_attribs = []
    for collection in collections:
        network, attribs = generate_tweet_retweet_dict_from_collection(
            collection)
        networks.append(network)
        message_attribs.append(attribs)

    combined_network = merge_multiple_dicts_of_list(*networks)
    combined_attribs = merge_multiple_dicts_of_attribs(*message_attribs)
    graph = nx.from_dict_of_lists(combined_network,
                                  create_using=graph_types[create_using])

    # set node attributes
    nx.set_node_attributes(graph, combined_attribs)

    return graph


def generate_collection_name(topic, depth):
    """Returns a corresponding name for a topic's depth

    Arguments:
        topic {str} -- topic
        depth {int} -- tweet's depth

    Returns:
        [type] -- [description]
    """
    if depth == 0:
        return topic + '-tweets'

    if depth == 1:
        return topic + '-retweets'

    if depth >= 2:
        return topic + f'-retweets-{depth}'


def get_topic_collection_names(topic, db, ignore_depth_0=False):
    """Given a database and a topic, return all collection that
    matches topic.
    """
    collection_names = db.list_collection_names()

    if ignore_depth_0:
        topic_collections = [collection_name
                             for collection_name in collection_names
                             if topic + '-retweets' in collection_name]
    else:
        topic_collections = [collection_name
                             for collection_name in collection_names
                             if topic + '-retweets' in collection_name or
                             topic + '-tweets' in collection_name]

    return sorted(topic_collections)


def collection_dates(*collections, return_type='date'):
    if return_type not in ['date', 'year']:
        return ValueError('Invalid value for "return_type": '
                          f'invalid choice: {return_type}. '
                          '(choose from "date", "year")')

    for collection in collections:
        date_return = {'_id': 0, 'created_at': 1, 'timestamp': 1}
        timestamps = collection.find({}, date_return)

        for timestamp in timestamps:
            if 'timestamp' in timestamp and return_type == 'date':
                yield timestamp['timestamp'].date()

            if 'created_at' in timestamp and return_type == 'date':
                date_ = parse(timestamp['created_at'])
                yield date_.date()

            if 'timestamp' in timestamp and return_type == 'year':
                yield timestamp['timestamp'].year

            if 'created_at' in timestamp and return_type == 'year':
                date_ = parse(timestamp['created_at'])
                yield date_.year


def get_topic_or_depth_names(collection_names, return_type=None):
    if return_type not in ['depth', 'topic']:
        raise ValueError()

    pattern = r'(?P<topic>[a-zA-Z0-9_-]+)-(?P<depth>tweets|retweets[0-9-]*)'
    prog = re.compile(pattern)
    if return_type == 'depth':
        return [prog.match(collection_name).group('depth')
                for collection_name in collection_names]

    if return_type == 'topic':
        return prog.match(collection_names[0]).group('topic')


def get_topic_and_depth_from_collection_name(collection_name):
    result = None, None

    pattern = r'(?P<topic>[a-zA-Z0-9_-]+)-(?P<depth>tweets|retweets[0-9-]*)'
    match = re.search(pattern, collection_name)

    if match:
        result = match.group('topic'), match.group('depth')

    return result


def create_collections_dataframe_data(*args, **kwargs):
    counters = {}
    all_topic_collection_names = []

    db = kwargs['db']
    topics = get_topics_in_db(db=db)
    for topic in set(args):
        depth_names = topics[topic]
        topic_collection_names = [topic + '-' + depth_name
                                  for depth_name in depth_names]
        all_topic_collection_names.extend(topic_collection_names)

    # get counters for all collections
    for collection_name in all_topic_collection_names:
        counters[collection_name] = Counter(collection_dates(
            db[collection_name]
            ))

    # get all the dates from counters
    if counters:
        dates = set(chain.from_iterable(counters.values()))

    for date_ in sorted(dates):
        yield [date_] + [counters[collection_name][date_]
                         for collection_name in all_topic_collection_names]


def get_topics_in_db(db=None):
    topics = defaultdict(list)

    collection_names = db.list_collection_names()

    for collection_name in collection_names:
        topic_name, depth_name = get_topic_and_depth_from_collection_name(
            collection_name=collection_name
        )

        if topic_name is not None and depth_name is not None:
            topics[topic_name].append(depth_name)

    topics = {topic_name: sort_topic_collection_names(topic_collection_names)
              for topic_name, topic_collection_names in topics.items()}

    return topics


def sort_topic_collection_names(topic_collection_names):
    topic_collection_names = sorted(topic_collection_names)

    tweet_collection_name = topic_collection_names[-1]
    retweet_collection_names = topic_collection_names[:-1]
    topic_collection_names = [tweet_collection_name] + retweet_collection_names

    return topic_collection_names


def create_collections_dataframe(*args, **kwargs):
    columns = ['date']
    db = kwargs['db']
    processed_topics = []

    topics = get_topics_in_db(db=db)
    for topic in set(args):
        if topic in topics:
            depth_names = topics[topic]
            topic_collection_names = [topic + '-' + depth_name
                                      for depth_name in depth_names]

            columns.extend(topic_collection_names)
            processed_topics.append(topic)

    return pd.DataFrame(create_collections_dataframe_data(*processed_topics,
                                                          db=db),
                        columns=columns)


def generate_user_dict_from_collection(collection=None):
    """Return a dict of user IDs and their respective retweeter IDs

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
    node_attribs = {}

    retweets = collection.find({})

    for retweet in retweets:
        retweeter_id = retweet['user']['id_str']
        original_tweeter_id = retweet['retweeted_status']['user']['id_str']
        network[original_tweeter_id].append(retweeter_id)

        # update node attributes
        original_tweeter = retweet['retweeted_status']['user']
        original_tweeter_date = parse(original_tweeter['created_at'])
        retweeter = retweet['user']
        retweeter_date = parse(retweeter['created_at'])

        # only update node attributes if user account's date is the latest
        if (original_tweeter_id not in node_attribs or
                parse(
                    node_attribs[original_tweeter_id]['created_at']
                    ) < original_tweeter_date):
            node_attribs[original_tweeter_id] = original_tweeter

        if (retweeter_id not in node_attribs or
                parse(
                    node_attribs[retweeter_id]['created_at']
                    ) < retweeter_date):
            node_attribs[retweeter_id] = retweeter

    return network, node_attribs


def create_user_network(*collections, create_using='simple'):
    """Returns a graph of user --> retweeter representation of one or more
    collections.

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
    user_attribs = []
    for collection in collections:
        network, attribs = generate_user_dict_from_collection(collection)
        networks.append(network)
        user_attribs.append(attribs)

    combined_network = merge_multiple_dicts_of_list(*networks)
    combined_user_attribs = merge_multiple_dicts_of_attribs(*user_attribs)

    graph = nx.from_dict_of_lists(combined_network,
                                  create_using=graph_types[create_using])

    # set node attr after creating the graph
    nx.set_node_attributes(graph, combined_user_attribs)

    return graph


def prune_topics(topics=None, db=None):
    """Remove topics that are not present in a database.

    Keyword Arguments:
        topics {[type]} -- [description] (default: {None})
        db {[type]} -- [description] (default: {None})

    Returns:
        [type] -- [description]
    """
    topics_ = set()
    available_topics = get_topics_in_db(db=db)

    for topic in topics:
        if topic in available_topics:
            topics_.add(topic)

    return topics_


def get_user_messages_from_network(messages_graph, *user_ids):
    """Returns messages of given user ID(s).

    Arguments:
        messages_graph {[type]} -- [description]

    Returns:
        [type] -- [description]
    """
    user_messages = {}

    for user_id in user_ids:
        user_messages.update({
            message_id: messages_graph.nodes[message_id]
            for message_id in messages_graph.nodes()
            if messages_graph.nodes[message_id]['user']['id_str'] == user_id
            })

    return user_messages


def tokenise_tweet(tweet_text):
    tokenizer = TweetTokenizer(strip_handles=True)
    pattern_1 = r'https?://[^\s<>"]+|www\.[^\s<>"]+|\S+@\S+'
    pattern_2 = r'\w+'

    prog_1 = re.compile(pattern_1)
    prog_2 = re.compile(pattern_2)

    tokens = set(tokenizer.tokenize(tweet_text))
    no_links = {token.lower() for token in tokens if not prog_1.match(token)}
    no_pun = {token for token in no_links if prog_2.match(token)}
    # final = {token for token in no_pun
    #         if token not in stopwords.words('english') + ['rt']}

    # tokenise without removing stopwords
    final = {token for token in no_pun if token != 'rt'}

    # returns a list of keywords in one message
    # consider return a generator maybe?
    return list(final)


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ''.join(re.sub(r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)",
                          " ", tweet))


def generate_random_id(length=10):
    '''Returns a unique string of specified length'''
    identifier = ""
    for _ in range(length):
        identifier = identifier + random.choice(string.ascii_letters)
    return identifier
