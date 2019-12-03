import datetime

from dateutil.parser import parse

import src.utils as utils


def get_users_mentioned_in_messages(messages):
    users_mentioned = []

    for _, message in messages.items():
        users_mentioned_in_message = message.get('entities', {}).get(
            'user_mentions', [])
        if users_mentioned_in_message:
            users_mentioned.extend(users_mentioned_in_message)

    return {user_mentioned['id_str'] for user_mentioned in users_mentioned}


def compute_social_homogeneity(*messages):
    """Computes social homogeneity index for vx ∈ V and vy ∈ V.
    This reflects the overlap of the sets of users they interact with.
    It is computed with the Jaccard similarity index that is defined as the
    size of the intersection of the sets divided by the size of their
    union.

    Returns:
        float -- social homogeneity index
    """
    src_user_tweets = messages[0]
    dest_user_tweets = messages[1]

    src_user_mv = get_users_mentioned_in_messages(src_user_tweets)
    dest_user_mv = get_users_mentioned_in_messages(dest_user_tweets)

    x = src_user_mv.intersection(dest_user_mv)
    y = src_user_mv.union(dest_user_mv)

    if len(y):
        return len(x) / len(y)
    return 0


def get_messages_with_user_mentions(messages):
    message_ids = []

    for message_id, message in messages.items():
        users_mentioned_in_message = message.get('entities', {}).get(
            'user_mentions', []
            )
        if users_mentioned_in_message:
            message_ids.append(message_id)

    return message_ids


def get_messages_without_user_mentions(messages):
    message_ids = []

    for message_id, message in messages.items():
        users_mentioned_in_message = message.get('entities', {}).get(
            'user_mentions', []
            )
        if not users_mentioned_in_message:
            message_ids.append(message_id)

    return message_ids


def compute_ratio_of_directed_tweets(messages):
    """Computes ratio of directed tweets for a user.
    This provides an idea about the role she plays in the spread
    of information.

    Arguments:
        user_id {str} -- User ID

    Returns:
        float -- user's ratio of directed tweets
    """
    n_dv = len(get_messages_with_user_mentions(messages))
    n_mv = len(messages)

    if n_mv:
        return n_dv / n_mv
    return 0


def compute_active_interaction(messages, dest_user_id):
    """Computes a boolean value for each user regarding the mentioning
    behaviour to capture the existence of an active interaction in the
    past. This feature can be somehow regarded as a “friendship” indicator
    in the case where both users have a positive value.

    Returns:
        boolean -- mentioning behaviour
    """
    mvx = get_users_mentioned_in_messages(messages)

    if dest_user_id in mvx:
        return 1
    return 0


def compute_ratio_of_retweets_to_tweets(messages):
    total_retweet_count = 0
    for _, user_tweet in messages.items():
        total_retweet_count += user_tweet.get('retweet_count', 0)

    total_number_of_tweets = len(messages)

    return total_retweet_count / total_number_of_tweets


def get_messages_with_hashtags(messages):
    message_ids = []

    for message_id, message in messages.items():
        users_mentioned_in_message = message.get('entities', {}).get(
            'hashtags', []
            )
        if users_mentioned_in_message:
            message_ids.append(message_id)

    return message_ids


def compute_avg_number_of_tweets_with_hastags(messages):
    n_tweets_with_hashtags = len(get_messages_with_hashtags(messages))
    total_number_of_tweets = len(messages)

    return n_tweets_with_hashtags / total_number_of_tweets


def compute_ratio_of_directed_and_nondirected_tweets(messages):
    """Computes ratio of directed tweets for a user.
    This provides an idea about the role she plays in the spread
    of information.

    Arguments:
        user_id {str} -- User ID

    Returns:
        float -- user's ratio of directed tweets
    """
    n_dv = len(get_messages_with_user_mentions(messages))
    n_ndv = len(get_messages_without_user_mentions(messages))

    if n_ndv:
        return n_dv / n_ndv
    return 0


def get_messages_with_urls(messages):
    message_ids = []

    for message_id, message in messages.items():
        users_mentioned_in_message = message.get('entities', {}).get(
            'urls', [])

        if users_mentioned_in_message:
            message_ids.append(message_id)

    return message_ids


def compute_avg_url_per_tweet(messages):
    n_tweets_with_url = len(get_messages_with_urls(messages))
    total_number_of_tweets = len(messages)

    return n_tweets_with_url / total_number_of_tweets


def get_messages_with_media(messages):
    message_ids = []

    for message_id, message in messages.items():
        users_mentioned_in_message = message.get(
            'extended_entities', {}).get('entities', {}).get('media', [])

        if users_mentioned_in_message:
            message_ids.append(message_id)

    return message_ids


def compute_avg_tweet_with_media(messages):
    n_tweets_with_media = len(get_messages_with_media(messages))
    total_number_of_tweets = len(messages)

    return n_tweets_with_media / total_number_of_tweets


def compute_ratio_of_follower_to_friends(user):
    number_of_followers = user.get('followers_count', 0)
    number_of_friends = user.get('friends_count', 0)

    if not number_of_friends:
        return 0

    return number_of_followers / number_of_friends


def compute_ratio_of_favourited_to_tweet(messages, user):
    number_of_favorited_tweets = user.get('favourites_count', 0)
    total_number_of_tweets = len(messages)

    return number_of_favorited_tweets / total_number_of_tweets


def compute_user_has_url(user):
    user_url = user.get('url')

    if user_url is None:
        return 0
    return 1


def compute_user_has_description(user):
    user_description = user.get('description')

    if user_description is None:
        return 0
    return 1


def compute_user_is_verified(user):
    user_verified = user.get('verified', False)

    if user_verified:
        return 1
    return 0


def compute_avg_number_of_followers(user):
    n_followers = user.get('followers_count', 0)

    return n_followers / 707


def compute_avg_number_friends(user):
    n_friends = user.get('friends_count', 0)

    return n_friends / 707


def compute_status_count(user):
    statuses_count = user.get('statuses_count', 0)

    return statuses_count


def compute_user_account_age(user):
    created_date = parse(user['created_at'])
    current_date = datetime.datetime.now(datetime.timezone.utc)

    diff = current_date - created_date

    return diff.days


def compute_avg_tweet_per_day(user):
    account_age = compute_user_account_age(user)
    statuses_count = user.get('statuses_count', 0)

    return statuses_count / account_age


def compute_info_features(*args):
    features = {}

    src_user_messages = args[0]
    dest_user_messages = args[1]
    src_user_profile = args[2]
    dest_user_id = args[3]

    features['info_1'] = compute_social_homogeneity(src_user_messages,
                                                    dest_user_messages)

    features['info_2'] = compute_ratio_of_directed_tweets(src_user_messages)

    features['info_3'] = compute_active_interaction(src_user_messages,
                                                    dest_user_id)

    features['info_4'] = compute_ratio_of_retweets_to_tweets(src_user_messages)

    features['info_5'] = compute_avg_number_of_tweets_with_hastags(
        src_user_messages)

    features['info_6'] = compute_ratio_of_directed_and_nondirected_tweets(
        src_user_messages)

    features['info_7'] = compute_avg_url_per_tweet(src_user_messages)

    features['info_8'] = compute_avg_tweet_with_media(src_user_messages)

    features['info_9'] = compute_ratio_of_follower_to_friends(src_user_profile)

    features['info_10'] = compute_ratio_of_favourited_to_tweet(
        src_user_messages, src_user_profile)

    return features


def compute_quoted_status(message):
    quoted_status = message.get('quoted_status', 0)

    if quoted_status:
        return 1
    return quoted_status


def compute_has_post_been_RTd(message):
    retweet_count = message.get('retweet_count', 0)

    return retweet_count


def compute_has_hashtags(message):
    hashtag = message.get('entities', {}).get('hashtags', [])

    if hashtag:
        return len(hashtag)
    return 0


def compute_has_url(message):
    urls = message.get('entities', {}).get('urls', [])

    if urls:
        return len(urls)
    return 0


def compute_has_mentions(message):
    user_mentions = message.get('entities', {}).get('user_mentions', [])

    if user_mentions:
        return len(user_mentions)
    return 0


def compute_has_media(message):
    media = message.get('extended_entities', {}).get('entities', {}).get(
        'user_mentions', [])

    if media:
        return len(media)
    return 0


def compute_tweet_length(message):
    n_words = len(utils.tokenise_tweet(message.get('text', '')))
    return n_words / 280


def compute_retweet_status(message):
    if 'retweeted_status' in message:
        return 1
    return 0


def compute_user_features(user_profile=None, label='src'):
    features = {}

    features[f'{label}_user_1'] = compute_user_has_url(user_profile)
    features[f'{label}_user_2'] = compute_user_has_description(user_profile)
    features[f'{label}_user_3'] = compute_user_is_verified(user_profile)
    features[f'{label}_user_4'] = compute_avg_number_of_followers(user_profile)
    features[f'{label}_user_5'] = compute_avg_number_friends(user_profile)
    features[f'{label}_user_6'] = compute_status_count(user_profile)
    features[f'{label}_user_7'] = compute_user_account_age(user_profile)
    features[f'{label}_user_8'] = compute_avg_tweet_per_day(user_profile)

    return features


def compute_message_features(message):
    features = {}

    features['message_feat_1'] = compute_quoted_status(message)
    features['message_feat_2'] = compute_has_post_been_RTd(message)
    features['message_feat_3'] = message.get('retweet_count', 0)
    features['message_feat_4'] = message.get('favorite_count', 0)
    features['message_feat_5'] = compute_has_hashtags(message)
    features['message_feat_6'] = compute_has_url(message)
    features['message_feat_7'] = compute_has_mentions(message)
    features['message_feat_8'] = compute_has_media(message)
    features['message_feat_9'] = compute_tweet_length(message)
    features['message_feat_10'] = compute_retweet_status(message)

    return features


def compute_dots_features(message=None):
    features = {}

    features['dots_1'] = message.get('sentiment', {}).get('positive')
    features['dots_2'] = message.get('sentiment', {}).get('negative')
    features['dots_3'] = message.get('sentiment', {}).get('neutral')
    features['dots_4'] = message.get('emotion', {}).get('Happy')
    features['dots_5'] = message.get('emotion', {}).get('Fear')
    features['dots_6'] = message.get('emotion', {}).get('Sad')
    features['dots_7'] = message.get('emotion', {}).get('Angry')
    features['dots_8'] = message.get('emotion', {}).get('Bored')

    feedback = message.get('intent', {}).get('feedback')
    if isinstance(feedback, dict):
        features['dots_9'] = feedback.get('score')
    else:
        features['dots_9'] = feedback
    features['dots_10'] = message.get('intent', {}).get('news')
    features['dots_11'] = message.get('intent', {}).get('query')
    features['dots_12'] = message.get('intent', {}).get('spam')
    features['dots_13'] = message.get('intent', {}).get('marketing')
    features['dots_14'] = message.get('abuse', {}).get('abusive')
    features['dots_15'] = message.get('abuse', {}).get('hate_speech')
    features['dots_16'] = message.get('abuse', {}).get('neither')

    return features


def compute_y(user_profile, message):
    original_tweeter_id = message.get('retweeted_status', {}).get(
        'user', {}).get('id_str', '')
    user_id = user_profile.get('id_str', '')

    if original_tweeter_id == user_id:
        return 1
    return 0
