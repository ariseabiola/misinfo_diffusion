import tweepy
from twitterscraper import query_tweets


def auth(consumer_key, consumer_secret, access_token, access_token_secret):
    """Authentication handler to Twitter API.

    Arguments:
        consumer_key {str} -- consumer token
        consumer_secret {str} -- consumer token secret
        access_token {str} -- access token
        access_token_secret {str} -- access token secret

    Returns:
        tweepy.API -- a wrapper for the API as provided by Twitter
    """
    auth_ = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth_.set_access_token(access_token, access_token_secret)
    return tweepy.API(
        auth_,
        wait_on_rate_limit=True,
        wait_on_rate_limit_notify=True,
        retry_count=3,
        retry_delay=5,
        retry_errors=set([401, 404, 500, 503]))


def scrap(query=None, limit=-1):
    """scrape for Tweets matching a specified query.

    Keyword Arguments:
        query {[type]} -- [description] (default: {None})
        limit {int} -- specifies the least number of tweets to scrap before
        stopping. Tweets are retrieved in batches of 20, this will
        always be a multiple of 20. set the limit to -1 to retrieve all tweets.
        You can at any time abort the scraping by pressing Ctrl+C,
        the scraped tweets will be returned. (default: {-1})

    Returns:
        list -- list of scraped tweets
    """
    if not limit:
        tweets = query_tweets(query)
    else:
        tweets = query_tweets(query, limit)
    return tweets


def get_retweets(api=None, tweet_id=None, count=None):
    """Returns up to 100 of the first retweets of the given tweet.

    Keyword Arguments:
        api {tweepy.api.API} -- A wrapper for the API as provided by Twitter.
        (default: {None})
        tweet_id {str} -- Specifies the ID of a tweet. (default: {None})
        count {int} -- Specifies the number of retweets to fetch.
        (default: {None})

    Returns:
        list -- list of Status objects
    """
    retweets = None

    try:
        if count is None:
            retweets = api.retweets(tweet_id)
        else:
            retweets = api.retweets(tweet_id, count)
    except tweepy.error.TweepError:
        raise

    return retweets


def get_tweet(api=None, tweet_id=None):
    """Returns a single status specified by the ID parameter.

    Keyword Arguments:
        api {tweepy.api.API} -- A wrapper for the API as provided by Twitter.
        (default: {None})
        tweet_id {str} -- Specifies the ID of a tweet. (default: {None})

    Returns:
        tweepy.models.Status -- Status object
    """
    status = None

    try:
        status = api.get_status(tweet_id)
        status = status._json
    except tweepy.error.TweepError:
        raise

    return status
