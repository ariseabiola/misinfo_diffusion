import tweepy
from twitterscraper import query_tweets


def auth(consumer_key, consumer_secret, access_token, access_token_secret):
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
    if not limit:
        tweets = query_tweets(query)
    else:
        tweets = query_tweets(query, limit)
    return tweets


def get_retweets(api=None, tweet_id=None, count=None):
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
    status = None

    try:
        status = api.get_status(tweet_id)
        status = status._json
    except tweepy.error.TweepError:
        raise

    return status
