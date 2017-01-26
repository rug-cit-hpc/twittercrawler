# -*- coding: utf-8 -*-
import sys
import time
import tweepy
import argparse

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

search_by = {
    'timeline': {'api': api.user_timeline, 'limit': ('statuses', '/statuses/user_timeline')},
    'search'  : {'api': api.search, 'limit': ('search', '/search/tweets')},
}

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--type',
                        help='REQUIRED: type of crawl, either \'timeline\' (input parameters: usernames), \'query\' (input parameter: search query), \'streaming\' (input parameters: start time, end time, keywords)',
                        required=True)
    parser.add_argument('-f',
                        '--format',
                        help='output format, either \'raw\' (default) or \'tsv\'',
                        default='raw')
    parser.add_argument('-c',
                        '--columns',
                        help='output columns for the tsv file, separated by spaces',
                        default=['id'],
                        nargs='+')
    parser.add_argument('-o',
                        '--output',
                        help='output file. Default: standard output',
                        default='stdout')
    parser.add_argument('parameters', metavar='search_parameter', type=str, nargs='+',
                    help='Input parameter for the crawler (e.g. user, query  or keyword). Separate multiple items by spaces.')
    args = parser.parse_args()
    return args

def limit_handled(cursor, method):
    limit_params = search_by[method]['limit']
    while True:
        if api.rate_limit_status()['resources'][limit_params[0]][limit_params[1]]['remaining'] < 5:
            time.sleep(15*60)
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)

def crawl(method, **params):
    tweets = []
    for page in limit_handled(tweepy.Cursor(search_by[method]['api'], **params).pages(), method):
        tweets.extend(page)
    
    return tweets

def by_timeline(screen_names):
    tweets = []
    for screen_name in screen_names:
        tweets.extend(crawl('timeline', screen_name=screen_name))
    
    return tweets

def by_search(query):
    return crawl('search', q=query)

def write_output(tweets, filename, format, columns):
    tweets_out = []
    if format == 'raw':
        tweets_out = [str(tweet._json) for tweet in tweets]
    elif format == 'tsv':
        tweets_out = ['\t'.join([str(getattr(tweet, col)) for col in cols]) for tweet in tweets]
    
    result = '\n'.join(tweets_out)
    
    if filename == 'stdout':
        print(result)
    else:
        with open(filename, 'w') as outfile:
            outfile.write(result)

if __name__ == '__main__':
    args = parse_args()
    tweets = locals()['by_'+args.type](args.parameters)
    write_output(tweets, args.output, args.format, args.columns)

