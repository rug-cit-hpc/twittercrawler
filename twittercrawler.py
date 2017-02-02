# -*- coding: utf-8 -*-
import time
import tweepy
import argparse
import local
import json

# create OAuthHandler for authentication process
auth = tweepy.OAuthHandler(local.consumer_key, local.consumer_secret)
auth.set_access_token(local.access_token, local.access_token_secret)

# Create instance of the tweepy api with the created authenticator
api = tweepy.API(auth)

# Dict that stores parameters for later use in function calls
search_by = {
    'timeline': {'api': api.user_timeline, 'limit': ('statuses', '/statuses/user_timeline')},
    'search': {'api': api.search, 'limit': ('search', '/search/tweets')},
}


# Parse arguments given to the python command
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


# Cursor method implemented to deal with request limits - 15 minute rests if limits are reached
def limit_handled(cursor, method):
    limit_params = search_by[method]['limit']
    while True:
        if api.rate_limit_status()['resources'][limit_params[0]][limit_params[1]]['remaining'] < 5:
            time.sleep(15*60)
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)


# Crawl through the tweets in sets of tweets called pages and collect the total set of tweets
def crawl(method, **params):
    tweets = []
    for page in limit_handled(tweepy.Cursor(search_by[method]['api'], **params).pages(), method):
        tweets.extend(page)
    
    return tweets


# Crawl va a set of account names
def by_timeline(screen_names):
    tweets = []
    for screen_name in screen_names:
        tweets.extend(crawl('timeline', screen_name=screen_name))
    
    return tweets


# Crawl via query
def by_search(query):
    return crawl('search', q=query)


# Output the tweets in the requested format
def write_output(tweets, filename, format, columns):
    tweets_out = []
    if format == 'raw':
        tweets_out = [json.dumps(tweet._json) for tweet in tweets]
    elif format == 'tsv':
        tweets_out = ['\t'.join([str(getattr(tweet, col)) for col in columns]) for tweet in tweets]
    
    result = '{"output": {"tweets": [' + ',\n'.join(tweets_out) + ']}}'
    
    if filename == 'stdout':
        print(result)
    else:
        with open(filename, 'w') as outfile:
            outfile.write(result)


# Main function
if __name__ == '__main__':
    args = parse_args()
    tweets = locals()['by_'+args.type](args.parameters)  # Call local crawl function based on crawl type
    write_output(tweets, args.output, args.format, args.columns)


def run_twittercrawler(type, *params):
    tweets = None
    if type == 'timeline':  # Call specific crawl function based on type
        tweets = by_timeline(params[0])
    elif type == 'search':
        tweets = by_search(params[0])
    elif type == 'streaming':
        print('Streaming functionality not yet implemented')
        return None

    return [tweet._json for tweet in tweets]
