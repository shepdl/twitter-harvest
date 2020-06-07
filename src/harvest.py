# coding=utf8
import sqlite3
from src.client_pool import ClientPool
import time
import codecs
import datetime
import simplejson as json
import json
import argparse
import logging

import config

from twitter.api import TwitterHTTPError


def write_config(query, config, output):
    config_file = codecs.open(
        '{}-starting-{}.conf'.format(
            output, datetime.datetime.now().isoformat().replace(':', 's')
        ), 
        'w', encoding='utf8'
    )

    conf_object = {
        'output_file': '{}.db'.format(output),
        'query_params': query,
        # 'config': config,
    }

    json.dump(conf_object, config_file)
    config_file.close()


def harvest(query, harvest_output, direction='backwards'):

    db = sqlite3.connect('{}.db'.format(harvest_output))

    cursor = db.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS tweets 
        (id PRIMARY KEY, twitter_id TEXT, body TEXT, username TEXT, sent_at TEXT, json TEXT)
    ''')

    db.commit()

    write_config(query, config, harvest_output)

    client_pool = ClientPool(config.CONFIG)

    logger = logging.getLogger('twitter-harvester')
    logging.basicConfig(
        filename='{}-harvest.log'.format(harvest_output), filemode='a',
        level=logging.INFO,
        format='[%(levelname): %(asctime)] -- %(message)s'
    )

    next_id = None
    if direction == 'backwards':
        row = cursor.execute('SELECT MIN(twitter_id) AS twitter_id FROM tweets').fetchone()[0]
    else:
        row = cursor.execute('SELECT MAX(twitter_id) AS twitter_id FROM tweets').fetchone()[0]
    if row is not None:
        logger.info('Found {} in database'.format(row))
        next_id = row

    insert_query = '''INSERT INTO tweets (twitter_id, body, username, sent_at, json) VALUES (?,?,?,?,?)'''

    error_count = 0 
    while True:
        params = query
        params['count'] = 100
        params['lang'] = 'en'
        if direction == 'backwards':
            params['max_id'] = next_id
        else:
            params['since_id'] = next_id
        try:
            available = client_pool.available_client()
            if not available.available:
                while True:
                    time.sleep(available.time_to_wait)
                    available = client_pool.available_client()
                    if available.available:
                        break
            twitter = available.client.to_twitter_client()
            tweets = twitter.search.tweets(**params)
        except IOError as ex:
            # The Twitter API can be flaky; just try again later.
            logger.info("IOError: %s" % (ex))
            continue
        except TwitterHTTPError as ex:
            print(ex.e.code)
            if ex.e.code == 429:
                client_pool.invalidate()
            logger.info(ex)
            continue
        if "statuses" not in tweets or len(tweets["statuses"]) == 0:
            error_count += 1
            if error_count > 4:
                logger.info('Believe tweets are exhausted; exiting ...')
                break
            continue

        error_count = 0
        tweet_data = []
        id_comparator = min if direction == 'backwards' else max
        next_id = id_comparator([t['id_str'] for t in tweets['statuses']])
        max_date = min([t['created_at'] for t in tweets['statuses']])
        for tweet in tweets['statuses']:
            tweet_data.append((
                tweet['id_str'],
                tweet['text'],
                tweet['user']['screen_name'],    
                tweet['created_at'],
                json.dumps(tweet)
            ))
        logger.info("Current ID: {} at {}".format(next_id, max_date))
        cursor.executemany(insert_query, tweet_data)
        db.commit()

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Do initial sampling of tweets')
    parser.add_argument('-q', '--query', action='store', help='Query string to search for. Will be URL-encoded.')
    parser.add_argument('-o', '--output', action='store', help='Database file to write to. Will be created if it does not exist, and will be added to if it does.', required=True)
    parser.add_argument('-g', '--geo', action='store', help="Lon,Lat,Radius. If you need to enter a negative longitude, prefix it with a '\\' -- this is becuase of a limitation in Python\'s argparse library")
    parser.add_argument('-d', '--direction', action='store', help="Direction in which to harvest: 'forwards' or 'backwards'. Default is backwards.", default='backwards')
    parser.add_argument('-u', '--until', action='store', help='Latest date to get tweets from')
    args = parser.parse_args()
    parameters = {}
    if args.query:
        parameters['q'] = args.query
    
    if args.geo:
        geo = args.geo
        if geo[0] == '\\':
            geo = geo[1:]
        parameters['geocode'] = geo
    direction = args.direction
    if args.until:
        parameters['until'] = args.until

    harvest(parameters, args.output, direction)
