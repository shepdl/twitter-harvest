from src.client_pool import ClientPool
import twitter
from twitter.api import TwitterHTTPError
import os
import config
import datetime
import codecs
import time
import sqlite3
import simplejson as json
import sys


LIMIT_DATE = datetime.datetime.strptime('{} +0000'.format(sys.argv[2]), '%Y-%m-%d %z')

STORAGE_FILE = sys.argv[1]


def storage_path_file(path):
    return '{}/{}'.format(STORAGE_FILE, path)


all_users = set([line.strip() for line in open(storage_path_file('all-users.txt')).readlines()])
already_found = set()
if os.path.exists(storage_path_file('found-users.txt')):
    already_found = set([line.strip() for line in open(storage_path_file('found-users.txt')).readlines()])


users_to_find = all_users - already_found

print("From the original set of {} users, {} remain.".format(len(all_users), len(users_to_find)))

user_data_file = codecs.open(storage_path_file('user-data.txt'), 'a')

current_path = storage_path_file('current-set')
try:
    os.makedirs(current_path)
except FileExistsError:
    pass


client_pool = ClientPool(config.CONFIG)

for user_entry in users_to_find:
    screen_name = user_entry.strip()
    
    print("Harvesting tweets by {} ...".format(screen_name))

    db = sqlite3.connect(storage_path_file('current-set/{}.db'.format(screen_name)))
    try:
        available = client_pool.available_client()
        if not available.available:
            while True:
                time.sleep(available.time_to_wait)
                available = client_pool.available_client()
                if available.available:
                    break
        twitter = available.client.to_twitter_client()
        result = twitter.statuses.user_timeline(
            screen_name=screen_name,
            count=200,
            include_rts=1
        )
    except ConnectionResetError:
        pass
    except TwitterHTTPError as ex:
        if ex.e.code == 401:
            print("""Skipping {} because tweets are private.""".format(screen_name))
            continue
        if ex.e.code == 404:
            print("""Skipping {} because not found.""".format(screen_name))
            continue
        elif ex.e.code == 88:
            print('Hit rate limit ...')
            available.client.remaining_requests = 0
            while True:
                available = client_pool.available_client()
                if available.available:
                    result = twitter.statuses.user_timeline(
                        screen_name=screen_name,
                        count=200,
                        include_rts=1
                    )
                    break
        else:
            print(ex)

    found_user_data = False

    last_tweet_date = None
    last_tweet_id = None
    ids_found = set()
    cursor = db.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS tweets (id PRIMARY KEY, original TEXT)""")
    while True:
        if len(result) < 2: # max id always returns 1, and I'd rather not have to parse it out
            break
        insert_start = datetime.datetime.now()
        for status in result:
            if not found_user_data:
                try:
                    user_data_file.write('{}\n'.format(json.dumps(result[0]['user'])))
                    found_user_data = True
                except:
                    pass
            if 'user' in status:
                del status['user']
            tweet_id = status['id_str']
            if tweet_id not in ids_found:
                cursor.execute("""INSERT INTO tweets (original) VALUES (?)""", 
                    (json.dumps(status),) 
                )
            last_tweet_id = status['id_str']
            last_tweet_date = datetime.datetime.strptime(
                status['created_at'],
                '%a %b %d %H:%M:%S %z %Y'
            )
            ids_found.add(tweet_id)
        insert_end = datetime.datetime.now()
        if last_tweet_date is None or last_tweet_date <= LIMIT_DATE:
            break
        try:
            available = client_pool.available_client()
            if not available.available:
                time.sleep(available.time_to_wait)
                while True:
                    available = client_pool.available_client()
                    if available.available:
                        break
            twitter = available.client.to_twitter_client()
            result = twitter.statuses.user_timeline(
                screen_name=screen_name,
                count=200,
                include_rts=1,
                # trim_user=True,
                max_id=last_tweet_id
            )

        except TwitterHTTPError as ex:
            if ex.e.code == 130:
                print("""Skipping {} because tweets are private.""".format(screen_name))
                continue
            elif ex.e.code == 88:
                print('Over rate limit; sleeping ...')
                time.sleep(15 * 60)
            elif ex.e.code == 401:
                print('User not authorized; skipping ...')
                break
            else:
                print(ex)
        insert_time = insert_end - insert_start

    db.commit()
    cursor.close()
    db.close()
    with open(storage_path_file('found-users.txt'), 'a') as found_users:
        found_users.write('{}\n'.format(screen_name))

