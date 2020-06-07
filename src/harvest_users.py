import glob
import os
import sqlite3
import codecs


users = set()
for in_dirname in glob.glob('./*'):
    if os.path.isdir(in_dirname):
        dir_name = in_dirname.split('/')[-1]
        in_filename = os.path.join(dir_name, '{}.db'.format(dir_name))
        print('Processing {}'.format(in_filename))
        with sqlite3.connect(in_filename) as conn:
            user_cursor = conn.cursor()
            for row in user_cursor.execute('SELECT username FROM tweets'):
                users.add(row[0])

with codecs.open('all-users.txt', 'w', encoding='utf-8') as out_file:
    for user in users:
        out_file.write('{}\n'.format(user))
