#!/usr/bin/env python
# encoding: utf-8
from pymongo import MongoClient

import configparser as ConfigParser

from logger import logger

logger = logger('server_context', stream_level='INFO')

DB_HOST = '127.0.0.1'
DB_NAME = '18_april_astro'


def read_config(config_file):
    Config = ConfigParser.ConfigParser()
    Config.read(config_file)
    return_values = dict()
    for section in Config.sections():
        for option in Config.options(section):
            return_values[option] = Config.get(section, option)
    return return_values


def connectDB(**kwargs):
    """ """
    c = MongoClient(kwargs['host'])
    db = c[kwargs['db']]
    return db


def get_lvl1(lvl1_file):
    logger.info('Getting the lvl1 file - {}'.format(lvl1_file))
    with open(lvl1_file, 'r') as f:
        return [int(line[:-1]) for line in f]


def check_loop():

    for loop_number in db.activity.distinct('loop_number'):
        count_per_loop = db.activity.find({'loop_number': loop_number}).count()
        print('{}:{}'.format(loop_number, count_per_loop))

config_file = './config/config.ini'
values = read_config(config_file)
db = connectDB(db=DB_NAME, host='127.0.0.1')
logger.info('Connect to db: {}'.format(db))
TYPE_ACTIVITY = ['statuses_count', 'friends_count', 'followers_count']
db_activity = db['activity']
db_context = db['context']
db_full_link = db['full_link']
set_lvl1 = set(get_lvl1(values['lvl1_file']))


loop_list = list()
for loop_number in db.activity.distinct('loop_number'):
    loop_list.append(loop_number)
dict_loop = dict()
for loop in loop_list:
    n=0
    m =0
    for record in db_full_link.find({'loop_number': loop}):
        if record['id_str'] in set_lvl1:
            activity = db.activity.find_one({'id_str': record['id_str'], 'loop_number': record['loop_number']})
            # print(activity)
            if activity:
                n +=1
            m +=1
    dict_loop[loop] = {'Total': m, 'Found': n}
    print(loop, dict_loop[loop])
print(dict_loop)
