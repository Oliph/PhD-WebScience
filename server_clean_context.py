#!/usr/bin/env python
# encoding: utf-8
from pymongo import MongoClient

import configparser as ConfigParser

from logger import logger

from server_context import contextTransforming

logger = logger('server_context', stream_level='INFO')

DB_HOST = 'localhost'
DB_NAME = '18_april_astro'


def read_config(config_file):
    Config = ConfigParser.ConfigParser()
    Config.read(config_file)
    return_values = dict()
    for section in Config.sections():
        for option in Config.options(section):
            return_values[option] = Config.get(section, option)
    return return_values


def get_lvl1(lvl1_file):
    logger.info('Getting the lvl1 file - {}'.format(lvl1_file))
    with open(lvl1_file, 'r') as f:
        return [int(line[:-1]) for line in f]


def connectDB(**kwargs):
    """ """
    c = MongoClient(kwargs['host'])
    db = c[kwargs['db']]
    return db


def list_or_tuple(x):
    return isinstance(x, (list, tuple))


def flatten(sequence, to_expand=list_or_tuple):
    for item in sequence:
        if to_expand(item):
            for subitem in flatten(item, to_expand):
                yield subitem
        else:
            yield item

config_file = './config/config.ini'
values = read_config(config_file)
db = connectDB(db=DB_NAME, host='10.7.4.164')
logger.info('Connect to db: {}'.format(db))
TYPE_ACTIVITY = ['statuses_count', 'friends_count', 'followers_count']
db_activity = db['activity']
context_db = db['context']
db_full_link = db['full_link']
set_lvl1 = set(get_lvl1(values['lvl1_file']))
contextClass = contextTransforming(db, set_lvl1)
n = 0
while True:

    for record in context_db.find({'raw_count': {'$exists': False}}).batch_size(5):
        list_size = False
        list_total_found = False
        list_abs_activity = False
        list_total = False
        user_activity = False
        user_profile = False
        to_update = dict()
        to_update['loop_number'] = record['loop_number']
        to_update['id_str'] = record['id_str']
        to_update['type_link'] = record['type_link']
        try:
            to_update['list_size'] = record['size_list']
        except KeyError:
            list_size = True
        try:
            to_update['list_total_found'] = record['total_user_found']
        except KeyError:
            list_total_found = True
        for k in TYPE_ACTIVITY:
            try:
                to_update['list_abs_activity_{}'.format(k)] = record['raw_{}'.format(k)]
            except KeyError:
                list_abs_activity = True
            try:
                to_update['list_total_{}'.format(k)] = record['total_in_list_{}'.format(k)]
            except KeyError:
                list_total = True
            try:
                to_update['user_activity_{}'.format(k)] = record['activity_{}'.format(k)]
            except KeyError:
                user_activity = True
            try:
                to_update['user_profile_{}'.format(k)] = record['total_user_profile_{}'.format(k)]
            except KeyError:
                user_profile = True

        if record['id_str'] in set_lvl1:
            activity = db_activity.find_one({'id_str': record['id_str'], 'loop_number': record['loop_number']})

            if user_activity is True:
                to_update = contextClass.get_activity(to_update)
            if user_profile is True:
                to_update = contextClass.inc_activity(to_update['id_str'], to_update['loop_number'], to_update)

            if list_size or list_total_found or list_abs_activity or list_total:
                record_list_link = db_full_link.find_one({'id_str': record['id_str'],
                                                          'loop_number': record['loop_number'],
                                                          'type_link': record['type_link']})
                list_link = list()
                for x in flatten(record_list_link['list']):
                    list_link.append(x)

            if list_size is True:
                to_update['list_size'] = len(list_link)

            for user in list_link:

                user_activity = contextClass.get_activity_user(int(user), record['loop_number'] -1)

                if user_activity is True:

                    if list_total_found:
                        to_update = contextClass.inc_total_user(to_update)

                    if list_abs_activity is True:
                        to_update = contextClass.get_raw_activity(to_update, int(user_activity))

                    if list_total is True:
                        to_update = contextClass.get_total_link(to_update, int(user_activity))

            unset_dict = {k: '' for k in [i for i in record if i not in to_update and i != '_id']}

            logger.info(to_update)
            if unset_dict:
                context_db.update({'_id': record['_id']}, {'$set': to_update, '$unset': unset_dict})
            else:
                context_db.update({'_id': record['_id']}, {'$set': to_update})
        n += 1
        logger.info('Record done: {}'.format(n))
