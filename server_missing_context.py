#!/usr/bin/env python
# encoding: utf-8
import time
from pymongo import MongoClient

import configparser as ConfigParser

from logger import logger

# from server_context import contextTransforming

logger = logger('check', stream_level='INFO')

DB_HOST = 'localhost'
DB_NAME = '10_may_astro'


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


def get_nb_lvl1(db, collection, set_lvl1, loop_number=None):
    """
    Count the number of lvl1 per collection
    """
    dict_loop = dict()
    n = 0
    if loop_number:
        search = db[collection].find({'loop_number': loop_number}, {'loop_number': 1, 'id_str': 1})
    else:
        loop = sorted(db[collection].distinct('loop_number'))
        return get_nb_lvl1(db, collection, set_lvl1, loop[0])
    for record in search:
        if record['id_str'] in set_lvl1:
            dict_loop[record['loop_number']] = dict_loop.get(record['loop_number'], 0)+ 1
        n+=1
    return dict_loop


def check_nb_lvl1(db, set_lvl1, loop):

    process_link_dict = get_nb_lvl1(db, 'context', set_lvl1, loop)
    try:
        total_context = process_link_dict[loop]
    except KeyError:
        total_context = 0

    context_link_dict = get_nb_lvl1(db, 'context_link', set_lvl1, loop)
    try:
        total_context_link = context_link_dict[loop]
    except KeyError:
        total_context_link = 0

    full_link = get_nb_lvl1(db, 'full_link', set_lvl1, loop)
    try:
        total_full_link = full_link[loop]
    except KeyError:
        total_full_link = 0

    total = total_context + total_context_link
    logger.info('Loop: {} - Context: {} - Context link: {} - Full link: {} - Total: {}'.format(loop,
                                                                                               total_context,
                                                                                               total_context_link,
                                                                                               total_full_link,
                                                                                               total))
    return total_context, total_context_link


def process_loop(db, set_lvl1, loop, previous_total, mode):

    total_process = 0
    process_link_dict = get_nb_lvl1(db, 'process_link', set_lvl1)
    for k in process_link_dict:
        loop = k
        total_process = process_link_dict[k]
    if bool(process_link_dict) is False:
        return previous_total

    total_context = 0
    context_link_dict = get_nb_lvl1(db, 'context_link', set_lvl1, loop)
    for k in context_link_dict:
        total_context = context_link_dict[k]

    total_current = 0
    current_link = get_nb_lvl1(db, 'current_link', set_lvl1, loop)
    for k in current_link:
        total_current = current_link[k]

    total = total_process + total_context + total_current
    if total != previous_total:
        logger.info('{} - Loop: {} - Process: {} - Context link: {} - Current link: {} - Total: {}'.format(mode, loop, total_process, total_context, total_current, total))
    return total


def process_loop_context(db, set_lvl1, loop, previous_total, mode):
    """
    """
    total_context_link = 0
    context_link_dict = get_nb_lvl1(db, 'context_link', set_lvl1)
    for k in context_link_dict:
        loop = k
        total_context_link = context_link_dict[k]
    if bool(context_link_dict) is False:
        return previous_total

    total_context_stored = 0
    context_stored_dict = get_nb_lvl1(db, 'context', set_lvl1, loop)
    for k in context_stored_dict:
        total_context_stored = context_stored_dict[k]

    total_full_link = 0
    context_full_link = get_nb_lvl1(db, 'full_link', set_lvl1, loop)
    for k in context_full_link:
        total_full_link = context_full_link[k]

    total = total_context_link + total_context_stored
    if total != previous_total:
        logger.info('{} - Loop: {} - Context_link: {} - Context Stored: {} - Total: {} - Full link: {}'.format(mode, loop, total_context_link,
                                                                                                               total_context_stored, total,
                                                                                                               total_full_link))
    return total


def main():
    """
    """
    config_file = './config/config.ini'
    values = read_config(config_file)
    db = connectDB(db=DB_NAME, host='10.7.4.164')
    logger.info('Connect to db: {}'.format(db))
    # TYPE_ACTIVITY = ['statuses_count', 'friends_count', 'followers_count']
    # db_activity = db['activity']
    # db_context = db['context']
    # db_context_link = db['context_link']
    # db_full_link = db['full_link']
    # db_process_link = db['process_link']
    # db_change_link = db['change_link']
    # db_current_link = db['current_link']
    set_lvl1 = set(get_lvl1(values['lvl1_file']))
    loop = 0
    previous_total_current = 0
    previous_total_context = 0
    while True:
        previous_total_current = process_loop(db, set_lvl1, loop, previous_total_current, 'Current')
        previous_total_context = process_loop_context(db, set_lvl1, loop, previous_total_context, 'Context')

        time.sleep(5)
    # get_nb_lvl1(db, 'context', set_lvl1)
    # get_nb_lvl1(db, 'full_link', set_lvl1)
if __name__ == "__main__":
    main()
