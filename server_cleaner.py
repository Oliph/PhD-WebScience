#!/usr/bin/env python
# encoding: utf-8

"""
Cleaner that parse the db and switch the "doing" into "todo" when the laps of time
is cleary due to an error from one client
"""

from pymongo import MongoClient
import configparser as ConfigParser
import datetime
import time

# Logging
from logger import logger
logger = logger(name='server_cleaner', stream_level='INFO', file_level='ERROR')


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


def queryDB(db):
    """ """
    for result in db.find({'doing': {'$exists': True}}):
        if check_values(result):
            update_value(db, result['_id'])


def check_values(result):
    """ """
    time_done = result['doing']
    laps = datetime.datetime.now() - time_done
    if laps.seconds >= 60* 20:  # 20 minutes
        logger.info('{}'.format(result))
        return True


def update_value(db, _id):
    db.update({'_id': _id}, {'$unset': {'doing': '', 'client_id': ''}})


def main():
    """ """
    config_file = './config/config.ini'

    values = read_config(config_file)
    db = connectDB(db=values['db_name'], host=values['host'])
    logger.info('Connect to db: {}'.format(db))
    while True:
        logger.info('New Check')
        queryDB(db['process_profile'])
        queryDB(db['process_link'])
        queryDB(db['process_tweet'])
        time.sleep(60* 20)


if __name__ == '__main__':
    main()
