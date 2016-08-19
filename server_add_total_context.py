#!/usr/bin/env python
# encoding: utf-8

"""
One shot script to run to add the total amount of followers/friends/statuses count for each list found on the
context collection.
COpy past the main function found in context
"""

# import pymongo
from pymongo import MongoClient

import configparser as ConfigParser

from logger import logger

logger = logger('server_context', stream_level='INFO')

DB_HOST = 'localhost'
DB_NAME = '18_april_astro'
config_file = './config/config.ini'
values = read_config(config_file)
db = connectDB(db=DB_NAME, host=DB_HOST)
db_context = db['context']
db_full_link = db['full_link']
db_activity = db['activity']
db_profile = db['stored_profile']
logger.info('Connect to db: {}'.format(db))
TYPE_ACTIVITY = ['statuses_count', 'friends_count', 'followers_count']


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


def inc_activity(id_str, loop_number, db_profile, db_activity, input_dict=dict(), output=True):
    """
    Calculate from the loop_number the right amount of counts
    Get the different loop since it is recorded in stored_profile
    and increment or decrement the value found in activity for these
    different loops
    """
    count_profile = db_profile.find_one({'id_str': id_str})
    loop_profile = count_profile['loop_number']
    for k in TYPE_ACTIVITY:
        input_dict['total_user_profile_{}'.format(k)] = count_profile['{}'.format(k)]
        if output is True:
            logger.info('Initial value of the {}: {}'.format(k, input_dict['total_user_profile_{}'.format(k)]))
    # If the loop_context and loop_profile are equal no need anything to remove
    # or add
    if loop_number != loop_profile:
        # Generate the number of loop between the stored profile and the context loop
        for loop in sorted([i for i in range(loop_number, loop_profile)], reverse=True):
            activity = db_activity.find_one({'loop_number': loop, 'id_str': id_str})
            # Then loop through it to find the past activity to remove or add it to calculate
            # the actual count
            for k in TYPE_ACTIVITY:
                # Do =- instead of =+ because need to invert (one added after mean the previous
                # score had one less
                # The record may not exists because no previous record (not a problem)
                # or because it was not collected the previous loop -- in that case just pass
                # Due to late change in a code, the abs_total didn't exist before
                try:
                    profile_value = input_dict['total_user_profile_{}'.format(k)]
                    try:
                        activity_value = activity['abs_{}'.format(k)]
                    except KeyError:
                        activity_value = activity['{}'.format(k)]
                    input_dict['total_user_profile_{}'.format(k)] = profile_value - activity_value
                except TypeError:
                    pass
    for k in TYPE_ACTIVITY:
        if output is True:
            logger.info('Value of the {}: {}'.format(k, input_dict['total_user_profile_{}'.format(k)]))
        if input_dict['total_user_profile_{}'.format(k)] < 0:
            # Due to missing sometime the loop can end up with a negative number
            input_dict['total_user_profile_{}'.format(k)]
    return input_dict


def main():
    """ """

    for context in db_context.find({'total_in_list_followers_count': {'$exists': False}},
                                {'loop_number': True, 'type_link': True, 'id_str': True}).batch_size(5):

        context = get_activity(context)


        n = 0
        count = dict()
        record = db_full_link.find_one({'loop_number': context['loop_number'], 'type_link': context['type_link'],
                                        'id_str': context['id_str']})
        for id_str in record['list']:
            try:
                result_id = inc_activity(id_str, record['loop_number'], db_profile, db_activity, output=False)
            except TypeError:
                pass
            for k in TYPE_ACTIVITY:
                try:
                    count['total_in_list_{}'.format(k)] = count.get('total_in_list_{}'.format(k), 0)+ result_id['total_user_profile_{}'.format(k)]
                    # logger.info('{}: {}'.format(k, record['total_list_{}'.format(k)]))
                except KeyError:
                    n +=1
                    count['total_in_list_{}'.format(k)] = count.get('total_in_list_{}'.format(k), 0)+ 0

        logger.info('Skipped {} - Over {}'.format(n, len(record['list'])))
        # print(count)
        # raise
        update = db_context.update({'_id': context['_id']}, {'$set': count})
        logger.info(update)
if __name__ == "__main__":
    main()
