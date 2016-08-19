#!/usr/bin/env python
# encoding: utf-8
import time

# from pymongo import DuplicateKeyError
# from bson.objectid import ObjectId
# Logging

import pymongo
from pymongo import MongoClient
import configparser as ConfigParser
from server_missing_context import check_nb_lvl1
from logger import logger
from multiprocessing import Pool
logger = logger('server_context', stream_level='INFO')


def list_or_tuple(x):
    return isinstance(x, (list, tuple))


def flatten(sequence, to_expand=list_or_tuple):
    for item in sequence:
        if to_expand(item):
            for subitem in flatten(item, to_expand):
                yield subitem
        else:
            yield item


class contextTransforming(object):

    def __init__(self, db, set_lvl1):
        self.db = db
        self.context_link = self.db['context_link']
        self.db_profile = self.db['stored_profile']
        self.db_process_profile = self.db['process_profile']
        self.db_activity = self.db['activity']

        self.db_context = self.db['context']
        self.db_context_error = self.db['context_error']
        self.db_full_link = self.db['full_link']
        self.set_lvl1 = set_lvl1
        self.TYPE_ACTIVITY = ['statuses_count', 'friends_count', 'followers_count']

    def get_list(self):
        """ Get the last list of link of the user """
        return self.context_link.find_one()

    def prepare_context(self):
        """Prepare the dictionary to insert context """
        dict_return = {'list_abs_activity_{}'.format(elt): 0 for elt in self.TYPE_ACTIVITY}
        return dict_return

    def get_activity_user(self, id_user, loop_number):
        """ Get the user from the list to obtain its activity measures"""
        return self.db_activity.find_one({'id_str': id_user, 'loop_number': loop_number})

    def get_raw_activity(self, context, user_result):
        """Update the raw measure of activity with all users measures founded"""
        for key in self.TYPE_ACTIVITY:
            dict_key = 'list_abs_activity_{}'.format(key)
            context[dict_key] = context.get(dict_key, 0) + abs(user_result[key])
        return context

    def inc_activity(self, id_str, loop_number, input_dict=dict(), output=True):
        """
        Calculate from the loop_number the right amount of counts
        Get the different loop since it is recorded in stored_profile
        and incremente or decremente the value found in activity for these
        different loops
        """

        count_profile = self.db_profile.find_one({'id_str': id_str})
        for profiles in self.db['profile_loop'].find({'id_str': id_str}):
            try:
                if profiles['loop_number'] > count_profile['loop_number']:
                    count_profile = profiles
            except TypeError:
                count_profile = profiles

        if count_profile is None:
            count_profile = self.db_profile.find_one({'id_str': id_str})
        loop_profile = count_profile['loop_number']

        for k in self.TYPE_ACTIVITY:
            input_dict['user_profile_{}'.format(k)] = count_profile['{}'.format(k)]
        # If the loop_context and loop_profile are equal no need anything to remove
        # or add
        if loop_number != loop_profile:
            # Generate the number of loop between the stored profile and the context loop
            for loop in sorted([i for i in range(loop_number, loop_profile)], reverse=True):
                activity = self.db_activity.find_one({'loop_number': loop, 'id_str': id_str})
                # Then loop through it to find the past activity to remove or add it to calculate
                # the actual count
                for k in self.TYPE_ACTIVITY:
                    # Do =- instead of =+ because need to invert (one added after mean the previous
                    # score had one less
                    # The record may not exists because no previous record (not a problem)
                    # or because it was not collected the previous loop -- in that case just pass
                    # Due to late change in a code, the abs_total didn't exist before
                    try:
                        profile_value = input_dict['user_profile_{}'.format(k)]
                        try:
                            activity_value = activity['abs_{}'.format(k)]
                        except KeyError:
                            activity_value = activity['{}'.format(k)]
                        input_dict['user_profile_{}'.format(k)] = profile_value - activity_value
                    except TypeError:
                        pass
        for k in self.TYPE_ACTIVITY:
            if input_dict['user_profile_{}'.format(k)] < 0:
                # Due to missing sometime the loop can end up with a negative number
                input_dict['user_profile_{}'.format(k)] = 0
        to_record = dict()
        to_record['loop_number'] = loop_number
        to_record['id_str'] = id_str
        for k in self.TYPE_ACTIVITY:
            to_record[k] = input_dict['user_profile_{}'.format(k)]
        try:
            self.db['profile_loop'].insert(to_record)
        except pymongo.errors.DuplicateKeyError:
            pass
        return input_dict

    def division(self, nb1, nb2):
        """" Operation of division for calculating context """
        try:
            return int(nb1) / int(nb2)
        except ZeroDivisionError:
            return 0

    def get_activity(self, context):
        """ """
        activity = self.db_activity.find_one({'id_str': context['id_str'], 'loop_number': context['loop_number']})
        for count in self.TYPE_ACTIVITY:
            try:
                context['user_activity_{}'.format(count)] = activity['{}'.format(count)]
            except TypeError:
                context['user_activity_{}'.format(count)] = 'Not found'
        return context

    def check_loop(self, context_loop, current_loop):
        """
        """
        try:
            if context_loop < current_loop:
                return True
            else:
                return False
        except TypeError:
            return False

    def get_total_link(self, context, id_str):
        """
        Parse all id_str in the list and add the total of count for each To get the grand total of it.
        """
        result_id = self.inc_activity(id_str, context['loop_number'], output=False)
        for k in self.TYPE_ACTIVITY:
            context['list_total_{}'.format(k)] = context.get(k, 0)+ result_id['user_profile_{}'.format(k)]
        return context

    def inc_total_user(self, context):
        """ Increment the number of user found in activity """
        context['list_total_found'] = context.get('list_total_found', 0)+ 1
        return context

    def run(self, to_do_loop):
        """ """
        previous_total = check_nb_lvl1(self.db, self.set_lvl1, to_do_loop)
        while True:
            record = self.context_link.find_one({'loop_number': to_do_loop}, {'_id': False})
            if record['id_str'] in self.set_lvl1:
                list_link = list()

                for x in flatten(record['list']):
                    list_link.append(x)
                context = dict()
                context['id_str'] = record['id_str']
                context['type_link'] = record['type_link']
                context['loop_number'] = record['loop_number']
                context['list_total_found'] = 0
                context['list_size'] = len(list_link)
                logger.info('Doing: {} - size of list: {}'.format(context['id_str'], len(list_link)))
                context = self.get_activity(context)
                # try:
                context = self.inc_activity(context['id_str'], context['loop_number'], context)
                # except TypeError:  # Sometime there is an error with a record that does not have activity or stored profile
                #     # Store in context_error db to check later
                #     self.db_context_error.insert(record)
                #     # Remove from the db
                #     self.context_link.remove({'id_str': record['id_str'],
                #                               'type_link': record['type_link'],
                #                               'loop_number': context['loop_number']})
                #     # Append the number of passed record
                #     passed_record +=1
                #     logger.warning('Number of record skipped: {}'.format(passed_record))
                #
                for user in list_link:
                    # current_loop -1 because the links that are collected are considered as new a the information
                    # about the id_str in the list not yet collected with the profile. It is done the next loop
                    # after the sample is done. So, need to be sure they have been collected for when the information
                    # about them is collected for the activity
                    user_activity = self.get_activity_user(int(user), context['loop_number'] -1)
                    if user_activity is not None:
                        context = self.get_total_link(context, int(user))
                        context = self.inc_total_user(context)
                        context = self.get_raw_activity(context, user_activity)
                # In case no user is founded in the activity, the keys are not init. Need to create them here
                # and set the value to 0
                if context['list_total_found'] == 0:
                    for k in self.TYPE_ACTIVITY:
                        context['list_total_{}'.format(k)] = 0

                logger.info('id_str: {} - loop: {} - type_link: {} - Size list: {} - List founded: {}'.format(context['id_str'],
                                                                                                              context['loop_number'],
                                                                                                              context['type_link'],
                                                                                                              context['list_size'],
                                                                                                              context['list_total_found']))

                self.db_context.insert(context)

                # Update the full_link to add the total_link found
                for k in self.TYPE_ACTIVITY:
                    record['total_{}'.format(k)] = context['list_total_{}'.format(k)]
                record['list_total_found'] = context['list_total_found']
                record['list_size'] = context['list_size']
                self.db_full_link.insert(record)
                # Remove the link
                self.context_link.remove({'id_str': record['id_str'], 'type_link': record['type_link'],
                                         'loop_number': record['loop_number']})

                total = check_nb_lvl1(self.db, self.set_lvl1, to_do_loop)
                if total < previous_total:
                    raise
                previous_total = total
            else:
                self.db_full_link.insert(record)
                self.context_link.remove({'id_str': record['id_str'], 'type_link': record['type_link'],
                                          'loop_number': record['loop_number']})


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


def get_current_loop(db):
    """
    Returne the current loop number in process_profile db
    """
    result = db.find_one({}, {'loop_number': True, '_id': False})
    try:
        return result['loop_number']
    except TypeError:
        return None


def main():
    """
    """
    config_file = './config/config.ini'
    values = read_config(config_file)
    db = connectDB(db=values['db_name'], host=values['host'])
    set_lvl1 = set(get_lvl1(values['lvl1_file']))
    logger.info('Connect to db: {}'.format(db))
    context = contextTransforming(db, set_lvl1)

    loops = [61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72]
    pool = Pool(processes=4)
    pool.map(context.run, loops)

    # ## KEEP THAT CODE BELOW IT IS THE WORKING VERSION -- THE POOL ONE
    # ## IS USED AT THE END OF THE DATA COLLECTION TO SPEED UP THE PROCESSUS

    # loops = list()
    # min_loop = db_link.find().sort({'loop_number': 1}).limit(1)
    # while True:
    #     for process_db in ['process_profile', 'process_link']:
    #         current_loop = get_current_loop(db[process_db])
    #         if current_loop is not None:
    #             break
    #     if current_loop is None:
    #         # Mean that all the process have been done for the last loop
    #         # and it is safe to start the context
    #         current_loop = 10000  # FIXME Need to add another type of check
    #     if len(loops) > 1:
    #         to_do_loop = loops[0]
    #         del loops[0]
    #     else:
    #         loops = sorted(db['context_link'].distinct("loop_number"))
    #         try:
    #             if loops[0] < current_loop:
    #                 to_do_loop = loops[0]
    #                 del loops[0]
    #         except IndexError:
    #             logger.info('TEST')
    #             break
    #     logger.info('Current loop: {}'.format(current_loop))
    #     logger.info('Loop to do: {}'.format(to_do_loop))
    #     if to_do_loop < current_loop:
    #         context.run(to_do_loop)
    #     else:
    #         logger.info('Sleep for 2 minutes')
    #         time.sleep(300)
    #
if __name__ == '__main__':
    main()
