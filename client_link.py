#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO: Optimize the comparison of two lists
#  Better algorithm to compare two huge sets
#  http://erikdemaine.org/papers/SODA2000/
# ## Structure of response:
# ##     {id_user: screen_name,
# ##      type_response: (profile, link, mention),
# ##      time: time_collected,
# ##      api_call: (None, db, number),
# ##      response: (dict(profile_info), list(link), list(mentions))
# ##     }

# Logging
from logger import logger
logger = logger('client_link', stream_level='INFO')

# FIXME need to pass the call_num in the params in the process
# need to create it during the client_init, here it is hardcoded


class LinkInfo(object):
    """
    """
    def __init__(self, twitter_client, db,
                 call_num=1, control_pause=False):
        ""
        self.twitter = twitter_client
        self.db = db
        self.call_num = call_num
        self.control_pause = control_pause

    def user_id_type(self, user_id):
        """ Get sure to have the proper user_id type: list - str - int"""
        # FIXME: a screen_name can be a sequence of number
        if isinstance(user_id, list):
            pass
        else:
            try:
                user_id = int(user_id)
            except ValueError:
                user_id = str(user_id)
            except TypeError:
                raise
        return user_id

    def do_call(self, call_num):
        """ """
        if call_num:
            return call_num + 1  # Need for comparison the length
        else:
            return None

    def get_twitter(self, type_link, user_id, call_num):
        """Return the proper API"""
        if type_link == 'followers':
            return self.twitter.followers_list(user_id, call_num)
        elif type_link == 'friends':
            return self.twitter.friends_list(user_id, call_num)

    def get_status(self, call):
        """ Get the status from the api response """
        if call.status == 200:
            return 'done'
        elif call.status == 404:
            return 'non-existing'
        elif call.status == 401:
            return 'protected'
        elif call.status == 403:
            return 'suspended'
        elif call.status == 429 or call.status == 88:
            return 'pause'
        else:
            raise
            # return 'error'

    # def get_ids(self, call, list_ids):
    #     """ return the list of ids appended in the existing list """
    #     return list_ids.extend(call.response)

    def create_id_str(self, user_id):
        """ Create a dict with the id_str """
        return {'id_str': user_id}

    def create_type(self, record, type_link):
        """ Append the dict with the type of link """
        record['type_link'] = type_link
        return record

    def create_list(self, record, list_ids):
        """ Append the dict with the list of ids """
        record['list'] = list_ids
        return record

    def create_time(self, record, time_):
        """ Append the dict with the time collected """
        record['time_collected'] = time_
        return record

    def create_loop(self, record, loop_number):
        """
        Append the dict to add the loop_number
        """
        record['loop_number'] = loop_number
        return record

    def insert_record(self, record):
        """ Insert the record in the database """
        self.db.insert_one(record)

    def return_record(self, status, api_call, user_id):
        """ return element of the record """
        return {'status': status, 'api_call': api_call, 'id_str': user_id}

    def process(self, user_id, type_link, loop_number):
        """ """
        user_id = self.user_id_type(user_id)
        call_num = self.do_call(self.call_num)
        twitter_api = self.get_twitter(type_link, user_id, call_num)

        # Several call are needed if > 5000 links the API return a yield
        list_ids = list()
        for call in twitter_api:
            status = self.get_status(call)
            time_collected = call.time_collected
            api_call = call.api_call
            # logger.info(call.response)
            try:
                list_ids.extend(call.response['ids'])
            # In case the second call is Pause or any error,
            # remove all the ids and avoid to record partial result.
            # lost one call but the user will be done another time
            # Error raised because twitterResponse return None
            except TypeError:
                list_ids = None

        if list_ids is not None:
            # Create the record to insert into the db
            record = self.create_id_str(user_id)
            record = self.create_type(record, type_link)
            record = self.create_list(record, list_ids)
            record = self.create_time(record, time_collected)
            record = self.create_loop(record, loop_number)
            self.insert_record(record)
        # Return a dict with all information for the client_REST_API
        return self.return_record(status, api_call, user_id)


def main():

    from pymongo import MongoClient

    def get_keys(twitter_file):
        """ """
        keydict = {}
        with open(twitter_file, 'r') as f:
            for line in f:
                key, val = line.split(':')
                keydict[key] = val[:-1]
        return keydict

    def get_lvl1(lvl1_file):
        with open(lvl1_file, 'r') as f:
            return [line[:-1] for line in f]

    c = MongoClient()
    db = c['test_link']
    coll = db['test1']
    twitter_file = './config/twitterKeys.txt'
    lvl1_file = './config/lvl1.txt'
    twitter_key = get_keys(twitter_file)
    lvl1 = get_lvl1(lvl1_file)
    # Protected, normal, non-existing, >5000
    lvl1 = [2466250, 564788318, 2746815946, 32132131231231231, 33177560]
    link = LinkInfo(twitter_key, coll, call_num=2, control_pause=True)
    for elt in lvl1:
        result = link.process(elt, 'friends')
        print(result)
###############################################################################
###############################################################################
if __name__ == '__main__':
    main()
    # pass
