#!/usr/bin/env python
# -*- coding: utf-8 -*-

# My own modules
from utils import slice_list
from params import dataScheme
# Logging
from logger import logger
logger = logger(name='client_profile', stream_level='INFO')


class ProfileInfo(dataScheme):
    """
    """
    def __init__(self, twitter_client, db, client_id, control_pause=False):
        ""
        dataScheme.__init__(self)
        self.client_id = client_id
        self.db = db
        self.twitter = twitter_client
        self.control_pause = control_pause

    def int_users(self, data):
        """ Check to be sure it is a list of int """
        return [int(elt) for elt in data]

    def get_twitter(self, data):
        """Get the data either with Twitter, either with database
            - Source: Which information is needed, from twitter or from DB
              (!= Twitter API)
        """
        return self.twitter.look_up(data)

    def get_twitter_single(self, data):
        """ """
        return self.twitter.get_user(data)

    def process_status(self, twitter_info):
        "Checking if twitter info is passed to continue or to exit"
        if twitter_info.status == 200:
            return 'done'
        elif twitter_info.status == 404:
            return 'non-existing'
        elif twitter_info.status == 401:
            return 'protected'
        elif twitter_info.status == 403:
            return 'suspended'
        elif twitter_info.status == 88:
            return 'pause'
        elif twitter_info.status == 429:
            return 'pause'
        else:
            return 'error'

    def select_info_twitter(self, user_info):
        """ Creation of the dictionary for comparison with LIST_INFO as keys
        """
        LIST_INFO = ['id_str', 'followers_count', 'friends_count',
                     'statuses_count', 'description', 'screen_name', 'lang',
                     'geo_enable', 'time_zone', 'created_at', 'location',
                     'protected']
        return_dict = {key: user_info[key]
                       for key in user_info if key in LIST_INFO}
        return return_dict

    def check_protected(self, user_info):
        """ Update the status to protected in case the user has a
            protected account to avoid later to parse it
        """
        if user_info['protected'] is 'true':
            return 'protected'

    def int_id_str(self, user_info):
        """ Be sure the id_str is transform in int() type
            before being added in the database
        """
        user_info['id_str'] = int(user_info['id_str'])
        return user_info

    def add_status(self, user_info, status):
        """ Add the genera status of the reply
        """
        user_info['status'] = status
        return user_info

    def add_time(self, user_info, time_to_add):
        """ Add the time when the information was collected
        """
        user_info['time_collected'] = time_to_add
        return user_info

    def add_client_id(self, user_info, client_id):
        """
        Add the unique client_id to know which one recorded it
        """
        user_info['client_id'] = client_id
        return user_info

    def add_loop_number(self, user_info, loop_number):
        """
        Add the loop_number
        """
        user_info['loop_number'] = loop_number
        return user_info

    def insert_data(self, user_info):
        """ Insert the processed response into the db """
        self.db.insert_one(user_info)

    def process(self, data, loop_number):
        """Get the data from db and from twitter"""
        data = self.int_users(data)
        twitter_result = self.get_twitter(data)
        status = self.process_status(twitter_result)
        try:
            for user in twitter_result.response:
                twitter_response = self.select_info_twitter(user)
                twitter_response = self.int_id_str(twitter_response)
                twitter_response = self.add_time(twitter_response, twitter_result.time_collected)
                twitter_response = self.add_client_id(twitter_response, self.client_id)
                twitter_response = self.add_loop_number(twitter_response, loop_number)
                self.insert_data(twitter_response)
                data.remove(twitter_response['id_str'])
                yield {'status': status, 'api_call': twitter_result.api_call, 'id_str': twitter_response['id_str']}
            # To output the rest of the list that didn't receive data
            # for reason that the profile is Non existent or suspended
            for user in data:
                yield {'status': 'no-data', 'api_call': twitter_result.api_call, 'id_str': user}
        except TypeError:
            for user in data:
                yield {'status': status, 'api_call': twitter_result.api_call, 'id_str': user}


def main():
    """
    """
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
    db = c['test_profile']
    coll = db['test1']
    twitter_file = './config/twitterKeys.txt'
    lvl1_file = './config/lvl1.txt'
    twitter_key = get_keys(twitter_file)
    lvl1 = get_lvl1(lvl1_file)
    profile = ProfileInfo(twitter_key, coll, control_pause=True)
    for list_ in slice_list(lvl1, 100):
        for result in profile.process(list_):
            print(result)


if __name__ == '__main__':
    pass
    # main()
