#!/usr/bin/env python
# encoding: utf-8

""" Class that deal with the tweets returned from the
    api twitter. Manage to insert the returned tweet
    and return a formatted answer for the client_REST_api.
    Deal with the pause and other type of response while create
    a generator to have the response managed as the same way as for
    the client_link and client_profile. Expect one user to check at a time
"""

from pymongo import errors as PyError

# Logging
from logger import logger
logger = logger('client_tweet', stream_level='INFO')


class TweetInfo(object):
    """ """
    def __init__(self, twitter_client, db, control_pause=False):
        ""
        self.twitter = twitter_client
        self.db = db
        self.control_pause = control_pause

    def get_twitter(self, user_id, max_id=None, since_id=None):
        """Return the proper API"""
        return self.twitter.tweet(user_id, max_id, since_id)

    def insert_record(self, db, tweet):
        """ """
        try:
            db.insert_one(tweet)
        # The collection is supposed to have a unique on id_ so error raised
        # if tweet already present
        except PyError.DuplicateKeyError:
            pass
        except TypeError:
            logger.info('Error in insert_record, not a dict to insert: {}'.format(tweet))
            raise

    def return_record(self, status, api_call, user_id, max_id, since_id):
        """ return element of the record """
        return {'status': status, 'api_call': api_call, 'id_str': user_id,
                'max_id': max_id, 'since_id': since_id}

    def process_status(self, call):
        """ Get the status from the api response """
        if call.status == 200 and len(call.response) == 0 and isinstance(call.response, list):
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
            return 'error'

    def process_tweet(result, dbinfo, id_str):
        for r in result:
            for tweet in r.response:
                tweet.update({'from_id_db': id_str})
                return tweet

    def process(self, user_id, loop_number, max_id=None, since_id=None):
        """ """
        if max_id:
            max_id = int(max_id)
        if since_id:
            since_id = int(since_id)
        api = self.get_twitter(user_id, max_id, since_id)
        for result in api:
            status = self.process_status(result)
            api_call = result.api_call
            # suppsoed to receive a list but TODO check later
            if isinstance(result.response, list):
                for tweet in result.response:
                    tweet['loop_number'] = loop_number
                    self.insert_record(self.db, tweet)
        return self.return_record(status, api_call, user_id, result.max_id, result.since_id)


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
    db = c['test_tweet']
    coll = db['test1']
    coll.create_index('id_str', unique=True)
    twitter_file = './config/twitterKeys.txt'
    # lvl1_file = './config/lvl1.txt'
    twitter_key = get_keys(twitter_file)
    # lvl1 = get_lvl1(lvl1_file)

    lvl1 = [14857809]
    # Protected, normal, non-existing, >5000
    # lvl1 = [564788318, 2746815946, 32132131231231231, 33177560]
    api = TweetInfo(twitter_key, coll, control_pause=True)
    for elt in lvl1:
        result = api.process(elt)
        print(result)
###############################################################################
###############################################################################
if __name__ == '__main__':
    main()
    # pass
