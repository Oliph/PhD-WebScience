#!/usr/bin/python
# -*- coding: utf-8 -*-


import time

from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

import random

from multiprocessing import Process, Queue, Event
from pymongo import MongoClient
from client_process_tweet import ProcessingTweet

# Logging
from logger import logger
logger = logger('Stream_api', stream_level='INFO')


class MyStream(StreamListener):
    """ """
    def __init__(self, loop_number, tweet_collection):
        """ """
        StreamListener.__init__(self)
        self.loop_number = loop_number
        self.tweet_collection = tweet_collection
        self.processedTweet = ProcessingTweet(self.tweet_collection)
        # self.disconnect_even = disconnect_event

    def keep_alive(self):
        """ """
        return

    def on_data(self, data):
        try:
            self.processedTweet.run(json.loads(data), self.loop_number)
        except Exception:
            pass

    def on_disconnect(self, notice):
        """ """
        print(notice)
        print('intodisconnect')
        return False

    def on_error(self, status_code):
        """ """
        logger.info(status_code)
        if status_code == 420:
            time.sleep(10)
        return True

    def on_limit(self, track):
        """ """
        print('on_limit')
        print(track)
        return

    def on_warning(self, notice):
        """ """
        print('warning')
        print(notice)


class TwitterStream(Process):
    """
    """

    def __init__(self, twitter_keys,
                 timeout=90, **kwargs):
        """
        """
        Process.__init__(self)
        self.twitter_keys = self.get_keys(twitter_keys)
        self.API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
        self.USER_AGENT = 'TwitterStream 1.0'  # This can be anything really

        client_key = self.twitter_keys['CONSUMER_KEY']
        client_secret = self.twitter_keys['CONSUMER_SECRET']
        token_key = self.twitter_keys['ACCESS_TOKEN']
        token_secret = self.twitter_keys['ACCESS_TOKEN_SECRET']
        self.auth = OAuthHandler(client_key, client_secret)
        self.auth.set_access_token(token_key, token_secret)
        self.tweet_collection = kwargs['current_tweet']
        self.db_process_link = kwargs['process_link']
        self.timeout = timeout
        self.list_mention = list()
        self.loop_number = 1

    def get_keys(self, twitter_keys):
        """ """
        if isinstance(twitter_keys, str):
            return_twitter_keys = dict()
            try:
                with open(twitter_keys, 'r') as f:
                    for line in f:
                        key, val = line.split(':')
                        return_twitter_keys[key] = val[:-1]
                return return_twitter_keys
            except OSError:
                raise "Impossible to obtain key, please verify the file containing\
                        them"
        elif isinstance(twitter_keys, dict):
            return twitter_keys
        else:
            raise "Need proper format of key, path to a file or dict"

    def return_new_loop(self):
        """ """
        return bool(self.db_process_link.find_one({'loop_number': self.loop_number+1}))

    def check_new_values(self):
        """ Check if there is new values from the queue """

        if self.return_new_loop() is True:
            print('Try to get new values')
            return True

    def get_new_values(self):
        """ """
        return self.get_list_record(self.db_process_profile, 5000)

    def get_list_record(self, db, size, control_type=None):
        """ """
        # TODO optimize this bit because it search for all all the time
        temp_list = list()
        while len(temp_list) < size:
            try:
                doc = self.find_record(db, 'to_do', 1)
                temp_list.append(doc['id_str'])
                self.loop_number  = doc['loop_number']
            except TypeError:
                try:
                    doc = self.find_record(db, 'to_do', 2)
                    temp_list.append(doc['id_str'])
                except TypeError:
                    try:
                        doc = self.find_record(db, 'extra', 2)
                        temp_list.append(doc['id_str'])
                    except TypeError:
                        break
        return temp_list

    def find_record(self, db, type_info, lvl):
        """ """
        return self.find_update(method='update',
                                db=db,
                                search={'get_info': type_info,
                                        'lvl': lvl,
                                        'stream': {'$exists': False}},
                                update={'$set': {'stream': 'doing'}})

    def create_new_filter(self, new_info):
        """" """
        def apply_limit(data, type_keyterm):
            """ apply the limit """
            limit = int()
            if type_keyterm == 'hashtag':
                limit = 400
            elif type_keyterm == 'user':
                limit = 5000
            try:
                return random.sample(data, limit)
            except ValueError:
                return data

        def get_hashtag(data):
            """ check if hashtag present """
            try:
                keyterm = data['hashtag']
                if len(keyterm) > 0:
                    keyterm = apply_limit(keyterm, 'hashtag')
                    return [str(e) for e in keyterm]
                else:
                    raise KeyError
            except KeyError:
                return None

        def get_user(data):
            """ check if user present """
            return [str(e) for e in data]

        new_filter = dict()
        new_filter['follow'] = get_user(new_info)
        # hashtag = get_hashtag(new_info)
        # if hashtag:
        # new_filter['track'] = hashtag
        return new_filter

    def run(self):
        """
        """
        while True:
            if self.check_new_values():
                new_info = self.get_new_values()
                new_filter = self.create_new_filter(new_info)
                try:
                    self.stream.disconnect()
                except AttributeError:
                    pass
                self.stream_control.set()
                self.my_stream = MyStream(self.loop_number, self.tweet_collection)
                self.stream = Stream(self.auth, self.my_stream)
                self.stream.filter(stall_warnings=True, async=True, **new_filter)


def main():
    try:
        c = MongoClient()
        db = c['test_stream']
        collection = db['current_tweet']
        stream_1 = Queue()
        with open('./config/lvl1.txt', 'r') as f:
            lvl1_list = [l[:-1] for l in f]
        hashtag_list = ['android', 'apple', 'bieber', 'WorldOceansDay', 'wwdc15', 'mockingjayPart2']
        stream_1.put({'loop_number': 1, 'lvl1': lvl1_list, 'lvl2': [], 'hashtag': hashtag_list})
        stream_2 = Event()
        stream_2.clear()
        stream = TwitterStream('./config/twitterKeys.txt', stream_2, stream_1, current_tweet=collection)
        stream.start()
        # time.sleep(10)
        # stream_2.clear()
        # stream_1.put({'loop_number': 2, 'lvl1': lvl1_list, 'lvl2': [], 'hashtag': ['apple', 'android']})
    except KeyboardInterrupt:
        stream.close()


if __name__ == '__main__':
    main()
