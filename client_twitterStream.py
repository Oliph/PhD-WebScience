#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2012 Gustav Arngården
# Copyright (C) 2014 Olivier PHILIPPE
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import time
import datetime
import pycurl
import urllib.parse as urllib
import json
# from requests_oauthlib import OAuth1Session as oauth

import requests
from requests_oauthlib import OAuth1Session


# import oauthlib
import random

from multiprocessing import Process, Queue, Event
from pymongo import MongoClient
from client_process_tweet import ProcessingTweet

# Logging
from logger import logger
logger = logger('Stream_api', stream_level='INFO')


class TimeOutTweet(Exception):
    """ Raised for when it received many tweet and didn't check for new
        Value for a while
    """
    # def __init__(self, message, *args):
    #     """ """
    #     self.message = message  # without this you may get DeprecationWarning
    #     # Special attribute you desire with your Error,
    #     # perhaps the value that caused the error?:
    #     # allow users initialize misc. arguments as any other builtin Error
    #     super(TimeOutTweet, self).__init__(message, *args)

class TwitterStream(Process):
    """
    """

    def __init__(self, twitter_keys, stream_control, stream_new_info,
                 timeout=90, **kwargs):
        """
        """
        Process.__init__(self)
        self.twitter_keys = self.get_keys(twitter_keys)
        self.API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
        self.USER_AGENT = 'TwitterStream 1.0'  # This can be anything really

        # self.oauth_token = oauth.Token(key=self.twitter_keys['ACCESS_TOKEN'],
        #                                secret=self.twitter_keys['ACCESS_TOKEN_SECRET'])
        # self.oauth_consumer = oauth.Consumer(key=self.twitter_keys['CONSUMER_KEY'],
        #                                      secret=self.twitter_keys['CONSUMER_SECRET'])

        client_key = self.twitter_keys['CONSUMER_KEY']
        client_secret=self.twitter_keys['CONSUMER_SECRET']
        resource_owner_key=self.twitter_keys['ACCESS_TOKEN']
        resource_owner_secret=self.twitter_keys['ACCESS_TOKEN_SECRET']
        self.oauth_header = OAuth1Session(client_key, client_secret,
                             resource_owner_key, resource_owner_secret,
                             signature_type='auth_header')


        print(self.oauth_header.__dict__)
        self.conn = None
        self.buffer = ''
        self.stream_new_info = stream_new_info
        # self.stream_result = stream_result
        self.stream_control = stream_control
        self.tweet_collection = kwargs['current_tweet']
        self.processedTweet = ProcessingTweet(self.tweet_collection)
        self.timeout = timeout
        self.nbr_check = 0
        self.nb_tweet = 0
        self.list_mention = list()

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
        elif isinstance(twitter_keys, dict()):
            return twitter_keys
        else:
            raise "Need proper format of key, path to a file or dict"

    def check_new_values(self):
        """ Check if there is new values from the queue """

        if self.stream_control.is_set() is False:
            self.get_new_values()
            # self.put_new_values()

    def get_new_values(self):

        try:
            new_info = self.stream_new_info.get()
            self.loop_number = new_info['loop_number']
            self.keyterm = new_info['lvl1'] + new_info['lvl2']
            if len(self.keyterm) > 5000:
                self.keyterm = random.sample(self.keyterm, 5000)
            logger.info('Get new information - nbr of id {} - loop nbr {}'.format(len(self.keyterm),
                                                                                  self.loop_number))
        except Exception as e:
            logger.critical('error in getting new values - {} - {}'.format(new_info, e))
            raise Exception(e)

    def put_new_values(self):
        self.stream_result.put(self.list_mention)
        self.list_mention = list()

    def setup_params(self):
        """ Setup the params for the connection """
        self.POST_PARAMS = self.create_param()
        self.POST_PARAMS = urllib.urlencode(self.POST_PARAMS)

    def create_param(self):
        """ Creating the parameters dictionary
        """
        POST_PARAMS = {'include_entities': 0,
                       'stall_warning': 'true'}
        # 'language': 'en'}
        track = self.do_track()
        POST_PARAMS.update(track)
        return POST_PARAMS

    def do_track(self):
        """ Transform the list of keyterms in track_dict
        """
        self.keyterm = [str(elt) for elt in self.keyterm]
        return {'track': ','.join(self.keyterm)}

    def close_connection(self):

        if self.conn:
            try:
                self.conn.close()
                self.buffer = ''
            except pycurl.error:
                # logger.warning('Error in closing connection, retry in 2 sec: {}'.format(e))
                time.sleep(2)
                self.close_connection()

    def setup_connection(self):
        """ Create persistant HTTP connection to Streaming API endpoint using cURL.
        """
        self.conn = pycurl.Curl()
        self.stream_control.set()
        # Restart connection if less than 1 byte/s is received during "timeout" seconds
        if isinstance(self.timeout, int):
            self.conn.setopt(pycurl.LOW_SPEED_LIMIT, 1)
            self.conn.setopt(pycurl.LOW_SPEED_TIME, self.timeout)
        self.conn.setopt(pycurl.URL, self.API_ENDPOINT_URL)
        self.conn.setopt(pycurl.USERAGENT, self.USER_AGENT)
        # Using gzip is optional but saves us bandwidth.
        self.conn.setopt(pycurl.ENCODING, 'deflate, gzip')
        self.conn.setopt(pycurl.POST, 1)
        # self.conn.setopt(pycurl.VERBOSE, 1)
        self.conn.setopt(pycurl.POSTFIELDS, self.POST_PARAMS)
        self.conn.setopt(pycurl.HTTPHEADER, ['Host: stream.twitter.com',
                                             'Authorization: {}'.format(self.oauth_header)])
        # self.handle_tweet is the method that are called to handle data
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)

    # def get_oauth_header(self):
    #     """ Create and return OAuth header.
    #     """
    #     params = {'oauth_version': '1.0',
    #               'oauth_nonce': oauth.generate_nonce(),
    #               'oauth_timestamp': int(time.time())}
    #     req = oauth.Request(method='POST', parameters=params,
    #                         url='{}?{}'.format(self.API_ENDPOINT_URL,
    #                                            self.POST_PARAMS))
    #     req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), self.oauth_consumer,
    #                      self.oauth_token)
    #     return req.to_header()['Authorization'].encode('utf-8')

    def run(self):
        """ Start listening to Streaming endpoint.
        Handle exceptions according to Twitter's recommendations.
        """
        backoff_network_error = 0.25
        backoff_http_error = 5
        backoff_rate_limit = 60
        while True:
            try:
                self.check_new_values()
                self.close_connection()
                self.setup_params()
                self.last_run = datetime.datetime.now()
                self.setup_connection()
                try:
                    print('listening')
                    self.conn.perform()
                except TimeOutTweet:
                    # logger.info('Need to check values {}'.format(e))
                    continue
                except Exception:
                    logger.info('Network error: {} wait for {}s'.format(self.conn.errstr(),
                                                                        backoff_network_error))
                    time.sleep(backoff_network_error)
                    backoff_network_error = min(backoff_network_error + 1, 16)
                    continue
                # HTTP Error
                sc = self.conn.getinfo(pycurl.HTTP_CODE)
                if sc == 420:
                    # Rate limit, use exponential back off starting with 1 minute and double each attempt
                    # logger.warning('Rate limit, waiting {}s seconds'.format(backoff_rate_limit))
                    time.sleep(backoff_rate_limit)
                    backoff_rate_limit *= 2
                else:
                    # HTTP error, use exponential back off up to 320 seconds
                    # logger.warning('HTTP error {}, {}'.format(sc, self.conn.errstr()))
                    logger.info('Waiting {} seconds - sc code {}'.format(backoff_http_error, sc))
                    time.sleep(backoff_http_error)
                    backoff_http_error = min(backoff_http_error * 2, 320)
            except KeyboardInterrupt:
                break

    def handle_tweet(self, data):
        """ This method is called when data is received through Streaming endpoint.
        """
        # self.nb_tweet += 1
        data = data.decode('utf-8')
        self.buffer += data
        if data.endswith('\r\n') and self.buffer.strip():
            try:
                message = json.loads(self.buffer)
                self.buffer = ''

                if message.get('limit'):
                    pass
                    # logger.warning('Rate limiting caused us to miss %s tweets' % (message['limit'].get('track')))
                elif message.get('disconnect'):
                    pass
                    logger.warning('Got disconnected'.format(message['disconnected'].get('reason')))
                    raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
                elif message.get('warning'):
                    logger.warning('Got warning: {}'.format(message['warning'].get('message')))
                    pass
                else:
                    print(message)
                    # self.process_tweet(message)
            except ValueError:
                pass

            if datetime.datetime.now() - self.last_run > datetime.timedelta(seconds=120):
                raise TimeOutTweet()

    def process_tweet(self, message):
        """ Processing the tweet outside """
        try:
            # self.list_mention.append(
            self.processedTweet.run(message, self.loop_number)
        except Exception:
            pass


if __name__ == '__main__':
    try:
        c = MongoClient()
        db = c['test_stream']
        collection = db['current_tweet']
        stream_1 = Queue()
        stream_1.put({'loop_number': 1, 'lvl1': ['apple'], 'lvl2': ['android']})
        stream_2 = Event()
        stream_2.clear()
        stream = TwitterStream('./config/twitterKeys.txt', stream_2, stream_1, current_tweet=collection)
        stream.start()
    except KeyboardInterrupt:
        stream.close()
