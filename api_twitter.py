#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Olivier PHILIPPE"

import urllib.parse as urllib
from requests_oauthlib import OAuth1Session
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from requests import ConnectionError
from datetime import datetime
import time  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Logging
from logger import logger
logger = logger(name='api_twitter', stream_level='INFO', file_level='ERROR')
# for app only: 
# http://www.programmableweb.com/news/how-to-build-twitter-hello-world-web-app-python/how-to/2015/06/16?page=3


# ### CONNECTION ################################################


class TwitterResponse(object):
    """ """
    def __init__(self, status, response, time_collected, api_call,
                 max_id=None, since_id=None):
        self.status = status
        self.response = response
        self.time_collected = time_collected
        self.api_call = api_call
        self.max_id = max_id
        self.since_id = since_id


class TwitterApi(object):
    """ """
    def __init__(self, twitter_keys=None, control_pause=False, type_client='app'):
        ""
        self.control_pause = control_pause
        self.twitter_keys = twitter_keys
        if self.twitter_keys is None:
            self.get_keys()
        self.client = self.create_client(type_client)

    def get_keys(self, file_path=None):
        """ Get the key from a file"""
        self.twitter_keys = dict()
        if file_path is None:
            file_path = 'twitterKeys.txt'
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    key, val = line.split(':')
                    self.twitter_keys[key] = val[:-1]
        except OSError:
            raise OSError

    def create_client(self, type_client='app'):
        """
        Create the client http://arxiv.org/pdf/1304.6257.pdfwith the auth keys. Works differently
        if it is app_only or user_auth
        Params:
        *type_client: app, user
        """
        client_key = self.twitter_keys['CONSUMER_KEY']
        client_secret = self.twitter_keys['CONSUMER_SECRET']
        try:
            if type_client == 'user':
                token_access = self.twitter_keys['ACCESS_TOKEN']
                token_secret = self.twitter_keys['ACCESS_TOKEN_SECRET']
                client = OAuth1Session(client_key,
                                       client_secret=client_secret,
                                       resource_owner_key=token_access,
                                       resource_owner_secret=token_secret)
                return client
            elif type_client == 'app':
                TOKEN_URL = 'https://api.twitter.com/oauth2/token'
                client = BackendApplicationClient(client_id=client_key)
                oauth = OAuth2Session(client=client)
                token = oauth.fetch_token(token_url=TOKEN_URL,
                                          client_id=client_key,
                                          client_secret=client_secret)

                return oauth
        except AttributeError:
            logger.critical('No Keys to connect, check the file')
            raise "No Keys to connect, check the file"

    def cursor_call(self, limit):
        "Used when a cursor methods is needed to parse results"
        if limit is None:
            while self.parameters['cursor'] != 0:
                result = self.create_URL()
                try:
                    self.parameters['cursor'] = result.response['next_cursor']
                    # result.response = result.response['ids']
                    # Return only the id list without the cursors
                except TypeError:  # In case of 404, response is None
                    self.parameters['cursor'] = 0
                    # result.response = None
                yield result
        else:
            loop = 0
            while self.parameters['cursor'] != 0 and loop < limit:
                loop += 1
                result = self.create_URL()
                try:
                    self.parameters['cursor'] = result.response['next_cursor']
                    # result.response = result.response['ids']
                    # Return only the id list without the cursors
                except TypeError:
                    self.parameters['cursor'] = 0
                    # result.response = None
                yield result

    def tweet_call(self):
        """Calling the next bunch of 200 tweets"""
        check = True
        # if a since_id and a max_id is given, the new since_id shouldn't be taken
        # into account because it will be lower than a previous record
        # the mnax_id is used when several call are needed
        if self.last_max_id is None and self.since_id is not None:
            self.since_id = None
        while check is True:
            result = self.create_URL()
            # Return a list of tweet, need to get the last tweet
            # To have the latest tweet. The -1 to avoid redundancies
            try:
                result.max_id = int(result.response[-1]['id']) - 1
                self.last_max_id = result.max_id
                self.parameters['max_id'] = result.max_id
                # self.parameters['since_id'] = result.since_id
                if self.since_id is not None:
                    result.since_id = self.since_id
                else:
                    self.since_id = int(result.response[0]['id'])
                    result.since_id = self.since_id
            # Last return is an empty list because the last max_id match the last tweet
            # When try to collect response from a protected account
            # return the str() "protected" and break here
            # so just pass an go straight to the yield result
            except (IndexError, TypeError):
                try:
                    result.max_id = self.last_max_id
                except AttributeError:
                    result.max_id = None
                try:
                    result.since_id = self.since_id
                except AttributeError:
                    result.since_id = None
                check = False
            yield result

    def create_URL(self):
        "Function to create the URL with the parameters"
        self.BEGIN = "https://api.twitter.com/1.1/"
        # True as second element of urrlib is to encode a list
        self.params = urllib.urlencode(self.parameters, True)
        self.url = '{}{}{}'.format(self.BEGIN, self.service, self.params)
        return self.create_call()

    def create_call(self):
        "Do the actual API call and return the response and status"
        try:
            try:
                # self.resp = self.client.request(self.url)
                self.resp = self.client.get(self.url)
            # In case of the connection is shutdown on the server level
            except ConnectionError:
                logger.error('Connection down on server level, restart in 30 sec')
                time.sleep(30)
                return self.create_URL()

            self.response = self.resp.json()
            self.status = self.resp.headers
            self.status_code = self.resp.status_code
            self.response_time = datetime.now()
        except ValueError as e:
            logger.error('Error in create call {} - Pause for 5 sec'.format(e))
            logger.error('Response from Twitter Call: {}'.format(self.resp))
            time.sleep(5)
            logger.error('Create Call - Ending Pause - Retry')
            self.create_call()
        return self.check_response()

    def check_response(self):
        "Error codes: https://dev.twitter.com/docs/error-codes-responses"
        try:
            api_call = (self.api_type,
                        int(self.status['x-rate-limit-remaining']),
                        int(self.status['x-rate-limit-limit']),
                        int(self.status['x-rate-limit-reset']),
                        self.status)
        # Sometime get a wrong answer from twitter like expiration in 1981
        # Retry after a pause. Need to check later if the error is not
        # something I do wrong but seems wrong on their behalf
        except KeyError:
            # api_call = (self.api_type, None, None, None, self.status)
            print(self.resp)
            logger.error(self.resp)
            logger.error(self.status)
            time.sleep(30)
            return self.create_URL()
        if self.status_code == 200:
            if 'error' in self.response:
                logger.error('Error in response: {}'.format(self.response))
                # not existing resource
                if self.response['error'][0]['code'] == '34':
                    return TwitterResponse(34, None,
                                           self.response_time, api_call)
                # rate limit for the specific resource
                elif self.response['error'][0]['code'] == '88':
                    if self.control_pause:
                        logger.info('Pause - 88')
                        return TwitterResponse(88, None,
                                               self.response_time,
                                               api_call)
                    else:
                        return self.pause_API()
            else:
                return TwitterResponse(200, self.response,
                                       self.response_time, api_call)
        # Supposedly not right resource, seems to raise when
        # Try to get information from a secured account
        elif self.status_code == 401:
            logger.error(self.response)
            return TwitterResponse(401, 'protected', self.response_time, api_call)
        elif self.status_code == 429:
            if self.control_pause:
                return TwitterResponse(429, None, self.response_time,
                                       api_call)
            else:
                return self.pause_API()
        elif self.status_code in [500, 502, 503, 504]:
            logger.error('Twitter Internal error pause for 30 sec: {}'.format(self.response))
            # print 'Check Response: Twitter Internal error: Pause for 30s'
            time.sleep(30)
            return self.create_URL()
        elif self.status_code == 404:
            return TwitterResponse(404, None,
                                   self.response_time, api_call)
        elif self.status_code == 403:
            return TwitterResponse(403, 'User suspended', self.response_time, api_call)
        else:
            return TwitterResponse(int(self.status_code), None, self.response_time, api_call)

    def pause_API(self):    # FIXME Error with the self.time_reset
                            # Check if it is a api limit more global
                            # Should be done by the elt[-1] == 0
                            # But sometime get a negative value
        "Pause the call and wait for the reset"
        reset = int(self.status['x-rate-limit-reset'])
        time_reset = (reset - time.time())+ 2
        time_vis = datetime.fromtimestamp(reset)
        logger.info('Pause Api {} seconds - starting at {}'.format(time_reset,
                                                                   time_vis))
        # FIXME Bug if the call is too soon, it pauses
        # Again and has the previous value
        # Resulting in a negative value
        if time_reset < 0:
            time_reset = 10
        else:
            pass
        time.sleep(time_reset)
        logger.info('Pause finished - restarting')
        return self.create_URL()

###############################################################################
    def check_user_type(self):  # FIXME Buggy as screen_name can be int
        """ Choose which method, screen_name or id_str """
        try:
            int(self.user)
            return {'user_id': self.user}
        except ValueError:
            return {'screen_name': self.user}

    def get_user(self, user):
        "Return a single user object - Limit of 180"
        self.user = user
        self.parameters = self.check_user_type()
        self.api_type = 'users'
        self.service = 'users/show.json?'
        return self.create_URL()

    def rate_limit(self, service):
        "Possible types: statuses, friends, users, followers, trends, help"
        self.service = 'application/rate_limit_status.json?'
        self.api_type = 'application'
        self.parameters = {'resources': service}
        return self.create_URL()

    def look_up(self, list_id):
        "Return a generator of 100 users objects"
        self.api_type = 'users'
        self.service = 'users/lookup.json?'
        if len(list_id) > 100:
            raise Exception('Too big list: it is a {} and cannot be higher than 100'.format(len(list_id)))
        list_params = [str(elt) for elt in list_id]
        self.parameters = {'user_id': list_params}
        list_user = self.create_URL()
        return TwitterResponse(list_user.status, list_user.response,
                               list_user.time_collected,
                               list_user.api_call)

    def followers_list(self, user, limit=None):  # 'count':''
        "return a list of of followers ids with a limit of 5000"
        self.user = user
        self.parameters = self.check_user_type()
        self.parameters['cursor'] = '-1'
        self.api_type = 'followers'
        self.service = 'followers/ids.json?'
        return self.cursor_call(limit)

    def friends_list(self, user, limit=None):  # 'count':''
        "return a list of of friends ids with a limit of 5000"
        self.user = user
        self.parameters = self.check_user_type()
        self.parameters['cursor'] = '-1'
        self.api_type = 'friends'
        self.service = 'friends/ids.json?'
        return self.cursor_call(limit)

    def tweet(self, user, since_id=None, max_id=None):
        """ Use the since_id: greater than and max_id: lesser than
            Use both id if they are passed so the application that use this API
            needs to deal with since_id and max_id before to be sure that all
            wanted tweets are collected
        """
        self.user = user
        self.parameters = self.check_user_type()
        self.parameters['count'] = 200
        # If a max_id is present that means that the last check wasn't complete
        if max_id:
            self.parameters['max_id'] = max_id
            self.last_max_id = max_id
        else:
            self.last_max_id = None
        if since_id:
            self.since_id = since_id
            self.parameters['since_id'] = since_id
        else:
            self.since_id = None
        self.api_type = 'statuses'
        self.service = 'statuses/user_timeline.json?'
        return self.tweet_call()

    def followers_id(self, user):
        "return a list of followers id object, only per 20 and a limit of 15/30"
        self.api_type = 'followers'
        self.service = 'followers/list.json?'
        self.parameters = {'screen_name': user, 'cursor': '-1'}
        # FIXME in the cursor_call, have the result['ids'] which
        # didn't work with this call
        return self.cursor_call()

    def friends_id(self, user):
        "return a list of friends id object, only per 20 and a limit of 15/30"
        self.api_type = 'friends'
        self.service = 'friends/list.json?'
        self.parameters = {'user_id': user, 'cursor': '-1', 'skip_status': 1}
        return self.cursor_call()

    # def get_research(self, keyterm):
    #     """ return the list of user that was searched """
    #     self.api_type = 'users'
    #     self.service = 'users/search.json'
    #     self.parameters = {'q':keyterm, 'count'=


def main():
    """ """
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
    # c = MongoClient()
    # db = c['test_profile']
    # coll = db['test1']
    twitter_file = './config/twitterKeys.txt'
    # lvl1_file = './config/test.txt'
    twitter_key = get_keys(twitter_file)
    # lvl1 = get_lvl1(lvl1_file)
    test_api = TwitterApi(twitter_key, control_pause=False)
    lvl1 = [14857809]
    for user in lvl1:
        api = test_api.tweet(user)
        for result in api:
            try:
                logger.info('User: {} - Status: {} - max_id {} - Since_id {} - len resp: {} - type resp: {}'.format(user, result.status, result.max_id, result.since_id, len(result.response), type(result.response)))
            except TypeError:
                logger.info('User: {} - Status: {} - max_id: {} - since_id: {} - response:{}'.format(user, result.status, result.max_id, result.since_id, result.response))


if __name__ == '__main__':
    """ """
    main()
    pass
