#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
from multiprocessing import Process
import time
# Need in the update record to double check that the result was properly updated
# from pymongo import ReturnDocument
from client_profile import ProfileInfo
from client_link import LinkInfo
from client_tweet import TweetInfo

from api_twitter import TwitterApi as Twitter
from utils import epoche_to_datetime
from params import dataScheme
# Logging
from logger import logger

logger = logger(name='client_REST_api', stream_level='DEBUG', file_level='ERROR')


class EmptyListError(Exception):
    """ """
    def __init__(self):
        pass


class PauseError(Exception):
    """ """
    def __init__(self):
        pass


class RESTAPI(Process, dataScheme):
    """ The API which deal with the REST API """

    def __init__(self, *args, **kwargs):

        """
        - loop_number = int of which loop it is
        - the type of control for the pause
        - the key for Twitter to connect
        - List of users to parse: lvl1, lvl2, lvl3
        - DB to write down result: db_profile, db_link
        """
        dataScheme.__init__(self)
        Process.__init__(self)
        self.twitter_keys = args[0]
        self.client_id = args[1]
        self.type_client = args[2]
        self.db_process_profile = kwargs['process_profile']
        self.db_process_link = kwargs['process_link']
        self.db_process_tweet = kwargs['process_tweet']
        self.db_profile = kwargs['current_profile']
        self.db_link = kwargs['current_link']
        self.db_stored_tweet = kwargs['stored_tweet']
        self.db_tweet_info = kwargs['tweet_info']
        self.control_pause = True
        self.call_num = 1
        self.twitter = Twitter(self.twitter_keys,
                               self.control_pause,
                               self.type_client)
        self.create_lock_file()

    def create_lock_file(self):
        """
        Create a lock file with a false value
        """
        with open('.lock_{}'.format(str(self.client_id)), 'w') as f:
            f.write('false')

    def process_profile(self, data, loop_number):
        """ """
        profile_user = ProfileInfo(twitter_client=self.twitter,
                                   db=self.db_profile,
                                   client_id=self.client_id,
                                   control_pause=self.control_pause)
        for result in profile_user.process(data, loop_number):
            yield result

    def process_link(self, list_to_parse, type_link, loop_number):
        """ """
        link_info = LinkInfo(twitter_client=self.twitter,
                             db=self.db_link,
                             call_num=self.call_num,
                             control_pause=self.control_pause)
        for user in list_to_parse:
            result = link_info.process(user, type_link, loop_number)
            yield result

    def process_tweet(self, list_to_parse, loop_number):
        """ """
        tweet_info = TweetInfo(twitter_client=self.twitter,
                               db=self.db_stored_tweet,
                               control_pause=self.control_pause)

        for user in list_to_parse:
            max_id, since_id, previous_loop = self.get_previous_tweet(user)
            logger.info('Get previous result for: {}. max_id: {} - since_id {}'.format(user, max_id, since_id))
            result = tweet_info.process(user, loop_number, max_id, since_id)
            yield result

    def get_previous_tweet(self, user):
        """ Get the max_id and since_id in stored profile """
        result = self.db_tweet_info.find_one({self.id_str: int(user)})
        try:
            try:
                max_id = result['max_id']
            except KeyError:
                max_id = None
            try:
                since_id = result['since_id']
            except KeyError:
                since_id = None
            try:
                previous_loop = result['loop_number']
            except KeyError:
                previous_loop = None
            return max_id, since_id, previous_loop
        except TypeError:
            return None, None, None

    # TODO: PARSE THE DB TWICE ONCE FOR THE CHECK ANOTHER TIME FOR THE PROCESS
    # NEED TO MAKE IT BETTER - ONLY ONCE AND OUTPUT THE RESULTS FOR THE PROCESS

    def check_tweet(self, list_to_parse, loop_number):
        """
        Create a check for every tweet to see if they have been done recently
        if yes, skip them to go faster
        """
        def need_to_do(previous_loop, loop_number):
            """
            Check the last time the tweets has been check
            """
            try:
                if int(loop_number) - int(previous_loop) > 1:
                    return True
                # In case of reuse previous db, the loop_number will be higher
                elif int(loop_number) - int(previous_loop) < 0:
                    return True
                else:
                    return False
            except TypeError:
                return True

        good_list = list()
        x = 0
        for user in list_to_parse:
            max_id, since_id, previous_loop = self.get_previous_tweet(user)
            if need_to_do(previous_loop, loop_number) is True:
                good_list.append(user)

            else:
                self.update_done_not(int(user), self.done,
                                     loop_number, self.tweets)
                x += 1

        logger.info('Good list is a lens of: {}'.format(len(good_list)))
        logger.info('Number of skipped and put in done is: {}'.format(x))
        return good_list

    def find_update(self, method, db, search=None, update=None, rand_num=None,
                    upsert=False, previous=False):
        """ """
        def remove_key(dict_, key_to_remove):
            """ """
            try:
                del dict_[str(key_to_remove)]
            except (KeyError, TypeError):
                pass
            return dict_
        if method == 'update':
            if search is None:
                search = {}
            return db.find_one_and_update(search, update, upsert=upsert)
        elif method == 'replace':
            update = remove_key(update, 'id_')
            return db.find_one_and_replace(search, update, upsert=upsert)
        elif method == 'remove':
            return db.find_one_and_delete(search)
        elif method == 'insert':
            return db.insert_one(update)

    def decide_db(self, type_db):
        """
        To select the good db in regard of the type of info parsed
        """
        if type_db == self.followers or type_db == self.friends:
            return self.db_process_link
        elif type_db == self.profile:
            return self.db_process_profile
        elif type_db == self.tweets:
            return self.db_process_tweet

    def select_size(self, info_type):
        """ Decide if the size of the temp list needs to be 15 or 100
            according to api call used
        """
        if info_type in [self.friends, self.followers]:
            return 15
        elif info_type == self.profile:
            return 100
        elif info_type == self.tweets:
            return 60

    def find_record(self, db, info_type, extra=False):
        """ """
        if extra is False:
            query = {'doing': {'$exists': False}, 'extra': {'$exists': False}}
        elif extra is True:
            query = {'doing': {'$exists': False}, 'extra': {'$exists': True}}
        if info_type in [self.friends, self.followers]:
            query['type_link'] = info_type
        return self.find_update(method='update', db=db, search=query,
                                update={'$set': {'client_id': self.client_id,
                                                 'doing': datetime.datetime.now()}})

    def get_list_record(self, info_type):
        """ """
        size = self.select_size(info_type)
        db = self.decide_db(info_type)
        temp_list = list()
        extra_list = list()
        loop_number = int()
        while len(temp_list) + len(extra_list) < size:
            try:
                doc = self.find_record(db, info_type, False)
                temp_list.append(doc[self.id_str])
                loop_number = doc[self.loop]
            # No more record to do going to check the extra
            except TypeError:
                try:
                    doc = self.find_record(db, info_type, True)
                    # temp_list.append(doc[self.id_str])
                    extra_list.append(doc[self.id_str])
                    loop_number = doc[self.loop]
                except TypeError:
                    break
        return temp_list, extra_list, loop_number

    def update_tweet_info(self, user, mode, loop_number=None):
        """ """
        id_search = {'id_str': user['id_str']}
        if mode == self.todo:
            update_dict = {'$set': {'max_id': user['max_id']}}
        elif mode == self.done:
            update_dict = {'$set': {'since_id': user['since_id'],
                                    'loop_number': loop_number},
                           '$unset': {'max_id': ''}}

        self.find_update(method='update', db=self.db_tweet_info,
                         search=id_search, update=update_dict, upsert=True)

    def update_done_not(self, to_update, mode, loop_number, type_link,
                        extra_list=None):
        """
        Receive either a list of id_str (int()) or an id_str
        and update the value to know if it is done, todo or doing
        """
        db = self.decide_db(type_link)
        if isinstance(to_update, list):
            for id_str in to_update:
                to_do_mode = mode
                # Need to check if it is in extra list to put it back as extra
                # otherwise become a Todo and it keeps continue
                if mode == 'todo' and len(extra_list) > 0:
                    if id_str in extra_list:
                        to_do_mode = 'extra'
                self.update_record(db, id_str, to_do_mode, type_link, loop_number)
        # get a dictionary and use only the key 'id_str' in case of link or profile
        elif isinstance(to_update, dict):
            self.update_record(db, to_update['id_str'], mode, type_link, loop_number)
        # Can receive the id_str directly from check_tweets
        elif isinstance(to_update, int):
            self.update_record(db, to_update, mode, type_link, loop_number)

    def update_record(self, db, id_str, mode, type_link, loop_number, retry=0):
        """ The function that update the record with the proper db """
        if type_link in [self.followers, self.friends]:
            query = {self.id_str: id_str, self.loop: loop_number, 'type_link': type_link}
        else:
            query = {self.id_str: id_str, self.loop: loop_number}
        if mode == self.todo:
            result = self.find_update(method='update', db=db, search=query,
                                      update={'$unset': {"client_id": "", "doing": ""}})
        elif mode == self.done:
            result = self.find_update(method='remove', db=db, search=query)
        elif mode == 'extra':
            result = self.find_update(method='update', db=db, search=query,
                                      update={'$set': {'extra': True},
                                              '$unset': {"client_id": "", "doing": ""}})

        # Sometime doesn't update to 'done' I don't think it is a concurrency
        # 26/01/2016 Seems that in that case, raise an error because result is
        # empty. So it seems that it is not updating to 'done' because it
        # cannot find the result.
        # logger.error('{} - {}'.format(mode, type_link))
        # logger.error(result)
        # except TypeError:
        #     logger.error('Cannot find the record with the following id_str: {}'.format(id_str))
        #     retry +=1
        #     if retry < 2:
        #         self.update_record(db, id_str, mode, type_link, loop_number, retry)

    def check_call(self, type_info):
        """ """
        if self.dict_control[type_info] is False:
            if self.dict_reset[type_info] < datetime.datetime.now():
                self.dict_control[type_info] = True
                return True
        else:
            return True

    def check_status(self, record, info_type):
        """ Check if it received a pause signal, if yes raise
            and exception catch in the run function
            If pause, update the dict_control to value Pause for later
            control
        """
        if record[self.status] == 'pause':
            logger.info('{} : PAUSE : {}'.format(info_type, self.dict_reset[info_type]))
            self.dict_control[info_type] = False
            raise PauseError()
        elif record[self.status] == 'done':
            pass
        elif record[self.status] == 'error':
            logger.info('status: {}'.format(record[self.status]))
        elif record[self.status] == 'non-existing':
            logger.info('status: {}'.format(record[self.status]))
        elif record[self.status] == 'protected':
            logger.info('status: {}'.format(record[self.status]))
        elif record[self.status] == 'suspend':
            logger.info('status: {}'.format(record[self.status]))
        elif record[self.status] == 'no-data':
            logger.info('status: {} for {}'.format(record[self.status], record['id_str']))
        else:
            logger.info('status: {}'.format(record[self.status]))

    def check_list(self, temp_list):
        """ if no record in the list, break it """
        if len(temp_list) == 0:
            raise EmptyListError()

    def check_lock(self):
        """
        Try to open the file with the client_id name, Check if a value
        "true" is in it, which means it has to pause. It helps to kill the
        running instance or doing other modification somewhere else
        """
        try:
            with open('.lock_{}'.format(str(self.client_id)), 'r') as f:
                lock = f.readline().rstrip()
                if lock == 'true':
                    return True
        except FileNotFoundError:
            self.create_lock_file()
            return False

    def run(self):
        """ """
        # Used to know which is in pause or not
        self.dict_control = {k: True for k in [self.profile, self.friends, self.followers, self.tweets]}
        self.dict_reset = {k: datetime.datetime.now() for k in [self.profile, self.friends, self.followers]}
        # to know if it is possible to reset and try again
        # infinite loop that parse the db and call Twitter with info from the db
        logger.info('Start the loop')
        while True:
            for info_type in [self.followers, self.friends, self.profile, self.tweets]:
                if self.check_call(info_type) is True:
                    try:
                        # Get a list of id to send to twitter from the db
                        list_to_parse, list_to_parse_extra, loop_number = self.get_list_record(info_type)
                        list_to_parse.extend(list_to_parse_extra)
                        self.check_list(list_to_parse)
                        logger.info('List to put into Twitter: {} : {}'.format(info_type, len(list_to_parse)))
                        if info_type == self.profile:
                            data_collected = self.process_profile(list_to_parse, loop_number)
                        elif info_type in [self.followers, self.friends]:
                            data_collected = self.process_link(list_to_parse, info_type, loop_number)
                        elif info_type == self.tweets:
                            list_to_parse = self.check_tweet(list_to_parse, loop_number)
                            data_collected = self.process_tweet(list_to_parse, loop_number)
                        # add the list to create a copy instead of a reference
                        list_to_check = list(list_to_parse)
                        for user in data_collected:
                            # Convert in epoch time to compare with time.time.now()
                            self.dict_reset[info_type] = epoche_to_datetime(user['api_call'][3])
                            self.check_status(user, info_type)
                            list_to_check.remove(user['id_str'])
                            if info_type == self.tweets:
                                # get a dictionary in case of tweet (with key 'max_id' and 'since_id')
                                self.update_tweet_info(user, self.done, loop_number)
                            self.update_done_not(user, self.done,
                                                 loop_number, info_type)
                    except EmptyListError:
                        pass
                    except PauseError:
                        logger.info('List to put back on todo: {} : {}'.format(info_type, len(list_to_check)))
                        if info_type == self.tweets:
                            # Need to try to parse the last one that raise Pause
                            # to update the max_id
                            self.update_tweet_info(user, self.todo)
                        self.update_done_not(list_to_check, self.todo,
                                             loop_number, info_type,
                                             list_to_parse_extra)
                if self.check_lock():
                    logger.info('Lock file is true, pause for 1 minute')
                    time.sleep(50)
                    logger.info('Start again in 10 sec')
                    time.sleep(5)
                    logger.info('Start again in 5 sec')
                    time.sleep(5)
                    logger.info('Start back')

                else:
                    time.sleep(1)


if __name__ == '__main__':
    pass
