#!/usr/bin/env python
# encoding: utf-8

# Python modules
import datetime
import time
import configparser as ConfigParser
import json
import uuid
import zmq

# Added modules
import pymongo
from pymongo import MongoClient
from pymongo import errors as PyError
# Own Modules

from logger import logger
logger = logger('server_init', stream_level='INFO')


class InitProcess(object):

    def __init__(self, config_file, restart):
        """ """
        self.config_file = config_file
        self.dbs_names = ['process_profile',
                          'current_profile',
                          'stored_profile',
                          'process_link',
                          'current_link',
                          'stored_link',
                          'change_link',
                          'context_link',
                          'context',
                          'activity',
                          'process_tweet',
                          'tweet_info',
                          'stored_tweet',
                          'rand_lvl2',
                          'rand_lvl3',
                          'full_link']

        self.restart = restart.lower()

    def init_values(self):
        """ """
        self.read_config(self.config_file)
        self.dbs = self.build_dbs(self.address_db, self.db_name)
        logger.info(self.dbs)

        self.loop_interval = int(self.loop_interval)
        self.time_lvl2 = self.get_time_lvl2()
        if self.restart == 'false':
            self.build_index(**self.dbs)
            self.twitter_key = self.get_keys(self.twitter_file)
            self.client_id = self.get_clients()
            lvl1_list = self.get_lvl1(self.lvl1_file)
            self.set_lvl1 = set([int(i) for i in lvl1_list])
            self.loop_number = 1
            self.nbr_client = set()
            self.record_lvl1(lvl1_list)
            # self.max_limit = Limit(self.loop_interval, len(self.client_id)).calculing_limit()
            self.start_time = datetime.datetime.now()
            # self.write_start()
            # self.stop_time = self.get_stop_time()
            # These values are set up in the config file
        elif self.restart == 'true':
            self.client_id = self.get_clients()
            lvl1_list = self.get_lvl1(self.lvl1_file)
            self.set_lvl1 = set([int(i) for i in lvl1_list])
            self.get_restart()

        else:
            raise('Need to enter "true" or "false"')

        return {'loop_interval': self.loop_interval,
                'databases': self.dbs,
                'time_lvl2': self.time_lvl2,
                'set_lvl1': self.set_lvl1,
                'loop_number': self.loop_number,
                'nbr_client': self.client_id}

    def get_restart(self):
        """ function to get the values from the db to restart """
        # self.nbr_client = set()
        # self.set_lvl1 = set()
        loops = set()
        logger.info('Doing Profile for loop')
        for profile in self.dbs['process_profile'].find({}, {'loop_number': True, '_id': False}):
            # self.nbr_client.add(profile.pop('profile_client_id', None))
            loop_number = profile['loop_number']
            if loop_number:
                loops.add(loop_number)
            if len(loops) == 2:
                logger.info('Get two loops from profile: {}'.format(loops))
                break
        logger.info('Doing Links for loop')
        for links in self.dbs['process_link'].find({}, {'loop_number': True, '_id': False}):
            # self.nbr_client.add(profile.pop('profile_client_id', None))
            loop_number = links['loop_number']
            if loop_number:
                loops.add(loop_number)
            if len(loops) == 2:
                logger.info('Get two loops from links: {}'.format(loops))
                break
        logger.info('Doing rand_3 for loop')
        for links in self.dbs['rand_lvl3'].find({}, {'loop_number': True, '_id': False}):
            # self.nbr_client.add(profile.pop('profile_client_id', None))
            loop_number = links['loop_number'] - 1
            if loop_number:
                loops.add(loop_number)
            if len(loops) == 2:
                logger.info('Get two loops from rand_lvl3: {}'.format(loops))
                break
        logger.info('Doing rand_2 for loop')
        for links in self.dbs['rand_lvl2'].find({}, {'loop_number': True, '_id': False}):
            # self.nbr_client.add(profile.pop('profile_client_id', None))
            loop_number = links['loop_number'] - 1
            if loop_number:
                loops.add(loop_number)
            if len(loops) == 2:
                logger.info('Get two loops from rand_lvl2: {}'.format(loops))
                break
                # self.set_lvl1.add(profile.pop('id_str', None))
        self.loop_number = min(int(s) for s in loops)
        logger.info('loop_number: {}'.format(self.loop_number))

    def write_start(self):
        """ """
        json.dump({'loop_interval': self.loop_interval,
                   'client_id': self.client_id,
                   'time_lvl2': self.time_lvl2,
                   'databases': str(self.dbs),
                   'start_time': str(self.start_time),
                   'lvl1': self.set_lvl1}, open('start_params.txt', 'w'))

    def read_config(self, config_file):
        Config = ConfigParser.ConfigParser()
        Config.read(config_file)
        for section in Config.sections():
            for option in Config.options(section):
                setattr(self, option, Config.get(section, option))

    def build_dbs(self, address, db_name):
        "" ""
        if address is None:
            c = MongoClient()
        else:
            c = MongoClient(address)
        db = c[db_name]
        return {k: db[k] for k in self.dbs_names}

    def build_index(self, **kwargs):
        """ """
        try:
            kwargs['process_profile'].create_index([('id_str', pymongo.DESCENDING),
                                                    ('loop_number', pymongo.ASCENDING)],
                                                   unique=True)
            kwargs['process_profile'].create_index('extra')
            kwargs['process_profile'].create_index('doing')

            kwargs['stored_profile'].create_index('id_str', unique=True)

            kwargs['process_link'].create_index([('id_str', pymongo.ASCENDING),
                                                ('type_link', pymongo.DESCENDING)],
                                                unique=True)
            kwargs['full_link'].create_index([('id_str', pymongo.ASCENDING),
                                              ('type_link', pymongo.DESCENDING),
                                              ('loop_number', pymongo.ASCENDING)],
                                             unique=True)

            kwargs['process_tweet'].create_index('id_str', unique=True)
            kwargs['process_tweet'].create_index('processing')

            kwargs['stored_link'].create_index('type_link')
            kwargs['stored_link'].create_index('id_str')

            kwargs['context_link'].create_index('loop_number')
            kwargs['context_link'].create_index('loop_number')

            kwargs['activity'].create_index('id_str')
            kwargs['activity'].create_index('loop_number')

            kwargs['context'].create_index('id_str')
            kwargs['stored_tweet'].create_index('id_str', unique=True)
            kwargs['tweet_info'].create_index('id_str', unique=True)
            kwargs['rand_lvl2'].create_index('id_str', unique=True)
            kwargs['rand_lvl3'].create_index('id_str', unique=True)

        except PyError.ServerSelectionTimeoutError:
            raise('Error in DBS connection, check if MongoDB is alive')

    def get_keys(self, twitter_file):
        """ """
        keydict = {}
        with open(twitter_file, 'r') as f:
            for line in f:
                key, val = line.split(':')
                keydict[key] = val[:-1]
        return keydict

    def get_clients(self):
        """ """
        client_id = list()

        context_status = zmq.Context()
        timeout_start = time.time()

        status_sock = context_status.socket(zmq.REP)
        status_sock.setsockopt(zmq.RCVTIMEO, 2000)
        # status_sock.setsockopt(zmq.RCVTIMEO, 2000)
        status_sock.bind("tcp://0.0.0.0:{}".format(self.status_port))
        while time.time() < (timeout_start + 10):
            try:
                client_to_connect = status_sock.recv()
                if client_to_connect.decode() == 'id_request':
                    _id = str(uuid.uuid4())
                    data = {'client_id': _id, 'dbs_names': self.dbs_names}
                    status_sock.send_json(data)
                    client_id.append(_id)
                else:
                    status_sock.send('too early'.encode('utf-8'))
            except zmq.error.Again:
                pass
        # time.sleep(2)

        logger.info('Get {} clients'.format(len(client_id)))
        logger.info('Close the socket')
        status_sock.close()
        context_status.term()
        logger.info('Socket closed')
        return client_id

    def get_time_lvl2(self):
        """docstring for time_restrain"""
        try:
            if int(self.time_lvl2) != 1:
                if int(self.time_lvl2) < int(self.loop_interval):
                    value = int(self.loop_interval) *4
                else:
                    value = int(self.loop_interval)* int(self.time_lvl2)
            else:
                value = int(self.time_lvl2)
        except ValueError:  # In case of None or string or anything
                            # not a number
            value = int(self.loop_interval)* 4
        return value

    def get_lvl1(self, lvl1_file):
        logger.info('Getting the lvl1 file - {}'.format(lvl1_file))
        with open(lvl1_file, 'r') as f:
            return [line[:-1] for line in f]

    def record_lvl1(self, lvl1_list):
        """ """
        for user in lvl1_list:
            info_user = dict()
            info_user['id_str'] = int(user)
            info_user['loop_number'] = 1

            self.dbs['process_profile'].insert_one(info_user)
            self.dbs['process_tweet'].insert_one(info_user)
            info_user['type_link'] = 'followers'
            self.dbs['process_link'].insert_one(info_user)
            info_user['type_link'] = 'friends'
            try:
                del info_user['_id']
            except KeyError:
                pass
            self.dbs['process_link'].insert_one(info_user)


def main():
    pass


if __name__ == '__main__':
    main()
