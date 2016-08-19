#!/usr/bin/env python
# encoding: utf-8

# Python modules
import json
import configparser as ConfigParser
import uuid
import zmq

from pymongo import MongoClient
# from pymongo import errors as PyError
# Added modules
# Own Modules
DBS_NAMES = ['process_profile',
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
             'rand_lvl3']


class InitProcess(object):

    def __init__(self, config_file, twitter_key_file, TYPE_APP):
        """ """
        self.config_file = config_file
        self.twitter_key_file = twitter_key_file
        self.type_app = TYPE_APP

    def init_values(self):
        """ """
        self.read_config(self.config_file)
        self.twitter_key = self.get_keys(self.twitter_key_file)
        self.get_connection()
        print('Build DBS')
        self.dbs = self.build_dbs()

        return {'twitter_key': self.twitter_key, 'client_id': self.client_id,
                'dbs': self.dbs, 'host': self.host,
                'status_port': self.status_port, 'pull_port': self.pull_port,
                'type_app': self.type_app}

    def read_config(self, config_file):
        """ In Config file, get the host - port """
        Config = ConfigParser.ConfigParser()
        Config.read(config_file)
        for section in Config.sections():
            for option in Config.options(section):
                setattr(self, option, Config.get(section, option))

    def get_connection(self):
        """ """

        context = zmq.Context()
        sock = context.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, 10000)
        sock.connect('tcp://{}:{}'.format(self.host, self.status_port))
        sock.send('id_request'.encode('utf-8'))
        try:
            data = sock.recv()
            self.parse_data(data)
            sock.close()
            context.term()
            print('End of connection to server_control')
        except zmq.error.Again:
            print('No connection - Init own values')
            print('Generate an unique uuid')
            self.client_id = str(uuid.uuid4())
            print('Get dbs_names')
            self.dbs_names = DBS_NAMES
            # sock.close()
            # context.term()
            return
        # time.sleep(15)

    def parse_data(self, data):
        """ Get a dict with keys
            - client_id: str()
            - dbs: str()
        """
        data = json.loads(data.decode())
        for key in data:
            print(key, data[key])
            setattr(self, key, data[key])

    def build_dbs(self):
        "" ""
        print("in")
        if self.host is None:
            print('none')
            c = MongoClient()
        else:
            c = MongoClient(self.host)
            print(c)
        db = c[self.db_name]
        print(db)
        return {k: db[k] for k in self.dbs_names}

    def get_keys(self, twitter_file):
        """ """
        keydict = {}
        with open(twitter_file, 'r') as f:
            for line in f:
                key, val = line.split(':')
                keydict[key] = val[:-1]
        return keydict


def main():
    pass


if __name__ == '__main__':
    main()
