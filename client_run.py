#!/usr/bin/env python
# encoding: utf-8
# Python modules
# from multiprocessing import Queue, Event
import time
import sys
# Own Modules
from client_init import InitProcess
from client_REST_api import RESTAPI as RESTApi
# from client_stream import TwitterStream as StreamApi
# from client_connect import clientConnection
# Logging
from logger import logger
logger = logger(name='client_main', stream_level='INFO', file_level='INFO')


TWITTER_KEYS_FILE = sys.argv[1]
TYPE_APP = sys.argv[2]


def main():
    """ """
    logger.info('Read value from the config file')
    init_values = InitProcess('./config/client_config.ini', TWITTER_KEYS_FILE, TYPE_APP)
    values = init_values.init_values()
    twitter_key = values['twitter_key']
    # client_id = values['client_id']
    databases = values['dbs']
    client_id = values['client_id']
    type_app = values['type_app']
    # host = values['host']
    # status_port = values['status_port']
    # pull_port = values['pull_port']

    # connect_control = Event()
    # connect_info = Queue()
    # profile_control = Event()
    # profile_info = Queue()
    # stream_control = Event()
    # stream_info = Queue()
    # Need to sleep until the server init is finished otherwise will received
    # the init value again. TODO change the client_connect to check the
    # type of data it received and set up a key to avoid confusion
    # client = clientConnection(connect_control, connect_info, client_id,
    # host, status_port, pull_port)
    profile_process = RESTApi(twitter_key, client_id, type_app, **databases)
    # stream_process = StreamApi(twitter_key, **databases)

    logger.info('Start the processes')
    time.sleep(20)
    # client.start()
    profile_process.start()
    # stream_process.start()
    # logger.info('Get into the loop')
    # while True:
    # logger.info('Get new list from server')
    # new_list = connect_info.get()
    # logger.info('Put the info into the client REST API')
    # profile_info.put(new_list)
    # logger.info('Launch the REST API')
    # profile_control.clear()
    # logger.info('Stop the Stream API')
    # stream_control.clear()
    # logger.info('Put the info into the Stream API')
    # stream_info.put(new_list)
    # logger.info('Wait for the info from the profile')
    # profile_control.wait()
    # logger.info('Launch the server connection')
    # connect_control.set()

if __name__ == '__main__':
    main()
