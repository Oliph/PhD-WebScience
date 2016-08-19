#!/usr/bin/env python
# encoding: utf-8
# Python modules
# from multiprocessing import Queue
import time
import sys
# Own Modules
from server_init import InitProcess
from server_control import ControlProcess as server_control
# from server_context import contextTransforming as context_metric
from server_link import processLink as process_link
from server_profile import processProfile as process_profile
# Logging
from logger import logger
logger = logger(name='server_main', file_level=None)

RESTART = sys.argv[1]


def main():
    logger.info('Read value from the config file')

    init_ = InitProcess('./config/config.ini', RESTART)
    values = init_.init_values()

    databases = values['databases']
    loop_interval = values['loop_interval']
    time_lvl2 = values['time_lvl2']
    set_lvl1 = values['set_lvl1']
    loop_number = values['loop_number']
    nbr_client = values['nbr_client']

    # control_data = Queue()
    # context_number = Queue()
    control_process = server_control(loop_interval, time_lvl2,
                                     set_lvl1, loop_number, nbr_client,
                                     **databases)
    process_links = process_link(set_lvl1, loop_interval, **databases)
    process_profiles = process_profile(**databases)

    # metric_process = context_metric(**databases)

    time.sleep(1)
    control_process.start()
    process_links.start()
    process_profiles.start()
    # metric_process.start()
    logger.info('Launching all the processes')


if __name__ == '__main__':
    main()
