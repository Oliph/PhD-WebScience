#!/usr/bin/env python
# encoding: utf-8

from multiprocessing import Process
import time
import datetime
# from server_link import LinkInfo
from server_limit import Limit
from server_rand import generateRandom
# from own_exception import dbEmpty
from logger import logger
from params import dataScheme
logger = logger('server_control', file_level=None, stream_level='DEBUG')


class ControlProcess(Process, dataScheme):
    """ """
    def __init__(self, *args, **kwargs):
        """ """
        dataScheme.__init__(self)
        Process.__init__(self)
        self.loop_interval = args[0]
        self.time_lvl2 = int(args[1])
        self.set_lvl1 = args[2]
        self.loop_number = args[3]
        self.nbr_client = args[4]
        self.db_process_profile = kwargs['process_profile']
        self.db_process_link = kwargs['process_link']
        self.db_process_tweet = kwargs['process_tweet']
        self.db_current_prof = kwargs['current_profile']
        self.db_current_link = kwargs['current_link']
        self.db_rand_2 = kwargs['rand_lvl2']
        self.db_rand_3 = kwargs['rand_lvl3']
        self.do_limit = Limit(self.loop_interval, max_user=True)
        self.limit = self.do_limit.calculing_limit()
        self.create_lock_file()

    def create_lock_file(self):
        """
        Create a lock file with a false value
        """
        with open('.lock_server_control', 'w') as f:
            f.write('false')

    def process_limit(self, nbr_client, size):
        """ """
        limit = self.do_limit.calculing_limit(nbr_client, size)
        logger.info('Max size sample. lvl1: {} - lvl2: {} - lvl3 - {}'.format(limit['lvl1'],
                                                                              limit['lvl2'],
                                                                              limit['lvl3']))
        return limit['lvl2'], limit['lvl3']

    def record_sample(self, loop_number, lvl2_size, lvl3_size):
        """
        Record the sample size taken for the loop
        """
        with open('sample_size', 'a') as f:
            f.write('{}::{}::LVL2 size - {} :: LVL3 size - {}'.format(datetime.datetime.now(),
                                                                      loop_number,
                                                                      lvl2_size,
                                                                      lvl3_size))
            f.write('\n')

    def check_lock(self):
        """
        Try to open the file with the client_id name, Check if a value
        "true" is in it, which means it has to pause. It helps to kill the
        running instance or doing other modification somewhere else
        """
        try:
            with open('.lock_server_control', 'r') as f:
                lock = f.readline().rstrip()
                if lock == 'true':
                    logger.info('Lock file is true, pause for 1 minute')
                    time.sleep(50)
                    logger.info('Start again in 10 sec')
                    time.sleep(5)
                    logger.info('Start again in 5 sec')
                    time.sleep(5)
        except FileNotFoundError:
            pass

    def keep_looking(self, db, query, lap_time=60):
        """
        Loop to keep checking until no record are return
        then exit
        """
        def check_todo(db, query):
            """ Check if there is still some profile or link to do """
            if query == 'todo':
                query = {'doing': {'$exists': False}, 'extra': {'$exists': False}}
            elif query == 'doing':
                query = {}
            return bool(db.find_one(query))

        lock = True
        while lock is True:
            lock = check_todo(db, query)
            time.sleep(lap_time)
        return

    def remove_extra_remaining(self, db):
        """ """
        query = query = {'doing': {'$exists': False}, 'extra': {'$exists': True}}
        db.delete_many(query)

    def recreate_lvl1(self):
        """
        Function to copy the set of lvl1 id_str created from the beggining
        to the new loop. Be sure to have the right one all the time instead of
        loosing it with some twitter errors
        """

        def generate_query(result):
            """
            Need to recreate the query after every insert otherwise some
            field are removed after the insertion in a db
            """
            query = result
            return query

        for id_str in self.set_lvl1:
            info_user = dict()
            info_user['id_str'] = int(id_str)
            info_user['loop_number'] = self.loop_number
            update ={'$unset': {'extra': ''}}

            search = generate_query(info_user)
            self.db_process_profile.update_one(search, update, upsert=True)
            search = generate_query(info_user)
            self.db_process_tweet.update_one(search, update, upsert=True)

            info_user['type_link'] = 'followers'
            search = generate_query(info_user)
            self.db_process_link.update_one(search, update, upsert=True)

            info_user['type_link'] = 'friends'
            search = generate_query(info_user)
            self.db_process_link.update_one(search, update, upsert=True)

    def remove_previous_loop(self, db):
        """ """
        db.delete_many({self.loop: self.loop_number})

    def run(self):
        """ """
        while True:
            # Continue to do activity and updating storing while client
            # Still have to do twitter call
            logger.info('New loop: {}'.format(self.loop_number))

            # logger.info('Clean the Rand_lvl2 db')
            # self.remove_previous_loop(self.db_rand_2)

            # logger.info('Clean the Rand_lvl3 db')
            # self.remove_previous_loop(self.db_rand_3)

            logger.info('Get lock until all links are finished')
            self.keep_looking(self.db_process_link, 'todo')
            self.check_lock()

            logger.info('Get lock until all profiles are finished')
            self.keep_looking(self.db_process_profile, 'todo')

            logger.info('Remove extra links')
            self.remove_extra_remaining(self.db_process_link)
            self.check_lock()

            logger.info('Remove extra profile')
            self.remove_extra_remaining(self.db_process_profile)
            self.check_lock()

            logger.info('Remove extra tweet')
            self.remove_extra_remaining(self.db_process_tweet)
            self.check_lock()

            logger.info('Wait until all links are done')
            self.keep_looking(self.db_process_link, 'doing')
            self.check_lock()

            logger.info('Wait until all profile are done')
            self.keep_looking(self.db_process_profile, 'doing')
            self.check_lock()

            logger.info('Calculate the limit of lvl2 and lvl3')
            lvl2_sample_size, lvl3_sample_size = self.process_limit(len(self.nbr_client), len(self.set_lvl1))

            logger.info('Record the sample size')
            self.record_sample(self.loop_number + 1, lvl2_sample_size, lvl3_sample_size)
            self.check_lock()

            logger.info('Pause until all tweets are completely finished')
            self.keep_looking(self.db_process_tweet, 'todo')

            logger.info('Get lock until all tweets are finished')
            self.keep_looking(self.db_process_tweet, 'doing')

            logger.info('Start the rand process')
            rand_list = []
            rand_list.append(generateRandom(2, self.db_process_profile,
                                            self.db_process_link,
                                            self.db_process_tweet,
                                            self.db_rand_2, lvl2_sample_size,
                                            self.loop_number))
            rand_list.append(generateRandom(3, self.db_process_profile,
                                            self.db_process_link,
                                            self.db_process_tweet,
                                            self.db_rand_3, lvl3_sample_size,
                                            self.loop_number))

            for rand_process in rand_list:
                rand_process.start()
                rand_process.join()
            self.check_lock()

            self.loop_number += 1
            # reset the number of client
            self.nbr_client = set()

            logger.info('Update the lvl1 to todo')
            self.recreate_lvl1()
            self.check_lock()


def main():
    """ """
    pass

if __name__ == '__main__':
    main()
