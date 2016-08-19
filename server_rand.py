#!/usr/bin/env python
# encoding: utf-8
import sys
import pymongo
from pymongo import MongoClient
import configparser as ConfigParser
from multiprocessing import Process

# from own_exception import dbEmpty
from logger import logger
from params import dataScheme
logger = logger('server_rand', file_level=None, stream_level='DEBUG')


class StopRandom(Exception):
    """ """
    def __init__(self):
        pass


class generateRandom(Process, dataScheme):
    """
    Class to output random id Use in a multiprocessing way.
    Receive a collection and a size of sample.
    When the sample output == the size, rename the others record as extra and
    output all of them in the db
    db_process_{profile/tweet/link} : the db that contains the sample
    sample_db: where to output the sampled record
    size: the size of the sample
    """
    def __init__(self, lvl_to_do, db_profile, db_link, db_tweet, sample_db, sample_size, loop_number):
        """ """
        dataScheme.__init__(self)
        Process.__init__(self)
        self.lvl_to_do = lvl_to_do
        self.db_profile = db_profile
        self.db_link = db_link
        self.db_tweet = db_tweet
        self.sample_db = sample_db
        self.sample_size = sample_size
        # Need to add one because the loop_number passed through is the
        # current loop, while the sampling is done for the next loop
        self.loop_number = loop_number +1

    def similar_sample_size(self, db, sample_size):
        """
        Check the size of the db with the pymongo count()
        if the db size is equal to the sample sample_size
        return True, else return False
        """
        self.db_size = db.count({'loop_number': self.loop_number})
        # logger.info('Size of the record to do: {}'.format(db_size))
        if self.db_size <= sample_size:
            return True
        else:
            return False

    def run(self):
        """ """
        self.duplicated = 0
        self.inserted = 0
        self.updated = 0
        if self.similar_sample_size(self.sample_db, self.sample_size) is True:
            logger.info('lvl {} lower than the dbsize - {}, simple copy: {}'.format(self.lvl_to_do, self.db_size, self.sample_size))
            if self.lvl_to_do == 2:
                self.simple_copy(profile=True, link=True, tweet=True)
            else:
                self.simple_copy(profile=True)
        else:
            logger.info('lvl {} higher than the dbsize - {}, random compy: {}'.format(self.lvl_to_do, self.db_size, self.sample_size))
            if self.lvl_to_do == 2:
                self.random_copy(self.sample_size, profile=True, link=True, tweet=True)
                # In case of lvl2 not being taken, being sure they are still recorded as
                # lvl2 otherwise not information is collected from them. So update the record
                # as if it was a lvl3
                # Can marginally create more lv3 than expected but on a limited number
                self.simple_copy(profile=True)
                # Then copy all the remaining as extra in the database.
                self.simple_copy(profile=False, link=True, tweet=True, extra=True)
            else:
                self.random_copy(self.sample_size, profile=True)
                # Then copy the remaining as extra
                self.simple_copy(profile=True, extra=True)
        logger.info('Total: {} - Inserted: {} - Updated: {}'.format(self.db_size, self.inserted, self.updated, self.duplicated))
        removed_result = self.sample_db.delete_many({'loop_number': self.loop_number})
        logger.info('Rand {} - Removed {} records'.format(self.lvl_to_do, removed_result.deleted_count))

    def simple_copy(self, profile=False, link=False, tweet=False, extra=False):
        """
        In case of sample_size if equal size of db,
        simple copy the entire db into the sample_db
        """
        for record in self.sample_db.find({'loop_number': self.loop_number}, {'_id': False}, no_cursor_timeout=True):

            self.process_field(record, profile, link, tweet, extra)

    def random_copy(self, sample_size, profile=False, link=False, tweet=False, size_sample=10000):
        """
        In case the db_sample_size is higher than the sample sample_size
        random the record and then record them in sample_db
        """
        i = 0

        while True:
            try:
                pipeline = [{'$match': {'loop_number': self.loop_number}}, {'$sample': {'size': size_sample}}]
                for result in self.sample_db.aggregate(pipeline):
                    self.process_field(result, profile, link, tweet)
                    self.sample_db.delete_one({'_id': result['_id']})
                    i +=1
                    if i == sample_size:
                        raise StopRandom()
            except StopRandom:
                break

    def process_field(self, result, profile=False, link=False, tweet=False, extra=False):
        """ """
        def generate_query(result, extra=False, link=False):
            """
            Need to recreate the query after every insert otherwise some
            field are removed after the insertion in a db
            """
            query = {self.id_str: result[self.id_str], self.loop: result[self.loop]}
            if extra is True:
                query['extra'] = True
            if link is not False:
                query['type_link'] = link
            return query

        if extra is True:
            mode = 'insert'
        else:
            mode = 'update'

        if profile is True:
            query = generate_query(result, extra)
            self.update_db(self.db_profile, mode, query)
        if tweet is True:
            query = generate_query(result, extra)
            self.update_db(self.db_tweet, mode, query)
        if link is True:
            query = generate_query(result, extra, link='friends')
            self.update_db(self.db_link, mode, query)

            query = generate_query(result, extra, link='followers')
            self.update_db(self.db_link, mode, query)

    def update_db(self, db, mode, query):
        """ """
        if mode == 'update':
            search = {self.id_str: query[self.id_str]}
            try:
                del query[self.id_str]
            except KeyError:
                pass
            try:
                search['type_link'] = query['type_link']
            except KeyError:
                pass
            update ={'$set': query, '$unset': {'extra': ''}}
            db.update_one(search, update, upsert=True)
            self.updated +=1
        elif mode == 'insert':
            try:
                self.inserted +=1
                db.insert_one(query)
            except pymongo.errors.DuplicateKeyError:
                self.duplicated += 1
                pass


def main():
    """ """
    def connectDB(**kwargs):
        """ """
        # c = MongoClient(kwargs['host'])
        c = MongoClient(kwargs['host'])

        db = c[kwargs['db']]
        return db

    def read_config(config_file):
        Config = ConfigParser.ConfigParser()
        Config.read(config_file)
        return_values = dict()
        for section in Config.sections():
            for option in Config.options(section):
                return_values[option] = Config.get(section, option)
        return return_values
    restart = sys.argv[1]
    logger.info(restart)
    lvl2_size = 100
    size_previous = 1000
    loop_number = 1
    db = connectDB(db='test', host='localhost')
    logger.info('Connect to db: {}'.format(db))

    db_process_profile = db['process_profile']
    db_process_link = db['process_link']
    db_process_tweet = db['process_tweet']
    db_rand_2 = db['rand_lvl2']

    # Clean the dbs
    logger.info('Clean process link')
    db_process_link.remove({})
    logger.info('Clean process profile')
    db_process_profile.remove({})
    logger.info('Clean process tweet')
    db_process_tweet.remove({})

    # Generate fake data:
    if restart.lower() in ['true', '1', 1, 'True', 'yes']:
        logger.info('Clean the rand_lvl2')
        db.rand_lvl2.remove({})
        logger.info('Regenerate data: {} size'.format(size_previous))
        for i in range(size_previous):
            db_rand_2.insert({'id_str': i, 'loop_number': loop_number})
        for i in range(44):
            db_rand_2.insert({'id_str': i, 'loop_number': 44})
    # while True:
    #     pipeline = [{'$match': {'loop_number': loop_number}}, {'$sample': {'size': 10000}}]
    #     for data in db.rand_lvl2.aggregate(pipeline):
    #         db.rand_lvl2.delete_one({'_id': data['_id']})
    #         print(data)

    rand_ = generateRandom(2, db_process_profile, db_process_link, db_process_tweet,
                           db_rand_2, lvl2_size, loop_number)
    rand_.run()


if __name__ == '__main__':
    main()
