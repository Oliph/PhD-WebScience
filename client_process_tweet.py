#!/usr/bin/env python
# -*- coding:UTf8 -*-
import datetime
from string import punctuation

from logger import logger

logger = logger(name='client_process_tweet', stream_level='INFO')

class ProcessingTweet(object):

    def __init__(self, collection):
        """ """
        self.collection = collection
        self.list_punctuation = list(punctuation)
        # self.list_id = list_id

    def run(self, tweet, loop):
        """ Process the tweet received throught the Stream
            Receive a dictionary
            Output a dict
        """
        try:
            self.tweet = tweet
            self.tweetdict = {}
            self.get_retweet()
            self.get_entities()
            self.parse_key()
            self.split_tweet()
            self.add_loop(loop)
            # self.add_id(unique_id)
            # self.insert_id()
            # self.insert_sentiment()
            self.record_tweet()
        except Exception as e:
            logger.info('{}: Error in processingTweet, {}, exit'.format(datetime.datetime.now(), e))

    def get_retweet(self):
        """ Get URL - Hashtag Txt and RT """

        try:
            self.tweet['retweeted_status']
            try:
                self.tweetdict['text'] = self.tweet['retweeted_status']['text']
                self.tweetdict['RT'] = self.tweet['retweet_count']
                self.entities = self.tweet['retweeted_status']['entities']
            except KeyError:
                pass
        except KeyError:
            try:
                self.tweetdict['text'] = self.tweet['text']
                self.tweetdict['RT'] = 0
                self.entities = self.tweet['entities']
            except KeyError:
                pass

    def get_entities(self):
        """ Get the URL and the Hashtag from entities dic """
        for key in self.entities:
            try:
                if len(self.entities[key]) > 0:
                    for key in ['urls', 'hashtags', 'user_mentions']:
                        try:
                            self.entities[key]
                            for elt in self.entities[key]:
                                if key == 'urls':
                                    self.tweetdict.setdefault(key,
                                                              []).append(elt['expanded_url'])
                                elif key == 'user_mentions':
                                    self.tweetdict.setdefault(key,
                                                              []).append(elt['id_str'])
                                else:
                                    self.tweetdict.setdefault(key,
                                                              []).append(elt[key])
                        except KeyError:
                            pass

            except UnboundLocalError as e:
                print('ERROR IN THE TWEET')
                raise e

    def parse_key(self):
        """ Parse the normal key from the tweet """
        tweet_key = ['created_at', 'id_str', 'coordinates']
        user_key = ['id_str', 'screen_name']
        try:
            for key in tweet_key:
                self.tweetdict[key] = self.tweet[key]
            for key in user_key:
                self.tweetdict.setdefault('user', {}).update({key: self.tweet['user'][key]})
        except KeyError as e:
            logger.info('{}: Process Tweet Missing field \
                            in the tweet - {}'.format(datetime.datetime.now(),  e))

    def split_tweet(self):
        """ Splitting tweet in list of word and cleaning @ and # """
        self.split_text = self.tweet['text'].lower()
        for p in self.list_punctuation:
            self.split_text = self.split_text.replace(p, '')
        self.split_text = self.split_text.split(' ')
        for word in self.split_text:
            if word[:3] is 'http':
                self.split_text.remove(word)
            # if len(word) == 2 and word is 'rt':
            #     self.split_text.remove(word)

    def add_loop(self, loop):
        self.tweetdict['loop_number'] = loop

    # def add_id(self, unique_id):
        # self.tweetdict['unique_id'] = unique_id

    def record_tweet(self):
        """ Record the tweet in MongoDB
        """
        try:
            if self.tweetdict:
                self.collection.insert(self.tweetdict)
        # FIXME Need to check the right error here - too vague, bug in writing body
        except Exception as e:
            logger.critical('{}: Error in inserting tweet in db: {}'.format(datetime.datetime.now(), e))

    # def insert_id(self):
    #     """ """
    #     print self.tweet['text']
    #     print self.split_text
    #     for id_str in self.list_id:
    #         match = set(self.split_text).intersection(set(id_str))
    #         if len(match) > 0:
    #             print match
    #         if len(match) == len(id_str):
    #             self.tweetdict.setdefault('list_id', []).append(id_str)

    # def insert_sentiment(self):
    #     """ """
    #     result = []  # is list with each word in tweets and value assigned
    #     x = 0
    #     for line in self.split_text:
    #         try:
    #             result.append(self.score[line])
    #         except:
    #             pass
    #     for score in result:
    #         x = x+float(score)
    #     avg = x/len(self.split_text)
    #     self.tweetdict['sentiment'] = avg
