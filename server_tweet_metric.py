#!/usr/bin/env python
# encoding: utf-8

from multiprocessing import Process
from logger import logger
logger = logger('tweet_metrics', stream_level='INFO')


class tweetMetric(Process):
    """ """
    def __init__(self, context_number, **kwargs):
        """ """
        Process.__init__(self)
        self.context_number = context_number
        self.current_tweet = kwargs['current_tweet']

    def run(self):
        """ """
        while True:

