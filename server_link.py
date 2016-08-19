#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import random
import time

from multiprocessing import Process
from params import dataScheme
# My own modules
from utils import find_update
from logger import logger
logger = logger('server_link_change', stream_level='INFO')


class LinkInfo(object):
    """
    """
    def __init__(self, loop_interval):
        ""
        self.loop_interval = loop_interval
        # FIXME HARD CODED NEED TO CHANGE THAT
        self.limit = 5000

    def create_set(self, input_list):
        """ """
        if len(input_list) > self.limit:
            return set(input_list[:self.limit]), set(input_list[self.limit:])
        else:
            return set(input_list), None

    def over_time(self, previous_loop, current_loop):
        """ If the last loop record is too old """
        try:
            if current_loop - previous_loop > ((self.loop_interval* 2) +1):
                return True
        except TypeError:  # last_loop is None in case of first record
            return False

    def over_limit(self, set_current_second, set_previous_second, mode):
        """ Check if the number is not too high for the comparison """
        if mode == 'added':
            if set_current_second is not None and set_previous_second is None:
                return True
        elif mode == 'removed':
            if set_previous_second is not None and set_current_second is None:
                return True

    def link_changed(self, to_rm1, to_rm2, to_check):
        """ """
        def second_check(result, second_check):
            """ check and remove if same id in the second list """
            for element in second_check:
                if element in result:
                        result.remove(element)
            return result

        def transform(result):
            """Transform set into a list or a None object"""
            if isinstance(result, set):
                if len(result) > 0:
                    return [elt for elt in result]
                else:
                    return None
            else:
                return None

        result = to_rm1 - to_rm2
        if to_check is not None:
            result = second_check(result, to_check)
        return transform(result)

    def process(self, *args):
        "Get the info from followers or friends and return a list"
        # TODO Knowing what to do if doesn't
        # Pass the check_data, right now
        # Just pass, nothing more
        previous_list = args[0]
        current_list = args[1]
        set_previous, set_previous_second = self.create_set(previous_list['list'])
        set_current, set_current_second = self.create_set(current_list['list'])
        result = dict()
        if self.over_time(previous_list['loop_number'], current_list['loop_number']) is True:
            result['added'] = 'Too old'
            result['removed'] = 'Too old'
        else:
            if self.over_limit(set_current_second, set_previous_second, 'added') is True:
                result['added'] = 'Too much'
            else:
                result['added'] = self.link_changed(set_current, set_previous, set_current_second)
            if self.over_limit(set_current_second, set_previous_second, 'removed') is True:
                result['removed'] = 'Too much'
            else:
                result['removed'] = self.link_changed(set_previous, set_current, set_previous_second)
        return result


class processLink(Process, dataScheme):
    """
    Class to processing the link -- compare and store into the db
    """
    def __init__(self, *args, **kwargs):
        """
        """
        dataScheme.__init__(self)
        Process.__init__(self)
        self.set_lvl1 = args[0]
        self.loop_interval = args[1]
        self.db_current_link = kwargs['current_link']
        self.db_stored_link = kwargs['stored_link']
        self.db_stored_profile = kwargs['stored_profile']
        self.db_rand_2 = kwargs['rand_lvl2']
        self.db_rand_3 = kwargs['rand_lvl3']
        self.db_change_link = kwargs['change_link']
        self.db_context_link = kwargs['context_link']
        self.do_link = LinkInfo(self.loop_interval)

    def process_link(self, current_link):
        """ """
        def sample_change(user, link, input_list):
            """
            Function to random the list based on the number of links the users has
            Return a list of lvl2 and a list of lvl3 according to the limit fixed
            Round up if the percentage is a float number to be sure having at leas
            one user change.
            Input:
            * user: id_str of user to return the right user
            * link: type of link to return the appropriate type of link number
            * input_list: list of users to return to lvl2 or lvl3
            """
            try:
                user_record = self.db_stored_profile.find_one({self.id_str: user})
                count = user_record['{}_count'.format(link)]
            # In case doesn't found the recocrd
            except TypeError:
                logger.error('Not finding stored profile for: {} with link: {}'.format(user, link))
                return input_list, []
            if count < 1000:
                limit = 1
            elif count >= 1000 and count < 2000:
                limit = 1

            elif count >= 2000 and count < 5000:
                limit = 0.8
            elif count >= 5000 and count < 10000:
                limit = 0.8
            elif count >= 10000:
                limit = 0.5
            # random the list
            random.shuffle(input_list)
            # calculate the index of the list
            x = math.ceil(len(input_list) * limit)
            return input_list[:x], input_list[x:]

        loop_number = current_link[self.loop]
        type_link = current_link[self.type_link]
        id_search = {self.id_str: current_link[self.id_str], self.type_link: type_link}
        if isinstance(current_link[self.list_link], list) is True:
            find_update('insert', self.db_context_link, update=current_link)
            for elt in current_link[self.list_link]:
                find_update('insert', self.db_rand_3, update={self.loop: loop_number +1, self.id_str: int(elt)})
            stored_link = find_update('update', self.db_stored_link, id_search, current_link)
        if stored_link and current_link[self.id_str] in self.set_lvl1:
            change = self.do_link.process(stored_link, current_link)
            for key in ['added', 'removed']:
                if change[key] is not None:
                    dict_to_insert = {self.loop: loop_number,
                                      self.time: current_link[self.time]}
                    dict_to_insert[key] = change[key]
                    dict_to_insert.update(id_search)
                    try:
                        find_update('insert', self.db_change_link, update=dict_to_insert)
                    except TypeError as e:
                        logger.error('Error, No Idea why: {}'.format(e))
                        # logger.error('Dict to insert: {}'.format(dict_to_insert))
                        # logger.error('Change to insert: {}'.format(change[key]))

                if isinstance(change[key], list):
                    list_lvl2, list_lvl3 = sample_change(current_link[self.id_str], type_link, change[key])
                    for user in list_lvl2:
                        if user not in self.set_lvl1:
                            find_update('insert', self.db_rand_2, update={self.loop: loop_number +1, self.id_str: int(user)})
                    for user in list_lvl3:
                        if user not in self.set_lvl1:
                            find_update('insert', self.db_rand_3, update={self.loop: loop_number +1, self.id_str: int(user)})

    def run(self):
        """ """
        while True:
            current_link = self.db_current_link.find_one_and_delete({})
            if current_link:
                self.process_link(current_link)
            else:
                time.sleep(60)

###############################################################################
###############################################################################
if __name__ == '__main__':
    pass
