#!/usr/bin/env python
# -*- coding: utf-8 -*-

from limit import Limit
from random_generator import random_lvl3, random_lvl2
# Logging
from logger import logger
logger = logger('loopCreation')


class loopCreation(object):

    def __init__(self, time_lvl2, loop_interval):
        self.time_lvl2 = time_lvl2
        self.loop_interval = loop_interval

    def run(self, *args):
        """ """
        def set_list(dict_input):
            for key in dict_input:
                dict_input[key] = list(set(dict_input[key]))
            return dict_input

        lvl1 = args[0]
        logger.info('Received lvl1 list: {}'.format(len(lvl1)))
        lvl2 = args[1]
        logger.info('Received lvl2 list: {}'.format(len(lvl2)))
        lvl3 = args[2]
        logger.info('Received lvl3 list: {}'.format(len(lvl3)))

        # calculate the limit in case of a lvl1 removed
        self.max_limit = Limit(self.loop_interval).current_list(lvl1)
        # add the loop value to the dict to parse it during the preparing
        # return dictionary
        lvl1 = self.preparing_lvl1(lvl1)
        lvl2, self.lvl2_loop = self.get_loop(lvl2, self.lvl2_loop)
        lvl2 = self.preparing_lvl2(lvl1, lvl2, self.time_lvl2, self.max_limit)
        lvl3 = self.preparing_lvl3(lvl1, lvl2, lvl3, self.max_limit)
        self.lvl2_loop = self.remove_loop(lvl2, self.lvl2_loop)
        return set_list(lvl1), set_list(lvl2), set_list(lvl3)

    def cleaning_user(self, input_user):
        """ """
        return_dict = {'process': [], 'protected': [], 'Non-existing': [], 'suspended': []}
        for dict_user in previous_list:
            try:
                if dict_user['response']['protected'] is True:
                    return_dict['protected'].append(dict_user['user'])
                else:
                    return_dict['process'].append(dict_user['user'])
            except TypeError:  # Error when the response is non-existing
                if dict_user['response'] == 'Non-existing':
                    return_dict['Non-existing'].append(dict_user['user'])
                elif dict_user['response'] == 'Suspended':
                    return_dict['suspended'].append(dict_user['user'])
        return return_dict

    def get_loop(self, lvl2, lvl2_loop):
        """ Add the loop value to the dict for the preparing lvl2"""

        for dict_user in lvl2:
            try:
                if dict_user['user'] in lvl2_loop:
                    value = lvl2_loop[dict_user['user']] + 1
                else:
                    value = 1
                dict_user['loop'] = value
                lvl2_loop.update({dict_user['user']: value})

            except KeyError as e:
                logger.error('Error in adding the loop number in lvl2 - {}'.format(e))
                pass
            except TypeError as e:
                logger.error('TypeError in adding loop_number in lvl2 - {}'.format(e))
                pass
        return lvl2, lvl2_loop

    def remove_loop(self, lvl2, lvl2_loop):
        """ """
        return_dict = {}
        for user in lvl2_loop:
            if user in lvl2['process']:
                return_dict[user] = lvl2_loop[user]
        return return_dict

    def preparing_lvl2(self, lvl1, lvl2, time_lvl2, max_limit):
        """ """
        return_dict = {'process': [], 'protected': [], 'Non-existing': [],
                       'loop': [], 'lvl_up': [], 'non-sampled': [],
                       'suspended': []}
        lvl1_list = [elt for elt in lvl1['process']]
        list_new = list()
        list_previous = list()

        # Removing the unusable users
        for dict_user in lvl2:
            try:
                if dict_user['loop'] >= time_lvl2:
                    return_dict['loop'].append(dict_user['user'])
                elif dict_user['response'] == 'Non-existing':
                    return_dict['Non-existing'].append(dict_user['user'])
                elif dict_user['response'] == 'Suspended':
                    return_dict['suspended'].append(dict_user['user'])
                elif dict_user['user'] in lvl1_list:
                    return_dict['lvl_up'].append(dict_user['user'])
                elif dict_user['response']['protected'] is True:
                    return_dict['protected'].append(dict_user['user'])
                lvl2.remove(dict_user)
            except TypeError:
                # In case of from interactive, response doesn't have protected
                # it is a string
                pass
            except KeyError as e:
                logger.warning('Error in preparing lvl2 - {} - {}'.format(e, dict_user))
                pass

        # Create the previous list to know the size and a new list to random
        # TODO as the previous is only use to calc the len, should do it without
        # the need to create a list for it
        for dict_user in lvl2:
            try:
                if dict_user['loop'] == 1:
                    list_new.append(dict_user['user'])
                else:
                    list_previous.append(dict_user['user'])
            except KeyError:  # Happens if the lvl2 and lv2_loop are empty
                logger.warning('Error in creating lvl2 list - {}'.format(dict_user))
                pass
            except TypeError:
                logger.warning('TypeError in adding loop on {}'.format(dict_user))
                pass
        try:
            sample_size = max_limit['lvl2'] - len(list_previous)
        except TypeError:  # If previous list is empty
            sample_size = max_limit['lvl2']

        # Generate a list with new item to add to the current list
        sample_list, non_sample_list = random_lvl2(list_new, sample_size)

        # Creating the list to use for this loop
        process = sample_list + list_previous

        return_dict['process'] = process
        return_dict['non-sampled'] = non_sample_list
        return return_dict

    def preparing_lvl3(self, lvl1, lvl2, lvl3, limit):
        """ """
        return_dict = {'process': [], 'protected': [], 'Non-existing': [],
                       'loop': [], 'lvl_up': [], 'non-sampled': []}
        limit = limit['lvl3']
        lvl_up_set = {elt for elt in lvl1['process']}.union({elt for elt in lvl2['process']})
        for user in lvl3:
            try:
                # if dict_user['response']['protected'] is True:
                    # return_dict['protected'].append(dict_user['user'])
                # elif dict_user['response'] == 'Non-existing':
                    # return_dict['Non-existing'].append(dict_user['user'])
                if user in lvl_up_set:
                    return_dict['lvl_up'].append(user)
                    lvl3.remove(user)
            except (KeyError, TypeError):
                pass

        list_lvl = [user for user in lvl3]
        sample, non_sample = random_lvl3(list_lvl, limit)
        return_dict['process'] = sample
        return_dict['non-sampled'] = non_sample
        return return_dict
