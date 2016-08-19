#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import pymongo
import collections
from datetime import datetime  # datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def cut_list(lst, n):
    """ cut a list into a number of chunk with approx same size """
    for i in range(n):
        yield lst[i* len(lst):(i+ 1)* len(lst)]


def slice_list(input_list, size):
    "Slice an input list into a specific size and yield shorten lists"
    for i in range(0, len(input_list), size):
        output_list = input_list[i:i + size]
        yield output_list
    return


def logging(script, function, other_info=None):
    "write db input_list"
    with open('log_{}.log'.format(script), 'a') as file:
        if other_info:
            file.write("{} - {} - {} - {}\n".format(script, function, other_info,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        else:
            file.write("{} - {} - {}\n".format(script, function,
                                               datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


def nested_dict_iter(nested):
    """
        Iterating keys in nested dictionary
        http://stackoverflow.com/questions/10756427/loop-through-all-nested-dictionary-values
    """
    for key, value in nested.iteritems():
        if isinstance(value, collections.Mapping):
            for inner_key, inner_value in nested_dict_iter(value):
                yield inner_key, inner_value
        else:
            yield key, value


class DictDiffer(object):
    """
        source: http://stackoverflow.com/questions/1165352/fast-comparison-between-two-python-dictionary/1165552#1165552
        Calculate the difference between two dictionaries as:
            (1) items added
            (2) items removed
            (3) keys same in both but changed values
            (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.current_keys, self.past_keys = [set(d.keys()) for d in (current_dict, past_dict)]
        self.intersect = self.current_keys.intersection(self.past_keys)

    def added(self):
        return self.current_keys - self.intersect

    def removed(self):
        return self.past_keys - self.intersect

    def changed(self):
        return set(o for o in self.intersect
                   if self.past_dict[o] != self.current_dict[o])

    def unchanged(self):
        return set(o for o in self.intersect
                   if self.past_dict[o] == self.current_dict[o])


def remove_duplicate(list):
    return [element for element in set(list)]


def datetime_to_epoche(input_time):
    return int(time.mktime(input_time.timetuple()))
    # return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time))


def epoche_to_datetime(input_time):
    """ Need a float and convert into datetime object """
    return datetime.fromtimestamp(input_time)
    # return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(float(input_time)))


def find_update(method, db, search=None, update=None, rand_num=None,
                upsert=True, set_option='$set'):
    """
    Wrapper for the pymongo method of collection operations. Remove
    the potential _id key from update dict() to avoid overriding that field
    Parameters:
    * method: str() [update, replace, remove, insert, random] - Type of
                method to use
    * db: pymongo collection object
    * search: dict() to search
    * update: dict() of field:value to update
    * rand_num: int() random number to simulate a random find of a record
    Returns:
    * dict() of the found record if one. Otherwise return None
    """
    def remove_key(dict_, key_to_remove):
        """ """
        try:
            del dict_[str(key_to_remove)]
        except (KeyError, TypeError):
            pass
        return dict_

    if search is None:
        search = {}
    if method == 'find':
        return db.find_one(search)
    elif method == 'update':
        update = remove_key(update, '_id')
        return db.find_one_and_update(search, {set_option: update}, upsert=upsert)
    elif method == 'replace':
        update = remove_key(update, '_id')
        return db.find_one_and_replace(search, update, upsert=upsert)
    elif method == 'remove':
        return db.find_one_and_delete(search)

    elif method == 'insert':
        try:
            return db.insert_one(update)
        except pymongo.errors.DuplicateKeyError:
            return None
    elif method == 'random':
        if search is None:
            search = {}
        return db.find(search, skip=int(rand_num), limit=-1)


if __name__ == '__main__':
    pass
