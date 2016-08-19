#!/usr/bin/env python
# encoding: utf-8

from pymongo import errors as PyError
# Logging

from params import dataScheme
from logger import logger
logger = logger('server_activity', stream_level='INFO')


class activityTransforming(dataScheme):

    def __init__(self, db_activity):

        dataScheme.__init__(self)
        self.db_activity = db_activity
        self.type_activity = ['statuses_count', 'friends_count',
                              'followers_count']

    def run(self, current_user, previous_user):
        """ """
        if previous_user is not None:
            final_activity = dict()
            final_activity[self.id_str] = current_user[self.id_str]
            final_activity[self.loop] = current_user[self.loop]
            for type_ in self.type_activity:
                final_activity[type_] = self.calculate_activity(current_user,
                                                                previous_user,
                                                                type_)
                final_activity['abs_{}'.format(type_)] = self.calculate_activity(current_user,
                                                                                 previous_user,
                                                                                 type_,
                                                                                 absV=True)
            if final_activity[type_] is not None:
                self.insert_activity(final_activity)

    def calculate_activity(self, current, previous, type_activity, absV=False):
        """ """
        current_activity = current[type_activity]
        previous_activity = previous[type_activity]
        try:
            if absV is False:
                return (int(current_activity) - int(previous_activity)) / (int(current[self.loop]) - int(previous[self.loop]))
            elif absV is True:
                return (int(current_activity) - int(previous_activity))
        except ZeroDivisionError:
            return 0

    def insert_activity(self, document):
        """ """
        try:
            self.db_activity.insert_one(document)
        except PyError.DuplicateKeyError:
            logger.error(document)


def main():
    """ """
    pass

if __name__ == '__main__':
    main()
