
from multiprocessing import Process

import time
from logger import logger
from params import dataScheme
from utils import find_update
from server_activity import activityTransforming
logger = logger('server_profile', file_level=None, stream_level='DEBUG')


class processProfile(Process, dataScheme):
    """ """
    def __init__(self, *args, **kwargs):
        """ """
        dataScheme.__init__(self)
        Process.__init__(self)
        self.db_activity = kwargs['activity']
        self.db_process_profile = kwargs['process_profile']
        self.db_current_profile = kwargs['current_profile']
        self.db_stored_profile = kwargs['stored_profile']
        self.do_activity = activityTransforming(db_activity=self.db_activity)

    def process_profile(self, profile):
        """ """
        try:
            del profile['_id']
        except KeyError:
            pass
        id_search = {self.id_str: profile[self.id_str]}
        previous_user = self.retrieve_stored_profile(id_search)
        # Compare activity
        if previous_user:
            self.do_activity.run(profile, previous_user)
        # Update the stored profile for comparison with activity
        self.update_stored_profile(id_search, profile)

    def retrieve_stored_profile(self, id_search):
        """ """
        return self.db_stored_profile.find_one(id_search)

    def update_stored_profile(self, id_search, current_user):
        """ """
        find_update(method='update', db=self.db_stored_profile,
                    search=id_search, update=current_user)

    def run(self):
        while True:
            current_profile = self.db_current_profile.find_one_and_delete({})
            if current_profile:
                self.process_profile(current_profile)
            else:
                time.sleep(60)

    # def check_lvl2(self, info_user):
    #     """
    #     Case of self.time_lvl2 > 1 need to check
    #     to see if can be push in the next loop
    #     """
    #     try:
    #         # If self.time_lvl2 > than the limit, not updating for the next
    #         # loop because already did all the screening set up
    #         if info_user[self.nbr_check] > self.time_lvl2:
    #             return
    #         else:
    #             pass
    #     # If a KeyError is raised is because the lvl2 doesn't come from
    #     # a previous screen but from a new change (doesn't have the key
    #     # set up so it means it is a new record
    #     except KeyError:
    #         # Set up the key for the record
    #         # Set to 0 because in the generateRandom() class the +1 is done
    #         # there
    #         info_user[self.nbr_check] = 0
    #     # Need to check if the lvl2 has been already recorded for next
    #     # loop from a previous loop or from another new change
    #     previous_lvl2 = find_update('find', self.db_process_profile,
    #                                 search={self.id_str: info_user[self.id_str],
    #                                         self.lvl: 2,
    #                                         self.loop: self.loop_number +1})
    #     try:
    #         if info_user[self.nbr_check] == 0 and previous_lvl2[self.nbr_check] != 0:
    #             # If info user == 0 it means it is a new record and have
    #             # the risk of not being picked up during the sampling
    #             # process
    #             # The solution is to force the nbr_check to be one
    #             # That reset the next check -- be sure it is pickup
    #             # while it is still be check for a little bit more
    #             info_user[self.nbr_check] = 1
    #             logger.info('{} found in previous lvl2 - reset check to 1'.format(info_user[self.id_str]))
    #         else:
    #             pass
    #     #  If error raised
    #     # meaning no other same id_str has been recorded as lvl2
    #     # and can process it normally
    #     except (KeyError, TypeError):
    #         pass
    #     return info_user

    # def lvl2_check(self):
    #     """
    #     Copy all the lvl2 that have more than one check, to ensure
    #     they are still screened and not going through the sampling process
    #     """
    #     def process_field(result, db, type_info):
    #         """ """
    #         result[self.profile] = type_info
    #         result[self.friends] = type_info
    #         result[self.followers] = type_info
    #         find_update(method='update', db=db,
    #                          search={self.id_str: result[self.id_str],
    #                                  self.loop: result[self.loop]},
    #                          update=result)
    #     n_size = 0
    #     while True:
    #         record = self.db_rand_2.find_one_and_delete({'nbr_check': {'$gte': 2}})
    #         if record is None:
    #             break
    #         else:
    #             process_field(record, self.db_process_profile, self.todo)
    #             n_size += 1
    #     return n_size
