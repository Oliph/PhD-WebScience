#!/usr/bin/env python
# -*- coding:UTf8 -*-
import math


class Limit(object):
    """ """
    def __init__(self, loop_interval, max_user=None, max_link=None):
        """ loop_interval: in number of 15 minutes, the limit reset in
            Twitter
            proportion_second is option to know if the second list need to be at
            a certain level of user.
            max_user= None/True/False is to maximize or not the second
                list
            max_link = N/T/F is to maximise the number of link
            The default behaviour is to maximise the number of user of the
            second list
        """
        self.loop_interval = loop_interval
        self.max_user = max_user
        self.max_link = max_link
        self.verify_option()

    def verify_option(self):
        """ Checking if two true to set up default behaviour """
        if self.max_user is True and self.max_link is True:
            self.max_user = True
            self.max_link = False

    def calculing_limit(self, nbr_client=1, main_user=None):
        """ Calculing the three limits according to the number of call provided
            - main_user = The maximum number of user in the main list
                          (2/3 of total)
            - second_user = The max number of user in the second list
                          (1/3 of total)
            - third_user = The max of total third list users for the look_up
        """
        def modulo(variable, divider):

            if variable % divider != 0:
                return 1
            else:
                return 0

        # Normally it is 30 per app but keep it
        # at 15 as I can need two call for the user
        # who have more than 5000 F/f
        nbr_client = 2
        total_user = (self.loop_interval* nbr_client)* 20
        if main_user is None:
            main_user = ((total_user/ 3))
            main_user += modulo(total_user, 3)
        second_user = total_user - main_user
        if second_user < 0:
            second_user = 0
        # To calculate the number of call needed to parse the profile of
        # the lvl1 and lvl2 maximum limits knowing that they also need to be
        # parse with profile
        look_up_user = total_user/ 100
        look_up_user += modulo(total_user, 100)
        # Calculate the maximum of users that it is possible to get
        third_user = (100* self.loop_interval* nbr_client* (180)) - (math.ceil(look_up_user)* 100)

        return {'total_user': math.floor(total_user), 'lvl1': math.floor(main_user),
                'lvl2': math.floor(second_user), 'lvl3': math.floor(third_user)}

    def current_list(self, main_user):
        """ Calculing the actual level """
        if isinstance(main_user, list) or isinstance(main_user, dict):
            main_user = len(main_user)

        elif isinstance(main_user, int):
            main_user = main_user

        elif isinstance(main_user, str):
            try:
                main_user = int(main_user)
            except:
                raise "Problem of value, parse a list or an int"

        return self.calculing_limit(main_user)


def main():
    test_limit = Limit(96, 2, max_user=True)
    value = test_limit.calculing_limit(750)
    print(value)

if __name__ == '__main__':
    main()
