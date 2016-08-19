#!/usr/bin/env python
# encoding: utf-8
import random

# # Found in
#http://stackoverflow.com/questions/352670/weighted-random-selection-with-and-without-replacement
# # Mention this article as the source of this algorythm ;
#http://www.sciencedirect.com/science/article/pii/S002001900500298X
# import heapq
# import math
# import random
#
# def WeightedSelectionWithoutReplacement(weights, m):
#
#     elt = [(math.log(random.random()) / weights[i], i) for i in range(len(weights))]
#     return [x[1] for x in heapq.nlargest(m, elt)]


def random_lvl2(list_in, limit):
    if len(set(list_in)) <= limit:
        return list(set(list_in)), list()
    else:
        n = 0
        sample = list()
        while n < limit:
            elt = random.choice(list_in)
            if elt not in sample:
                sample.append(elt)
                n += 1
        non_sample = set(list_in) - set(sample)
        return list(sample), list(non_sample)


def random_lvl3(list_in, limit):

    if len(set(list_in)) <= limit:
        return list(set(list_in)), list()
    else:
        n = 0
        sample = list()
        while n < limit:
            elt = random.choice(list_in)
            if elt not in sample:
                sample.append(elt)
                n += 1
        non_sample = set(list_in) - set(sample)
        return list(sample), list(non_sample)
