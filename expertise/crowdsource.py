#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: crowdsource.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    This module contains helper functions to make crowdsource data feeds
    for geoexpert evaluation.
"""

import sys
import csv
import json
import pymongo
from datetime import datetime, timedelta
import numpy as np


db = pymongo.MongoClient().geoexpert.checkin


class CheckinMap(object):

    """A set of places that a user have been to"""

    def __init__(self, n=60):
        """Construct an empty CheckinMap

        :n: @todo

        """
        self._n = n
        self._pids = set()
        self._pool = list()

    def append_ck(self, ck):
        """Appnd a checkin to the map

        :ck: @todo
        :returns: @todo

        """
        if len(self._pids) >= self._n:
            return False
        if ck['place']['id'] not in self._pids:
            self._pids.add(ck['place']['id'])
            self._pool.append({
                'id': ck['place']['id'],
                'name': ck['place']['name'],
                'cate': ck['place']['category']['zero_category_name'],
                'cate_id': ck['place']['category']['id'],
                'lat': ck['place']['bounding_box']['coordinates'][0][0][1],
                'lng': ck['place']['bounding_box']['coordinates'][0][0][0]})
        return True

    def get_profile(self):
        """ Return a list of checkins that could placed on a map
        :returns: @todo

        """
        return self._pool


class CategoryTimeline(object):

    """A checkin distribution over timeline for each category"""

    CATE = {k: idx for idx, k in enumerate([
        "Arts & Entertainment",
        "College & University",
        "Food",
        "Nightlife Spot",
        "Outdoors & Recreation",
        "Professional & Other Places",
        "Residence",
        "Shop & Service",
        "Travel & Transport"
    ])}

    def __init__(self, bin_size, refdate, bin_num=36):
        """ Constructing an object of to representing

        :bin_size: python's timedelta object
        :refdate: the reference of timeline (datetime)
        :bin_num: @todo

        """
        self._bin_size = bin_size
        self._bin_num = bin_num
        self._refdate = refdate
        self._ref_o = refdate - bin_num * bin_size
        self._timelines = np.zeros((len(CategoryTimeline.CATE), bin_num),
                                   dtype=np.int)

    def append_ck(self, ck):
        """Append a checkin to the timelines

        :ck: @todo
        :returns: @todo

        """
        cate_idx = CategoryTimeline.CATE[
            ck['place']['category']['zero_category_name']]
        tick = int((self._ref_o - ck['created_at']).total_seconds()
                   / self._bin_size.total_seconds())
        if 0 <= tick < self._bin_num:
            self._timelines[cate_idx][tick] += 1

    def get_profile(self):
        """Return the profile of the user
        :returns: @todo

        """
        return {y: [int(x) for x in self._timelines[CategoryTimeline.CATE[y]]]
                for y in CategoryTimeline.CATE}


class POITimeline(object):

    """A checkin distribution over timeline for each category"""

    def __init__(self, bin_size, refdate, topn=10, bin_num=60):
        """ Constructing an object of to representing

        :bin_size: python's timedelta object
        :refdate: the reference of timeline (datetime)
        :bin_num: @todo

        """
        self._bin_size = bin_size
        self._bin_num = bin_num
        self._refdate = refdate
        self._topn = topn
        self._ref_o = refdate - bin_num * bin_size
        self._timelines = np.zeros((topn, bin_num), dtype=np.int)
        self.poi_pool = dict()

    def append_ck(self, ck):
        """Append a checkin to the timelines

        :ck: @todo
        :returns: @todo

        """
        key = (ck['place']['id'],
               ck['place']['name'],
               ck['place']['category']['name'],
               ck['place']['category']['zero_category_name'],
               )
        if key not in self.poi_pool:
            self.poi_pool[key] = list()
        self.poi_pool[key].append(ck['created_at'])

    def get_profile(self):
        """Return the profile of the user

        :returns: @todo

        """
        keys = sorted(self.poi_pool.iterkeys(),
                      key=lambda x: len(self.poi_pool[x]),
                      reverse=True)[:self._topn]
        for idx, k in enumerate(keys):
            for t in self.poi_pool[k]:
                tick = int((self._ref_o - t).total_seconds()
                           / self._bin_size.total_seconds())
                if 0 <= tick < self._bin_num:
                    self._timelines[idx][tick] += 1
        return {k[0]: {'name': k[1],
                       'category': k[2],
                       'zcate': k[3],
                       'timeline': [int(x) for x in self._timelines[idx]]}
                for idx, k in enumerate(keys)}


def user_profile(screen_name):
    """ Generating a data entry for a user

    :screen_name: @todo
    :returns: @todo

    """
    ckmap = CheckinMap()
    catetl = CategoryTimeline(timedelta(days=30), datetime(2013, 8, 1))
    poitl = POITimeline(timedelta(days=30), datetime(2013, 8, 1))
    for ck in db.find({'user.screen_name': screen_name}):
        ckmap.append_ck(ck)
        catetl.append_ck(ck)
        poitl.append_ck(ck)
    return ckmap.get_profile(), catetl.get_profile(), poitl.get_profile()


def mkcsv(csv_input):
    """@todo: Docstring for test.
    :returns: @todo

    """
    with open(csv_input, 'rb') as fin:
        wr = csv.writer(sys.stdout)
        for row in csv.reader(fin):
            uname = row[0]
            expertise = row[1]
            ckmap, catetl, poitl = user_profile(uname)
            wr.writerow([uname,
                         expertise,
                         json.dumps(ckmap),
                         json.dumps(catetl),
                         json.dumps(poitl)])


def test():
    """@todo: Docstring for test.
    :returns: @todo

    """
    ckmap, catetl, poitl = user_profile('keniehuber')
    print ckmap
    print catetl
    print poitl


if __name__ == '__main__':
    mkcsv(sys.argv[1])
    #test()
