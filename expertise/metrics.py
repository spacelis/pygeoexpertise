#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: metrics.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
    Metrics for evaluating users geo expertise
"""

import numpy as np
import pandas as pd
from datetime import datetime
from itertools import groupby


class KnowledgeBase(object):
    """ KnowledgeBase stores all check-in information to support expert
        querying.
    """
    def __init__(self, ck_profiles):
        """ Loading the dataframe of check-ins and construct the KnowledgeBase
        """
        super(KnowledgeBase, self).__init__()
        self.ck_profiles = ck_profiles

    @classmethod
    def fromTSV(cls, filename):
        """ docstring for fromFile
        """
        ck_profiles = pd.read_csv(
            filename,
            sep='\t',
            header=None,
            names=['uid', 'category', 'created_at', 'poi'],
            parse_dates=[2])
        return cls(ck_profiles)

    @staticmethod
    def dotpath(obj, dpath):
        """ get value through dotpath
        """
        for n in dpath.split('.'):
            obj = obj.get(n)
        return obj

    DEFAULT_PROJECTION = {'id': 1,
                          'user.screen_name': 1,
                          'place.id': 1,
                          'place.name': 1,
                          'place.category.name': 1,
                          'created_at': 1}

    @staticmethod
    def toDataFrame(obj_iter, keylist):
        """ iterating through the obj_iter and extract values of the keys
        """
        obj_list = list()
        for obj in obj_iter:
            obj_list.append([KnowledgeBase.dotpath(obj, k) for k in keylist])
        return pd.DataFrame(obj_list, columns=keylist)

    @classmethod
    def fromMongo(cls,
                  collection,
                  query=None,
                  projection=None):
        """ docstring for fromMongo
        """
        projection = projection or KnowledgeBase.DEFAULT_PROJECTION
        cur = collection.find(query, projection)
        ck_profiles = KnowledgeBase.toDataFrame(
            cur,
            list(projection.iterkeys()))
        ck_profiles['created_date'] = ck_profiles['created_at'].map(
            lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
        return cls(ck_profiles)

    # TODO make use of multi-level index, which may ease grouping
    def rankCheckinProfile(self, metrics):
        """ Rank the profile based on checkins
        """
        ck_profiles = self.ck_profiles
        profiles = ck_profiles.groupby('user.screen_name')
        rank, scores = metrics(profiles)
        return rank, scores

    def rankActiveDayProfile(self, metrics):
        """ Rank the profiles based on active days
        """
        day_profiles = self.ck_profiles.drop_duplicates(
            cols=['user.screen_name',
                  'created_date',
                  'place.id'])
        profiles = day_profiles.groupby('user.screen_name')
        rank, scores = metrics(profiles)
        return rank, scores


def naive_metrics(profiles, topk=-1, **kargs):
    """ using number of visitings / active days themselves for ranking
    """
    mrank = profiles['id'].count().order(ascending=False)
    if topk > 0:
        return mrank.index.values[:topk], mrank.values[:topk]
    else:
        return mrank.index.values, mrank.values


@np.vectorize
def _time_diff(t1, t2):
    """ Return the difference between two time points
    """
    return (t1 - t2).days


def recency_metrics(profiles, topk=-1, **kargs):
    """ A metrics boosting recent visits
    """
    refdate = kargs.get('refdate', datetime.strptime('2013-08-01', '%Y-%m-%d'))
    decay_rate = kargs.get('decay_rate', 1. / 60)
    mrank = profiles['created_at'].agg(
        lambda x: np.sum(np.exp(_time_diff(x, refdate) * decay_rate)))
    if topk > 0:
        return mrank.index.values[:topk], mrank.values[:topk]
    else:
        return mrank.index.values, mrank.values


def _count_iter(it):
    """ Count the number of element that would return by an iterator
    """
    cnt = 0
    for _ in it:
        cnt += 1
    return cnt


def diversity_metrics(profiles, topk=-1, **kargs):
    """ A metrics boosting diverse visits
    """
    mrank = profiles['place.id'].agg(
        lambda x: np.sum([np.log2(_count_iter(y) + 1)
                          for _, y in groupby(np.sort(x))]))
    if topk > 0:
        return mrank.index.values[:topk], mrank.values[:topk]
    else:
        return mrank.index.values, mrank.values


def RD_metrics(profiles, topk=-1, **kargs):
    """ A metrics boosting diverse visits
    """
    # TODO not finished, planned to use row wise aggregation function
    # for the job
    # NOTICE the log function for diversity and exp for recency looks
    # funny when putting them together
    refdate = kargs.get('refdate', datetime.strptime('2013-08-01', '%Y-%m-%d'))
    mrank = profiles.agg(
        lambda row: np.sum([np.log2(_count_iter(y) + 1)
                          for _, y in groupby(np.sort(x))]))
    if topk > 0:
        return mrank.index.values[:topk], mrank.values[:topk]
    else:
        return mrank.index.values, mrank.values
