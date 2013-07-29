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
import pandasmongo
from datetime import datetime
from itertools import groupby


class KnowledgeBase(object):
    """ KnowledgeBase stores all check-in information to support expert
        querying.

        Mapping from json to table columns:
            id.....................................id
            user.screen_name.......................user
            place.id...............................pid
            place.bounding_box.coordinates.0.0.1...lat
            place.bounding_box.coordinates.0.0.0...lng
            place.name.............................place
            place.category.name....................category
            place.category.id......................cid
            place.category.zero_category_name......z_category
            place.category.zero_category...........zcid
            created_at.............................created_at
    """
    def __init__(self, checkins):
        """ Loading the dataframe of check-ins and construct the KnowledgeBase
        """
        super(KnowledgeBase, self).__init__()
        self.checkins = checkins

    @classmethod
    def fromTSV(cls, filename):
        """ docstring for fromFile
        """
        checkins = pd.read_csv(
            filename,
            sep='\t',
            header=None,
            names=['user', 'category', 'created_at', 'pid'],
            parse_dates=[2])
        return cls(checkins)

    DEFAULT_PROJECTION = {'id': 'id',
                          'user.screen_name': 'user',
                          'place.id': 'pid',
                          'place.bounding_box.coordinates.0.0.1': 'lat',
                          'place.bounding_box.coordinates.0.0.0': 'lng',
                          'place.name': 'place',
                          'place.category.name': 'category',
                          'place.category.id': 'cid',
                          'place.category.zero_category_name': 'z_category',
                          'place.category.zero_category': 'zcid',
                          'created_at': 'created_at'}

    @classmethod
    def fromMongo(cls,
                  collection,
                  query=None,
                  projection=None):
        """ Constructing the knowledgebase from a set of check-ins
            queryed against the given collection in a MongoDB instance
            :param collection: the collection instance where the check-ins
                stored
            :param query: a query to fetch a set of check-in that meets
                certain criteria
            :param projection: the final fields that should include in the
                queried
            :return: a KnowledgeBase instance containing the check-ins
        """
        projection = projection or KnowledgeBase.DEFAULT_PROJECTION
        query = query or dict()
        checkins = pandasmongo.getDataFrame(collection, query, projection)
        checkins['created_date'] = checkins['created_at'].map(
            lambda x: x.replace(hour=0, minute=0, second=0, microsecond=0))
        return cls(checkins)

    def rank(self, profile_type, metrics, cutoff=5):
        """Rank the userbase based on the given profile_type and metrics

        :profile_type: @todo
        :metrics: @todo
        :cutoff: @todo
        :returns: @todo

        """
        return profile_type(self.checkins, metrics, cutoff=cutoff)


# TODO make use of multi-level index, which may ease grouping
def rankCheckinProfile(checkins, metrics, **kargs):
    """ Rank the profile based on checkins
    """
    profiles = checkins.groupby('user')
    rank, scores = metrics(profiles, **kargs)
    return rank, scores


def rankActiveDayProfile(checkins, metrics, **kargs):
    """ Rank the profiles based on active days
    """
    day_profiles = checkins.drop_duplicates(
        cols=['user',
              'created_date',
              'pid'])
    day_profiles['created_at'] = day_profiles['created_date']
    profiles = day_profiles.groupby('user')
    rank, scores = metrics(profiles, **kargs)
    return rank, scores


def naive_metrics(profiles, cutoff=-1, **kargs):
    """ using number of visitings / active days themselves for ranking
    """
    mrank = profiles['id'].count().order(ascending=False)
    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


@np.vectorize
def _time_diff(t1, t2):
    """ Return the difference between two time points
    """
    return (t1 - t2).days


def recency_metrics(profiles, cutoff=-1, **kargs):
    """ A metrics boosting recent visits
    """
    refdate = kargs.get('refdate', datetime.strptime('2013-08-01', '%Y-%m-%d'))
    decay_rate = kargs.get('decay_rate', 1. / 60)
    mrank = profiles['created_at'].agg(
        lambda x: np.sum(np.exp(_time_diff(x, refdate) * decay_rate))) \
        .order(ascending=False)
    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


def _count_iter(it):
    """ Count the number of element that would return by an iterator
    """
    cnt = 0
    for _ in it:
        cnt += 1
    return cnt


def diversity_metrics(profiles, cutoff=-1, **kargs):
    """ A metrics boosting diverse visits
    """
    mrank = profiles['pid'].agg(
        lambda x: np.sum([np.log2(_count_iter(y) + 1)
                          for _, y in groupby(np.sort(x))]))\
        .order(ascending=False)

    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


#def RD_metrics(profiles, cutoff=-1, **kargs):
    #""" A metrics boosting diverse visits
    #"""
    ## TODO not finished, planned to use row wise aggregation function
    ## for the job
    ## NOTICE the log function for diversity and exp for recency looks
    ## funny when putting them together
    #refdate = kargs.get('refdate',
                         #datetime.strptime('2013-08-01', '%Y-%m-%d'))
    #mrank = profiles.agg(
        #lambda row: np.sum([np.log2(_count_iter(y) + 1)
                            #for _, y in groupby(np.sort(row))]))
    #if cutoff > 0:
        #return mrank.index.values[:cutoff], mrank.values[:cutoff]
    #else:
        #return mrank.index.values, mrank.values
