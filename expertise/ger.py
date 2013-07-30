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

import sys
import logging
import numpy as np
import pandas as pd
import pymongo
import expertise.pandasmongo as pandasmongo
from itertools import groupby
import uuid


REGIONS = {
    'CHICAGO': {'name': 'CHICAGO',
                'value': {"place.bounding_box.coordinates.0.0.1":
                          {'$gt': 41.4986, '$lt': 42.0232},
                          "place.bounding_box.coordinates.0.0.0":
                          {'$gt': -88.1586, '$lt': -87.3573}}},
    'New York': {'name': 'New York',
                 'value': {"place.bounding_box.coordinates.0.0.1":
                           {'$gt': 40.4110, '$lt': 40.9429},
                           "place.bounding_box.coordinates.0.0.0":
                           {'$gt': -74.2918, '$lt': -73.7097}}},
    'Los Angeles': {'name': 'Los Angeles',
                    'value': {"place.bounding_box.coordinates.0.0.1":
                              {'$gt': 33.7463, '$lt': 34.2302},
                              "place.bounding_box.coordinates.0.0.0":
                              {'$gt': -118.6368, '$lt': -117.9053}}},
    'San Francisco': {'name': 'San Francisco',
                      'value': {"place.bounding_box.coordinates.0.0.1":
                                {'$gt': 37.7025, '$lt': 37.8045},
                                "place.bounding_box.coordinates.0.0.0":
                                {'$gt': -122.5349, '$lt': -122.3546}}}}


class KnowledgeBase(object):
    """ KnowledgeBase stores all check-in information to support expert
        querying.

        Mapping from json to table columns:
            id.....................................id
            user.screen_name.......................user
            user.id................................uid
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
                          'user.id': 'id',
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


ONEDAY = np.timedelta64(1, 'D')


def _time_diff(t_array, t):
    """ Return the difference between two time points
    """
    return (t_array - t) / ONEDAY


def recency_metrics(profiles, cutoff=-1, **kargs):
    """ A metrics boosting recent visits
    """
    refdate = kargs.get('refdate', np.datetime64('2013-08-01T00:00:00+02'))
    decay_rate = kargs.get('decay_rate', 1. / 60)
    mrank = profiles['created_at'].agg(
        lambda x: np.sum(np.exp(_time_diff(x.values, refdate) * decay_rate))) \
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


class GeoExpertRetrieval(object):
    """ A class represent a survey consisting of a bunch of questionnaires
    """
    def __init__(self, name, collection):
        super(GeoExpertRetrieval, self).__init__()
        self.name = name
        self.collection = collection
        self._logger = logging.getLogger(
            '%s.%s' % (__name__, type(self).__name__))

    @staticmethod
    def _getid():
        """ Return an unique identifier
        """
        return uuid.uuid4()

    def rankExperts(self, query, rank_method, profile_type, cutoff=10):
        """ Return a set of parameters for setting up questionnaires
            :param query: a dict() object holding topic and region for query
                {topic:{name:, value:}, region:{name:, value:} }
            :param rank_method: the ranking method name
            :param profile_type: the ranking profile type
            :param cutoff: the length of the returned list
            :return: a set of rows containing information for setting up
                     a set of questions
        """
        q = dict()
        q.update(query['region']['value'])
        q.update(query['topic']['value'])
        kbase = KnowledgeBase.fromMongo(self.collection, q)
        rank, scores = kbase.rank(profile_type, rank_method, cutoff=cutoff)
        ranking = pd.DataFrame([{
            'rank_id': '%s-%s' % (self.name, GeoExpertRetrieval._getid()),
            'topic_id': query['topic_id'],
            'region': query['region']['name'],
            'topic': query['topic']['name'],
            'user_screen_name': r,
            'rank_method': rank_method.__name__,
            'rank': i + 1,
            'score': s,
        } for i, (r, s) in enumerate(zip(rank, scores))])
        return ranking

    def formatQuery(self, topic_id, name, ident, region_name, region_value,
                    is_cate=False):
        """ Format query

        :topic_id: @todo
        :name: @todo
        :ident: @todo
        :region_name: @todo
        :is_cate: @todo
        :returns: @todo

        """
        if is_cate:
            return {'topic_id': topic_id,
                    'topic': {'name': name,
                              'value': {'place.category.id': ident}},
                    'region': {'name': region_name,
                               'value': region_value}}
        else:
            return {'topic_id': topic_id,
                    'topic': {'name': name,
                              'value': {'place.id': ident}},
                    'region': {'name': region_name,
                               'value': region_value}}

    RANK_SCHEMA = ['topic_id', 'rank', 'user_screen_name', 'score',
                   'rank_method', 'region', 'topic', 'rank_id']

    def batchQuery(self, topics, metrics, profile_type):
        """ batchquery
        """
        rankings = pd.DataFrame(columns=GeoExpertRetrieval.RANK_SCHEMA)
        for t in topics.values:
            t = dict(zip(topics.columns, t))
            q = self.formatQuery(t['topic_id'], t['topic'],
                                 t['associate_id'], t['region'],
                                 REGIONS[t['region']]['value'],
                                 'cate' in t['topic_id'])
            self._logger.info('Processing %(topic_id)s...', q)
            try:
                for mtc in metrics:
                    for pf_type in profile_type:
                        rank = self.rankExperts(q, mtc, pf_type, 5)
                        rankings = rankings.append(rank)
            except:
                self._logger.error('Failed at %(topic_id)s', q)
        return rankings


METRICS = [naive_metrics, recency_metrics, diversity_metrics]
PROFILE_TYPES = [rankCheckinProfile, rankActiveDayProfile]


def rankExperts(outfile, topicfile):
    """ Running a set of queries to generate the self-evaluation survey
        for geo-experts.
    """
    topics = pd.read_csv(topicfile)
    checkin_collection = pymongo.MongoClient().geoexpert.checkin
    survey = GeoExpertRetrieval('selfeval', checkin_collection)
    rankings = survey.batchQuery(topics, METRICS, PROFILE_TYPES)
    rankings.to_csv(outfile, float_format='%.3f', index=False,
                    names=GeoExpertRetrieval.RANK_SCHEMA)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    rankExperts(sys.argv[1], sys.argv[2])
