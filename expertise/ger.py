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
import argparse
import logging
import numpy as np
import pandas as pd
import pymongo
import expertise.pandasmongo as pandasmongo

CKLAT = 'place.bounding_box.coordinates.0.0.1'
CKLON = 'place.bounding_box.coordinates.0.0.0'
REFDATE_DEFAULT = np.datetime64('2013-08-01T00:00:00+02')
DECAYRATE_DEFAULT = 1. / 180
ONEDAY = np.timedelta64(1, 'D')


REGIONS = {
    'Chicago': {'name': 'Chicago',
                'value': {CKLAT: {'$gt': 41.4986, '$lt': 42.0232},
                          CKLON: {'$gt': -88.1586, '$lt': -87.3573}}},
    'New York': {'name': 'New York',
                 'value': {CKLAT: {'$gt': 40.4110, '$lt': 40.9429},
                           CKLON: {'$gt': -74.2918, '$lt': -73.7097}}},
    'Los Angeles': {'name': 'Los Angeles',
                    'value': {CKLAT: {'$gt': 33.7463, '$lt': 34.2302},
                              CKLON: {'$gt': -118.6368, '$lt': -117.9053}}},
    'San Francisco': {'name': 'San Francisco',
                      'value': {CKLAT: {'$gt': 37.7025, '$lt': 37.8045},
                                CKLON: {'$gt': -122.5349, '$lt': -122.3546}}},
    # 'US': {'name': 'US',
    #        'value': {CKLAT: {'$gt': 24.5210, '$lt': 49.3845},
    #                  CKLON: {'$gt': -124.7625, '$lt': -66.9326}}}
}


def get_region(lat, lon):
    """ return the name of the region of the given coordinates

    :lat: @todo
    :lon: @todo
    :returns: @todo

    """
    for v in REGIONS.itervalues():
        if ((v['value'][CKLAT]['$gt'] < lat < v['value'][CKLAT]['$lt']) and
                v['value'][CKLON]['$lt'] < lon < v['value'][CKLON]['$gt']):
            return v['name']


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
                          'user.id': 'uid',
                          'place.id': 'pid',
                          CKLAT: 'lat',
                          CKLON: 'lng',
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
        checkins['created_date'] = checkins['created_at'].apply(
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


def rankCheckinProfile(checkins, metrics, **kargs):
    """ Rank the profile based on checkins
    """
    profiles = checkins.groupby('user')
    rank, scores = metrics(profiles, **kargs)
    return rank, scores


def rankActiveDayProfile(checkins, metrics, **kargs):
    """ Rank the profiles based on active days

        All check-ins on the same day are considered as only one check-in
    """
    checkins.sort('created_at', ascending=True, inplace=True)
    day_profiles = checkins.drop_duplicates(
        cols=['user',
              'created_date',
              'pid'],
        take_last=True
    )
    day_profiles['created_at'] = day_profiles['created_date']
    profiles = day_profiles.groupby('user')
    rank, scores = metrics(profiles, **kargs)
    return rank, scores


def naive_metrics(profiles, cutoff=-1, **_):
    """ using number of visitings / active days themselves for ranking
        score_u = N_ck(u, p)
    """
    mrank = profiles['id'].count().order(ascending=False)
    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


def random_metrics(profiles, cutoff=-1, **_):
    """ As a random baseline

    :profiles: @todo
    :cutoff: @todo
    :**kargs: @todo
    :returns: @todo

    """
    mrank = profiles['id'].count().index.values.copy()
    np.random.shuffle(mrank)
    if cutoff > 0:
        return mrank[:cutoff], np.zeros(len(mrank))
    else:
        return mrank, np.zeros(len(mrank))


def _time_diff(t_array, t):
    """ Return the difference in days between two time points
    """
    return (t_array - t) / ONEDAY


def recency_metrics(profiles, cutoff=-1, **kargs):
    """ A metrics boosting recent visits
        score_{u} = sum_{c \in T} exp d*(t_c - t_ref)
    """
    refdate = kargs.get('refdate', REFDATE_DEFAULT)
    decay_rate = kargs.get('decay_rate', DECAYRATE_DEFAULT)
    mrank = profiles['created_at'].agg(
        lambda x: np.sum(np.exp(_time_diff(x.values, refdate) * decay_rate))) \
        .order(ascending=False)
    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


def diversity_metrics(profiles, cutoff=-1, **_):
    """ A metrics boosting diverse visits
        score_{u} = sum_{p in T} log_2 N_{ck}(u, p)
    """
    mrank = profiles.apply(
        lambda x: np.sum(np.log2(x.groupby('pid').apply(len))))\
        .order(ascending=False)

    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


def RD_metrics(profiles, cutoff=-1, **kargs):
    """ A metrics boosting diverse visits
        score_u = sum_{l in T} log_2 sum_{l_c = l} exp d*(t_c - t_ref)
    """
    refdate = kargs.get('refdate', REFDATE_DEFAULT)
    decay_rate = kargs.get('decay_rate', DECAYRATE_DEFAULT)
    mrank = profiles.apply(
        lambda x: np.sum(np.log2(x.groupby('pid').apply(
            lambda x: np.sum(np.exp(_time_diff(x['created_at'].values, refdate)
                                    * decay_rate))
        ))))\
        .order(ascending=False)
    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


def drawMat(old_val, val, cand, M, MM):
    """ Draw the mat for inspection

    :val: TODO
    :M: TODO
    :MM: TODO
    :returns: TODO

    """
    from matplotlib import pyplot as plt
    from mpldatacursor import datacursor
    from matplotlib import cm
    fig, ax = plt.subplots(5, 1)
    ax[1].matshow(old_val.reshape(1, -1), cmap=cm.gray)
    ax[0].matshow(val.reshape(1, -1), cmap=cm.gray)
    ax[2].matshow(np.dot(M.T, val).reshape(1, -1), cmap=cm.gray)
    ax[3].matshow(M, cmap=cm.gray)
    ax[4].matshow(MM, cmap=cm.gray)
    datacursor(display='single')
    plt.show()


def converge(func, init, **kwargs):
    """Iteratively apply func to a value starting with init until it converge

    :func: a function to apply
    :init: the initial value
    :count: the max number of iterations (0 means inf)
    :atol: the absolute tolerance (see numpy.allclose)
    :rtol: the relevant tolerance (see numpy.allclose)
    :returns: the converged value
    """
    count = kwargs.get('count', 5000)
    old_val = init
    all_close_params = {k: kwargs[k] for k in kwargs.keys() if k in ['atol', 'rtol']}
    if count > 0:
        it = range(count)
    else:
        def inf():
            while True:
                yield -1
        it = inf()
    for _ in it:
        val = func(old_val)
        val = val / val.sum()
        drawMat(old_val, val, kwargs['cand'], kwargs['M'], kwargs['MM'])
        if np.allclose(val, old_val, **all_close_params):
            return val
        prev_val = old_val
        old_val = val
        if np.any(np.isnan(val)):
            raise ValueError('Not converge\n%s !=\n%s', prev_val, val)
    raise ValueError('Not converge\n%s !=\n%s', prev_val, val)


def bao2012_metrics(profiles, cutoff=-1, **_):
    """ A method based on hub-auth score.
    http://en.wikipedia.org/wiki/HITS_algorithm

    :profiles: grouped profiles of users
    :cutoff: the cutoff of the length of the returned list
    :returns: (users in rank, score)

    """
    visits = profiles.apply(lambda x: x.groupby('pid').apply(lambda x: pd.Series(len(x))))\
        .reset_index().rename(columns={0: 'cks'})
    if 'cks' not in visits:
        return [], []
    # prepare initial values
    candidates = visits.groupby('user')['cks'].sum()
    A = candidates.values.astype(np.float64)
    logging.debug('A=%s', A)
    M = visits.pivot('user', 'pid', 'cks').fillna(0).values.astype(np.float64)
    # M = (visits.pivot('user', 'pid', 'cks') > 0).values.astype(np.float64)
    logging.debug('M=%s', M)
    # Normalize
    P = np.dot(M, M.T)
    # Power Iteration
    A = converge(lambda x: np.dot(P, x), A, cand=candidates, M=M, MM=P, rtol=0.001)

    # Format results
    mrank = pd.Series(A.flatten(),
                      index=candidates.index.values).order(ascending=False)

    if cutoff > 0:
        return mrank.index.values[:cutoff], mrank.values[:cutoff]
    else:
        return mrank.index.values, mrank.values


class GeoExpertRetrieval(object):
    """ A class managing querying the geoexperts.
    """
    def __init__(self, name, collection):
        super(GeoExpertRetrieval, self).__init__()
        self.name = name
        self.collection = collection
        self._logger = logging.getLogger(
            '%s.%s' % (__name__, type(self).__name__))

    def rankExperts(self, query, rank_method, profile_type, cutoff=5):
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
            'topic_id': query['topic_id'],
            'region': query['region']['name'],
            'topic': query['topic']['name'],
            'associate_id': query['topic']['associate_id'],
            'candidate': r,
            'rank_method': rank_method.__name__,
            'profile_type': profile_type.__name__,
            'rank': i + 1,
            'score': s,
        } for i, (r, s) in enumerate(zip(rank, scores))])
        return ranking

    @staticmethod
    def formatQuery(topic_id,
                    topic,
                    assicate_id,
                    region_name,
                    region_value,
                    topic_type):
        """ Format query

        :topic_id: The id of the topic
        :topic: the text representation of the topic
        :assicate_id: The reference identifier for the queried topic
        :region_name: The name of the region ref: REGIONS
        :topic_type: The type of the topic['z'='zcate', 'c'='cate', p='poi']
        :returns: The dict of the formated query
        """
        if topic_type == 'c':
            return {'topic_id': topic_id,
                    'topic': {
                        'name': topic,
                        'associate_id': assicate_id,
                        'value': {'place.category.id': assicate_id}},
                    'region': {
                        'name': region_name,
                        'value': region_value}}
        elif topic_type == 'z':
            return {'topic_id': topic_id,
                    'topic': {
                        'name': topic,
                        'associate_id': assicate_id,
                        'value': {
                            'place.category.zero_category': assicate_id}},
                    'region': {'name': region_name,
                               'value': region_value}}
        elif topic_type == 'p':
            return {'topic_id': topic_id,
                    'topic': {
                        'name': topic,
                        'associate_id': assicate_id,
                        'value': {'place.id': assicate_id}},
                    'region': {
                        'name': region_name,
                        'value': region_value}}

    RANK_SCHEMA = ['topic_id', 'rank', 'candidate', 'score',
                   'rank_method', 'profile_type', 'region', 'topic',
                   'associate_id']

    def batchQuery(self, topics, metrics, profile_type, cutoff=5):
        """ batchquery
        """
        rankings = pd.DataFrame(columns=GeoExpertRetrieval.RANK_SCHEMA)
        for t in topics.values:
            t = dict(zip(topics.columns, t))
            q = GeoExpertRetrieval.formatQuery(
                t['topic_id'],
                t['topic'],
                t['associate_id'],
                t['region'],
                REGIONS[t['region']]['value'],
                t['topic_id'][0])
            self._logger.info('Processing %(topic_id)s...', q)
            try:
                for mtc in metrics:
                    if ('poi' in t['topic_id']) and mtc == diversity_metrics:
                        continue
                    for pf_type in profile_type:
                        rank = self.rankExperts(q, mtc, pf_type, cutoff)
                        rankings = rankings.append(rank)
            except ValueError:
                self._logger.exception('Failed at %(topic_id)s', q)
        return rankings


METRICS = [naive_metrics,
           recency_metrics,
           diversity_metrics,
           random_metrics,
           bao2012_metrics,
           RD_metrics]
PROFILE_TYPES = [rankCheckinProfile,
                 rankActiveDayProfile]


def run_experiment(outfile, topicfile, db='geoexpert', coll='checkin',
                   cutoff=5):
    """ Running a set of queries to generate ranking lists to topics.
    """
    topics = pd.read_csv(topicfile)
    checkin_collection = pymongo.MongoClient()[db][coll]
    ger = GeoExpertRetrieval('all', checkin_collection)

    # Do batch ranking with all the parameters
    rankings = ger.batchQuery(topics, METRICS, PROFILE_TYPES, cutoff)
    rankings.to_csv(outfile, float_format='%.3f', index=False,
                    names=GeoExpertRetrieval.RANK_SCHEMA)


def console():
    """ An interface for console invoke
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    parser = argparse.ArgumentParser(
        description='Geo-expertise retrieval is a IR system focus on'
        'the geolocation information. Specifically, it help users search '
        'for knowledgeable users on social networks w.r.t. location '
        'information',
        epilog='Wen.Li@tudelft.nl 2013')
    parser.add_argument(
        '-o', '--output', dest='output', action='store',
        metavar='FILE', default=sys.stdout,
        help='Using the file as the output instead of STDIN.')
    parser.add_argument(
        '-d', '--db', dest='db', action='store',
        metavar='DB', default='geoexpert',
        help='The name of the db instance in mongodb')
    parser.add_argument(
        '-c', '--collection', dest='collection', action='store',
        metavar='COLLECTION', default='checkin',
        help='The collection containing the check-in profile of condidates')
    parser.add_argument(
        '-k', '--cutoff', dest='cutoff', action='store',
        metavar='COLLECTION', default=5, type=int,
        help='The collection containing the check-in profile of condidates')
    parser.add_argument(
        'topic', metavar='TOPIC', nargs=1,
        help='The topic file used for experiments.')
    args = parser.parse_args()
    run_experiment(args.output, args.topic[0],
                   db=args.db, coll=args.collection,
                   cutoff=args.cutoff)

if __name__ == '__main__':
    console()
