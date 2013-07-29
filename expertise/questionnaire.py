#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: questionnaire.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
    Make data for questionnaires
"""

import sys
import uuid
import pymongo
import pandas as pd
import expertise.metrics as mt
from expertise.topics import REGIONS


def _getid():
    """ Return an unique identifier
    """
    return uuid.uuid4()


class GeoExpertRetrieval(object):
    """ A class represent a survey consisting of a bunch of questionnaires
    """
    def __init__(self, name, collection):
        super(GeoExpertRetrieval, self).__init__()
        self.name = name
        self.collection = collection

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
        kbase = mt.KnowledgeBase.fromMongo(self.collection, q)
        rank, scores = kbase.rank(profile_type, rank_method, cutoff=cutoff)
        ranking = pd.DataFrame([{
            'rank_id': '%s-%s' % (self.name, _getid()),
            'topic_id': query['topic_id'],
            'region': query['region']['name'],
            'topic': query['topic']['name'],
            'user_screen_name': r,
            'rank_method': rank_method.__name__,
            'rank': i + 1,
            'score': s,
        } for i, (r, s) in enumerate(zip(rank, scores))])
        return ranking

    def formatQuery(self, topic_id, name, ident, region_name, is_cate=False):
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
                               'value': REGIONS[region_name]['value']}}
        else:
            return {'topic_id': topic_id,
                    'topic': {'name': name,
                              'value': {'place.id': ident}},
                    'region': {'name': region_name,
                               'value': REGIONS[region_name]['value']}}

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
                                 'cate' in t['topic_id'])
            print >> sys.stderr, 'Processing %s...' % (q['topic_id'])
            try:
                for mtc in metrics:
                    for pf_type in profile_type:
                        rank = self.rankExperts(q, mtc, pf_type, 5)
                        rankings = rankings.append(rank)
            except:
                print >> sys.stderr, 'Failed at %s' % (q['topic_id'])
        return rankings


METRICS = [mt.naive_metrics, mt.recency_metrics, mt.diversity_metrics]

PROFILE_TYPES = [mt.rankCheckinProfile, mt.rankActiveDayProfile]


def self_eval_survey(outfile, topicfile):
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
    self_eval_survey(sys.argv[1], sys.argv[2])
