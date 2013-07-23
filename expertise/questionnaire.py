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
from . import metrics as mt


def _getid():
    """ Return an unique identifier
    """
    return uuid.uuid4()


class Survey(object):
    """ A class represent a survey consisting of a bunch of questionnaires
    """
    def __init__(self, name, collection):
        super(Survey, self).__init__()
        self.name = name
        self.collection = collection

    def rankExperts(self, query, rank_method, profile_type, cutoff=10):
        """ Return a set of parameters for setting up questionnaires
            :param ranking: a DataFrame containing ranking of experts
            :param region: {'name':..., 'value':...} for region (city)
            :param topic: {'name':..., 'value':...} for topic (POI/category)
            :param rank_method: the ranking method name
            :param profile_type: the ranking profile type
            :return: a set of rows containing information for setting up
                     a set of questions
        """
        q = dict()
        q.update(query['region']['value'])
        q.update(query['topic']['value'])
        kbase = mt.KnowledgeBase.fromMongo(self.collection, query)
        rank, scores = rank_method(kbase.checkins, profile_type, cutoff=cutoff)
        qset = pd.DataFrame([{
            'ex_id': '%s-%s' % (self.name, _getid()),
            'region': query['region']['name'],
            'topic': query['topic']['name'],
            'user_screen_name': r,
            'rank_method': rank_method.__name__,
            'rank': i + 1,
            'score': s,
        } for i, r, s in zip(range(len(r)), rank, scores)])
        return qset

    def batchQuestionSet(self, expert_queries, metrics, profile_type):
        """ Find all experts about each POI in the poi_list
        """
        qset = pd.DataFrame()
        for mtc in metrics:
            for pf_type in profile_type:
                for q in expert_queries:
                    qset.append(
                        self.rankExperts(q, mtc, pf_type),
                        ignore_index=True)
        return qset


REGIONS = {
    'CHICAGO': {'name': 'CHICAGO',
                'value': {"place.bounding_box.coordinates.0.0.1":
                          {'$gt': 41.4986, '$lt': 42.0232},
                          "place.bounding_box.coordinates.0.0.0":
                          {'$gt': -88.1586, '$lt': -87.3573},
                          "place.category": {'$exists': True}}},
    'New York': {'name': 'New York',
                 'value': {"place.bounding_box.coordinates.0.0.1":
                           {'$gt': 40.4110, '$lt': 40.9429},
                           "place.bounding_box.coordinates.0.0.0":
                           {'$gt': -74.2918, '$lt': -73.7097},
                           "place.category": {'$exists': True}}},
    'Los Angeles': {'name': 'Los Angeles',
                    'value': {"place.bounding_box.coordinates.0.0.1":
                              {'$gt': 33.7463, '$lt': 34.2302},
                              "place.bounding_box.coordinates.0.0.0":
                              {'$gt': -118.6368, '$lt': -117.9053},
                              "place.category": {'$exists': True}}},
    'San Francisco': {'name': 'San Francisco',
                      'value': {"place.bounding_box.coordinates.0.0.1":
                                {'$gt': 37.7025, '$lt': 37.8045},
                                "place.bounding_box.coordinates.0.0.0":
                                {'$gt': -122.5349, '$lt': -122.3546},
                                "place.category": {'$exists': True}}}
}

METRICS = [mt.naive_metrics, mt.recency_metrics, mt.diversity_metrics]

PROFILE_TYPES = [mt.rankCheckinProfile, mt.rankActiveDayProfile]


def self_eval_survey(outfile, poi_samples, cate_samples):
    """ Running a set of queries to generate the self-evaluation survey
        for geo-experts.
    """
    checkin_collection = pymongo.MongoClient().geoexpert.checkin
    survey = Survey('selfeval', checkin_collection)
    query_set = list()
    for topic, pid in poi_samples:
        for r in REGIONS:
            qt = {'topic': {'name': topic, 'value': {'place.id': pid}},
                  'region': REGIONS[r]}
            query_set.append(qt)
    for topic, cid in cate_samples:
        for r in REGIONS:
            qt = {'topic': {'name': topic,
                            'value': {'place.category.id': cid}},
                  'region': REGIONS[r]}
            query_set.append(qt)
    survey_data = survey.batchQuestionSet(query_set, METRICS, PROFILE_TYPES)
    survey_data.to_csv(outfile)


if __name__ == '__main__':
    self_eval_survey(sys.argv[1], sys.argv[2], sys.argv[3])
