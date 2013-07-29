#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: topics.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Generating topics by using stratified sampling.
"""

import sys
import pandas as pd
from pymongo import MongoClient
from expertise.metrics import KnowledgeBase
from stratified import stratified_samples

db = MongoClient().geoexpert


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


def newId(name):
    """ Generating an ID based on the given name.

    :name: @todo
    :returns: @todo

    """
    i = 0
    while True:
        yield '%s-%04d' % (name, i)
        i += 1

POI_ID = newId('poi')
CATE_ID = newId('cate')
ZCATE_ID = newId('zcate')


COLS = ['topic_id',
        'topic',
        'region',
        'associate_id',
        'zcategory',
        'group']


def sampling_poi_topics(region, size, g_percentages):
    """ Sampling poi topics from the database
    """
    topics = pd.DataFrame(columns=COLS)
    kbase = KnowledgeBase.fromMongo(db.checkin, region['value'])
    kbase.checkins.drop_duplicates(cols=['pid', 'user'], inplace=True)
    for zcate, group in kbase.checkins.groupby('z_category'):
        pidgroup = [pid + '\t' + pname
                    for pid, pname in group[['pid', 'place']].values]
        for gid, g in enumerate(stratified_samples(pidgroup,
                                                   g_percentages,
                                                   size / 9)):
            for s in g:
                pid, pname = s.split('\t')
                topics = topics.append([{'topic_id': POI_ID.next(),
                                         'topic': pname,
                                         'region': region['name'],
                                         'associate_id': pid,
                                         'zcategory': zcate,
                                         'group': gid}])
    return topics


def sampling_cate_topics(regions, size, g_percentages):
    """ Sampling poi topics from the database
    """
    topics = pd.DataFrame(columns=COLS)
    checkins = None
    for r in regions:
        kbase = KnowledgeBase.fromMongo(db.checkin, r['value'])
        if checkins is not None:
            checkins.append(kbase.checkins, ignore_index=True)
        else:
            checkins = kbase.checkins
    checkins.drop_duplicates(cols=['pid', 'user'], inplace=True)
    for zcate, group in checkins.groupby('z_category'):
        cidgroup = [cid + '\t' + cname
                    for cid, cname in group[['cid', 'category']].values]
        for gid, g in enumerate(stratified_samples(cidgroup,
                                                   g_percentages,
                                                   size / 9)):
            for s in g:
                cid, cname = s.split('\t')
                for r in regions:
                    topics = topics.append([{'topic_id': CATE_ID.next(),
                                             'topic': cname,
                                             'region': r['name'],
                                             'associate_id': cid,
                                             'zcategory': zcate,
                                             'group': gid}])
    return topics


def zcate_category(regions):
    """ Generating the top categories as topics

    :regions: @todo
    :returns: @todo

    """
    topics = pd.DataFrame(columns=COLS)
    checkins = None
    for r in regions:
        kbase = KnowledgeBase.fromMongo(db.checkin, r['value'])
        if checkins is not None:
            checkins.append(kbase.checkins, ignore_index=True)
        else:
            checkins = kbase.checkins
    checkins.drop_duplicates(cols=['pid', 'user'], inplace=True)
    for zcate, group in checkins.groupby('z_category'):
        for r in regions:
            topics = topics.append([{'topic_id': ZCATE_ID.next(),
                                    'topic': zcate,
                                    'region': r['name'],
                                    'associate_id': group['zcid'].values[0],
                                    'zcategory': zcate}])
    return topics


def get_topics():
    """Return all topics generate from the database
    :returns: @todo

    """
    topic_set = pd.DataFrame(columns=COLS)
    t = sampling_cate_topics(list(REGIONS.itervalues()), 18, [0.1, 0.9])
    topic_set = topic_set.append(t, ignore_index=True)
    t = zcate_category(list(REGIONS.itervalues()))
    topic_set = topic_set.append(t, ignore_index=True)
    for r in REGIONS.itervalues():
        t = sampling_poi_topics(r, 45, [0.1, 0.8, 0.1])
        topic_set = topic_set.append(t, ignore_index=True)
    topic_set.to_csv(sys.stdout, index=False, na_rep='N/A', cols=COLS)

if __name__ == '__main__':
    get_topics()
