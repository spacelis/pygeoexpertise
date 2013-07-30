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
import logging
import pandas as pd
from pymongo import MongoClient
from expertise.ger import KnowledgeBase
from expertise.ger import REGIONS
from stratified import stratified_samples

db = MongoClient().geoexpert

_LOGGER = logging.getLogger(__name__)

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


TOPIC_SCHEMA = ['topic_id',
        'topic',
        'region',
        'associate_id',
        'zcategory',
        'group']


def sampling_poi_topics(region, size, g_percentages):
    """ Sampling poi topics from the database
    """
    topics = pd.DataFrame(columns=TOPIC_SCHEMA)
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
    topics = pd.DataFrame(columns=TOPIC_SCHEMA)
    checkins = None
    for r in regions:
        kbase = KnowledgeBase.fromMongo(db.checkin, r['value'])
        if checkins is not None:
            checkins = checkins.append(kbase.checkins, ignore_index=True)
        else:
            checkins = kbase.checkins
    _LOGGER.info('%d checkins loaded for cate_topics', len(checkins))
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
    topics = pd.DataFrame(columns=TOPIC_SCHEMA)
    checkins = None
    for r in regions:
        kbase = KnowledgeBase.fromMongo(db.checkin, r['value'])
        if checkins is not None:
            checkins= checkins.append(kbase.checkins, ignore_index=True)
        else:
            checkins = kbase.checkins
    _LOGGER.info('%d checkins loaded for cate_topics', len(checkins))
    checkins.drop_duplicates(cols=['pid', 'user'], inplace=True)
    for zcate, group in checkins.groupby('z_category'):
        for r in regions:
            topics = topics.append([{'topic_id': ZCATE_ID.next(),
                                    'topic': zcate,
                                    'region': r['name'],
                                    'associate_id': group['zcid'].values[0],
                                    'zcategory': zcate}])
    return topics


def gen_topics(outfile):
    """Return all topics generate from the database
    :returns: @todo

    """
    topic_set = pd.DataFrame(columns=TOPIC_SCHEMA)
    t = sampling_cate_topics(list(REGIONS.itervalues()), 18, [0.1, 0.9])
    topic_set = topic_set.append(t, ignore_index=True)
    t = zcate_category(list(REGIONS.itervalues()))
    topic_set = topic_set.append(t, ignore_index=True)
    for r in REGIONS.itervalues():
        t = sampling_poi_topics(r, 45, [0.1, 0.8, 0.1])
        topic_set = topic_set.append(t, ignore_index=True)
    with open(outfile, 'w') as fout:
        topic_set.to_csv(fout, index=False, na_rep='N/A',
                         cols=TOPIC_SCHEMA, encoding='utf-8')

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    gen_topics(sys.argv[1])
