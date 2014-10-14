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
import argparse
import pandas as pd
from pymongo import MongoClient
from expertise.ger import KnowledgeBase
from expertise.ger import REGIONS
from expertise.ger import get_region
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
    _LOGGER.info('# POI_topics: %d', len(topics))
    return topics


def sampling_cate_topics(regions, size, g_percentages):
    """ Sampling poi topics from the database
    """
    topics = pd.DataFrame(columns=TOPIC_SCHEMA)
    checkins = None
    cate_set = set()
    for r in regions:
        kbase = KnowledgeBase.fromMongo(db.checkin, r['value'])
        if checkins is not None:
            cate_set = set(kbase.checkins['cid'].unique())
            checkins = checkins.append(kbase.checkins, ignore_index=True)
        else:
            cate_set &= set(kbase.checkins['cid'].unique())
            checkins = kbase.checkins
    _LOGGER.info('%d checkins loaded for cate_topics', len(checkins))
    checkins.drop_duplicates(cols=['pid', 'user'], inplace=True)
    for zcate, group in checkins.groupby('z_category'):
        cidgroup = [cid + '\t' + cname
                    for cid, cname in group[['cid', 'category']].values
                    if cid in cate_set]
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
    _LOGGER.info('# CATE_topics: %d', len(topics))
    for zcate, group in checkins.groupby('z_category'):
        for r in regions:
            topics = topics.append([{'topic_id': ZCATE_ID.next(),
                                     'topic': zcate,
                                     'region': r['name'],
                                     'associate_id': group['zcid'].values[0],
                                     'zcategory': zcate}])
    _LOGGER.info('# Total Cate_topics: %d', len(topics))
    return topics


def output_topics(topics, fout):
    """Return all topics generate from the database
    :returns: @todo

    """
    topic_set = pd.from_records(topics)
    topic_set.to_csv(fout, index=False, na_rep='N/A',
                     cols=TOPIC_SCHEMA, encoding='utf-8')
    _LOGGER.info('# Total Topics: %d', len(topic_set))


def make_random_topics():
    """ Generating topics from randomly from data set.

    :returns: @todo

    """
    for t in sampling_cate_topics(list(REGIONS.itervalues()), 18, [0.1, 0.9]):
        yield t
    for r in REGIONS.itervalues():
        for t in sampling_poi_topics(r, 45, [0.1, 0.8, 0.1]):
            yield t


def make_cate_topic(cate_id, topic_id, region):
    """@todo: Docstring for make_cate_topic.

    :cate_id: @todo
    :returns: @todo

    """
    cate = db.category.find_one({'id': cate_id})
    return {
        'topic_id': topic_id,
        'topic': cate['name'],
        'region': region,
        'associate_id': cate_id,
        'zcategory': cate['category']['zero_cateogry']
    }


def make_poi_topic(poi_id, topic_id):
    """@todo: Docstring for make_poi_topic.

    :poi_id: @todo
    :returns: @todo

    """
    poi = db.checkin.find_on({'place.id': poi_id, })['place']
    region = get_region(poi['boundingbox']['coordinates'][0][0][1],
                        poi['boundingbox']['coordinates'][0][0][0])
    return {
        'topic_id': topic_id,
        'topic': poi['name'],
        'region': region,
        'associate_id': poi_id,
        'zcategory': poi['category']['zero_cateogry']
    }


def console():
    """ Processing the incoming arguments from commandline.
    :returns: @todo

    """
    parser = argparse.ArgumentParser(
        description='Generating topics randomly or from lists.')
    parser.add_argument('-r', dest='random',
                        action='store_true', default=False,
                        help='Generating topic randomly from data set.')
    parser.add_argument('-p', dest='pois', action='store',
                        help='Generating topics fom a list of ids.')
    parser.add_argument('-c', dest='categories', action='store',
                        help='Generating topics fom a list of ids.')

    args = parser.parse_args()
    if args.random:
        output_topics(make_random_topics(sys.stdout), sys.stdout)
    else:
        if args.pois:
            with open(args.pois) as fin:
                output_topics((make_poi_topic(l) for l in fin), sys.stdout)
        if args.categories:
            with open(args.categories) as fin:
                output_topics((make_cate_topic(*l.split(',', 2))
                               for l in fin),
                              sys.stdout)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    console()
