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
import pandas as pd


db = pymongo.MongoClient().geoexpert.checkin


def user_checkins(screen_name):
    """ Return a list of simplified check-ins

    :screen_name: @todo
    :returns: @todo

    """
    cklist = list()
    for ck in db.find({'user.screen_name': screen_name})\
            .sort('created_at', 1).limit(1000):
        cklist.append({
            'id': ck['id'],
            'created_at': ck['created_at'].strftime('%Y-%m-%dT%H:%M:%S'),
            'text': ck['text'],
            'place': {
                'id': ck['place']['id'],
                'name': ck['place']['name'],
                'lat': ck['place']['bounding_box']['coordinates'][0][0][1],
                'lng': ck['place']['bounding_box']['coordinates'][0][0][0],
                'cate_id': ck['place']['category']['id'],
                'category': ck['place']['category']['name'],
                'zcate': ck['place']['category']['zero_category_name'],
                'zcate_id': ck['place']['category']['zero_category'],
            }
        })
    return cklist


def exportdata(csv_input, csv_topics, csv_expert):
    """@todo: Docstring for mkcsv4ck.

    :csv_input: @todo
    :csv_topics: @todo
    :csv_expert: @todo
    :returns: @todo

    """
    topics = dict()
    expertise = pd.read_csv(csv_input)
    with open(csv_expert, 'wb') as fe:
        w = csv.DictWriter(fe, ['screen_name', 'expertise', 'checkins'])
        w.writeheader()
        for r in expertise.iterrows():
            r = r[1]
            w.writerow({'screen_name': r['twitter_id'],
                        'expertise': r['expertise'],
                        'checkins': json.dumps(user_checkins(
                            r['twitter_id']))})
            for t in json.loads(r['expertise']):
                if t['topic_id'] not in topics:
                    topics[t['topic_id']] = t
                    t['experts'] = set([r['twitter_id']])
                else:
                    topics[t['topic_id']]['experts'].add(r['twitter_id'])

    with open(csv_topics, 'wb') as ft:
        w = csv.DictWriter(ft, ['topic_id', 'topic', 'region', 'experts'])
        w.writeheader()
        for topic_id, topic in topics.iteritems():
            w.writerow({'topic_id': topic_id,
                        'topic': topic['topic'],
                        'region': topic['region'],
                        'experts': json.dumps(list(topic['experts']))})


def test2():
    """@todo: Docstring for test.
    :returns: @todo

    """
    print user_checkins('keniehuber')


if __name__ == '__main__':
    # take an input as csv in format of screen_name,expertise,... with no
    # headers
    exportdata(sys.argv[1], sys.argv[2], sys.argv[3])
    #test2()
