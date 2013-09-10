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
import sqlite3


db = pymongo.MongoClient().geoexpert


def user_checkins(screen_name):
    """ Return a list of simplified check-ins

    :screen_name: The screen_name of the Twitter user
    :returns: A list of check-ins

    """
    cklist = list()
    for ck in db.checkin.find({'user.screen_name': screen_name})\
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
                'zcate_id': ck['place']['category']['zero_category'],
                'zcategory': ck['place']['category']['zero_category_name'],
            }
        })
    return cklist


def topic_detail(topic_id, topic, associate_id):
    """Return some extra information about the topic

    :topic_id: The topic_id of a given topic, e.g. "cate-0001"
    :topic: The text of a given topic
    :associate_id: The id of given topic
    :returns: Related entity of the given topics with their explanations

    """
    related_to = list()
    if 'zcate' in topic_id:
        related_to = [{
            'name': topic,
            'filter_name': topic,
            'explanation': 'A top-level category'
        }]
    elif 'cate' in topic_id:
        cate = db.category.find_one({'id': associate_id})
        related_to = [
            {
                'name': topic,
                'filter_name': topic,
                'explanation': 'A lower-level category'
            },
            {
                'name': cate['zero_category_name'],
                'filter_name': cate['zero_category_name'],
                'explanation': 'The top-level category that %s belongs to.'
                % (topic, )
            }
        ]
    elif 'poi' in topic_id:
        poi = db.checkin.find_one({'place.id': associate_id})['place']
        related_to = [
            {
                'name': topic,
                'filter_name': associate_id,
                'explanation':
                'A place of interest belongs to category [%s, %s].'
                % (poi['category']['name'],
                   poi['category']['zero_category_name'])
            },
            {
                'name': poi['category']['name'],
                'filter_name': poi['category']['name'],
                'explanation': 'A lower category that %s belongs to.'
                % (topic, )
            },
            {
                'name': poi['category']['zero_category_name'],
                'filter_name': poi['category']['zero_category_name'],
                'explanation': 'A top-level category that %s belongs to.'
                % (topic, )
            }
        ]
    return related_to


def exportdata(sqlfile):
    """ Generating two CSV outputs, one for experts and one for topics

    :sqlfile: The sqlite3 database file with Expert - Topic mapping table
                and Topic table
    :returns: None

    """
    csv_expert = '.'.join(sqlfile.split('.')[:-1]) + 'expert.csv'
    csv_topic = '.'.join(sqlfile.split('.')[:-1]) + 'topic.csv'
    conn = sqlite3.connect(sqlfile)
    cur = conn.cursor()

    with open(csv_expert, 'wb') as fe:
        w = csv.DictWriter(fe, ['screen_name', 'topics', 'checkins'])
        w.writeheader()
        for row in cur.execute('SELECT screen_name, '
                               'group_concat(topic_id, "\t") '
                               'FROM expert GROUP BY screen_name'):
            w.writerow({'screen_name': row[0],
                        'topics': json.dumps(row[1].split('\t')),
                        'checkins': json.dumps(user_checkins(row[0]))})

    with open(csv_topic, 'wb') as ft:
        w = csv.DictWriter(ft, ['topic_id', 'topic', 'region',
                                'experts', 'related_to'])
        w.writeheader()
        for row in cur.execute('SELECT e.topic_id, topic, region, '
                               'group_concat(screen_name, "\t"), '
                               'associate_id '
                               'FROM expert as e left join topic as t '
                               'ON e.topic_id = t.topic_id '
                               'GROUP BY e.topic_id'):
            w.writerow({'topic_id': row[0],
                        'topic': row[1],
                        'region': row[2],
                        'experts': json.dumps(row[3].split('\t')),
                        'related_to': json.dumps(
                            topic_detail(row[0], row[1], row[4]))})


def test2():
    """@todo: Docstring for test.
    :returns: @todo

    """
    print user_checkins('keniehuber')


if __name__ == '__main__':
    exportdata(sys.argv[1])
