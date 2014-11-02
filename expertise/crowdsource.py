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

import csv
import json
import pymongo
import pandas as pd
import logging
import click


db = pymongo.MongoClient().geoexpert


def strip_checkin(tweet):
    """ Strip useless information from tweets as a checkin.

    :tweet: @todo
    :returns: @todo

    """
    place = tweet['place']
    return {
        'created_at': tweet['created_at'].isoformat(),
        'retweeted': tweet['retweeted'],
        'retweet_count': tweet['retweet_count'],
        'in_reply_to_status_id': tweet['in_reply_to_status_id'],
        'in_reply_to_screen_name': tweet['in_reply_to_screen_name'],
        'in_reply_to_user_id': tweet['in_reply_to_user_id'],
        'favorited': tweet['favorited'],
        'favorite_count': tweet['favorite_count'],
        'id': tweet['id'],
        'text': tweet['text'],
        'place': {
            'place_type': place['place_type'],
            'lng': place['bounding_box']['coordinates'][0][0][0],
            'lat': place['bounding_box']['coordinates'][0][0][1],
            'name': place['name'],
            'full_name': place['full_name'],
            'id': place['id'],
            'category': place['category'],
        },
        'user': {
            'id': tweet['user']['id'],
            'screen_name': tweet['user']['screen_name']
        }
    }


def user_checkins(screen_name):
    """ Return a list of simplified check-ins

    :screen_name: The screen_name of the Twitter user
    :returns: A list of check-ins

    """
    cks = db.checkin\
        .find({'user.screen_name': screen_name})\
        .sort('created_at', -1)\
        .limit(1000)
    return [strip_checkin(c) for c in cks]


def topic_info(associate_id, level):
    """Return some extra information about the topic

    :topic_id: The topic_id of a given topic, e.g. "cate-0001"
    :topic: The text of a given topic
    :associate_id: The id of given topic
    :returns: Related entity of the given topics with their explanations

    """
    if level == 'POI':
        info = db.checkin.find_one({'place.id': associate_id})['place']
    elif level == 'CATEGORY':
        info = db.checkin.find_one(
            {'place.category.id': associate_id})['place']['category']
    return info


@click.command()
@click.option('-t', '--infocsv', default='geoentities.csv')
@click.option('-c', '--checkincsv', default='checkins.csv')
@click.argument('rankcsv', nargs=1)
def export(rankcsv, infocsv, checkincsv):
    """ Generating two CSV outputs, one for experts and one for topics

    :rankcsv: The csv file with Expert - Topic mapping table and Topic table
    :returns: None

    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

    ranking = pd.read_csv(rankcsv)

    logging.info('Generating check-in subset for candidates')
    with open(checkincsv, 'wb') as fe:
        w = csv.DictWriter(fe, ['screen_name', 'checkins'])
        w.writeheader()
        for c in ranking.candidate.unique():
            logging.info('Retrieving checkins for %s', c)
            w.writerow({'screen_name': c,
                        'checkins': json.dumps(user_checkins(c))})

    logging.info('Generating topic entities')
    with open(infocsv, 'wb') as ft:
        w = csv.DictWriter(ft, ['associate_id', 'name', 'info',
                                'url', 'level'])
        w.writeheader()
        for _, row in ranking.drop_duplicates(['topic', 'associate_id'])\
                .iterrows():
            logging.info('Retrieving info for %s', row['topic'])

            level = 'POI' if row['topic_id'].startswith('p') else 'CATEGORY'
            info = topic_info(row['associate_id'], level)
            url = None if 'url' not in info else info['url']

            w.writerow({'associate_id': row['associate_id'],
                        'name': row['topic'],
                        'info': json.dumps(info),
                        'url': url,
                        'level': level})


def test2():
    """@todo: Docstring for test.
    :returns: @todo

    """
    print user_checkins('keniehuber')


if __name__ == '__main__':
    export()
