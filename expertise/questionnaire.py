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
import pymongo
import pandas as pd
import expertise.metrics as mt
from expertise.ger import GeoExpertRetrieval


METRICS = [mt.naive_metrics, mt.recency_metrics, mt.diversity_metrics]

PROFILE_TYPES = [mt.rankCheckinProfile, mt.rankActiveDayProfile]


def generate_ses_topics(outfile, topicfile):
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
    generate_ses_topics(sys.argv[1], sys.argv[2])
