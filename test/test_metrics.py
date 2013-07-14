#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: t_mongo_import.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
"""

import pymongo as mg
import expertise.metrics as mt


def test_fromMongo():
    """ test
    """
    c = mg.MongoClient().testdb.checkin
    k = mt.KnowledgeBase.fromMongo(c)


def test_fromTSV():
    """ test
    """
    pass


def test_NaiveMetrics():
    """ docstring for test_NaiveMetrics
    """
    c = mg.MongoClient().testdb.checkin
    k = mt.KnowledgeBase.fromMongo(c)
    rank, score = k.rankCheckinProfile(mt.naive_metrics)
    rank, score = k.rankActiveDayProfile(mt.naive_metrics)


def test_RecencyMetrics():
    """ docstring for test_RecencyMetrics
    """
    c = mg.MongoClient().testdb.checkin
    k = mt.KnowledgeBase.fromMongo(c)
    rank, score = k.rankCheckinProfile(mt.recency_metrics)
    rank, score = k.rankActiveDayProfile(mt.recency_metrics)


def test_DiversityMetrics():
    """ a
    """
    c = mg.MongoClient().testdb.checkin
    k = mt.KnowledgeBase.fromMongo(c)
    rank, score = k.rankCheckinProfile(mt.diversity_metrics)
    rank, score = k.rankActiveDayProfile(mt.diversity_metrics)


if __name__ == '__main__':
    #test_NaiveMetrics()
    #test_RecencyMetrics()
    test_DiversityMetrics()
