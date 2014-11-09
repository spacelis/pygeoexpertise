#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: t_mongo_import.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
"""

import pandas as pd
import pymongo as mg
import expertise.ger as mt
import unittest


class TestClass(unittest.TestCase):  # pylint: disable=too-many-public-methods

    """Test case docstring."""

    def setUp(self):
        self.checkin = mg.MongoClient(port=27018).geoexpert_test.checkin_sf_rock_top10
        self.klb = mt.KnowledgeBase.fromMongo(self.checkin)

    def tearDown(self):
        pass

    def test_NaiveMetrics(self):
        """ docstring for test_NaiveMetrics
        """
        rank, score = mt.rankCheckinProfile(self.klb.checkins, mt.naive_metrics)
        print 'naive - checkin'
        print pd.DataFrame(score, index=rank, columns=['score'])

        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.naive_metrics)
        print 'naive - activeday'
        print pd.DataFrame(score, index=rank, columns=['score'])
        self.assertTrue(False)

    def test_RecencyMetrics(self):
        """ docstring for test_RecencyMetrics
        """
        rank, score = mt.rankCheckinProfile(self.klb.checkins, mt.recency_metrics)
        print 'recency - checkin'
        print pd.DataFrame(score, index=rank, columns=['score'])

        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.recency_metrics)
        print 'recency - activeday'
        print pd.DataFrame(score, index=rank, columns=['score'])
        self.assertTrue(False)

    def test_DiversityMetrics(self):
        """ test_DiversityMetrics
        """
        rank, score = mt.rankCheckinProfile(self.klb.checkins, mt.diversity_metrics)
        print 'diversity - checkin'
        print pd.DataFrame(score, index=rank, columns=['score'])

        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.diversity_metrics)
        print 'diversity - activeday'
        print pd.DataFrame(score, index=rank, columns=['score'])
        self.assertTrue(False)


    def test_RDMetrics(self):
        """ test_RDMetrics
        """
        rank, score = mt.rankCheckinProfile(self.klb.checkins, mt.RD_metrics)
        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.RD_metrics)
        print 'RD - checkin'
        print pd.DataFrame(score, index=rank, columns=['score'])

        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.RD_metrics)
        print 'RD - activeday'
        print pd.DataFrame(score, index=rank, columns=['score'])
        self.assertTrue(False)

    def test_bao2012(self):
        """ test_RDMetrics
        """
        rank, score = mt.rankCheckinProfile(self.klb.checkins, mt.bao2012_metrics)
        print 'bao2012_metrics - checkin'
        print pd.DataFrame(score, index=rank, columns=['score'])

        rank, score = mt.rankActiveDayProfile(self.klb.checkins, mt.bao2012_metrics)
        print 'bao2012_metrics - activeday'
        print pd.DataFrame(score, index=rank, columns=['score'])
        self.assertTrue(False)

if __name__ == '__main__':
    pass
