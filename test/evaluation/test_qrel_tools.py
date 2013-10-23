#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: test_anno_merge.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    testing
"""

import unittest
import pandas as pd

import evaluation.qrel_tools as qt


class TestAgreement(unittest.TestCase):

    """ Test the agreement"""

    def setUp(self):
        """ a general texture for testing"""
        self.jd = pd.DataFrame.from_records([
            {'topic_id': 't1', 'candidate': 'a', 'score': '1'},
            {'topic_id': 't1', 'candidate': 'a', 'score': '2'},
            {'topic_id': 't1', 'candidate': 'a', 'score': '2'},
            {'topic_id': 't1', 'candidate': 'a', 'score': '3'},
            {'topic_id': 't1', 'candidate': 'b', 'score': '1'},
            {'topic_id': 't1', 'candidate': 'b', 'score': '2'},
            {'topic_id': 't1', 'candidate': 'b', 'score': '3'},
            {'topic_id': 't1', 'candidate': 'b', 'score': '3'},
        ])

    def tearDown(self):
        pass

    def test_take_most_freq(self):
        """ max_vote_agreement
        """
        ag = qt.major_vote(self.jd, 'score')
        self.assertEqual(ag['score'].values.tolist(), ['2', '3'])

    def test_kappa(self):
        """ test_kappa
        """
        TT = [(1, 1)] * 20
        TF = [(1, 0)] * 5
        FT = [(0, 1)] * 10
        FF = [(0, 0)] * 15
        data = zip(*(TT + TF + FT + FF))
        self.assertAlmostEqual(
            qt.cohen_kappa_score(data[0], data[1]),
            0.4)

        TT = [(1, 1)] * 25
        TF = [(1, 0)] * 5
        FT = [(0, 1)] * 35
        FF = [(0, 0)] * 35
        data = zip(*(TT + TF + FT + FF))
        self.assertAlmostEqual(
            qt.cohen_kappa_score(data[0], data[1]),
            0.25925925925925924)
