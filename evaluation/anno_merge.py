#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: anno_merge.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Functions used for merging judgement from crowdsourcing etc.
"""


import pandas as pd
from collections import Counter


def filterDf(df, g_filter, groupkeys):
    """ Return a df filtered in group-wise
    """
    return pd.concat([g_filter(g) for _, g in df.groupby(groupkeys)])


def filter_at(df, n=5, after=True):
    """ Return jdugement after judges having done (n - 1) tasks
    """
    def g(df):
        """ dummy function for get part of df"""
        df = df.sort(['created_at'], ascending=[1])
        if after:
            return df[n:]
        else:
            return df[:n]
    return filterDf(df, g, ['judge_id'])


def filter_topic(df, pop=0):
    """ Return judgement on given popular topics
    """
    return df[df['group'] == pop]


def take_most_freq(jd, col):
    """ Generate agreement by max vote
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    max_vote = lambda df: \
        df[df.score == Counter(df[col].values).most_common(1)[0][0]].head(1)
    return pd.concat([max_vote(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])


def take_avg(jd):
    """ Generate agreement by averaging votes
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    def avg_votes(df):
        s = df.score.mean()
        rdf = df.tail(1).copy()
        rdf.score = s
        return rdf
    return pd.concat([avg_votes(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])
