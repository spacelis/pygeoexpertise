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


def max_vote_agreement(jd):
    """ Generate agreement by max vote
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    def max_vote(df):
        """ get the most frequent item"""
        s, _ = Counter(df.score.values).most_common(1)[0]
        return df[df.score == s].head(1)
    return pd.concat([max_vote(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])


def avg_vote_agreement(jd):
    """ Generate agreement by averaging votes
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    def avg_votes(df):
        """ Get the average value of all items"""
        s = df.score.mean()
        rdf = pd.DataFrame(df.tail(1))
        rdf.score = s
        return rdf
    return pd.concat([avg_votes(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])
