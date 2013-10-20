#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: qrel_tools.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Functions used for merging judgement from crowdsourcing etc.
"""


import dateutil
from collections import Counter

import pandas as pd
import numpy as np
import json


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


def major_vote(jd, col):
    """ Generate agreement by max vote
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    max_vote = lambda df: \
        df[df.score == Counter(df[col].values).most_common(1)[0][0]].head(1)
    return pd.concat([max_vote(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])


def avg_vote(jd, col):
    """ Generate agreement by averaging votes
        :jd: Dataframe[candidate, topic_id, score]
        :return: Dataframe with aggreed judgement
    """
    def avg_votes(df):
        """ dummy """
        s = df[col].mean()
        rdf = df.tail(1).copy()
        rdf.score = s
        return rdf
    return pd.concat([avg_votes(g) for _, g in
                      jd.groupby(['candidate', 'topic_id'])])


def load_judgement(ljson):
    """ Load the raw dump of judgement in format of ljson

    :ljson: Judgement raw dump filename
    :returns: @todo

    """
    with open(ljson) as fin:
        df = pd.DataFrame.from_records([json.loads(l) for l in fin])
        df = expand_field(df, 'scores', 'topic_id', 'score')
        df = normalize(df)
        return df


def despammer(filename):
    """ return a function check whether the judge is blacklisted

    :filename: @todo
    :returns: @todo

    """
    with open(filename) as fin:
        blacklist = [l.strip() for l in fin]

    return lambda x: x not in blacklist


def normalize(judgement_df):
    """ Normalize the dataframe columns, e.g. time

    :judgement_df: The judgement_df need to normalize
    :returns: @todo

    """
    if judgement_df.created_at.dtype == str:
        judgement_df['created_at'] = judgement_df['created_at']\
            .apply(dateutil.parser.parse)
    judgement_df.sort(['created_at'], inplace=True)
    judgement_df.score = judgement_df.score.apply(int)
    judgement_df = judgement_df[judgement_df.score > 0]
    judgement_df.sort(['created_at'])
    judgement_df.drop_duplicates(['judge_id', 'candidate'],
                                 take_last=False, inplace=True)
    return judgement_df


def expand_field(df, fieldname, keyname, valname):
    """ Expand a field which is a dict()

    :df: The dataframe
    :fieldname: The name of field to expand
    :keyname: The column name used for keys in new DF
    :valname: the column name used for vals in new DF
    :returns: @todo

    """
    field_df = pd.concat([pd.DataFrame.from_records(
        [{keyname: k, valname: int(v)}], index=[ix])
        for (ix, cell) in df[fieldname].iteritems()
        for k, v in cell.iteritems()])
    newdf = pd.merge(df, field_df,
                     left_index=True, right_index=True,
                     how='right')
    del newdf[fieldname]
    return newdf


def cohen_kappa(jdf1, jdf2, join_on, field):
    """ Calculate the Cohen's kappa for inter-rater aggreement

        http://en.wikipedia.org/wiki/Cohen's_kappa

    :jdf1: the judgement from rater 1
    :jdf2: the judgement from rater 2
    :join_on: A list of columns used for joining two tables
    :field: The field holding judgement scores
    :returns: @todo

    """
    field_1 = field + '_1'
    field_2 = field + '_2'
    jpair = pd.merge(jdf1, jdf2,
                     left_on=join_on,
                     right_on=join_on,
                     how='inner', suffixes=['_1', '_2'])
    n = len(set(jpair[field_1] + jpair[field_2]))
    s = np.zeros((n, n))
    score_map = {s: p for p, s in enumerate(
        set(jpair[field_1].unique()) | set(jpair[field_2].unique()))}
    for _, p in jpair.iterrows():
        s[score_map[p[field_1]], score_map[p[field_2]]] += 1

    return _cohen_kappa(s), len(jpair)


def _cohen_kappa(mat):
    """ Calculate kappa from matrix

    :mat: @todo
    :returns: @todo

    """
    total = float(np.sum(mat.flatten()))
    Pa = np.trace(mat) / total
    Pu = np.sum(mat, axis=0) / total
    Pv = np.sum(mat, axis=1) / total
    Pe = np.dot(Pu, Pv)
    return (Pa - Pe) / (1 - Pe)
