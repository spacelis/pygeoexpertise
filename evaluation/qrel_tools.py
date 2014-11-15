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


import csv
import dateutil
from collections import Counter
from collections import defaultdict

import pandas as pd
import numpy as np
from numpy import dtype
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


def merge_votes(jd, col, method='avg', indexedby=None):
    """ Merge multiple vote on one item by avg or mode
        :jd: Dataframe[candidate, topic_id, score]
        :method: the method used for merge
            'avg' is for averaging the scores per item
            'mode' is for use the most frequent vote per item
        :indexedby: (default=['candidate', 'topic_id'])
                  The columns labeling scores
        :return: Dataframe with aggreed judgement
    """
    def avg_merge(df):
        """ dummy """
        return pd.Series(df[col].apply(float).mean())

    def mode_merge(df):
        return pd.Series(Counter(df[col].apply(float).values).most_common(1)[0][0])

    if not indexedby:
        indexedby = ['candidate', 'topic_id']
    return jd.groupby(indexedby).apply(locals()[method + '_merge']).rename(columns={0: col})


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
    if judgement_df.created_at.dtype != dtype('<M8[ns]'):
        judgement_df['created_at'] = judgement_df['created_at']\
            .apply(dateutil.parser.parse)
    judgement_df.score = judgement_df.score.apply(int)
    judgement_df.sort(['created_at'], inplace=True)
    return judgement_df


def distill(judgement_df):
    """ Remove scores that are below 0

    :judgement_df: The dataframe containing judgement
    :returns: @todo

    """
    judgement_df = judgement_df[judgement_df.score > 0]
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
    if isinstance(df[fieldname][0], dict):
        celliter = lambda cell: cell.iteritems()
    else:
        celliter = lambda cell: json.loads(cell).iteritems()
    field_df = pd.concat([pd.DataFrame.from_records(
        [{keyname: k, valname: int(v)}], index=[ix])
        for (ix, cell) in df[fieldname].iteritems()
        for k, v in celliter(cell)])
    newdf = pd.merge(df, field_df,
                     left_index=True, right_index=True,
                     how='right')
    del newdf[fieldname]
    return newdf


def cohen_kappa_score(arr1, arr2, extra_info=False):
    """ Calculate cohen_kappa from two array

    :arr1: The first array of scores
    :arr2: The second array of scores
    :extra_info: (default: False) Whether return more info and pack as a tuple
    :returns: kappa [, (total, Pa, Pe) ]

    """
    score_map = {s: p for p, s in enumerate(set(list(arr1) + list(arr2)))}
    dist = np.zeros((len(score_map), len(score_map)))
    for x, y in zip(arr1, arr2):
        dist[score_map[x], score_map[y]] += 1

    total = float(np.sum(dist.flatten()))
    Pa = np.trace(dist) / total
    Pu = np.sum(dist, axis=0) / total
    Pv = np.sum(dist, axis=1) / total
    Pe = np.dot(Pu, Pv)
    if extra_info:
        return (Pa - Pe) / (1 - Pe), (total, Pa, Pe)
    else:
        return (Pa - Pe) / (1 - Pe)


def cohen_kappa_df(jdf1, jdf2, join_on, field):
    """ Calculate the Cohen's kappa for inter-rater aggreement

        http://en.wikipedia.org/wiki/Cohen's_kappa

    :jdf1: the judgement from rater 1
    :jdf2: the judgement from rater 2
    :join_on: A list of columns used for joining two tables
    :field: The field holding judgement scores
    :returns: Cohen Kappa of interrater judgement

    """
    field_1 = field + '_1'
    field_2 = field + '_2'
    jpair = pd.merge(jdf1, jdf2,
                     left_on=join_on,
                     right_on=join_on,
                     how='inner', suffixes=['_1', '_2'])
    return cohen_kappa_score(jpair[field_1].values,
                             jpair[field_2].values)


def csv_zip_rows(csv_file, groupers):
    """ Zip each pair of consequent rows into a fixed schema by
        grouping dynamic headers into json objects

    :csv_file: @todo
    :groupers: @todo
    :returns: @todo

    """
    def ingrouper(k):
        """ embedded func for checking whether a key is in a group"""
        for g, gf in groupers.iteritems():
            if gf(k):
                return g
        return None

    with open(csv_file, 'rb') as fin:
        r = csv.reader(fin)
        try:
            while True:
                h = r.next()
                l = r.next()
                jdict = defaultdict(dict)
                for k, v in zip(h, l):
                    g = ingrouper(k)
                    if g:
                        jdict[g][k] = v
                    else:
                        jdict[k] = v
                yield jdict
        except StopIteration:
            pass
