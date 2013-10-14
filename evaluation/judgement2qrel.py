#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: judgement2qrel.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Covert judgemt.ljson to qrel for evaluating by trec_eval
"""

import sys
import json
import dateutil
import pandas as pd


from anno_merge import max_vote_agreement


def toQrel(judgement_df):
    """ Convert the judgement_df into qrel file

    :judgement_df: @todo
    :returns: @todo

    """
    for _, (topic_id, score, candidate) in \
            judgement_df[['topic_id', 'score', 'candidate']].iterrows():
        score = int(score)
        if score >= 0:
            print topic_id, 'Q0', candidate, score


def normalize(judgement_df):
    """ Normalize the dataframe column

    :judgement_df: The judgement_df need to normalize
    :returns: @todo

    """
    judgement_df['created_at'] = judgement_df['created_at']\
        .apply(dateutil.parser.parse)


def expand_field(df, fieldname, keyname, valname):
    """ Expand a field which is a dict()

    :df: The dataframe
    :fieldname: The name of field to expand
    :keyname: The column name used for keys in new DF
    :valname: the column name used for vals in new DF
    :returns: @todo

    """
    field_df = pd.concat([pd.DataFrame.from_records(
        [{keyname: k, valname: v}], index=[ix])
        for (ix, cell) in df[fieldname].iteritems()
        for k, v in cell.iteritems()])
    newdf = pd.merge(df, field_df,
                     left_index=True, right_index=True,
                     how='right')
    return newdf


def transform(jfile, tfile):
    """Convert judgement.ljson into qrel and print it to stdout

    :jfile: judgement.ljson filename
    :tfile: topics csv filename
    :returns: None

    """
    topics = pd.read_csv(tfile)
    with open(jfile) as fin:
        judgement = pd.DataFrame.from_records(
            [json.loads(line) for line in fin])
        normalize(judgement)
        judgement.drop_duplicates(['judge_id', 'candidate'])
        judgement = expand_field(judgement, 'scores', 'topic_id', 'score')
        judgement = pd.merge(judgement, topics,
                             left_on='topic_id', right_on='topic_id',
                             how='left', suffixes=('', '_t'))
        # FIXME make the condition generic from parameters
        selected = judgement[judgement['group'] == 0]
        aggrement = max_vote_agreement(
            selected[['topic_id', 'candidate', 'score']])
        toQrel(aggrement)


if __name__ == '__main__':
    transform(sys.argv[1], sys.argv[2])
