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
import pandas as pd


from qrel_tools import take_most_freq, take_avg, normalize, expand_field


def toQrel(judgement_df):
    """ Convert the judgement_df into qrel file

    :judgement_df: @todo
    :returns: @todo

    """
    for _, (topic_id, score, candidate) in \
            judgement_df[['topic_id', 'score', 'candidate']].iterrows():
        score = int(score)
        print topic_id, 'Q0', candidate, score


def select(df, tfile):
    """ Select a part of judgement to be output

    :df: @todo
    :tfile: topics csv filename
    :returns: @todo

    """
    topics = pd.read_csv(tfile)
    df = pd.merge(df, topics,
                  left_on='topic_id', right_on='topic_id',
                  how='left', suffixes=('', '_t'))
    # FIXME make the condition generic from parameters
    selected = df[df['group'] == 0]
    return selected


def transform(jfile):
    """Convert judgement.ljson into qrel and print it to stdout

    :jfile: judgement.ljson filename
    :returns: None

    """
    take_last = False
    with open(jfile) as fin:
        judgement = pd.DataFrame.from_records(
            [json.loads(line) for line in fin])
        judgement = expand_field(judgement, 'scores', 'topic_id', 'score')
        normalize(judgement)
        print >> sys.stderr, 'Drop Duplication TAKE_LAST =', take_last
        aggrement = take_avg(
            judgement[['topic_id', 'candidate', 'score']], 'score')
        toQrel(aggrement)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: judgement2qrel <jsonfile> <topics>'
    transform(sys.argv[1])
