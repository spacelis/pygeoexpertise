#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: grank2rank.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Evaluation based on trec_eval for expert retrieval
"""

import sys
import os
import re
import pandas as pd
from tempfile import NamedTemporaryFile
from subprocess import check_output
from itertools import groupby

SEP = re.compile(r'\s+')
TREC_EVAL_CMD = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'trec_eval')
if os.path.exists(TREC_EVAL_CMD):
    print >> sys.stderr, 'trec_eval = ' + TREC_EVAL_CMD
else:
    print >> sys.stderr, 'Error: trec_eval Not Found'
    sys.exit(-1)
TREC_EVAL_M = ['P.5', 'map']


def output2dict(output, method, profile):
    """ Parsing trec_eval output into rows

    :output: The output string from trec_eval
    :method: The method tagged to this output evaluation
    :profile: The profile tagged to this output evaluation
    :returns: A generator of rows in dict()

    """
    def gktv():
        """ generator for k,v from line"""
        for line in output.split('\n'):
            if len(line) == 0:
                continue
            k, t, v = SEP.split(line.strip())
            yield k, t, v
    for t, g in groupby(gktv(), key=lambda x: x[1]):
        row = {k: v for k, _, v in g}
        row['_method'] = method
        row['_profile'] = profile
        row['_topic'] = t
        row['_topic_type'] = t.split('-')[0]
        yield row


def multi_trec_eval(qrel, rankings, eval_methods=None):
    """ Running the trec_eval against rankres generated by ger.py

    :qrel: The path to qrel file used by trec_eval
    :rankres: The ranking list in DataFrame generated by ger.py
    :returns: None

    """
    if eval_methods is None:
        eval_methods = TREC_EVAL_M
    def evalmethod(df):
        """ evaluating one method """
        with NamedTemporaryFile(delete=True) as fout:
            for _, r in df.iterrows():
                print >> fout, r['topic_id'], 'Q0', r['candidate'], \
                    r['rank'], r['score'], \
                    r['rank_method'] + "_" + r['profile_type']

            fout.flush()
            scores = output2dict(
                check_output([TREC_EVAL_CMD, '-q']
                             + ['-m' + m for m in eval_methods]
                             + [qrel, fout.name]),
                df['rank_method'].iat[0], df['profile_type'].iat[0])
        return pd.DataFrame.from_records(list(scores))

    return rankings[['topic_id', 'candidate', 'rank', 'score', 'rank_method', 'profile_type']]\
        .groupby(['rank_method', 'profile_type']).apply(evalmethod)


def main():
    """ main
    :returns: @todo

    """
    qrel = sys.argv[1]
    rankres = sys.argv[2]
    include_topic = None
    if len(sys.argv) > 3:
        with open(sys.argv[3]) as fin:
            ts = {'topic_id': [l.strip() for l in fin]}
            include_topic = pd.DataFrame(ts)
    assert os.path.exists(qrel)
    assert os.path.exists(rankres)
    evalres = multi_trec_eval(qrel, rankres)
    if include_topic:
        pd.merge(evalres, include_topic,
                 left_on='_topic', right_on='topic_id',
                 how='inner').to_csv(sys.stdout, index=False)
    else:
        multi_trec_eval(qrel, rankres).to_csv(sys.stdout, index=False)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print >> sys.stderr, \
            'Usage: mtrec_eval <qrel> <rankres.csv> [<topics>]'
        sys.exit(-1)
    main()
