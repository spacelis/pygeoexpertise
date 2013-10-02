#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: grank2rank.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Convert pandas.Datafram.csv to rank result file
"""

import sys
import os
import re
import pandas as pd
from tempfile import NamedTemporaryFile
from subprocess import check_output
from itertools import groupby

SEP = re.compile(r'\s+')
TREC_EVAL_CMD = os.path.join(os.path.dirname(__file__), 'trec_eval')
TREC_EVAL_M = ['P.5', 'map']


def output2dict(output, method, profile):
    """@todo: Docstring for output2dict.

    :output: @todo
    :returns: @todo

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
        yield row


def multi_trec_eval(qrel, rfile):
    """ Convert ranking from Dataframe to TREC result

    :rfile: @todo
    :returns: @todo

    """
    evalres = list()
    for _, group in pd.read_csv(rfile).\
            groupby(['rank_method', 'profile_type']):
        with NamedTemporaryFile(delete=True) as fout:
            for row in group.iterrows():
                r = row[1]
                print >> fout, r['topic_id'], 'Q0', r['user_screen_name'], \
                    r['rank'], r['score'], \
                    r['rank_method'] + "_" + r['profile_type']

            fout.flush()
            scores = output2dict(
                check_output([TREC_EVAL_CMD, '-q']
                             + ['-m' + m for m in TREC_EVAL_M]
                             + [qrel, fout.name]),
                r['rank_method'],
                r['profile_type'])
        evalres.append(pd.DataFrame
                       .from_records(list(scores))
                       .set_index('_method', '_profile', '_topic'))

    erdf = pd.concat(evalres)
    erdf.to_csv(sys.stdout, index=False)


if __name__ == '__main__':
    multi_trec_eval(sys.argv[1], sys.argv[2])
