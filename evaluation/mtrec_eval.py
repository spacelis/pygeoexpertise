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

SEP = re.compile(r'\s+')
TREC_EVAL_CMD = os.path.join(os.path.dirname(__file__), 'trec_eval')


def output2dict(output):
    """@todo: Docstring for output2dict.

    :output: @todo
    :returns: @todo

    """
    def g():
        """ generator for k,v from line"""
        for line in output.split('\n'):
            if len(line) == 0:
                continue
            k, _, v = SEP.split(line.strip())
            yield k, v
    return {k: v for k, v in g()}


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
                check_output([TREC_EVAL_CMD, qrel, fout.name]))
        evalres.append(pd.DataFrame(scores, index=[scores['runid']]))

    erdf = pd.concat(evalres)
    erdf.to_csv(sys.stdout)


if __name__ == '__main__':
    multi_trec_eval(sys.argv[1], sys.argv[2])
