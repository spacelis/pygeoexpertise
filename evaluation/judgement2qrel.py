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


def conv(jfile):
    """Convert judgement.ljson into qrel and print it to stdout

    :jfile: judgement.ljson filename
    :returns: None

    """
    with open(jfile) as fin:
        for line in fin:
            jdg = json.loads(line)
            scores = jdg['scores']
            for s in scores:
                score = int(scores[s]) - 1
                if score >= 0:
                    print s, 'Q0', jdg['candidate'], score


if __name__ == '__main__':
    conv(sys.argv[1])
