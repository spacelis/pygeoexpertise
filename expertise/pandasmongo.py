#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: pandasmongo.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
    Make DataFrame from mongodb
"""

import re
import types
import pandas as pd


class DotPathEvaluator(object):

    """ Evaluating the dot path on a json object."""

    INT = re.compile('^[0-9]+$')

    def __init__(self, path):
        """ Initialize the object with the path.

        :path: @todo

        """
        self._path = path
        peval = eval('lambda self, x: x' + ''.join(
            [("[%d]" % (int(k), ))
             if DotPathEvaluator.INT.match(k)
             else "['%s']" % (k, )
             for k in path.split('.')]))
        self.extract = types.MethodType(peval, self, self.__class__)

    def __str__(self):
        """ __str__
        :returns: The dotpath of this evaluator

        """
        return self._path


def getDataFrame(collection, query, projection):
    """ Return a pandas.DataFrame filled with data from query and use
        projection to flatten the data.
    """
    keys, vals = zip(*projection.iteritems())
    keyevals = [DotPathEvaluator(k) for k in keys]
    obj_list = list()
    for obj in collection.find(query):
        obj_list.append([ke.extract(obj) for ke in keyevals])
    return pd.DataFrame(obj_list, columns=vals)


def appendToDataFrame(df, collection, query, projection):
    """ append new rows from query
    """
    ndf = getDataFrame(collection, query, projection)
    return pd.concat([df, ndf], ignore_index=True)
