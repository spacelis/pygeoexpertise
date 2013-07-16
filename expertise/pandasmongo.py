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

import pandas as pd


def dotpath(obj, dpath):
    """ get value through dotpath
    """
    for n in dpath.split('.'):
        obj = obj.get(n)
    return obj


def getDataFrame(collection, query, projection):
    """ Return a pandas.DataFrame filled with data from query and use
        projection to flatten the data.
    """
    keys, vals = zip(*projection.iteritems())
    obj_list = list()
    for obj in collection.find(query, projection):
        obj_list.append([dotpath(obj, k) for k in keys])
    return pd.DataFrame(obj_list, columns=vals)

def appendToDataFrame(df, collection, query, projection):
    """ append new rows from query
    """
    ndf = getDataFrame(collection, query, projection)
    return pd.concat([df, ndf], ignore_index=True)
