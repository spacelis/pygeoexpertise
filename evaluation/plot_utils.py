#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: plot_utils.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
GitHub: http://github.com/spacelis
Description:
    Making plotting easier
"""

import pandas as pd
from collections import Counter


UNITS = {
    'Y': lambda x: x.strftime('%Y'),
    'M': lambda x: x.strftime('%Y-%m'),
    'D': lambda x: x.strftime('%Y-%m-%d'),
    'H': lambda x: x.strftime('%Y-%m-%d %H'),
    'm': lambda x: x.strftime('%Y-%m-%d %H:%M'),
    'S': lambda x: x.strftime('%Y-%m-%d %H:%M:%S')
}


def plot_timeline(s, unit, start=None, end=None, ax=None):
    """ Ploting the time binned histogram

    :s: a series with datetime as labels
    :unit: a unit applied for aggregation
    :start: the start of range to plot
    :end: the end of range to plot
    :returns: a plot of event frequence distribution

    """
    if not start:
        start = UNITS[unit](s.min())
    if not end:
        end = UNITS[unit](s.max())
    load = Counter(s.apply(UNITS[unit]))
    axis = pd.date_range(start, end, freq=unit)
    load = pd.Series([load[UNITS[unit](d)] for d in axis], index=axis)
    p = load.plot(ax=ax, kind='bar')
    return p
