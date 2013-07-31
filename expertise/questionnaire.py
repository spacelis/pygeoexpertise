#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: questionnaire.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: none
Description:
    Make data for questionnaires
"""

import sys
import json
import logging
import pandas as pd


def merge_expertise(expertise_file, ranking_file):
    """ Merge estimated expertise for each expert

    :expertise_file: csv_file for outputing users' expertise
    :ranking_file: csv_file storing the ranking lists
    :returns: None

    """
    EXPERTISE_SCHEMA = ['twitter_id', 'expertise',
                        'visit', 'cate_expert']

    def merge_topics(gdf):
        """Generating a json object for user's expertise

        :gdf: grouped DataFrame
        :returns: @todo

        """
        return [{'topic': t if 'poi' in d else 'places in category of ' + t,
                 'region': r, 'topic_id': d}
                for t, r, d in
                zip(gdf['topic'], gdf['region'], gdf['topic_id'])]

    def merge_region(gdf):
        """Generating a json object for user's expertise

        :gdf: grouped DataFrame
        :returns: @todo

        """
        return [{'region': t} for t in gdf['region'].unique()]

    rankings = pd.read_csv(ranking_file)
    rankings.drop_duplicates(cols=['user_screen_name', 'topic_id'],
                             inplace=True)
    ue_list = list()
    for expert, df in rankings.groupby('user_screen_name'):
        r = json.dumps(merge_region(df))
        ue_list.append((expert, json.dumps(merge_topics(df)), r, r))
    ue_df = pd.DataFrame(ue_list, columns=EXPERTISE_SCHEMA)
    ue_df.to_csv(expertise_file, index=False,
                 cols=EXPERTISE_SCHEMA, encoding='utf-8')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    merge_expertise(sys.argv[1], sys.argv[2])
