#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: sendsurvey.py
Author: SpaceLis
Email: Wen.Li@tudelft.nl
Github: http://github.com/spacelis
Description:
    Sending out tweets advertising the survay based on Google Forms.
    The input file is a csv file with first column as users' screen names and
    the second column as id of the form the users are expected to fill in.
    For example,
        "spacelis", "1234567689abcdef"
        "wenli","1234567689abcdef"
"""

import re
import sys
import csv
import random
import yaml
import twitter
import argparse
import time


TEMPLATE = [
    #'@%(twitter_id)s Hi, would you like to support a PhD project by doing a survey? %(link)s',
    #'@%(twitter_id)s Science needs you! Plz help us with our survey. %(link)s',
    '@%(twitter_id)s Could you help our non-profit project on geographic information retrieval. %(link)s',
    '@%(twitter_id)s Could you join us in our non-profit study on perception of Place of Interest annotation. %(link)s',
    #'@%(twitter_id)s You have done a LOOOOT of checkins, would you like to help us by doing a survey? %(link)s',
    #'@%(twitter_id)s Hey, you seems an expert knowing your neighborhood. Do you want to help us with a survey? %(link)s'
    '@%(twitter_id)s Hey, you seem an expert of your neighborhood. Could you help our non-profit project? %(link)s'
    ]


HLINK = re.compile('https?://[a-z\-_./]+[a-zA-Z0-9\-\.\?\,\'\/\\\+&amp;%\$#_=]*')


def count_char(txt):
    """ Count the char number in twitter

    :txt: @todo
    :returns: @todo

    """
    return len(HLINK.subn('http://t.co/xxxxxxxxxx', txt)[0])


def getFormURL(form_id):
    """ Return a form URL based on form_id
    """
    return 'https://docs.google.com/forms/d/%s/viewform' % (form_id, )


def sendtweet(api, template, para_list, dryrun=True, interval=60):
    """ Sending out tweets
    """
    for p in para_list:
        txt = template[random.randint(0, len(template) - 1)] % p
        if not dryrun:
            #api.PostDirectMessage(txt, screen_name=p['twitter_id'])
            try:
                api.PostUpdate(txt)
                print 'Sent to', p['twitter_id']
            except twitter.TwitterError:
                print 'Fail at', p['twitter_id']
                print >> sys.stderr, p['twitter_id'] + ',' + p['link']
        else:
            print count_char(txt), txt
        intv = random.gauss(interval, 10)
        if not dryrun:
            time.sleep(intv if intv > 0 else 10)
        else:
            print 'sleep', intv


def distribute(csvfile, dryrun=True, interval=360):
    """ docstring for distribute
    """
    with open('cred.yaml') as fin:
        api = twitter.Api(**yaml.load(fin))
    with open(csvfile) as fin:
        reader = csv.reader(fin)
        para_list = [{'twitter_id': p[0], 'link': getFormURL(p[1])}
                     for p in reader]
        sendtweet(api, TEMPLATE, para_list, dryrun=dryrun, interval=interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sending out form. '
                                     'Using -f to force send out tweets.')
    parser.add_argument('-f', dest='force', action='store_true', default=False,
                        help='Force sending out tweets without dryrun.')
    parser.add_argument('source', metavar='FILE', nargs=1, help='Input file.')
    args = parser.parse_args()
    distribute(args.source[0], not args.force)
