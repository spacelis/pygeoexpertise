# PyGeoexpert

This repository contains tools and scripts for analyzing users' geoexpertise on social media platforms.

The core package is expertise containing:

 - topics.py is for generating topics via stratified sampling
 - ger.py contains ranking algorithms for geoexpertise
 - questionnaire.py generates a csv file that can be used for populating self-evaluating questionnaires for geoexpert candidate.
 - QuestionnaireBot.gs is a script for Google's spreadsheet which create self-evaluating questionnaires via Google Forms
 - crowdsource.py provides functions for generating CSV files to feed the geoexpertise annotating platform

The scripts/modules in the evaluation directory are used for evaluating the algorithms for ranking geoexperts.

The script sendsurvey.py is used for sending out tweets to notify our candidates to evaluate our ranking.


# LICENSE

The MIT License (MIT)

Copyright (c) 2014 Wen Li

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
