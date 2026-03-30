#!/usr/bin/env python
import os
import json

from hysds.es_util import get_mozart_es
from grq2 import app


mozart_es = get_mozart_es()
HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']

# get doc type mapping
current_directory = os.path.dirname(__file__)
path = os.path.join(current_directory, '..', 'config', 'hysds_ios.mapping')
path = os.path.abspath(path)
path = os.path.normpath(path)
with open(path) as f:
    body = json.load(f)

    # create destination index
    mozart_es.es.indices.create(HYSDS_IOS_INDEX, body, ignore=400)
