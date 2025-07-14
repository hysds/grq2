#!/usr/bin/env python
from future import standard_library
standard_library.install_aliases()

import os
import json

from hysds.es_util import get_mozart_es
from grq2 import app


mozart_es = get_mozart_es()
HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']

# get doc type mapping
path = os.path.join(app.root_path, '..', 'config', 'hysds_ios.mapping')
with open(path) as f:
    body = json.load(f)

    # create destination index
    mozart_es.es.indices.create(HYSDS_IOS_INDEX, body, ignore=400)
