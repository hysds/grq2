#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import json
import requests
import sys
from elasticsearch import Elasticsearch

from grq2 import app
from grq2.lib.utils import parse_config


# get destination index and doctype
dest = sys.argv[1]
doctype = sys.argv[2]

# get connection and create destination index
es_url = app.config['ES_URL']
es = Elasticsearch(hosts=[es_url])
es.indices.create(index=dest, ignore=400)
