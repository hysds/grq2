#!/usr/bin/env python
import json, requests, sys
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
