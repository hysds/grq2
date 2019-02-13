#!/usr/bin/env python
import json
import requests
import sys
from elasticsearch import Elasticsearch

from grq2 import app
from grq2.lib.utils import parse_config


# get source and destination index
src = sys.argv[1]
dest = sys.argv[2]
doctype = sys.argv[3]

# get connection and create destination index
es_url = app.config['ES_URL']
es = Elasticsearch(hosts=[es_url])

# index all docs from source index to destination index
query = {
    "fields": "_source",
    "query": {
        "match_all": {}
    },
    "sort": [{"_timestamp": {"order": "desc"}}]
}
r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' %
                  (es_url, src), data=json.dumps(query))
scan_result = r.json()
count = scan_result['hits']['total']
scroll_id = scan_result['_scroll_id']
results = []
while True:
    r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
    res = r.json()
    scroll_id = res['_scroll_id']
    if len(res['hits']['hits']) == 0:
        break
    for hit in res['hits']['hits']:
        doc = hit['_source']
        if 'metadata' not in doc:
            continue
        doc['metadata']['resolution'] = [
            "(1 day, 40 standard pressure levels, .07 degree, .07 degree)",
        ]
        doc['metadata']['coordinates'] = [
            "(time, level, lat, lon)",
        ]
        doc['metadata']['dimensions'] = 4
        ret = es.index(
            index=dest, doc_type=hit['_type'], id=hit['_id'], body=doc)
        print(("indexed %s" % hit['_id']))
