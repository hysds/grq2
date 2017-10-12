#!/usr/bin/env python
import json, requests, sys
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
  "sort":[{"_timestamp":{"order":"desc"}}]
}
r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' % (es_url, src), data=json.dumps(query))
scan_result = r.json()
count = scan_result['hits']['total']
scroll_id = scan_result['_scroll_id']
results = []
while True:
    r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
    res = r.json()
    scroll_id = res['_scroll_id']
    if len(res['hits']['hits']) == 0: break
    for hit in res['hits']['hits']:
        doc = hit['_source']
        if 'metadata' not in doc: continue
        doc['dataset'] = "AIRX3SPD-daily-subset"
        doc['metadata']['data_product_name'] = "AIRX3SPD-daily-subset"
        doc['metadata']['temporal_resolution'] = "1 day"
        doc['metadata']['spatial_resolution'] = [
            "(100 extra pressure levels, 1 degree, 1 degree)",
            "(7 cloud phases, 1 degree, 1 degree)",
        ]
        ret = es.index(index=dest, doc_type=hit['_type'], id=hit['_id'], body=doc)
        print "indexed %s" % hit['_id']
