#!/usr/bin/env python
from future import standard_library
standard_library.install_aliases()

import json
import requests
import sys

from grq2 import app
# from grq2.lib.utils import parse_config


# get source and destination index
src = sys.argv[1]
dest = sys.argv[2]
mapping_file = sys.argv[3]
with open(mapping_file) as f:
    mapping = f.read()

# get connection and create destination index
es_url = app.config['ES_URL']
r = requests.put("{}/{}".format(es_url, dest), data=mapping)

# add all mappings from GRQ product indexes using alias
grq_index = app.config['GRQ_INDEX']
r = requests.get("{}/{}/_mapping".format(es_url, grq_index))
r.raise_for_status()
mappings = r.json()
for idx in mappings:
    r = requests.put("{}/{}/_mapping".format(es_url, dest), data=json.dumps(mappings[idx]['mappings']))
    r.raise_for_status()

# index all docs from source index to destination index
query = {
    "fields": "_source",
    "query": {
        "match_all": {}
    }
}

scroll_percolator_url = '{}/{}/.percolator/_search?search_type=scan&scroll=60m&size=100'.format(es_url, src)
r = requests.post(scroll_percolator_url, data=json.dumps(query))
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
        r = requests.post("{}/{}/.percolator/".format(es_url, dest), data=json.dumps(doc))
        result = r.json()
        if r.status_code != 201:
            print("Failed to insert rule: %s" % json.dumps(doc, indent=2))
            continue

        print("indexed %s" % hit['_id'])
