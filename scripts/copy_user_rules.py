#!/usr/bin/env python
import json, requests, sys

from grq2 import app
from grq2.lib.utils import parse_config


# get source and destination index
src = sys.argv[1]
dest = sys.argv[2]
mapping_file = sys.argv[3]
with open(mapping_file) as f:
    mapping = f.read()

# get connection and create destination index
es_url = app.config['ES_URL']
r = requests.put("%s/%s" % (es_url, dest), data=mapping)

# add all mappings from GRQ product indexes using alias
grq_index = app.config['GRQ_INDEX']
r = requests.get("%s/%s/_mapping" % (es_url, grq_index))
r.raise_for_status()
mappings = r.json()
for idx in mappings:
    for doc_type in mappings[idx]['mappings']:
        r = requests.put("%s/%s/_mapping/%s" % (es_url, dest, doc_type),
                         data=json.dumps(mappings[idx]['mappings'][doc_type]))
        r.raise_for_status()

# index all docs from source index to destination index
query = {
  "fields": "_source",
  "query": {
    "match_all": {}
  }
}
r = requests.post('%s/%s/.percolator/_search?search_type=scan&scroll=60m&size=100' % (es_url, src), data=json.dumps(query))
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
        #doc['query_string'] = doc['query_string'].replace('spacecraftName', 'platform')
        #doc['query'] = json.loads(doc['query_string'])
        #conn.index(hit['_source'], dest, '.percolator', hit['_id'])
        r = requests.post("%s/%s/.percolator/" % (es_url, dest), data=json.dumps(doc))
        result = r.json()
        if r.status_code != 201:
            print "Failed to insert rule: %s" % json.dumps(doc, indent=2)
            continue
        
        print "indexed %s" % hit['_id']
