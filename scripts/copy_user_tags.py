#!/usr/bin/env python
import json, requests, sys
from pprint import pprint

from grq2 import app
from grq2.lib.utils import parse_config


# get source and destination index
src = sys.argv[1]
dest = sys.argv[2]
doc_type = sys.argv[3]

# get url
es_url = app.config['ES_URL']

# index all docs from source index to destination index
query = {
  "query": {
    "match_all": {}
  }
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
        user_tags = doc['metadata'].get('user_tags', None)
        if user_tags is None: continue

        # upsert new document
        new_doc = {
            "doc": { "metadata": { "user_tags": user_tags } },
            "doc_as_upsert": True
        }
        r = requests.post('%s/%s/%s/%s/_update' % (es_url, dest, doc_type, hit['_id']), data=json.dumps(new_doc))
        result = r.json()
        if r.status_code != 200:
            app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" % 
                             (id, r.status_code, json.dumps(result, indent=2)))
        r.raise_for_status()
