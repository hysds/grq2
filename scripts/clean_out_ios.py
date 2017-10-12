#!/usr/bin/env python
import json, requests, sys

from grq2 import app


# get connection and create destination index
es_url = app.config['ES_URL']

# index all docs from source index to destination index
query = {
  "fields": "_source",
  "query": {
    "match_all": {}
  },
  "sort": [{"_id":{"order":"asc"}}]
}
r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' % (es_url, "hysds_ios"), data=json.dumps(query))
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
        ident = hit["_id"]
        yn = raw_input("Do you want to delete: %s " % ident)
        if yn == "y":
            sure = raw_input("Are you sure? ID: %s " % ident)
            if not sure.startswith("y") and sure != "":
                print "Skipping: %s" % ident
                continue
            r = requests.delete("%s/hysds_ios/hysds_io/%s" % (es_url,ident)) 
            r.raise_for_status()
            print "Deleted: %s" % ident 
