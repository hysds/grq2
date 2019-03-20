#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import input
from future import standard_library
standard_library.install_aliases()
import json
import requests
import sys

from grq2 import app


# get connection and create destination index
es_url = app.config['ES_URL']

# index all docs from source index to destination index
query = {
    "fields": "_source",
    "query": {
        "match_all": {}
    },
    "sort": [{"_id": {"order": "asc"}}]
}
r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' %
                  (es_url, "hysds_ios"), data=json.dumps(query))
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
        ident = hit["_id"]
        yn = eval(input("Do you want to delete: %s " % ident))
        if yn == "y":
            sure = eval(input("Are you sure? ID: %s " % ident))
            if not sure.startswith("y") and sure != "":
                print(("Skipping: %s" % ident))
                continue
            r = requests.delete("%s/hysds_ios/hysds_io/%s" % (es_url, ident))
            r.raise_for_status()
            print(("Deleted: %s" % ident))
