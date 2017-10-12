#!/usr/bin/env python
import sys, re, json, requests
from datetime import datetime

from grq2 import app


SENSING_RE = re.compile(r'(S1-.*?_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2}).*?orb)')


def getDatetimeFromString(dtStr, dayOnly=False):
    (year,month,day,hour,minute,second) = getTimeElementsFromString(dtStr)
    if dayOnly:
        return datetime(year=year, month=month, day=day)
    else:
        return datetime(year=year, month=month, day=day, hour=hour,
                        minute=minute, second=second)


def getTemporalSpanInDays(dt1, dt2):
    temporal_diff = getDatetimeFromString(dt1) - getDatetimeFromString(dt2)
    #print(temporal_diff.days)
    #print(temporal_diff.seconds)
    temporal_span = abs(temporal_diff.days)
    if abs(temporal_diff.seconds) >= 43200.:
        temporal_span += 1
    return temporal_span


def getTimeElementsFromString(dtStr):
    match = re.match(r'^(\d{4})[/-](\d{2})[/-](\d{2})[\s*T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?Z?$',dtStr)
    if match: (year,month,day,hour,minute,second) = map(int,match.groups())
    else:
        match = re.match(r'^(\d{4})[/-](\d{2})[/-](\d{2})$',dtStr)
        if match:
            (year,month,day) = map(int,match.groups())
            (hour,minute,second) = (0,0,0)
        else: raise(RuntimeError("Failed to recognize date format: %s" % dtStr))
    return (year,month,day,hour,minute,second)


def main():
    src = sys.argv[1]
    
    #id = "S1-IFG_RM_M1S1_TN120_20161101T232831-20161008T232803_s1-poeorb-7b8c-v1.1.2-standard"
    
    #match = SENSING_RE.search(id)
    #sensing_start, sensing_stop = sorted(["%s-%s-%sT%s:%s:%s" % match.groups()[1:7],
    #                                      "%s-%s-%sT%s:%s:%s" % match.groups()[7:]])
    #print(sensing_start, sensing_stop)
    
    # index all docs from source index to destination index
    query = {
      "fields": "temporal_span",
      "query": {
        "match_all": {}
      },
      "sort": [
          {   
              "creation_timestamp": {
                  "order": "asc"
              }
          }
      ]
    }
    #query = {
    #  "fields": "_source",
    #  "query": {
    #    "match_all": {}
    #  }
    #}
    es_url = app.config['ES_URL']
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
            print(json.dumps(hit, indent=2))
            id = hit['_id']
            old_span = hit['fields']['temporal_span'][0]
            print("old temporal span: %d" % old_span)
            match = SENSING_RE.search(hit['_id'])
            sensing_start, sensing_stop = sorted(["%s-%s-%sT%s:%s:%s" % match.groups()[1:7],
                                                  "%s-%s-%sT%s:%s:%s" % match.groups()[7:]])
            new_span = getTemporalSpanInDays(sensing_stop, sensing_start)
            print("new temporal span: %d" % new_span)

            if new_span == old_span:
                print("%s already fixed." % hit['_id'])
                continue

            # upsert new document
            new_doc = {
                "doc": { "temporal_span": new_span },
                "doc_as_upsert": True
            }
            r = requests.post('%s/%s/%s/%s/_update' % (es_url, src, hit['_type'], hit['_id']), data=json.dumps(new_doc))
            result = r.json()
            if r.status_code != 200:
                app.logger.debug("Failed to update user_tags for %s. Got status code %d:\n%s" % 
                                 (hit['_id'], r.status_code, json.dumps(result, indent=2)))
            r.raise_for_status()


if __name__ == "__main__": main()
