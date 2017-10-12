#!/usr/bin/env python
import json, requests
import sys

def migrate(index_name):
    src = index_name
    dest = index_name[0:index_name.index('_update')]
    #index sizes should be the same on both indices before copying. If not, error out.
    r = requests.get('http://localhost:9200/%s/_count' % src)
    src_count = r.json()['count']
    
    r = requests.get('http://localhost:9200/%s/_count' % dest)
    dest_count = r.json()['count']    
    
    if src_count != dest_count:
        print 'ERROR: index %s (%d) does not appear to match index %s (%d)' % (src, src_count, dest, dest_count)
        sys.exit(1)
    
    # index all docs from source index to destination index
    query = {
      "fields": "_source",
      "query": {
        "match_all": {}
      }
    }
    r = requests.post('http://localhost:9200/%s/_search?search_type=scan&scroll=60m&size=100' % src, data=json.dumps(query))
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    results = []
    while True:
        r = requests.post('http://localhost:9200/_search/scroll?scroll=60m', data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            doc = hit['_source']
            # conn.index(hit['_source'], dest, hit['_type'], hit['_id'])
            post_request = 'http://localhost:9200/%s/%s/%s' % (dest, hit['_type'], hit['_id'])
            r = requests.post(post_request, data=json.dumps(hit['_source']))
            if r.status_code == 200:
                print "INFO: indexed \"%s\" on \"%s\"" % (hit['_id'], dest)
            else:
                print 'ERROR: post request returned status code %d: %s' % (r.status_code, post_request)
                print(r.json())
                sys.exit(1)

# Query for all indices that end with _update      
r = requests.get('http://localhost:9200/*_update/_stats')
result = r.json()
for index in result['indices'].keys():
    migrate(index)

