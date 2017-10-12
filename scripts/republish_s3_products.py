#!/usr/bin/env python
import json, requests, sys, os
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from hysds.orchestrator import submit_job

from grq2 import app
from grq2.lib.utils import parse_config


# get source and destination index
src = "grq_v02_wvcc_merged_data"

# bucket
bucket_name = "wvcc-dataset-bucket"

# region
region = "us-east-1"

# get s3 connection
s3_conn = S3Connection()
bucket = s3_conn.get_bucket(bucket_name)

# get connection and create destination index
es_url = app.config['ES_URL']

# index all docs from source index to destination index
query = {
    "query": {
        "query_string": {
            "query": "\"%s\"" % bucket_name
        }
    },
    "fields": [ "_id", "urls" ]
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
        doc = hit['fields']
        prefix = "%s/" % doc['urls'][0].replace('http://%s.s3-website-%s.amazonaws.com/' % (bucket_name, region), '')
        print doc['_id'], prefix
        localize_urls = []
        for i in bucket.list(prefix):
            #localize_urls.append({ 'url': 's3://%s/%s' % (bucket_name, i.name), 'local_path': '%s/' % os.path.basename(prefix[0:-1]) })
            localize_urls.append({ 'url': 'http://%s.s3-website-%s.amazonaws.com/%s' % (bucket_name, region, i.name), 'local_path': '%s/' % os.path.basename(prefix[0:-1]) })
        payload = {
            "job_type": "job:ingest_dataset",
            "payload": {
                "dataset": doc['_id'],
                "dataset_urls": localize_urls
            }
        }
        #print json.dumps(payload, indent=2)
        submit_job.apply_async((payload,), queue="jobs_processed")
        #sys.exit()
