#!/usr/bin/env python
import json, requests

import boto3
from urlparse import urlparse
import sys

from elasticsearch import Elasticsearch

from grq2 import app
from grq2.lib.utils import parse_config

file = open(sys.argv[1], 'w')
client = boto3.client('s3')

def move_s3_files(url, target_path):
    parsed_url = urlparse(url)
    bucket = parsed_url.hostname.split('.', 1)[0]
    results = client.list_objects(Bucket=bucket, Delimiter='/', Prefix=parsed_url.path[1:] + '/')
        
    if results.get('Contents'):
        for result in results.get('Contents'):
            file_url = parsed_url.scheme + "://" + parsed_url.hostname + '/' + result.get('Key')
            filename = result.get('Key').split('/')[-1]
            print 'INFO: Copying object \"%s\" to \"%s\"' % (file_url, target_path + '/' + filename)
            copy_source = bucket + '/' + result.get('Key')
            key = target_path + '/' + filename
            r = client.copy_object(Bucket=bucket, CopySource=copy_source, Key=key)
            if r['ResponseMetadata']['HTTPStatusCode'] != 200:
                print 'ERROR: %d' % r['ResponseMetadata']['HTTPStatusCode']
                print 'ERROR: Problem occured copying object \"%s\"' % file_url
                print 'ERROR: CopySource=%s, Key=%s' % (copy_source, key)
                sys.exit(1)
            else:
                if r.has_key('Error'):
                    print 'ERROR: Problem occured copying object \"%s\"' % file_url
                    print 'ERROR: CopySource=%s, Key=%s' % (copy_source, key)
                    print 'ERROR: %s: %s' % (r['Error']['Code'], r['Error']['Message'])
                    sys.exit(1) 
                else:
                    # Delete object here?
                    file.write(file_url + "\n")
    
    if results.get('CommonPrefixes'):
        for result in results.get('CommonPrefixes'):
            # Prefix values have a trailing '/'. Let's remove it to be consistent with our dir urls
            folder = parsed_url.scheme + "://" + parsed_url.hostname + '/' + result.get('Prefix')[:-1]
            # Get the sub-directory name
            sub_directory = result.get('Prefix')[:-1].split('/')[-1]
            updated_target_path = target_path + '/' + sub_directory
            move_s3_files(folder, updated_target_path)
         
def create_index(index, doctype):

    # get connection and create index
    es_url = 'http://localhost:9200'
    es = Elasticsearch(hosts=[es_url])
    es.indices.create(index, ignore=400)


url_keys = ['urls', 'browse_urls']

# query
r = requests.get('http://localhost:9200/grq_aria/_search?search_type=scan&scroll=10m&size=100')
if r.status_code != 200:
    app.logger.debug("Failed to query ES. Got status code %d:\n%s" % (r.status_code, json.dumps(r.json(), indent=2)))
    r.raise_for_status()
    #app.logger.debug("result: %s" % pformat(r.json()))

scan_result = r.json()
scroll_id = scan_result['_scroll_id']
while (True):
    r = requests.post('http://localhost:9200/_search/scroll?scroll=10m', data=scroll_id)
    datasets = r.json()
    scroll_id = datasets['_scroll_id']
    if len(datasets['hits']['hits']) == 0:
        file.close() 
        break
    for hit in datasets['hits']['hits']:
        updated_urls = {}
        updated_doc = hit
        for url_key in url_keys:
            new_urls = []
            for url in updated_doc['_source'][url_key]:
                if 's3-browse.jpl.nasa.gov' in url:
                    new_urls.append(url)
                else:
                    parsed_url = urlparse(url)
                    if (parsed_url.scheme == 's3') or ('hysds-aria-products.s3' in parsed_url.hostname):
                        date = updated_doc['_source']['starttime'].split('T')[0].split('-')  
                        path = (updated_doc['_source']['dataset_type'] + '/'
                                + updated_doc['_source']['system_version']
                                + '/' + date[0] + '/' + date[1] + '/' + date[2] + '/'
                                + updated_doc['_source']['id'])
                        if parsed_url.scheme == 's3':
                            # Parses out the date field from the timestamp and tokenizes it into year, month, day                  
                            new_urls.append(parsed_url.scheme + "://" + parsed_url.hostname + '/' + path)
                        elif 'hysds-aria-products.s3' in parsed_url.hostname:
                            new_location = parsed_url.scheme + "://" + parsed_url.hostname + '/' + path
                            if url != new_location:
                                print 'INFO: New target path: %s' % path
                                move_s3_files(url, path)
                                new_urls.append(new_location)
                            else:
                                print 'INFO: URL appears to already conform to correct naming convention: %s. Will not move.' % url
                                new_urls.append(url)
            if new_urls:
                updated_urls[url_key] = new_urls
        
        if updated_urls:
            # sort the urls first before updating the document
            # Add s3-browse link to the end of the list
            for url_key in updated_urls:
                sorted_urls = []
                s3_browse_url = None
                for url in updated_urls[url_key]:
                    if 's3-browse.jpl.nasa.gov' in url:
                        s3_browse_url = url
                    else:
                        sorted_urls.append(url)
                if s3_browse_url is not None:
                    sorted_urls.append(s3_browse_url)
                updated_doc['_source'][url_key] = sorted_urls
        
            updated_index = updated_doc['_index'] + '_update'
            create_index(updated_index, updated_doc['_type'])
            post_request = 'http://localhost:9200/%s/%s/%s' % (updated_index, updated_doc['_type'], updated_doc['_id'])
            r = requests.post(post_request, data=json.dumps(updated_doc['_source']))
            if r.status_code == 200 or r.status_code == 201:
                print 'SUCCESS: Successfully posted updated document for %s in index %s' % (updated_doc['_id'], updated_index)
                r.raise_for_status()
            else:
                print 'ERROR: Post request returned status code %d: %s' % (r.status_code, post_request)
                print(r.json())
                sys.exit(1)
        else:
            print 'SKIP: Skipping \"%s\" dataset from index \"%s\". No S3 URLs to update' % (updated_doc['_id'], updated_doc['_index'])
