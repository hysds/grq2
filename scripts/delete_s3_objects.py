#!/usr/bin/env python
import boto3
import sys
from urlparse import urlparse

file = open(sys.argv[1])
client = boto3.client('s3')

for line in file:
    parsed_url = urlparse(line.strip())
    bucket = parsed_url.hostname.split('.', 1)[0]
    print 'Deleting bucket=%s, key=%s' % (bucket, parsed_url.path[1:])
    respone = client.delete_object(Bucket=bucket, Key=parsed_url.path[1:])

file.close()
