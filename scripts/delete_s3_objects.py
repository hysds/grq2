#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()
import boto3
import sys
from urllib.parse import urlparse

file = open(sys.argv[1])
client = boto3.client('s3')

for line in file:
    parsed_url = urlparse(line.strip())
    bucket = parsed_url.hostname.split('.', 1)[0]
    print(('Deleting bucket=%s, key=%s' % (bucket, parsed_url.path[1:])))
    respone = client.delete_object(Bucket=bucket, Key=parsed_url.path[1:])

file.close()
