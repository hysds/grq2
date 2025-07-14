#!/usr/bin/env python
from future import standard_library
standard_library.install_aliases()
import json
import requests
import sys
from elasticsearch import Elasticsearch, helpers

from grq2 import app
from grq2.lib.utils import parse_config


# get source and destination index
src = sys.argv[1]
dest = sys.argv[2]
doctype = sys.argv[3]

# get connection and create destination index
es_url = app.config['ES_URL']

# Initialize the client with modern settings
es = Elasticsearch(
    hosts=[es_url],
    request_timeout=60,
    retry_on_timeout=True,
    verify_certs=False  # Only if using self-signed certs in development
)

def main():
    try:
        # index all docs from source index to destination index
        query = {
            "query": {
                "match_all": {}
            }
        }

        # Use the modern scan/scroll helper
        scroll_size = 100
        total_docs = 0

        print(f"Starting to copy documents from {src} to {dest}...")
        
        # Process documents in batches
        for doc in helpers.scan(
            es,
            index=src,
            query=query,
            size=scroll_size,
            scroll='60m',
            _source=True
        ):
            # Process each document
            doc_id = doc['_id']
            doc_source = doc['_source']
            
            # Index the document in the destination index
            es.index(
                index=dest,
                id=doc_id,
                document=doc_source,
                doc_type=doctype
            )
            total_docs += 1
            
            if total_docs % 100 == 0:
                print(f"Copied {total_docs} documents so far...")

        print(f"Successfully copied {total_docs} documents from {src} to {dest}")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
