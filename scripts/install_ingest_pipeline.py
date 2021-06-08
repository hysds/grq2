#!/usr/bin/env python
import os
import json

from grq2 import grq_es


current_directory = os.path.dirname(__file__)

ingest_file = os.path.join(current_directory, '..', 'config', 'ingest_pipeline.json')
ingest_file = os.path.abspath(ingest_file)
ingest_file = os.path.normpath(ingest_file)


if __name__ == '__main__':
    # TODO: delete pipeline here with a try except

    with open(ingest_file) as f:
        pipeline_settings = json.load(f)
        print(json.dumps(pipeline_settings, indent=2))

        pipeline_name = 'dataset_pipeline'

        # https://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.IngestClient
        grq_es.es.ingest.put_pipeline(id=pipeline_name, body=pipeline_settings, ignore=400)
