import os
import json
import requests

from hysds.celery import app


es_url = app.conf['GRQ_ES_URL']

current_directory = os.path.dirname(__file__)

ingest_file = os.path.join(current_directory, '..', 'config', 'ingest_pipeline.json')
ingest_file = os.path.abspath(ingest_file)
ingest_file = os.path.normpath(ingest_file)

with open(ingest_file) as f:
    pipeline_settings = json.load(f)
    print(json.dumps(pipeline_settings, indent=2))

    pipeline_name = 'dataset_pipeline'
    endpoint = '%s/_ingest/pipeline/%s' % (es_url, pipeline_name)

    requests.delete(endpoint)

    headers = {'Content-Type': 'application/json'}
    r = requests.put(endpoint, data=json.dumps(pipeline_settings), headers=headers)
    r.raise_for_status()
    print(r.json())
    print("Successfully installed ingest_pipeline: %s " % pipeline_name)
