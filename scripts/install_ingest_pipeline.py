import os
import json
import requests

from hysds.celery import app


if __name__ == '__main__':
    es_url = app.conf['GRQ_ES_URL']

    ingest_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'ingest_pipeline.json'))
    ingest_file = os.path.normpath(ingest_file)

    with open(ingest_file) as f:
        pipeline_settings = json.load(f)

        pipeline_name = 'dataset_pipeline'
        endpoint = '%s/_ingest/pipeline/%s' % (es_url, pipeline_name)

        requests.delete(endpoint)

        headers = {'Content-Type': 'application/json'}
        r = requests.put(endpoint, data=json.dumps(pipeline_settings), headers=headers)
        r.raise_for_status()
        print(r.json())
        print("Successfully installed ingest_pipeline: %s " % pipeline_name)

