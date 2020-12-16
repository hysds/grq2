from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json

from flask import request
from flask_restx import Resource, fields

from hysds.task_worker import do_submit_task
from hysds.celery import app as celery_app
from hysds_commons.action_utils import check_passthrough_query

from grq2 import app, mozart_es
from .service import grq_ns


HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']
ON_DEMAND_DATASET_QUEUE = celery_app.conf['ON_DEMAND_DATASET_QUEUE']


@grq_ns.route('/on-demand', endpoint='on-demand')
@grq_ns.doc(responses={200: "Success", 500: "Execution failed"},
            description="Retrieve on demand jobs")
class OnDemandJobs(Resource):
    """On Demand Jobs API."""

    resp_model = grq_ns.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure")
    })

    def get(self):
        query = {
            "_source": ["id", "job-specification", "label", "job-version"],
            "sort": [{"label.keyword": {"order": "asc"}}],
            "query": {
                "exists": {
                    "field": "job-specification"
                }
            }
        }

        documents = mozart_es.query(index=HYSDS_IOS_INDEX, body=query)
        documents = [{
            'hysds_io': row['_source']['id'],
            'job_spec': row['_source']['job-specification'],
            'version': row['_source']['job-version'],
            'label': row['_source']['label']
        } for row in documents]

        return {
            'success': True,
            'result': documents
        }

    def post(self):
        """
        submits on demand job
        :return: submit job id?
        """
        # TODO: add user auth and permissions
        request_data = request.json
        if not request_data:
            request_data = request.form

        tag = request_data.get('tags', None)
        job_type = request_data.get('job_type', None)
        hysds_io = request_data.get('hysds_io', None)
        queue = request_data.get('queue', None)
        priority = int(request_data.get('priority', 0))
        query_string = request_data.get('query', None)
        kwargs = request_data.get('kwargs', '{}')
        time_limit = request_data.get('time_limit', None)
        soft_time_limit = request_data.get('soft_time_limit', None)
        disk_usage = request_data.get('disk_usage', None)

        try:
            query = json.loads(query_string)
            query_string = json.dumps(query)
        except (ValueError, TypeError, Exception) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid JSON query'
            }, 400

        if tag is None or job_type is None or hysds_io is None or queue is None or query_string is None:
            return {
                'success': False,
                'message': 'missing field: [tags, job_type, hysds_io, queue, query]'
            }, 400

        doc = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
        if doc['found'] is False:
            return {
                'success': False,
                'message': '%s job not found' % hysds_io
            }, 400

        params = doc['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        rule = {
            'username': 'example_user',
            'workflow': hysds_io,
            'priority': priority,
            'enabled': True,
            'job_type': job_type,
            'rule_name': tag,
            'kwargs': kwargs,
            'query_string': query_string,
            'query': query,
            'passthru_query': is_passthrough_query,
            'query_all': False,
            'queue': queue
        }

        if time_limit and isinstance(time_limit, int):
            if time_limit <= 0 or time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                rule['time_limit'] = time_limit

        if soft_time_limit and isinstance(soft_time_limit, int):
            if soft_time_limit <= 0 or soft_time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'soft_time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                rule['soft_time_limit'] = soft_time_limit

        if disk_usage:
            rule['disk_usage'] = disk_usage

        payload = {
            'type': 'job_iterator',
            'function': 'hysds_commons.job_iterator.iterate',
            'args': ["tosca", rule],
        }
        celery_task = do_submit_task(payload, ON_DEMAND_DATASET_QUEUE)

        return {
            'success': True,
            'result': celery_task.id
        }


@grq_ns.route('/job-params', endpoint='job-params')
@grq_ns.doc(responses={200: "Success", 500: "Execution failed"},
            description="Retrieve on job params for specific jobs")
class JobParams(Resource):
    """Job Params API."""

    resp_model = grq_ns.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure")
    })

    parser = grq_ns.parser()

    def get(self):
        job_type = request.args.get('job_type')
        if not job_type:
            return {'success': False, 'message': 'job_type not provided'}, 400

        query = {
            "query": {
                "term": {"job-specification.keyword": job_type}
            }
        }
        hysds_io = mozart_es.search(index=HYSDS_IOS_INDEX, body=query)

        if hysds_io['hits']['total']['value'] == 0:
            error_message = '%s not found' % job_type
            return {'success': False, 'message': error_message}, 404

        hysds_io = hysds_io['hits']['hits'][0]
        job_params = hysds_io['_source']['params']
        job_params = list(filter(lambda x: x['from'] == 'submitter', job_params))

        job_spec = mozart_es.get_by_id(index=JOB_SPECS_INDEX, id=job_type, ignore=404)
        if job_spec.get('found', False) is False:
            return {
                'success': False,
                'message': '%s not found in job_specs' % job_type
            }, 404

        return {
            'success': True,
            'submission_type': hysds_io['_source'].get('submission_type'),
            'hysds_io': hysds_io['_source']['id'],
            'params': job_params,
            'time_limit': job_spec['_source']['time_limit'],
            'soft_time_limit': job_spec['_source']['soft_time_limit'],
            'disk_usage': job_spec['_source']['disk_usage']
        }
