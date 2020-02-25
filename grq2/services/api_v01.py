from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import traceback
from datetime import datetime

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_restplus import Api, apidoc, Resource, fields
# from flask_login import login_required

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task

from grq2 import app, mozart_es
from grq2.lib.dataset import update as update_dataset
from hysds_commons.action_utils import check_passthrough_query

NAMESPACE = "grq"

services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
api = Api(services, ui=False, version="0.1", title="Mozart API", description="API for GRQ Services.")
ns = api.namespace(NAMESPACE, description="GRQ operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
USER_RULES_INDEX = app.config['USER_RULES_INDEX']
ON_DEMAND_DATASET_QUEUE = celery_app.conf['ON_DEMAND_DATASET_QUEUE']


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@ns.route('/dataset/index', endpoint='dataset_index')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Dataset index.")
class IndexDataset(Resource):
    """Dataset indexing API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = api.parser()
    parser.add_argument('dataset_info', required=True, type=str, location='form',  help="HySDS dataset info JSON")

    @api.marshal_with(resp_model)
    @api.expect(parser, validate=True)
    def post(self):
        info = request.form.get('dataset_info', request.args.get('dataset_info', None))
        if info is None:
            return {'success': False, 'message': 'dataset_info must be supplied'}, 400

        try:
            info = json.loads(info)
        except Exception as e:
            message = "Failed to parse dataset info JSON."
            app.logger.debug(message)
            return {
                'success': False,
                'message': message,
                'job_id': None
            }, 500

        try:
            return update_dataset(info)
        except Exception as e:
            message = "Failed index dataset. {0}:{1}\n{2}".format(type(e), e, traceback.format_exc())
            app.logger.debug(message)
            return {
                'success': False,
                'message': message
            }, 500


@hysds_io_ns.route('/list', endpoint='hysds_io-list')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class GetHySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""
    resp_model_job_types = api.model('HySDS IO List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of hysds-io types")
    })

    @api.marshal_with(resp_model_job_types)
    def get(self):
        query = {
            "query": {
                "match_all": {}
            }
        }
        hysds_ios = mozart_es.query(HYSDS_IOS_INDEX, query)
        ids = [hysds_io['_id'] for hysds_io in hysds_ios]
        return {
            'success': True,
            'message': "",
            'result': ids
        }


@hysds_io_ns.route('/type', endpoint='hysds_io-type')
@api.doc(responses={200: "Success", 500: "Queue listing failed"},
         description="Gets info on a hysds-io specification.")
class GetHySDSIOType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('HySDS IO Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.Raw(required=True, description="HySDS IO Object")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        """Gets a HySDS-IO specification by ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'missing parameter: id'}, 400

        hysds_io = mozart_es.get_by_id(HYSDS_IOS_INDEX, _id, safe=True)
        if hysds_io['found'] is False:
            return {'success': False, 'message': ""}, 404

        return {
            'success': True,
            'message': "",
            'result': hysds_io['_source']
        }


@hysds_io_ns.route('/add', endpoint='hysds_io-add')
@api.doc(responses={200: "Success", 500: "Adding JSON failed"},
         description="Adds a hysds-io specification")
class AddHySDSIOType(Resource):
    """Add job spec"""

    resp_model = api.model('HySDS IO Addition Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.String(required=True, description="HySDS IO ID")
    })
    parser = api.parser()
    parser.add_argument('spec', required=True, type=str, help="HySDS IO JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        """Add a HySDS IO specification"""
        spec = request.form.get('spec', request.args.get('spec', None))
        if spec is None:
            return {'success': False, 'message': 'spec must be supplied'}, 400

        try:
            obj = json.loads(spec)
            _id = obj['id']
        except (ValueError, KeyError, json.decoder.JSONDecodeError, Exception) as e:
            return {'success': False, 'message': e}, 400

        mozart_es.index_document(HYSDS_IOS_INDEX, obj, _id)
        return {
            'success': True,
            'message': "%s added to index: %s" % (_id, HYSDS_IOS_INDEX),
            'result': _id
        }


@hysds_io_ns.route('/remove', endpoint='hysds_io-remove')
@api.doc(responses={200: "Success", 500: "Remove JSON failed"},
         description="Removes a hysds-io specification.")
class RemoveHySDSIOType(Resource):
    resp_model = api.model('HySDS IO Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        """Remove HySDS IO for the given ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id must be supplied'}, 400

        mozart_es.delete_by_id(HYSDS_IOS_INDEX, _id)
        app.logger.info('deleted %s from index: %s' % (_id, HYSDS_IOS_INDEX))

        return {
            'success': True,
            'message': "removed %s from index %s" % (_id, HYSDS_IOS_INDEX)
        }


@ns.route('/on-demand', endpoint='on-demand')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on demand jobs")
class OnDemandJobs(Resource):
    """On Demand Jobs API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
    })
    parser = api.parser()

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

        documents = mozart_es.query(HYSDS_IOS_INDEX, query)
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

        query = json.loads(query_string)
        query_string = json.dumps(query)

        if tag is None or job_type is None or hysds_io is None or queue is None or query_string is None:
            return {
                'success': False,
                'message': 'missing field: [tags, job_type, hysds_io, queue, query]'
            }, 400

        doc = mozart_es.get_by_id(HYSDS_IOS_INDEX, hysds_io, safe=True)
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


@ns.route('/job-params', endpoint='job-params')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class JobParams(Resource):
    """Job Params API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
    })

    parser = api.parser()

    def get(self):
        job_type = request.args.get('job_type')
        if not job_type:
            return {'success': False, 'message': 'job_type not provided'}, 400

        query = {
            "query": {
                "term": {"job-specification.keyword": job_type}
            }
        }
        documents = mozart_es.search(HYSDS_IOS_INDEX, query)

        if documents['hits']['total']['value'] == 0:
            error_message = '%s not found' % job_type
            return {'success': False, 'message': error_message}, 404

        job_type = documents['hits']['hits'][0]
        job_params = job_type['_source']['params']
        job_params = list(filter(lambda x: x['from'] == 'submitter', job_params))

        return {
            'success': True,
            'submission_type': job_type['_source'].get('submission_type'),
            'hysds_io': job_type['_source']['id'],
            'params': job_params
        }


@ns.route('/user-rules', endpoint='user-rules')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class UserRules(Resource):
    """User Rules API"""

    def get(self):
        # TODO: add user role and permissions
        _id = request.args.get('id')

        if _id:
            user_rule = mozart_es.get_by_id(USER_RULES_INDEX, _id, safe=True)
            if user_rule['found'] is False:
                return {
                    'success': False,
                    'message': 'rule %s not found' % _id
                }, 404
            user_rule = {**user_rule, **user_rule['_source']}
            return {
                'success': True,
                'rule': user_rule
            }

        query = {
            "query": {
                "match_all": {}
            }
        }
        user_rules = mozart_es.query(USER_RULES_INDEX, query)

        parsed_user_rules = []
        for rule in user_rules:
            rule_copy = rule.copy()
            rule_temp = {**rule_copy, **rule['_source']}
            rule_temp.pop('_source')
            parsed_user_rules.append(rule_temp)

        return {
            'success': True,
            'rules': parsed_user_rules
        }

    def post(self):
        request_data = request.json or request.form

        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = int(request_data.get('priority', 0))
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs', '{}')
        queue = request_data.get('queue')

        username = "ops"  # TODO: add user role and permissions, hard coded to "ops" for now

        if not rule_name or not hysds_io or not job_spec or not query_string or not queue:
            return {
                'success': False,
                'message': 'All params must be supplied: (rule_name, hysds_io, job_spec, query_string, queue)',
                'result': None,
            }, 400

        try:
            parsed_query = json.loads(query_string)
            query_string = json.dumps(parsed_query)
        except (ValueError, TypeError, Exception) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid elasticsearch query JSON'
            }, 400

        # check if rule name already exists
        rule_exists_query = {
            "query": {
                "term": {
                    "rule_name": rule_name
                }
            }
        }
        existing_rules_count = mozart_es.get_count(USER_RULES_INDEX, rule_exists_query)
        if existing_rules_count > 0:
            return {
                'success': False,
                'message': 'user rule already exists: %s' % rule_name
            }, 409

        # check if job_type (hysds_io) exists in Elasticsearch
        job_type = mozart_es.get_by_id(HYSDS_IOS_INDEX, hysds_io, safe=True)
        if job_type is False:
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 400

        params = job_type['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        new_doc = {
            "workflow": hysds_io,
            "job_spec": job_spec,
            "priority": priority,
            "rule_name": rule_name,
            "username": username,
            "query_string": query_string,
            "kwargs": kwargs,
            "hysds_io": hysds_io,
            "job_type": hysds_io,
            "enabled": True,
            "query": json.loads(query_string),
            "passthru_query": is_passthrough_query,
            "query_all": False,
            "queue": queue,
            "modified_time": now,
            "creation_time": now,
        }

        result = mozart_es.index_document(USER_RULES_INDEX, new_doc, refresh=True)
        return {
            'success': True,
            'message': 'rule created',
            'result': result
        }

    def put(self):  # TODO: add user role and permissions
        request_data = request.json or request.form

        _id = request_data.get('id')
        if not _id:
            return {'result': False, 'message': 'id not included'}, 400

        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = request_data.get('priority')
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs')
        queue = request_data.get('queue')
        enabled = request_data.get('enabled')

        # check if job_type (hysds_io) exists in elasticsearch (only if we're updating job_type)
        if hysds_io:
            job_type = mozart_es.get_by_id(HYSDS_IOS_INDEX, hysds_io, safe=True)
            if job_type['found'] is False:
                return {
                    'success': False,
                    'message': 'job_type not found: %s' % hysds_io
                }, 400

        existing_rule = mozart_es.get_by_id(USER_RULES_INDEX, _id, safe=True)
        if existing_rule['found'] is False:
            return {
                'success': False,
                'message': 'rule %s not found' % _id
            }, 404

        update_doc = {}
        if rule_name:
            update_doc['rule_name'] = rule_name
        if hysds_io:
            update_doc['hysds_io'] = hysds_io
            update_doc['job_type'] = hysds_io
        if job_spec:
            update_doc['job_spec'] = job_spec
        if priority:
            update_doc['priority'] = int(priority)
        if query_string:
            update_doc['query_string'] = query_string
            update_doc['query'] = json.loads(query_string)
        if kwargs:
            try:
                json.loads(kwargs)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {'success': False, 'message': 'invalid JSON: kwargs'}, 400
            update_doc['kwargs'] = kwargs
        if queue:
            update_doc['queue'] = queue
        if enabled is not None:
            update_doc['enabled'] = enabled
        update_doc['modified_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        app.logger.info('new user rule: %s', json.dumps(update_doc))
        mozart_es.update_document(USER_RULES_INDEX, _id, update_doc, refresh=True)
        app.logger.info('user rule %s updated' % _id)
        return {
            'success': True,
            'id': _id,
            'updated': update_doc
        }

    def delete(self):
        # TODO: need to add user rules and permissions
        _id = request.args.get('id')
        if not _id:
            return {'result': False, 'message': 'id not included'}, 400

        mozart_es.delete_by_id(USER_RULES_INDEX, _id)
        app.logger.info('user rule %s deleted' % _id)

        return {
            'success': True,
            'message': 'user rule deleted',
            'id': _id
        }
