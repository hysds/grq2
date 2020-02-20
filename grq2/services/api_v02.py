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
from flask_login import login_required

from elasticsearch import Elasticsearch, NotFoundError

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task

from grq2 import app
from grq2.lib.dataset import update as updateDataset
import hysds_commons.hysds_io_utils
import hysds_commons.mozart_utils
from hysds_commons.metadata_rest_utils import get_by_id
from hysds_commons.action_utils import check_passthrough_query
from hysds_commons.elasticsearch_utils import get_es_scrolled_data

ES_URL = app.config['ES_URL']
NAMESPACE = "grq"

services = Blueprint('api_v0-2', __name__, url_prefix='/api/v0.2')
api = Api(services, ui=False, version="0.2", title="Mozart API",
          description="API for GRQ Services.")
ns = api.namespace(NAMESPACE, description="GRQ operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@ns.route('/dataset/index', endpoint='dataset_index')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Dataset index.")
class IndexDataset(Resource):
    """Dataset indexing API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = api.parser()
    parser.add_argument('dataset_info', required=True, type=str,
                        location='form',  help="HySDS dataset info JSON")

    @api.marshal_with(resp_model)
    @api.expect(parser, validate=True)
    def post(self):

        # get info
        info = request.form.get(
            'dataset_info', request.args.get('dataset_info', None))
        if info is not None:
            try:
                info = json.loads(info)
            except Exception as e:
                message = "Failed to parse dataset info JSON."
                app.logger.debug(message)
                return {'success': False,
                        'message': message,
                        'job_id': None}, 500

        # update
        try:
            return updateDataset(info)
        except Exception as e:
            message = "Failed index dataset. {0}:{1}\n{2}".format(type(e), e, traceback.format_exc())
            app.logger.debug(message)
            return {'success': False,
                    'message': message}, 500


@hysds_io_ns.route('/hysds-io', endpoint='hysds_io-list')
@api.doc(responses={200: "Success", 500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class HySDSIO(Resource):
    """Get list of registered hysds-io and return as JSON."""
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    def get(self):
        """List HySDS IO specifications"""
        ident = request.args.get('id', None)
        es_url = app.config["ES_URL"]

        try:
            if ident:
                result = hysds_commons.hysds_io_utils.get_hysds_io(es_url, ident, logger=app.logger)
            else:
                result = hysds_commons.hysds_io_utils.get_hysds_io_types(es_url, logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO types. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500

        return {
            'success': True,
            'message': "hysds_io retrieved",
            'result': result
        }

    def post(self):
        """Add a HySDS IO specification"""
        es_url = app.config["ES_URL"]
        try:
            spec = request.form.get('spec', request.args.get('spec', None))
            if spec is None:
                raise Exception("'spec' must be supplied")
            obj = json.loads(spec)
            ident = hysds_commons.hysds_io_utils.add_hysds_io(es_url, obj, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {
            'success': True,
            'message': "hysds_io added",
            'result': ident
        }

    def delete(self):
        """Remove HySDS IO for the given ID"""
        es_url = app.config["ES_URL"]
        try:
            ident = request.args.get('id', None)
            hysds_commons.hysds_io_utils.remove_hysds_io(es_url, ident, logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {
            'success': True,
            'message': 'hysds_io removed %s' % ident
        }


@ns.route('/on-demand', endpoint='on-demand')
@api.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on demand jobs")
class OnDemandJobs(Resource):
    """On Demand Jobs API."""

    resp_model = api.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    def get(self):
        """List available on demand jobs"""
        query = {
            "_source": ["id", "job-specification", "label", "job-version"],
            "sort": [{"label.keyword": {"order": "asc"}}],
            "query": {
                "exists": {
                    "field": "job-specification"
                }
            }
        }

        documents = get_es_scrolled_data(ES_URL, 'hysds_ios', query)
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
        kwargs = request_data.get('kwargs', None)

        query = json.loads(query_string)
        query_string = json.dumps(query)

        es = Elasticsearch([ES_URL])
        try:
            doc = es.get(index='hysds_ios', id=hysds_io)
        except Exception as e:
            app.logger.error('failed to fetch %s' % hysds_io)
            app.logger.error(e)
            return {'success': False, 'message': '%s not found' % hysds_io}, 404

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

        celery_task = do_submit_task(payload, celery_app.conf['ON_DEMAND_DATASET_QUEUE'])

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
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = api.parser()
    # parser.add_argument('dataset_info', required=True, type=str,
    #                     location='form',  help="HySDS dataset info JSON")

    # @api.marshal_with(resp_model)
    def get(self):
        es = Elasticsearch([ES_URL])

        job_type = request.args.get('job_type')
        if not job_type:
            return {'success': False, 'message': 'job_type not provided'}, 400

        query = {
            "query": {
                # "term": {"_id": job_type}
                "term": {"job-specification.keyword": job_type}
            }
        }
        documents = es.search(index='hysds_ios', body=query)

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
        id = request.args.get('id')
        user_rules_index = app.config['USER_RULES_INDEX']

        if id:
            es = Elasticsearch([ES_URL])
            try:
                user_rule = es.get(index=user_rules_index, id=id)
                user_rule = {**user_rule, **user_rule['_source']}
                return {
                    'success': True,
                    'rule': user_rule
                }
            except NotFoundError as e:
                app.logger.error(e)
                return {
                    'success': False,
                    'rule': None
                }, 404
            except Exception as e:
                app.logger.error(e)
                raise Exception("Something went wrong with Elasticsearch")

        query = {"query": {"match_all": {}}}
        user_rules = get_es_scrolled_data(ES_URL, user_rules_index, query)

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

        es = Elasticsearch([ES_URL])
        user_rules_index = app.config['USER_RULES_INDEX']

        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = int(request_data.get('priority', 0))
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs', '{}')
        queue = request_data.get('queue')

        username = "ops"  # TODO: add user role and permissions, hard coded to "ops" for now

        if not rule_name or not hysds_io or not job_spec or not query_string or not queue:
            missing_params = []
            if not rule_name:
                missing_params.append('rule_name')
            if not hysds_io:
                missing_params.append('workflow')
            if not job_spec:
                missing_params.append('job_spec')
            if not query_string:
                missing_params.append('query_string')
            if not queue:
                missing_params.append('queue')
            return {
                'success': False,
                'message': 'Params not specified: %s' % ', '.join(missing_params),
                'result': None,
            }, 400

        try:
            parsed_query = json.loads(query_string)
            query_string = json.dumps(parsed_query)
        except (ValueError, TypeError) as e:
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
        existing_rules = es.count(index=user_rules_index, body=rule_exists_query)
        if existing_rules['count'] > 0:
            return {
                'success': False,
                'message': 'user rule already exists: %s' % rule_name
            }, 409

        # check if job_type (hysds_io) exists in elasticsearch
        job_type = get_by_id(ES_URL, 'hysds_ios', hysds_io, safe=True, logger=app.logger)
        if not job_type:
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 400

        params = job_type['params']
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

        try:
            result = es.index(index=user_rules_index, body=new_doc, refresh=True)
            return {
                'success': True,
                'message': 'rule created',
                'result': result
            }
        except Exception as e:
            app.logger.error('failed to index document %s' % rule_name)
            app.logger.error(e)
            return {
                'success': False,
                'message': '%s failed to add user rule' % rule_name
            }, 500

    def put(self):  # TODO: add user role and permissions
        request_data = request.json or request.form

        _id = request_data.get('id')
        if not _id:
            return {
                'result': False,
                'message': 'id not included'
            }, 400

        es = Elasticsearch([ES_URL])
        user_rules_index = app.config['USER_RULES_INDEX']

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
            job_type = get_by_id(ES_URL, 'hysds_ios', hysds_io, safe=True, logger=app.logger)
            if not job_type:
                return {
                    'success': False,
                    'message': 'job_type not found: %s' % hysds_io
                }, 400

        try:
            app.logger.info('finding existing user rule: %s' % _id)
            es.get(index=user_rules_index, id=_id)
        except NotFoundError as e:
            app.logger.error(e)
            return {
                'result': False,
                'message': 'user rule not found: %s' % _id
            }, 404
        except Exception as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'Unable to edit user rule'
            }, 500

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
                return {
                    'success': False,
                    'message': 'invalid JSON: kwargs'
                }, 400

            update_doc['kwargs'] = kwargs
        if queue:
            update_doc['queue'] = queue
        if enabled is not None:
            update_doc['enabled'] = enabled
        update_doc['modified_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        new_doc = {
            'doc_as_upsert': True,
            'doc': update_doc
        }

        try:
            es.update(user_rules_index, id=_id, body=new_doc, refresh=True)
            return {
                'success': True,
                'id': _id,
                'updated': update_doc
            }
        except Exception as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'failed to edit user rule: %s' % rule_name
            }, 500

    def delete(self):
        # TODO: need to add user rules and permissions
        _id = request.args.get('id')
        if not _id:
            return {
                'result': False,
                'message': 'id not included'
            }, 400

        es = Elasticsearch([ES_URL])
        user_rules_index = app.config['USER_RULES_INDEX']

        try:
            es.delete(index=user_rules_index, id=_id)
            return {
                'success': True,
                'message': 'user rule deleted',
                'id': _id
            }
        except NotFoundError as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'user rule id %s not found' % _id
            }, 404
        except Exception as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'Unable to delete user rule id %s' % _id
            }, 500
