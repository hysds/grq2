from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()
import os
import sys
import json
import requests
import types
import re
import traceback
from datetime import datetime

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_restplus import Api, apidoc, Resource, fields
from flask_login import login_required

from elasticsearch import Elasticsearch

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

services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
api = Api(services, ui=False, version="0.1", title="Mozart API",
          description="API for GRQ Services.")
ns = api.namespace(NAMESPACE, description="GRQ operations")

HYSDS_IO_NS = "hysds_io"
hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)


@ns.route('/dataset/index', endpoint='dataset_index')
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
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
            message = "Failed index dataset. {0}:{1}\n{2}".format(
                type(e), e, traceback.format_exc())
            app.logger.debug(message)
            return {'success': False,
                    'message': message}, 500


@hysds_io_ns.route('/list', endpoint='hysds_io-list')
@api.doc(responses={200: "Success",
                    500: "Query execution failed"},
         description="Gets list of registered hysds-io specifications and return as JSON.")
class GetHySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""
    resp_model_job_types = api.model('HySDS IO List Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.List(fields.String, required=True,
                               description="list of hysds-io types")
    })
    @api.marshal_with(resp_model_job_types)
    def get(self):
        '''
        List HySDS IO specifications
        '''
        try:
            ids = hysds_commons.hysds_io_utils.get_hysds_io_types(
                app.config["ES_URL"], logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO types. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ids}


@hysds_io_ns.route('/type', endpoint='hysds_io-type')
@api.doc(responses={200: "Success",
                    500: "Queue listing failed"},
         description="Gets info on a hysds-io specification.")
class GetHySDSIOType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = api.model('HySDS IO Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.Raw(required=True, description="HySDS IO Object")
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Gets a HySDS-IO specification by ID
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            spec = hysds_commons.hysds_io_utils.get_hysds_io(app.config["ES_URL"], ident, logger=app.logger,
                                                             hysds_io_type='_doc')
        except Exception as e:
            message = "Failed to query ES for HySDS IO object. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': spec}


@hysds_io_ns.route('/add', endpoint='hysds_io-add')
@api.doc(responses={200: "Success",
                    500: "Adding JSON failed"},
         description="Adds a hysds-io specification")
class AddHySDSIOType(Resource):
    """Add job spec"""

    resp_model = api.model('HySDS IO Addition Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
        'result':  fields.String(required=True, description="HySDS IO ID")
    })
    parser = api.parser()
    parser.add_argument('spec', required=True, type=str,
                        help="HySDS IO JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        '''
        Add a HySDS IO specification
        '''
        try:
            spec = request.form.get('spec', request.args.get('spec', None))
            if spec is None:
                raise Exception("'spec' must be supplied")
            obj = json.loads(spec)
            ident = hysds_commons.hysds_io_utils.add_hysds_io(app.config["ES_URL"], obj, logger=app.logger,
                                                              hysds_io_type='_doc')
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': "",
                'result': ident}


@hysds_io_ns.route('/remove', endpoint='hysds_io-remove')
@api.doc(responses={200: "Success",
                    500: "Remove JSON failed"},
         description="Removes a hysds-io specification.")
class RemoveHySDSIOType(Resource):
    """Remove job spec"""

    resp_model = api.model('HySDS IO Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="if 'false', " +
                                  "encountered exception; otherwise no errors " +
                                  "occurred"),
        'message': fields.String(required=True, description="message describing " +
                                 "success or failure"),
    })
    parser = api.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO ID")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def get(self):
        '''
        Remove HySDS IO for the given ID
        '''
        try:
            ident = request.form.get('id', request.args.get('id', None))
            hysds_commons.hysds_io_utils.remove_hysds_io(app.config["ES_URL"], ident, logger=app.logger,
                                                         hysds_io_type='_doc')
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(
                type(e), str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return {'success': True,
                'message': ""}


@ns.route('/on-demand', endpoint='on-demand')
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
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

    parser = api.parser()
    # parser.add_argument('dataset_info', required=True, type=str,
    #                     location='form',  help="HySDS dataset info JSON")

    # @api.marshal_with(resp_model)
    def get(self):
        """List available on demand jobs"""
        es = Elasticsearch([ES_URL])

        query = {
            "_source": ["id", "job-specification", "label", "job-version"],
            "sort": [{"label.keyword": {"order": "asc"}}],
            "query": {
                "exists": {
                    "field": "job-specification"
                }
            }
        }
        page = es.search(index='hysds_ios', scroll='2m', size=100, body=query)

        sid = page['_scroll_id']
        documents = page['hits']['hits']
        page_size = page['hits']['total']['value']

        # Start scrolling
        while page_size > 0:
            page = es.scroll(scroll_id=sid, scroll='2m')

            # Update the scroll ID
            sid = page['_scroll_id']

            scroll_document = page['hits']['hits']

            # Get the number of results that we returned in the last scroll
            page_size = len(scroll_document)

            documents.extend(scroll_document)

        documents = [{
            'value': row['_source']['job-specification'],
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
        post_data = request.json
        if not post_data:
            post_data = request.form

        tag = post_data.get('tags', None)
        job_type = post_data.get('job_type', None)
        hysds_io = post_data.get('hysds_io', None)
        queue = post_data.get('queue', None)
        priority = int(post_data.get('priority', 0))
        query_string = post_data.get('query', None)
        kwargs = post_data.get('kwargs', None)

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
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
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
@api.doc(responses={200: "Success",
                    500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class UserRules(Resource):
    """User Rules API"""

    def get(self):
        user_rules_index = app.config['USER_RULES_INDEX']

        # TODO: add user role and permissions
        query = {"query": {"match_all": {}}}
        user_rules = get_es_scrolled_data(ES_URL, user_rules_index, query)

        return {
            'success': True,
            'rules': user_rules
        }

    def post(self):
        post_data = request.json or request.form

        es = Elasticsearch([ES_URL])
        user_rules_index = app.config['USER_RULES_INDEX']

        rule_name = post_data.get('rule_name')
        hysds_io = post_data.get('workflow')
        priority = int(post_data.get('priority', 0))
        query_string = post_data.get('query_string')
        kwargs = post_data.get('kwargs')
        queue = post_data.get('queue')

        username = "ops"  # TODO: add user role and permissions, hard coded to "ops" for now

        if not hysds_io:
            return {
                'success': False,
                'message': "Workflow not specified.",
                'result': None,
            }, 400

        # check if rule name already exists
        rule_exists_query = {
            "query": {
                "term": {
                    "rule_name": rule_name
                }
            }
        }
        existing_rules = es.search(index=user_rules_index, body=rule_exists_query)
        if existing_rules['hits']['total']['value'] > 0:
            return {
                'success': False,
                'message': 'user rule already exists: %s' % rule_name
            }, 409

        # check if job_type (hysds_io) exists in elasticsearch
        job_type = get_by_id_safe(ES_URL, 'hysds_ios', '_doc', hysds_io, safe=True, logger=app.logger)
        if not job_type:
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 400

        params = job_type['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        new_doc = {
            "workflow": hysds_io,
            "priority": priority,
            "rule_name": rule_name,
            "username": username,
            "query_string": query_string,
            "kwargs": kwargs,
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
            result = es.index(index=user_rules_index, doc_type='_doc', body=new_doc)
        except Exception as e:
            app.logger.error('failed to index document %s' % rule_name)
            app.logger.error(e)
            return {
                'success': False,
                'message': '%s failed to add user rule' % rule_name
            }, 500

        return {
            'success': True,
            'message': 'rule created',
            'result': result
        }
