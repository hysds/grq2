from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
from datetime import datetime

from flask import request
from flask_restx import Resource

from hysds.celery import app as celery_app
from hysds_commons.action_utils import check_passthrough_query

from grq2 import app, mozart_es
from .service import grq_ns


HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
USER_RULES_INDEX = app.config['USER_RULES_INDEX']
ON_DEMAND_DATASET_QUEUE = celery_app.conf['ON_DEMAND_DATASET_QUEUE']


@grq_ns.route('/user-rules', endpoint='user-rules')
@grq_ns.doc(responses={200: "Success", 500: "Execution failed"},
         description="Retrieve on job params for specific jobs")
class UserRules(Resource):
    """User Rules API"""

    def get(self):
        # TODO: add user role and permissions
        _id = request.args.get("id", None)
        _rule_name = request.args.get("rule_name", None)

        if _id:
            user_rule = mozart_es.get_by_id(index=USER_RULES_INDEX, id=_id, ignore=404)
            if user_rule.get("found", False) is False:
                return {
                    'success': False,
                    'message': 'rule %s not found' % _id
                }, 404
            user_rule = {**user_rule, **user_rule["_source"]}
            user_rule.pop("_source", None)
            return {
                'success': True,
                'rule': user_rule
            }
        elif _rule_name:
            result = mozart_es.search(index=USER_RULES_INDEX, q="rule_name:{}".format(_rule_name), ignore=404)
            if result.get("hits", {}).get("total", {}).get("value", 0) == 0:
                return {
                    "success": False,
                    "message": "rule {} not found".format(_rule_name)
                }, 404
            user_rule = result.get("hits").get("hits")[0]
            user_rule = {**user_rule, **user_rule["_source"]}
            user_rule.pop("_source", None)
            return {
                "success": True,
                "rule": user_rule
            }

        user_rules = mozart_es.query(index=USER_RULES_INDEX)

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
        tags = request_data.get('tags', [])
        time_limit = request_data.get('time_limit', None)
        soft_time_limit = request_data.get('soft_time_limit', None)
        disk_usage = request_data.get('disk_usage', None)

        username = "ops"  # TODO: add user role and permissions, hard coded to "ops" for now

        if not rule_name or not hysds_io or not job_spec or not query_string or not queue:
            return {
                'success': False,
                'message': 'All params must be supplied: (rule_name, hysds_io, job_spec, query_string, queue)',
                'result': None,
            }, 400

        if len(rule_name) > 64:
            return {
                "success": False,
                "message": "rule_name needs to be less than 64 characters",
                "result": None,
            }, 400

        try:
            json.loads(query_string)
        except (ValueError, TypeError, Exception) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid elasticsearch query JSON'
            }, 400

        try:
            json.loads(kwargs)
        except (ValueError, TypeError) as e:
            app.logger.error(e)
            return {
                'success': False,
                'message': 'invalid JSON: kwargs'
            }, 400

        # check if rule name already exists
        rule_exists_query = {
            "query": {
                "term": {
                    "rule_name": rule_name
                }
            }
        }
        existing_rules_count = mozart_es.get_count(index=USER_RULES_INDEX, body=rule_exists_query)
        if existing_rules_count > 0:
            return {
                'success': False,
                'message': 'user rule already exists: %s' % rule_name
            }, 409

        # check if job_type (hysds_io) exists in Elasticsearch
        job_type = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
        if job_type['found'] is False:
            return {
                'success': False,
                'message': '%s not found' % hysds_io
            }, 400

        params = job_type['_source']['params']
        is_passthrough_query = check_passthrough_query(params)

        if type(tags) == str:
            tags = [tags]

        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        new_doc = {
            "workflow": hysds_io,
            "job_spec": job_spec,
            "priority": priority,
            "rule_name": rule_name,
            "username": username,
            "query_string": query_string,
            "kwargs": kwargs,
            "job_type": hysds_io,
            "enabled": True,
            "passthru_query": is_passthrough_query,
            "query_all": False,
            "queue": queue,
            "modified_time": now,
            "creation_time": now,
            "tags": tags
        }

        if time_limit and isinstance(time_limit, int):
            if time_limit <= 0 or time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                new_doc['time_limit'] = time_limit

        if soft_time_limit and isinstance(soft_time_limit, int):
            if soft_time_limit <= 0 or soft_time_limit > 86400 * 7:
                return {
                    'success': False,
                    'message': 'soft_time_limit must be between 0 and 604800 (sec)'
                }, 400
            else:
                new_doc['soft_time_limit'] = soft_time_limit

        if disk_usage:
            new_doc['disk_usage'] = disk_usage

        result = mozart_es.index_document(index=USER_RULES_INDEX, body=new_doc, refresh=True)
        return {
            'success': True,
            'message': 'rule created',
            'result': result
        }

    def put(self):  # TODO: add user role and permissions
        request_data = request.json or request.form
        _id = request_data.get("id", None)
        _rule_name = request_data.get("rule_name", None)

        if not _id and not _rule_name:
            return {
                "success": False,
                "message": "Must specify id or rule_name in the request"
            }, 400

        rule_name = request_data.get('rule_name')
        hysds_io = request_data.get('workflow')
        job_spec = request_data.get('job_spec')
        priority = request_data.get('priority')
        query_string = request_data.get('query_string')
        kwargs = request_data.get('kwargs')
        queue = request_data.get('queue')
        enabled = request_data.get('enabled')
        tags = request_data.get('tags')
        time_limit = request_data.get('time_limit', None)
        soft_time_limit = request_data.get('soft_time_limit', None)
        disk_usage = request_data.get('disk_usage', None)

        # check if job_type (hysds_io) exists in elasticsearch (only if we're updating job_type)
        if hysds_io:
            job_type = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=hysds_io, ignore=404)
            if job_type.get("found", False) is False:
                return {
                    'success': False,
                    'message': 'job_type not found: %s' % hysds_io
                }, 400

        if _id:
            existing_rule = mozart_es.get_by_id(index=USER_RULES_INDEX, id=_id, ignore=404)
            if existing_rule.get("found", False) is False:
                return {
                    'success': False,
                    'message': 'rule %s not found' % _id
                }, 404
        elif _rule_name:
            result = mozart_es.search(index=USER_RULES_INDEX, q="rule_name:{}".format(_rule_name), ignore=404)
            if result.get("hits", {}).get("total", {}).get("value", 0) == 0:
                return {
                    'success': False,
                    'message': 'rule %s not found' % _rule_name
                }, 404
            else:
                _id = result.get("hits").get("hits")[0].get("_id")

        update_doc = {}
        if rule_name:
            if len(rule_name) > 64:
                return {
                    "success": False,
                    "message": "rule_name needs to be less than 64 characters",
                    "result": None,
                }, 400
            update_doc['rule_name'] = rule_name
        if hysds_io:
            update_doc['workflow'] = hysds_io
            update_doc['job_type'] = hysds_io
        if job_spec:
            update_doc['job_spec'] = job_spec
        if priority:
            update_doc['priority'] = int(priority)
        if query_string:
            update_doc['query_string'] = query_string
            try:
                json.loads(query_string)
            except (ValueError, TypeError, Exception) as e:
                app.logger.error(e)
                return {
                    'success': False,
                    'message': 'invalid elasticsearch query JSON'
                }, 400
        if kwargs:
            update_doc['kwargs'] = kwargs
            try:
                json.loads(kwargs)
            except (ValueError, TypeError) as e:
                app.logger.error(e)
                return {'success': False, 'message': 'invalid JSON: kwargs'}, 400
        if queue:
            update_doc['queue'] = queue
        if enabled is not None:
            if isinstance(enabled, str):
                if enabled.lower() == "false":
                    value = False
                else:
                    value = True
                update_doc["enabled"] = value
            else:
                update_doc["enabled"] = enabled
        if tags is not None:
            if type(tags) == str:
                tags = [tags]
            update_doc['tags'] = tags
        update_doc['modified_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        if 'time_limit' in request_data:  # if submitted in editor
            if time_limit is None:
                update_doc['time_limit'] = None
            else:
                if isinstance(time_limit, int) and 0 < time_limit <= 86400 * 7:
                    update_doc['time_limit'] = time_limit
                else:
                    return {
                        'success': False,
                        'message': 'time_limit must be between 0 and 604800 (sec)'
                    }, 400

        if 'soft_time_limit' in request_data:  # if submitted in editor
            if soft_time_limit is None:
                update_doc['soft_time_limit'] = None
            else:
                if isinstance(soft_time_limit, int) and 0 < soft_time_limit <= 86400 * 7:
                    update_doc['soft_time_limit'] = time_limit
                else:
                    return {
                        'success': False,
                        'message': 'time_limit must be between 0 and 604800 (sec)'
                    }, 400

        if 'disk_usage' in request_data:
            update_doc['disk_usage'] = disk_usage

        app.logger.info('new user rule: %s', json.dumps(update_doc))
        doc = {
            "doc_as_upsert": True,
            "doc": update_doc
        }
        mozart_es.update_document(index=USER_RULES_INDEX, id=_id, body=doc, refresh=True)
        app.logger.info('user rule %s updated' % _id)
        return {
            'success': True,
            'id': _id,
            'updated': update_doc
        }

    def delete(self):
        # TODO: need to add user rules and permissions
        _id = request.args.get("id", None)
        _rule_name = request.args.get("rule_name", None)

        if not _id and not _rule_name:
            return {"success": False,
                    "message": "Must specify id or rule_name in the request"
                    }, 400

        if "id" in request.args:
            _id = request.args.get('id')
            mozart_es.delete_by_id(index=USER_RULES_INDEX, id=_id, ignore=404)
            app.logger.info('user rule %s deleted' % _id)
            return {
                'success': True,
                'message': 'user rule deleted',
                'id': _id
            }
        elif "rule_name" in request.args:
            _rule_name = request.args.get("rule_name")
            query = {
                "query": {
                    "match": {
                        "rule_name": _rule_name
                    }
                }
            }
            mozart_es.es.delete_by_query(index=USER_RULES_INDEX, body=query, ignore=404)
            app.logger.info('user rule %s deleted' % _rule_name)
            return {
                'success': True,
                'message': 'user rule deleted',
                'rule_name': _rule_name
            }
