from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json

from flask import request
from flask_restx import Namespace, Resource, fields

from grq2 import app, mozart_es


HYSDS_IO_NS = "hysds_io"
hysds_io_ns = Namespace(HYSDS_IO_NS, description="HySDS IO operations")

HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']


@hysds_io_ns.route('/list', endpoint='hysds_io-list')
@hysds_io_ns.doc(responses={200: "Success", 500: "Query execution failed"},
                 description="Gets list of registered hysds-io specifications and return as JSON.")
class GetHySDSIOTypes(Resource):
    """Get list of registered hysds-io and return as JSON."""
    resp_model_job_types = hysds_io_ns.model('HySDS IO List Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.List(fields.String, required=True, description="list of hysds-io types")
    })

    @hysds_io_ns.marshal_with(resp_model_job_types)
    def get(self):
        hysds_ios = mozart_es.query(index=HYSDS_IOS_INDEX, _source=False)
        ids = [hysds_io['_id'] for hysds_io in hysds_ios]
        return {
            'success': True,
            'message': "",
            'result': ids
        }


@hysds_io_ns.route('/type', endpoint='hysds_io-type')
@hysds_io_ns.doc(responses={200: "Success", 500: "Queue listing failed"},
                 description="Gets info on a hysds-io specification.")
class GetHySDSIOType(Resource):
    """Get list of job queues and return as JSON."""

    resp_model = hysds_io_ns.model('HySDS IO Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.Raw(required=True, description="HySDS IO Object")
    })
    parser = hysds_io_ns.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
    def get(self):
        """Gets a HySDS-IO specification by ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'missing parameter: id'}, 400

        hysds_io = mozart_es.get_by_id(index=HYSDS_IOS_INDEX, id=_id, ignore=404)
        if hysds_io['found'] is False:
            return {'success': False, 'message': ""}, 404

        return {
            'success': True,
            'message': "",
            'result': hysds_io['_source']
        }


@hysds_io_ns.route('/add', endpoint='hysds_io-add')
@hysds_io_ns.doc(responses={200: "Success", 500: "Adding JSON failed"},
                 description="Adds a hysds-io specification")
class AddHySDSIOType(Resource):
    """Add job spec"""

    resp_model = hysds_io_ns.model('HySDS IO Addition Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'result':  fields.String(required=True, description="HySDS IO ID")
    })
    parser = hysds_io_ns.parser()
    parser.add_argument('spec', required=True, type=str, help="HySDS IO JSON Object")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
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

        mozart_es.index_document(index=HYSDS_IOS_INDEX, body=obj, id=_id)
        return {
            'success': True,
            'message': "%s added to index: %s" % (_id, HYSDS_IOS_INDEX),
            'result': _id
        }


@hysds_io_ns.route('/remove', endpoint='hysds_io-remove')
@hysds_io_ns.doc(responses={200: "Success", 500: "Remove JSON failed"},
                 description="Removes a hysds-io specification.")
class RemoveHySDSIOType(Resource):
    resp_model = hysds_io_ns.model('HySDS IO Removal Response(JSON)', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
    })
    parser = hysds_io_ns.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO ID")

    @hysds_io_ns.expect(parser)
    @hysds_io_ns.marshal_with(resp_model)
    def get(self):
        """Remove HySDS IO for the given ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id must be supplied'}, 400

        mozart_es.delete_by_id(index=HYSDS_IOS_INDEX, id=_id, ignore=404)
        app.logger.info('deleted %s from index: %s' % (_id, HYSDS_IOS_INDEX))

        return {
            'success': True,
            'message': "removed %s from index %s" % (_id, HYSDS_IOS_INDEX)
        }
