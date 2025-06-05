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


@hysds_io_ns.route('/list', endpoint='hysds_ios')
@hysds_io_ns.doc(responses={200: "Success", 500: "Query execution failed"},
                 description="Gets list of registered hysds-io specifications and return as JSON.")
class HySDSIOTypes(Resource):
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


@hysds_io_ns.route('', endpoint='hysds_io')
@hysds_io_ns.doc(responses={200: "Success", 500: "Query execution failed"},
                 description="Gets list of registered hysds-io specifications and return as JSON.")
class HySDSio(Resource):
    """Get list of registered hysds-io and return as JSON."""
    parser = hysds_io_ns.parser()
    parser.add_argument('id', required=True, type=str, help="HySDS IO Type ID")

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
            'message': "{} added to index: {}".format(_id, HYSDS_IOS_INDEX),
            'result': _id
        }

    def delete(self):
        """Remove HySDS IO for the given ID"""
        _id = request.form.get('id', request.args.get('id', None))
        if _id is None:
            return {'success': False, 'message': 'id must be supplied'}, 400

        mozart_es.delete_by_id(index=HYSDS_IOS_INDEX, id=_id, ignore=404)
        app.logger.info('deleted {} from index: {}'.format(_id, HYSDS_IOS_INDEX))

        return {
            'success': True,
            'message': "removed {} from index {}".format(_id, HYSDS_IOS_INDEX)
        }
