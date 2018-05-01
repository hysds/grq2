import os, sys, json, requests, types, re, traceback

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_restplus import Api, apidoc, Resource, fields
from flask_login import login_required

from grq2 import app
from grq2.lib.dataset import update as updateDataset
import hysds_commons.hysds_io_utils


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
@api.doc(responses={ 200: "Success",
                     500: "Execution failed" },
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
        info = request.form.get('dataset_info', request.args.get('dataset_info', None))
        if info is not None:
            try: info = json.loads(info)
            except Exception, e:
                message = "Failed to parse dataset info JSON."
                app.logger.debug(message)
                return { 'success': False,
                         'message': message,
                         'job_id': None }, 500

        # update
        try:
            return updateDataset(info)
        except Exception, e:
            message = "Failed index dataset. {0}:{1}\n{2}".format(type(e),e,traceback.format_exc())
            app.logger.debug(message)
            return { 'success': False,
                     'message': message }, 500
@hysds_io_ns.route('/list', endpoint='hysds_io-list')
@api.doc(responses={ 200: "Success",
                     500: "Query execution failed" },
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
            ids = hysds_commons.hysds_io_utils.get_hysds_io_types(app.config["ES_URL"],logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO types. {0}:{1}".format(type(e),str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return { 'success': True,
                 'message': "",
                 'result': ids }

@hysds_io_ns.route('/type', endpoint='hysds_io-type')
@api.doc(responses={ 200: "Success",
                     500: "Queue listing failed" },
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
            ident = request.form.get('id',request.args.get('id', None))
            spec = hysds_commons.hysds_io_utils.get_hysds_io(app.config["ES_URL"],ident,logger=app.logger)
        except Exception as e:
            message = "Failed to query ES for HySDS IO object. {0}:{1}".format(type(e),str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return { 'success': True,
                 'message': "",
                 'result': spec }

@hysds_io_ns.route('/add', endpoint='hysds_io-add')
@api.doc(responses={ 200: "Success",
                     500: "Adding JSON failed" },
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
    parser.add_argument('spec', required=True, type=str, help="HySDS IO JSON Object")

    @api.expect(parser)
    @api.marshal_with(resp_model)
    def post(self):
        '''
        Add a HySDS IO specification
        '''
        try:
            spec = request.form.get('spec',request.args.get('spec', None))
            if spec is None:
                raise Exception("'spec' must be supplied")
            obj = json.loads(spec)
            ident = hysds_commons.hysds_io_utils.add_hysds_io(app.config["ES_URL"],obj,logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(type(e),str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return { 'success': True,
                 'message': "",
                 'result': ident }

@hysds_io_ns.route('/remove', endpoint='hysds_io-remove')
@api.doc(responses={ 200: "Success",
                     500: "Remove JSON failed" },
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
            ident = request.form.get('id',request.args.get('id', None))
            hysds_commons.hysds_io_utils.remove_hysds_io(app.config["ES_URL"],ident,logger=app.logger)
        except Exception as e:
            message = "Failed to add ES for HySDS IO. {0}:{1}".format(type(e),str(e))
            app.logger.warning(message)
            app.logger.warning(traceback.format_exc(e))
            return {'success': False, 'message': message}, 500
        return { 'success': True,
                 'message': ""}

