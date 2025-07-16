from future import standard_library
standard_library.install_aliases()

import json
import traceback

from flask import request
from flask_restx import Resource, fields

from grq2 import app
from .service import grq_ns
from grq2.lib.dataset import update as update_dataset


@grq_ns.route('/dataset/index', endpoint='dataset_index')
@grq_ns.doc(responses={200: "Success", 500: "Execution failed"}, description="Dataset index.")
class IndexDataset(Resource):
    """Dataset indexing API."""

    resp_model = grq_ns.model('JsonResponse', {
        'success': fields.Boolean(required=True, description="Boolean, whether the API was successful"),
        'message': fields.String(required=True, description="message describing success or failure"),
        'objectid': fields.String(required=True, description="ID of indexed dataset"),
        'index': fields.String(required=True, description="dataset index name"),
    })

    parser = grq_ns.parser()
    parser.add_argument('dataset_info', required=True, type=str, location='form',  help="HySDS dataset info JSON")

    @grq_ns.marshal_with(resp_model)
    @grq_ns.expect(parser, validate=True)
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
            message = f"Failed index dataset. {type(e)}:{e}\n{traceback.format_exc()}"
            app.logger.error(message)
            return {
                'success': False,
                'message': message
            }, 500
