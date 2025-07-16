from future import standard_library
standard_library.install_aliases()

from flask import Blueprint
from flask_restx import Api, apidoc, Namespace

from .specs import hysds_io_ns


services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
api = Api(services, ui=False, version="0.1", title="GRQ API", description="API for GRQ Services.")

NAMESPACE = "grq"
grq_ns = Namespace(NAMESPACE, description="GRQ operations")

api.add_namespace(grq_ns)
api.add_namespace(hysds_io_ns)


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)
