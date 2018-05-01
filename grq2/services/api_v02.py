import os, sys, json, requests, types, re

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from flask_restplus import Api, apidoc, Resource, fields
from flask_login import login_required

from grq2 import app
from grq2.lib.dataset import update as updateDataset


NAMESPACE = "grq"

services = Blueprint('api_v0-2', __name__, url_prefix='/api/v0.2')
api = Api(services, ui=False, version="0.2", title="Mozart API",
          description="API for GRQ Services.")
ns = api.namespace(NAMESPACE, description="GRQ operations")


@services.route('/doc/', endpoint='api_doc')
def swagger_ui():
    return apidoc.ui_for(api)
