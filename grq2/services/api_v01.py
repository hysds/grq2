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
from flask_restx import Api, apidoc, Resource, fields

from hysds.celery import app as celery_app
from hysds.task_worker import do_submit_task

from grq2 import app, mozart_es, grq_es
from grq2.lib.dataset import update as update_dataset
from hysds_commons.action_utils import check_passthrough_query

# NAMESPACE = "grq"

# services = Blueprint('api_v0-1', __name__, url_prefix='/api/v0.1')
# api = Api(services, ui=False, version="0.1", title="GRQ API", description="API for GRQ Services.")
# ns = api.namespace(NAMESPACE, description="GRQ operations")

# HYSDS_IO_NS = "hysds_io"
# hysds_io_ns = api.namespace(HYSDS_IO_NS, description="HySDS IO operations")

# HYSDS_IOS_INDEX = app.config['HYSDS_IOS_INDEX']
# JOB_SPECS_INDEX = app.config['JOB_SPECS_INDEX']
# USER_RULES_INDEX = app.config['USER_RULES_INDEX']
# ON_DEMAND_DATASET_QUEUE = celery_app.conf['ON_DEMAND_DATASET_QUEUE']


# @services.route('/doc/', endpoint='api_doc')
# def swagger_ui():
#     return apidoc.ui_for(api)
