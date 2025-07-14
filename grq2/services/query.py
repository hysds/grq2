from future import standard_library
standard_library.install_aliases()

from flask import jsonify, Blueprint, request, Response, render_template, make_response
from redis import Redis

from grq2 import app


mod = Blueprint('services/query', __name__)


@mod.route('/datasets', methods=['GET'])
def datasets():
    """Return available datsets."""

    return jsonify({
        'success': True,
        'message': "",
        'datasets': list(app.config['GRQ_DATASET_DOCTYPES'].keys())
    })


@mod.route('/grq/redis/setnx', methods=['GET'])
def redis_setnx():
    """Return redis setnx results."""

    # get key and value
    key = "{}__{}".format(app.config['GRQ_INDEX'], request.args.get('key'))
    value = request.args.get('value')

    # execute
    red = Redis.from_url(app.config['REDIS_URL'])
    if red.setnx(key, value):
        return jsonify({
            'status': True,
            'id': value
        })
    else:
        return jsonify({
            'status': False,
            'id': red.get(key)
        })
