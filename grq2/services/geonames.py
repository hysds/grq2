import json, requests, types
from flask import jsonify, Blueprint, request, Response, render_template, make_response
from pprint import pformat

from grq2 import app
from grq2.lib.geonames import get_cities, get_continents


mod = Blueprint('services/geonames', __name__)


@mod.route('/geonames/cities', methods=['GET'])
def cities():
    """
    Spatial search of top populated cities within a bounding box.
    """

    # status and message
    success = True
    message = ""

    # get query
    polygon = request.args.get('polygon', None)
    pop_th = int(request.args.get('population', 1000000))
    size = int(request.args.get('size', 10))

    # if no polygon passed, show help
    if polygon is None:
        return jsonify({
            'success': False,
            'message': "No polygon specified.",
            'result': [],
            'count': 0
        }), 500
    else:
        try:
            polygon = eval(polygon)
        except:
            return jsonify({
                'success': False,
                'message': "Invalid polygon specification: %s" % polygon,
                'result': [],
                'count': 0
            }), 500

    # get results
    try:
        cities = get_cities(polygon, pop_th, size)
    except Exception, e:
        return jsonify({
            'success': False,
            'message': str(e),
            'result': [],
            'count': 0
        })

    return jsonify({
        'success': success,
        'message': message,
        'result': cities,
        'count': len(cities)
    })


@mod.route('/geonames/continents', methods=['GET'])
def continents():
    """
    Spatial search of closest continents to the specified geo point.
    """

    # status and message
    success = True
    message = ""

    # get query
    lon = request.args.get('lon', None)
    lat = request.args.get('lat', None)

    # if no polygon passed, show help
    if lon is None or lat is None:
        return jsonify({
            'success': False,
            'message': "Please specify lon and lat values.",
            'result': [],
            'count': 0
        }), 500

    # get results
    try:
        continents = get_continents(float(lon), float(lat))
    except Exception, e:
        return jsonify({
            'success': False,
            'message': str(e),
            'result': [],
            'count': 0
        })

    return jsonify({
        'success': success,
        'message': message,
        'result': continents,
        'count': len(continents)
    })
