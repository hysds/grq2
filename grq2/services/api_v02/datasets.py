from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import traceback

from flask import request
from flask_restx import Resource, fields

from shapely.geometry import shape
from elasticsearch.exceptions import ElasticsearchException

from grq2 import app, grq_es
from .service import grq_ns
from grq2.lib.dataset import update as update_dataset, map_geojson_type

from grq2.lib.geonames import get_cities, get_nearest_cities, get_continents
from grq2.lib.time_utils import getTemporalSpanInDays as get_ts


_POINT = 'Point'
_MULTIPOINT = 'MultiPoint'
_LINESTRING = 'LineString'
_MULTILINESTRING = 'MultiLineString'
_POLYGON = 'Polygon'
_MULTIPOLYGON = 'MultiPolygon'

GEOJSON_TYPES = {_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING, _POLYGON, _MULTIPOLYGON}


def get_es_index(prod_json):
    """
    get ES index name for dataset
    :param prod_json: Dict[any]
    :return: str
    """
    version = prod_json['version']  # get version
    dataset = prod_json.get('dataset', "dataset")  # determine index name

    index = '%s_%s_%s' % (app.config['GRQ_INDEX'], version, dataset)  # get default index

    aliases = []
    if 'index' in prod_json:
        if 'suffix' in prod_json['index']:
            index = '%s_%s' % (app.config['GRQ_INDEX'], prod_json['index']['suffix'])
        aliases.extend(prod_json['index'].get('aliases', []))
        del prod_json['index']
    return index.lower(), aliases


def reverse_geolocation(prod_json):
    """
    retrieves the dataset's city, the nearest cities and continent
    :param prod_json: Dict[any]; dataset metadata
    """
    if 'location' in prod_json:
        location = {**prod_json['location']}  # copying location to be used to create a shapely geometry object
        loc_type = location['type']

        geo_json_type = map_geojson_type(loc_type)
        location['type'] = geo_json_type  # setting proper GEOJson type, ex. multipolygon -> MultiPolygon
        prod_json['location']['type'] = geo_json_type.lower()

        # add center if missing
        if 'center' not in prod_json:
            geo_shape = shape(location)
            centroid = geo_shape.centroid
            prod_json['center'] = {
                'type': 'point',
                'coordinates': [centroid.x, centroid.y]
            }

        # extract coordinates from center
        lon, lat = prod_json['center']['coordinates']

        # add cities
        if geo_json_type in (_POLYGON, _MULTIPOLYGON):
            mp = True if geo_json_type == _MULTIPOLYGON else False
            coords = location['coordinates'][0]
            cities = get_cities(coords, multipolygon=mp)
            if cities:
                prod_json['city'] = cities
        elif geo_json_type in (_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING):
            nearest_cities = get_nearest_cities(lon, lat)
            if nearest_cities:
                prod_json['city'] = nearest_cities
        else:
            raise TypeError('%s is not a valid GEOJson type (or un-supported): %s' % (geo_json_type, GEOJSON_TYPES))

        # add closest continent
        continents = get_continents(lon, lat)
        if continents:
            prod_json['continent'] = continents[0]['name'] if len(continents) > 0 else None

    # set temporal_span
    if prod_json.get('starttime', None) is not None and prod_json.get('endtime', None) is not None:
        if isinstance(prod_json['starttime'], str) and isinstance(prod_json['endtime'], str):
            start_time = prod_json['starttime']
            end_time = prod_json['endtime']
            prod_json['temporal_span'] = get_ts(start_time, end_time)


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
        datasets = json.loads(request.json)

        docs_bulk = []
        for ds in datasets:
            _id = ds["id"]
            index, aliases = get_es_index(ds)
            reverse_geolocation(ds)
            docs_bulk.append({"index": {"_index": index, "_id": _id}})
            docs_bulk.append(ds)

        try:
            response = grq_es.es.bulk(body=docs_bulk)
            if response["errors"] is True:
                app.logger.error(response)
                delete_docs = []
                for doc in docs_bulk:
                    if "index" in doc:
                        delete_docs.append({"delete": doc["index"]})
                grq_es.es.bulk(delete_docs)
                return {
                    "success": False,
                    "message": response["items"]
                }, 400
            return {
                "success": True,
                "message": response
            }
        except ElasticsearchException as e:
            message = "Failed index dataset. {0}:{1}\n{2}".format(type(e), e, traceback.format_exc())
            app.logger.error(message)
            return {
                'success': False,
                'message': message
            }, 400
