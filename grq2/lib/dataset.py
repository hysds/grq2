from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library

import json
import traceback

from shapely.geometry import shape

from grq2 import app, grq_es
from grq2.lib.geonames import get_cities, get_nearest_cities, get_continents
from grq2.lib.time_utils import getTemporalSpanInDays as get_ts
standard_library.install_aliases()


_POINT = 'Point'
_MULTIPOINT = 'MultiPoint'
_LINESTRING = 'LineString'
_MULTILINESTRING = 'MultiLineString'
_POLYGON = 'Polygon'
_MULTIPOLYGON = 'MultiPolygon'

GEOJSON_TYPES = {_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING, _POLYGON, _MULTIPOLYGON}


def map_geojson_type(geo_type):
    """
    maps the GEOJson type to the correct naming
    GEOJson types are case sensitive and can be these 6 types:
        'Point', 'MultiPoint', 'LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'
    :param geo_type: str, GEOJson type
    :return: str
    """
    # TODO: ES7.1's geo_shape doesn't support Point & MultiPoint but 7.7+ does, will need to revisit later

    if geo_type in GEOJSON_TYPES:
        return geo_type

    geo_type_lower = geo_type.lower()
    if _POINT.lower() == geo_type_lower:
        return _POINT
    elif _MULTIPOINT.lower() == geo_type_lower:
        return _MULTIPOINT
    elif _MULTILINESTRING.lower() == geo_type_lower:
        return _MULTILINESTRING
    elif _LINESTRING.lower() == geo_type_lower:
        return _LINESTRING
    elif _MULTIPOLYGON.lower() == geo_type_lower:
        return _MULTIPOLYGON
    elif _POLYGON.lower() == geo_type_lower:
        return _POLYGON
    else:
        return None


def update(update_json):
    """Update GRQ metadata and urls for a product."""
    version = update_json['version']  # get version

    dataset = update_json.get('dataset', None)  # determine index name
    index_suffix = dataset

    index = '%s_%s_%s' % (app.config['GRQ_INDEX'], version, index_suffix)  # get default index

    # get custom index and aliases
    aliases = []
    if 'index' in update_json:
        if 'suffix' in update_json['index']:
            index = '%s_%s' % (app.config['GRQ_INDEX'], update_json['index']['suffix'])
        aliases.extend(update_json['index'].get('aliases', []))
        del update_json['index']

    index = index.lower()  # ensure compatible index name

    # add reverse geo-location data
    if 'location' in update_json:
        location = {**update_json['location']}  # copying location to be used to create a shapely geometry object
        loc_type = location['type']

        geo_json_type = map_geojson_type(loc_type)
        location['type'] = geo_json_type  # setting proper GEOJson type, ex. multipolygon -> MultiPolygon
        update_json['location']['type'] = geo_json_type.lower()

        # add center if missing
        if 'center' not in update_json:
            geo_shape = shape(location)
            centroid = geo_shape.centroid
            update_json['center'] = {
                'type': 'point',
                'coordinates': [centroid.x, centroid.y]
            }

        # extract coordinates from center
        lon, lat = update_json['center']['coordinates']

        # add cities
        if geo_json_type in (_POLYGON, _MULTIPOLYGON):
            mp = True if geo_json_type == _MULTIPOLYGON else False
            coords = location['coordinates'][0]
            update_json['city'] = get_cities(coords, multipolygon=mp)
        elif geo_json_type in (_POINT, _MULTIPOINT, _LINESTRING, _MULTILINESTRING):
            update_json['city'] = get_nearest_cities(lon, lat)
        else:
            raise TypeError('%s is not a valid GEOJson type (or un-supported): %s' % (geo_json_type, GEOJSON_TYPES))

        # add closest continent
        continents = get_continents(lon, lat)
        update_json['continent'] = continents[0]['name'] if len(continents) > 0 else None

    # set temporal_span
    if update_json.get('starttime', None) is not None and update_json.get('endtime', None) is not None:
        if isinstance(update_json['starttime'], str) and isinstance(update_json['endtime'], str):
            start_time = update_json['starttime']
            end_time = update_json['endtime']
            update_json['temporal_span'] = get_ts(start_time, end_time)

    result = grq_es.index_document(index=index, body=update_json, id=update_json['id'])
    app.logger.debug("%s" % json.dumps(result, indent=2))

    # update custom aliases (Fixing HC-23)
    if len(aliases) > 0:
        try:
            actions = list()
            for index_alias in aliases:
                actions.append({"add": {"index": index, "alias": index_alias}})

            update_alias = {"actions": actions}
            grq_es.es.indices.update_aliases(update_alias)
        except Exception as e:
            app.logger.debug("Got exception trying to add aliases to index: %s\n%s\nContinuing on." %
                             (str(e), traceback.format_exc()))

    return {
        'success': True,
        'message': result,
        'objectid': update_json['id'],
        'index': index,
    }
