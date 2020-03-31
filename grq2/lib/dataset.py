from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import json
import traceback
import re

from grq2 import app, grq_es
from grq2.lib.geonames import get_cities, get_continents
from grq2.lib.geo import get_center
from grq2.lib.time_utils import getTemporalSpanInDays as get_ts
# from grq2.lib.utils import parse_config

POLYGON_RE = re.compile(r'^polygon$', re.I)
MULTIPOLYGON_RE = re.compile(r'^multipolygon$', re.I)
LINESTRING_RE = re.compile(r'^linestring$', re.I)


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
        # get coords and if it's a multipolygon
        loc_type = update_json['location']['type']
        mp = False
        if POLYGON_RE.search(loc_type):
            coords = update_json['location']['coordinates'][0]
        elif MULTIPOLYGON_RE.search(loc_type):
            coords = update_json['location']['coordinates'][0]
            mp = True
        elif LINESTRING_RE.search(loc_type):
            coords = update_json['location']['coordinates']

        # add cities
        update_json['city'] = get_cities(coords, pop_th=0, multipolygon=mp)

        # add center if missing
        if 'center' not in update_json:
            if mp:
                center_lon, center_lat = get_center(coords[0])
                update_json['center'] = {
                    'type': 'point',
                    'coordinates': [center_lon, center_lat]
                }
            else:
                center_lon, center_lat = get_center(coords)
                update_json['center'] = {
                    'type': 'point',
                    'coordinates': [center_lon, center_lat]
                }

        # add closest continent
        lon, lat = update_json['center']['coordinates']
        continents = get_continents(lon, lat)
        update_json['continent'] = continents[0]['name'] if len(
            continents) > 0 else None

    # set temporal_span
    if update_json.get('starttime', None) is not None and \
       update_json.get('endtime', None) is not None:

        if isinstance(update_json['starttime'], str) and \
           isinstance(update_json['endtime'], str):

            start_time = update_json['starttime']
            end_time = update_json['endtime']
            update_json['temporal_span'] = get_ts(start_time, end_time)

    result = grq_es.index_document(index, update_json, update_json['id'])  # indexing to ES
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
