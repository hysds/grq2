import json, traceback, re, requests, types
from flask import jsonify, Blueprint, request
from pprint import pformat
from elasticsearch import Elasticsearch

from grq2 import app
from grq2.lib.geonames import get_cities, get_continents
from grq2.lib.geo import get_center
from grq2.lib.utils import parse_config
from grq2.lib.time_utils import getTemporalSpanInDays as get_ts


POLYGON_RE = re.compile(r'^polygon$', re.I)
MULTIPOLYGON_RE = re.compile(r'^multipolygon$', re.I)
LINESTRING_RE = re.compile(r'^linestring$', re.I)


def update(update_json):
    """Update GRQ metadata and urls for a product."""

    #app.logger.debug("update_json:\n%s" % json.dumps(update_json, indent=2))

    # get version
    version = update_json['version']

    # determine index name
    dataset = update_json.get('dataset', None)
    index_suffix = dataset

    # get default index and doctype
    doctype = dataset
    index = '%s_%s_%s' % (app.config['GRQ_INDEX'], version, index_suffix.lower())

    # get custom index and aliases
    aliases = []
    if 'index' in update_json:
        if 'suffix' in update_json['index']:
            index = '%s_%s' % (app.config['GRQ_INDEX'],
                               update_json['index']['suffix'].lower())
        aliases.extend(update_json['index'].get('aliases', []))
        del update_json['index']

    # add reverse geolocation data
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
            center_lon, center_lat = get_center(coords)
            update_json['center'] = {
                'type': 'point',
                'coordinates': [ center_lon, center_lat ]
            }

        # add closest continent
        lon, lat = update_json['center']['coordinates']
        continents = get_continents(lon, lat)
        update_json['continent'] = continents[0]['name'] if len(continents) > 0 else None

    # set temporal_span
    if update_json.get('starttime', None) is not None and \
       update_json.get('endtime', None) is not None:

        if isinstance(update_json['starttime'], types.StringTypes) and \
           isinstance(update_json['endtime'], types.StringTypes):
            update_json['temporal_span'] = get_ts(update_json['starttime'], update_json['endtime'])
        
    #app.logger.debug("update_json:\n%s" % json.dumps(update_json, indent=2))

    # update in elasticsearch
    try:
        es = Elasticsearch(hosts=[app.config['ES_URL']])
        ret = es.index(index=index, doc_type=doctype, id=update_json['id'], body=update_json)
    except Exception, e:
        message = "Got exception trying to index dataset: %s\n%s" % (str(e), traceback.format_exc())
        app.logger.debug(message)
        return jsonify({
            'success': False,
            'message': message,
            'update_json': update_json
        }), 500

    app.logger.debug("%s" % json.dumps(ret, indent=2))

    # update custom aliases
    if len(aliases) > 0:
        try:
            alias_ret = es.indices.update_aliases({
                "actions" : [
                    { "add" : { "index" : index, "alias" : aliases } }
                ]
            })
            #app.logger.debug("alias_ret: %s" % json.dumps(alias_ret, indent=2))
        except Exception, e:
            app.logger.debug("Got exception trying to add aliases to index: %s\n%s\nContinuing on." %
                             (str(e), traceback.format_exc()))

    return {
        'success': True,
        'message': ret,
        'objectid': update_json['id'],
        'index': index,
    }
