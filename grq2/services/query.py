import json, requests, types, re
from flask import jsonify, Blueprint, request, Response, render_template, make_response
from pprint import pformat
from shapely.geometry import box
from shapely.geometry.polygon import LinearRing
from redis import StrictRedis

from grq2 import app


mod = Blueprint('services/query', __name__)


@mod.route('/datasets', methods=['GET'])
def datasets():
    """Return available datsets."""

    return jsonify({
        'success': True,
        'message': "",
        'datasets': app.config['GRQ_DATASET_DOCTYPES'].keys()
    })


@mod.route('/grqByID', methods=['GET'])
def grqByID():
    """
    Return georegion info result for objectid as JSON.

    Example query DSL:
        {
          "fields": [
            "_timestamp", 
            "_source" 
          ], 
          "query": {
            "ids": {
              "values": [
                "EL20130830_674045_3058858.6.2"
              ]
            }
          }
        }
    """

    # status and message
    success = True
    message = ""

    # get query
    objectid = request.args.get('objectid', None)
    version = request.args.get('version', None)
    response_groups = request.args.get('responseGroups', 'Large')
    response_format = request.args.get('format', 'json')

    # if no objectid passed, show help
    if objectid is None:
        return render_template('geoRegionQuery_help.html'), 500

    # get dataset
    dataset = None
    for ds, regex in app.config['OBJECTID_DATASET_MAP'].items():
        match = re.search(regex, objectid)
        if match:
            dataset = ds
            break

    # error if cannot match dataset
    if dataset is None:
        return jsonify({
            'success': False,
            'message': "Cannot determine dataset for objectid %s." % objectid,
            'result': [],
            'count': 0
        }), 500

    # get index
    if version is not None:
        if version == 'latest':
            version = app.config['GRQ_UPDATE_INDEX_VERSION'][dataset]
        index = '%s_%s_%s' % (app.config['GRQ_INDEX'], version.replace('.', ''), dataset.lower())
    else:
        index = '%s_%s' % (app.config['GRQ_INDEX'], dataset.lower())
    
    # query
    es_url = app.config['ES_URL']
    query = {
        "fields": [
            "_timestamp",
            "_source"
        ],
        "query": {
            "ids": {
                "values": [ objectid ]
            }
        }
    }
    #app.logger.debug("ES query for grqByID(): %s" % json.dumps(query, indent=2))
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query))
    res = r.json()
    result = []
    if res['hits']['total'] > 0:
        # emulate result format from ElasticSearch <1.0
        if '_source' in res['hits']['hits'][0]:
            res['hits']['hits'][0]['fields'].update(res['hits']['hits'][0]['_source'])

        # get urls and metadata
        urls = res['hits']['hits'][0]['fields']['urls']
        metadata = res['hits']['hits'][0]['fields']['metadata']

        # add objectid as id
        metadata['id'] = res['hits']['hits'][0]['fields']['id']

        # add data system version
        metadata['data_system_version'] = res['hits']['hits'][0]['fields'].get('system_version', None)
    
        # add dataset
        metadata['dataset'] = res['hits']['hits'][0]['fields'].get('dataset', None)
    
        # return plain list of urls
        if response_format == 'text':
            if response_groups not in ['Url', 'Urls']:
                return jsonify({
                    'success': False,
                    'message': "format 'text' supported only for responseGroup 'Url' and 'Urls'",
                    'result': [],
                    'count': 0
                }), 500
            response = make_response('\n'.join(urls))
            response.content_type = 'text/plain'
            return response

        # return metadata with appropriate urls
        if response_groups == 'Large': metadata['url'] = urls
        else:
            if len(urls) > 0: metadata['url'] = urls[0]

        result.append(metadata) 

    return jsonify({
        'success': success,
        'message': message,
        'resultSet': result,
    })


@mod.route('/grq', methods=['GET'])
def grq():
    """
    Spatial/temporal search of datasets.

    Example query DSL:
        {
          "sort": {
            "_id": {
              "order": "desc"
            }
          }, 
          "fields": [
            "_timestamp", 
            "_source" 
          ], 
          "filter": {
            "and": [
              {
                "bool": {
                  "must": [
                    {
                      "range": {
                        "starttime": {
                          "lte": "2013-07-24T23:59:59Z"
                        }
                      }
                    }, 
                    {
                      "range": {
                        "endtime": {
                          "gte": "2013-07-24T00:00:00Z"
                        }
                      }
                    }
                  ]
                }
              }, 
              {
                "geo_shape": {
                  "location": {
                    "shape": {
                      "type": "envelope", 
                      "coordinates": [
                        [
                          "-119", 
                          "44"
                        ], 
                        [
                          "-110", 
                          "23"
                        ]
                      ]
                    }
                  }
                }
              }
            ]
          }, 
          "query": {
            "match_all": {}
          }
        }
    """

    # status and message
    success = True
    message = ""

    # get query
    dataset = request.args.get('dataset', None)
    level = request.args.get('level', None)
    version = request.args.get('version', None)
    starttime = request.args.get('starttime', request.args.get('sensingStart', None))
    endtime = request.args.get('endtime', request.args.get('sensingStop', None))
    lat_min = request.args.get('lat_min', request.args.get('latMin', -90.))
    lat_max = request.args.get('lat_max', request.args.get('latMax', 90.))
    lon_min = request.args.get('lon_min', request.args.get('lonMin', -180.))
    lon_max = request.args.get('lon_max', request.args.get('lonMax', 180.))
    response_groups = request.args.get('responseGroups', 'Medium')
    response_format = request.args.get('format', 'json')
    spatial = request.args.get('spatial', None)

    # set es_index to dataset
    # if dataset starts with CSK, use CSK index
    if dataset is not None and dataset.startswith('CSK'):
        es_index = 'CSK'
    else:
        es_index = dataset

    # use spacecraft name if dataset is None
    if dataset is None:
        if request.args.get('platform', '').startswith('CSK'):
            es_index = 'CSK'

    # let platform override datasets
    if request.args.get('platform', None) is not None:
        es_index = 'CSK'

    # loop through for non-standard query params
    other_params = {}
    for k in request.args.keys():
        if k in ('dataset', 'level', 'version', 'starttime', 'endtime',
                 'lat_min', 'lat_max', 'lon_min', 'lon_max', 'responseGroups',
                 'format', 'sensingStart', 'sensingStop', 'latMin', 'latMax',
                 'lonMin', 'lonMax', 'spatial'): continue
        other_params[k] = request.args.get(k)
    #app.logger.debug(pformat(other_params))

    # if no dataset passed, show help
    if dataset is None:
        return render_template('geoRegionQuery_help.html'), 500

    # date range filter
    if starttime is None and endtime is None:
        date_filter = None
    elif starttime is not None and endtime is not None:
        date_filter = {
            "bool": {
                "must": [
                    {
                        "range": {
                            "starttime": {
                                "lte": endtime
                            }
                        }
                    },
                    {
                        "range": {
                            "endtime": {
                                "gte": starttime
                            }
                        }
                    }
                ]
            }
        }
    else:
        return render_template('geoRegionQuery_help.html'), 500

    # location filter
    if lat_min == -90. and lat_max == 90. and lon_min == -180. and lon_max == 180.:
        loc_filter = None
        loc_filter_box = None
    else:
        loc_filter = {
            "geo_shape" : {
                "location" : {
                    "shape": {
                        "type": "envelope",
                        "coordinates": [
                            [ lon_min, lat_max ],
                            [ lon_max, lat_min ]
                        ]
                    }
                }
            }
        }
        loc_filter_box = box(*map(float, [lon_min, lat_min, lon_max, lat_max]))

    # build query
    query = {
        "sort": {
            "_id": {
                "order": "desc"
            }
        },
        "fields": [
            "_timestamp",
            "_source"
        ],
        "query": {
            "term": { "dataset": dataset }
        }
    }

    # add filters or query_string queries
    qs_queries = []
    filters = []
    if date_filter is not None: filters.append(date_filter)
    if loc_filter is not None: filters.append(loc_filter)
    for k in other_params:
        # fields to run query_string on (exact values and ranges)
        if k in ('latitudeIndexMin', 'latitudeIndexMax'):
            qs_query = {
                'query_string': {
                    'fields': [ k ],
                    'query': other_params[k]
                }
            }
            qs_queries.append(qs_query)
        else:
            term_filter = { 'term': {} }
            term_filter['term']['metadata.%s' % k] = other_params[k]
            filters.append(term_filter)
    if len(filters) > 0: query['filter'] = { "and": filters }
    if len(qs_queries) > 0: query['query'] = { "bool": { "must": qs_queries } }

    # get index
    if version is not None:
        if version == 'latest':
            version = app.config['GRQ_UPDATE_INDEX_VERSION'][es_index]
        index = '%s_%s_%s' % (app.config['GRQ_INDEX'], version.replace('.', ''), es_index.lower())
    else:
        index = '%s_%s' % (app.config['GRQ_INDEX'], es_index.lower())
    
    # query for results
    es_url = app.config['ES_URL']
    #app.logger.debug("ES query for grq(): %s" % json.dumps(query, indent=2))
    r = requests.post('%s/%s/_search?search_type=scan&scroll=10m&size=100' % (es_url, index), data=json.dumps(query))
    if r.status_code != 200:
        return jsonify({
            'success': False,
            'message': r.json(),
            'result': [],
            'count': 0
        })
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    results = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=10m' % es_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        for hit in res['hits']['hits']:
            # emulate result format from ElasticSearch <1.0
            if '_source' in hit: hit['fields'].update(hit['_source'])

            # spatial filter within vs. intersects (default)
            if spatial == 'within' and loc_filter_box is not None:
                #print hit['fields']['location']
                lr = LinearRing(hit['fields']['location']['coordinates'][0])
                #print loc_filter_box, lr
                if not lr.within(loc_filter_box): continue

            # get browse_urls, urls and metadata
            browse_urls = hit['fields']['browse_urls']
            urls = hit['fields']['urls']
            metadata = hit['fields']['metadata']

            # add objectid as id
            metadata['id'] = hit['fields']['id']
    
            # add location
            if 'location' not in metadata:
                metadata['location'] = hit['fields']['location']

            # add data system version
            metadata['data_system_version'] = hit['fields'].get('system_version', None)
    
            # add dataset
            metadata['dataset'] = hit['fields'].get('dataset', None)
    
            # return metadata with appropriate urls
            if response_groups == 'Large':
                metadata['browse_url'] = browse_urls
                metadata['url'] = urls
            else:
                if len(urls) > 0: metadata['url'] = urls[0]
                if len(browse_urls) > 0: metadata['browse_url'] = browse_urls[0]

            results.append(metadata)

    # return plain list of urls
    if response_format == 'text':
        urls = []
        for m in results:
            if isinstance(m['url'], types.ListType) and len(m['url']) > 0:
                urls.append(m['url'][0])
            else: urls.append(m['url'])
        if response_groups not in ['Url', 'Urls']:
            return jsonify({
                'success': False,
                'message': "format 'text' supported only for responseGroup 'Url' and 'Urls'",
                'result': [],
                'count': 0
            }), 500
        response = make_response('\n'.join(urls))
        response.content_type = 'text/plain'
        return response

    return jsonify({
        'success': success,
        'message': message,
        'result': results,
        'count': len(results)
    })


@mod.route('/grq_es', methods=['GET'])
def grq_es():
    """
    Query ElasticSearch directly returning GRQ JSON results.
    """

    # status and message
    success = True
    message = ""

    # set default query
    default_source = {
        "query": {
            "match_all": {}
        },
        "sort":[{"_timestamp":{"order":"desc"}}],
        "fields":["_timestamp","_source"]
    }

    # get query
    dataset = request.args.get('dataset', None)
    source = request.args.get('source', json.dumps(default_source))

    # if no dataset passed, show help
    if dataset is None:
        return render_template('grq_es_help.html'), 500

    # query for results
    es_url = app.config['ES_URL']
    index = '%s_%s' % (app.config['GRQ_INDEX'], dataset.lower())
    #app.logger.debug("ES query for grq(): %s" % json.dumps(query, indent=2))
    r = requests.post('%s/%s/_search?search_type=scan&scroll=60m&size=100' % (es_url, index), data=source)
    if r.status_code != 200:
        return jsonify({
            'success': False,
            'message': r.json(),
            'result': [],
            'count': 0
        })
    scan_result = r.json()
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']

    # stream JSON output a page at a time for better performance and lower memory footprint
    def stream_json(scroll_id):
        yield '{\n  "count": %d,\n  "message": "",\n  "result": [' % count
        res_count = 0
        while True:
            r = requests.post('%s/_search/scroll?scroll=60m' % es_url, data=scroll_id)
            res = r.json()
            scroll_id = res['_scroll_id']
            if len(res['hits']['hits']) == 0: break
            for hit in res['hits']['hits']:
                res_count += 1
                # emulate result format from ElasticSearch <1.0
                if '_source' in hit: hit['fields'].update(hit['_source'])
                metadata = hit['fields']['metadata']
    
                # get browse_urls, urls and metadata
                metadata['browse_url'] = hit['fields'].get('browse_urls', [])
                metadata['url'] = hit['fields'].get('urls', [])
    
                # add objectid as id
                metadata['id'] = hit['fields']['id']
        
                # add location
                metadata['location'] = hit['fields'].get('location', None)
    
                # add data system version
                metadata['data_system_version'] = hit['fields'].get('system_version', None)
        
                # add dataset
                metadata['dataset'] = hit['fields'].get('dataset', None)
        
                if res_count == 1: yield '\n%s' % json.dumps(metadata, indent=2)
                else: yield ',\n%s' % json.dumps(metadata, indent=2)
        yield '\n  ],\n  "success": true\n}'

    return Response(stream_json(scroll_id), mimetype="application/json")


@mod.route('/grq/redis/setnx', methods=['GET'])
def redis_setnx():
    """Return redis setnx results."""

    # get key and value
    key = "%s__%s" % (app.config['GRQ_INDEX'], request.args.get('key'))
    value = request.args.get('value')

    # execute
    red = StrictRedis.from_url(app.config['REDIS_URL'])
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
