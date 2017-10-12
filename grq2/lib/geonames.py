import json, requests, types
from pprint import pformat

from grq2 import app


def get_cities(polygon, pop_th=1000000, size=20, multipolygon=False):
    """
    Spatial search of top populated cities within a bounding box.

    Example query DSL:
      {
        "sort": {
          "population": {
            "order": "desc"
          }
        },
        "filter": {
          "and": [
            {
              "term": {
                "feature_class": "P"
              }
            },
            {
              "geo_polygon": {
                "location": {
                  "points": [
                    [
                      -119,
                      44
                    ],
                    [
                      110,
                      44
                    ],
                    [
                      110,
                      23
                    ],
                    [
                      -119,
                      23
                    ],
                    [
                      -119,
                      44
                    ]
                  ]
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

    # build query DSL
    query = {
        "size": size,
        "sort": {
            "population": {
                "order": "desc"
            }
        },
        "filter": {
            "and": [
                {
                    "term": {
                        "feature_class": "P"
                    }
                },
                {
                    "numeric_range": {
                        "population": {
                            "gte": pop_th,
                        }
                    }
                }
            ]
        },
        "query": {
            "match_all": {}
        }
    }

    # multipolygon?
    if multipolygon:
        or_filters = []
        for p in polygon:
            or_filters.append({
                "geo_polygon": {
                    "location": {
                        "points": p,
                    }
                }
            })
        query['filter']['and'].append({
            "or": or_filters
        })
    else:
        query['filter']['and'].append({
            "geo_polygon": {
                "location": {
                    "points": polygon,
                }
            }
        })

    # query for results
    es_url = app.config['ES_URL']
    index = app.config['GEONAMES_INDEX']
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query))
    app.logger.debug("get_cities(): %s" % json.dumps(query, indent=2))
    if r.status_code != 200:
        raise RuntimeError("Failed to get cities: %s" % pformat(r.json()))
    res = r.json()
    results = []
    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results


def get_continents(lon, lat):
    """
    Spatial search of closest continents to the specified geo point.

    Example query DSL:
      {
        "filter": {
          "and": [
            {
              "term": {
                "feature_class": "L"
              }
            },
            {
              "term": {
                "feature_code": "CONT"
              }
            }
          ]
        },
        "sort": [
          {
            "_geo_distance": {
              "location": [
                -84.531233,
                -78.472148
              ],
              "order": "asc",
              "unit": "km"
            }
          }
        ],
        "query": {
          "match_all": {}
        }
      }
    """

    # build query DSL
    query = {
        "filter": {
            "and": [
                {
                    "term": {
                        "feature_class": "L"
                    }
                },
                {
                    "term": {
                        "feature_code": "CONT"
                    }
                }
            ]
        },
        "sort": [
            {
                "_geo_distance": {
                    "location": [ lon, lat ],
                    "order": "asc",
                    "unit": "km"
                }
            }
        ],
        "query": {
            "match_all": {}
        }
    }

    # query for results
    es_url = app.config['ES_URL']
    index = app.config['GEONAMES_INDEX']
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query))
    app.logger.debug("get_continents(): %s" % json.dumps(query, indent=2))
    if r.status_code != 200:
        raise RuntimeError("Failed to get cities: %s" % pformat(r.json()))
    res = r.json()
    results = []
    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results
