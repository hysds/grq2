from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
import json
import requests
import types
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
      "query": {
        "bool": {
          "must": [],
          "filter": [
            {
              "term": {
                "feature_class": "P"
              }
            },
            {
              "range": {
                "population": {
                  "gte": 0
                }
              }
            },
            {
              "geo_polygon": {
                "location": {
                  "points": [
                    [-119, 44],
                    [110, 44],
                    [110, 23],
                    [-119, 23],
                    [-119, 44]
                  ]
                }
              }
            }
          ]
        }
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
        "query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "feature_class": "P"
                        }
                    },
                    {
                        "range": {
                            "population": {
                                "gte": 0
                            }
                        }
                    }
                ]
            }
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
        # TODO: need to figure out how to add multi polygon logic (maybe use "should"???)
        query['query']['bool']['filter'].append({
            "or": or_filters
        })
    else:
        query['query']['bool']['filter'].append({
            "geo_polygon": {
                "location": {
                    "points": polygon,
                }
            }
        })

    # query for results
    es_url = app.config['ES_URL']
    index = app.config['GEONAMES_INDEX']

    headers = {'Content-Type': 'application/json'}
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query), headers=headers)

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
      "sort": [
        {
          "_geo_distance": {
            "location": [-84.531233, -78.472148],
            "order": "asc",
            "unit": "km"
          }
        }
      ],
      "query": {
        "bool": {
          "filter": [
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
        }
      }
    }
    """

    # build query DSL
    query = {
        "sort": [
            {
                "_geo_distance": {
                    "location": [lon, lat],
                    "order": "asc",
                    "unit": "km"
                }
            }
        ],
        "query": {
            "bool": {
                "filter": [
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
            }
        }
    }

    # query for results
    es_url = app.config['ES_URL']
    index = app.config['GEONAMES_INDEX']

    headers = {'Content-Type': 'application/json'}
    r = requests.post('%s/%s/_search' % (es_url, index), data=json.dumps(query), headers=headers)

    app.logger.debug("get_continents(): %s" % json.dumps(query, indent=2))

    if r.status_code != 200:
        raise RuntimeError("Failed to get cities: %s" % pformat(r.json()))

    res = r.json()
    results = []

    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results
