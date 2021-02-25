from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import json
from grq2 import app, grq_es
from elasticsearch.exceptions import RequestError

ILLEGAL_ARGUMENT_EXCEPTION = "illegal_argument_exception"
TOO_FEW_POINTS_ERROR = "too few points"
GEO_POLYGON = "geo_polygon"


def get_cities(polygon, size=5, multipolygon=False):
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
                                "gte": 1
                            }
                        }
                    }
                ]
            }
        }
    }

    # multipolygon?
    if multipolygon:
        # filtered is removed, using bool + should + minimum_should_match instead
        or_filters = []
        for p in polygon:
            or_filters.append({
                "geo_polygon": {
                    "location": {
                        "points": p,
                    }
                }
            })
        # filtered is removed, using bool + should + minimum_should_match instead
        query['query']['bool']['should'] = or_filters
        query['query']['bool']['minimum_should_match'] = 1

    else:
        query['query']['bool']['filter'].append({
            "geo_polygon": {
                "location": {
                    "points": polygon,
                }
            }
        })

    index = app.config['GEONAMES_INDEX']
    res = dict()
    try:
        res = grq_es.search(index=index, body=query)  # query for results
        app.logger.debug("get_cities(): %s" % json.dumps(query))
    except RequestError as re:
        app.logger.error("Request Error returned: status_code={}, error={}, info={}".format(
            re.status_code, re.error, json.dumps(re.info, indent=2)))
        reason = re.info.get("error", {}).get("reason", "")
        if re.error == ILLEGAL_ARGUMENT_EXCEPTION and TOO_FEW_POINTS_ERROR in reason and GEO_POLYGON in reason:
            app.logger.debug("Quietly proceeding with dataset ingest. This will be resolved when we upgrade to ES 7.9")
            pass
        else:
            raise re

    results = []
    for hit in res.get("hits", {}).get("hits", []):
        results.append(hit['_source'])
    return results


def get_nearest_cities(lon, lat, size=5):
    """
    :param lon: float lon of center (ex. -122.61067217547183)
    :param lat: float lat of center (ex. 40.6046338643702)
    :param size: return size of results
    :return: List[Dict]
    """
    query = {
        "size": size,
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
                            "feature_class": "P"
                        }
                    },
                    {
                        "range": {
                            "population": {
                                "gte": 1
                            }
                        }
                    },
                    {
                        "geo_distance": {
                            "distance": "100km",
                            "location": [lon, lat]
                        }
                    }
                ]
            }
        }
    }
    index = app.config['GEONAMES_INDEX']  # query for results
    res = grq_es.search(index=index, body=query)
    app.logger.debug("get_continents(): %s" % json.dumps(query, indent=2))

    results = []
    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results


def get_continents(lon, lat):
    """
    Spatial search of closest continents to the specified geo point.
    :param lon: float lon of center (ex. -122.61067217547183)
    :param lat: float lat of center (ex. 40.6046338643702)

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

    index = app.config['GEONAMES_INDEX']  # query for results
    res = grq_es.search(index=index, body=query)
    app.logger.debug("get_continents(): %s" % json.dumps(query, indent=2))

    results = []
    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results
