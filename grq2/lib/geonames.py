from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import json
from grq2 import app, grq_es


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
    res = grq_es.search(index, query)  # query for results
    app.logger.debug("get_cities(): %s" % json.dumps(query, indent=2))

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

    index = app.config['GEONAMES_INDEX']  # query for results
    res = grq_es.search(index, query)
    app.logger.debug("get_continents(): %s" % json.dumps(query, indent=2))

    results = []
    for hit in res['hits']['hits']:
        results.append(hit['_source'])
    return results
