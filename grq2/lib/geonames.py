from future import standard_library
standard_library.install_aliases()

import json
import elasticsearch.exceptions
import opensearchpy.exceptions

from grq2 import app, grq_es
from hysds_commons.search_utils import JitteredBackoffException

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
        query['query']['bool']['filter'].append({  # noqa
            "geo_polygon": {
                "location": {
                    "points": polygon,
                }
            }
        })
    index = app.config['GEONAMES_INDEX']
    try:
        res = grq_es.search(index=index, body=query)  # query for results
        app.logger.debug("get_cities(): %s" % json.dumps(query))

        results = []
        for hit in res['hits']['hits']:
            results.append(hit['_source'])
        return results
    except (elasticsearch.exceptions.NotFoundError,
            opensearchpy.exceptions.NotFoundError,
            JitteredBackoffException):
        return None
    except Exception as e:
        raise Exception(e)


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
    try:
        res = grq_es.search(index=index, body=query)
        app.logger.debug("get_continents(): %s" % json.dumps(query))

        results = []
        for hit in res['hits']['hits']:
            results.append(hit['_source'])
        return results
    except (elasticsearch.exceptions.NotFoundError,
            opensearchpy.exceptions.NotFoundError,
            JitteredBackoffException):
        return None
    except Exception as e:
        raise Exception(e)


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
    try:
        res = grq_es.search(index=index, body=query)
        app.logger.debug("get_continents(): %s" % json.dumps(query))

        results = []
        for hit in res['hits']['hits']:
            results.append(hit['_source'])
        return results
    except (elasticsearch.exceptions.NotFoundError,
            opensearchpy.exceptions.NotFoundError,
            JitteredBackoffException):
        return None
    except Exception as e:
        raise e
