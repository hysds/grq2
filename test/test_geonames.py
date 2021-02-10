#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()

import json
from pprint import pprint

from shapely.geometry import shape

from grq2.lib.geonames import get_cities, get_continents


def main():
    # test linestring
    with open('output.json') as f:
        m = json.load(f)

    location = m['metadata']['location']
    coords = location['coordinates']

    lon, lat = m['metadata']['center']['coordinates']

    cities = get_cities(coords, pop_th=0)
    continents = get_continents(lon, lat)

    if len(continents) > 0:
        continent = continents[0]['name']
    else:
        continent = None

    pprint(cities)
    print(continent)

    geo_shape = shape(location)
    centroid = geo_shape.centroid
    print(centroid.x, centroid.y)


if __name__ == "__main__":
    main()
