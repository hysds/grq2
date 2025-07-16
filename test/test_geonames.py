#!/usr/bin/env python
from future import standard_library
standard_library.install_aliases()

import json

from grq2.lib.geonames import get_cities, get_continents


def main():
    # test linestring
    with open('output.json') as f:
        m = json.load(f)

    location = m['metadata']['location']
    coords = location['coordinates']

    lon, lat = m['metadata']['center']['coordinates']

    # cities = get_cities(coords)
    continents = get_continents(lon, lat)

    if len(continents) > 0:
        continent = continents[0]['name']
    else:
        continent = None
    print(continent)


if __name__ == "__main__":
    main()
