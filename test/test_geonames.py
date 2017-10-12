#!/usr/bin/env python
import os, sys, json
from pprint import pprint

from grq2.lib.geonames import get_cities, get_continents
from grq2.lib.geo import get_center


def main():
    # test linestring
    with open('output.json') as f:
        m = json.load(f)
    coords = m['metadata']['location']['coordinates']
    lon, lat =  m['metadata']['center']['coordinates']
    cities = get_cities(coords, pop_th=0)
    continents = get_continents(lon, lat)
    if len(continents) > 0:
        continent = continents[0]['name']
    else:
        continent = None
    pprint(cities)
    print continent
    print get_center(coords)


if __name__ == "__main__":
    main()
