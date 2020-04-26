#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from builtins import dict
from builtins import zip
from builtins import int
from builtins import map
from future import standard_library
standard_library.install_aliases()

import traceback
from elasticsearch import Elasticsearch


'''
geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated varchar(5000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for
                    display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US
                    see file admin2Codes.txt;varchar(80)
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int)
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30''
                    (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format
'''

CONTINENT_CODES = {
    'AF': 'Africa',
    'AS': 'Asia',
    'EU': 'Europe',
    'NA': 'North America',
    'OC': 'Oceania',
    'SA': 'South America',
    'AN': 'Antarctica'
}


FIELD_NAMES = [
    'geonameid',
    'name',
    'asciiname',
    'alternatename',
    'latitude',
    'longitude',
    'feature_class',
    'feature_code',
    'country_code',
    'cc2',
    'admin1_code',
    'admin2_code',
    'admin3_code',
    'admin4_code',
    'population',
    'elevation',
    'dem',
    'timezone',
    'modification_date'
]


INDEX = 'geonames'
MAPPING = {
    'properties': {
        'geonameid': {'type': 'keyword'},
        'name': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'asciiname': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'alternatename': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'latitude': {'type': 'double'},
        'longitude': {'type': 'double'},
        'feature_class': {'type': 'keyword'},
        'feature_code': {'type': 'keyword'},
        'country_code': {'type': 'keyword'},
        'cc2': {'type': 'keyword'},
        'admin1_code': {'type': 'keyword'},
        'admin2_code': {'type': 'keyword'},
        'admin3_code': {'type': 'keyword'},
        'admin4_code': {'type': 'keyword'},
        'population': {'type': 'long'},
        'elevation': {'type': 'long'},
        'dem': {'type': 'text'},
        'timezone': {'type': 'text'},
        'modification_date': {'type': 'date'},
        'location': {'type': 'geo_point'},
        'continent_code': {'type': 'keyword'},
        'continent_name': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'country_name': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'admin1_name': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        },
        'admin2_name': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword'}}
        }
    }
}


ES_URL = 'http://localhost:9200'


def get_admin_dict(admin_file):
    d = {}
    with open(admin_file) as f:
        for line in f:
            vals = line.strip().split('\t')
            d[vals[0]] = vals[1:]
    return d


def get_country_dict(country_file):
    d = {}
    with open(country_file) as f:
        for line in f:
            if line.startswith('#'):
                continue
            vals = line.strip().split('\t')
            d[vals[0]] = vals[1:]
    return d


def parse(csv_file):

    # get adm dicts
    adm1 = get_admin_dict('admin1CodesASCII.txt')
    adm2 = get_admin_dict('admin2Codes.txt')

    # get country dict
    cntries = get_country_dict('countryInfo.txt')

    # get ElasticSearch connection
    es = Elasticsearch(hosts=[ES_URL])
    es.indices.create(INDEX, {'mappings': MAPPING}, ignore=400)
    print('%s index created!!' % INDEX)

    # iterate and index
    line_number = 0
    try:
        with open(csv_file) as f:
            for line in f:
                row = dict(list(zip(FIELD_NAMES, line.strip().split('\t'))))
                line_number += 1
                for k in row:
                    if row[k] == '':
                        if k in ('alternatename', 'cc2'):
                            row[k] = []
                        else:
                            row[k] = None
                    else:
                        if k in ('alternatename', 'cc2'):
                            row[k] = list(map(str.strip, row[k].split(',')))
                    if k in ('latitude', 'longitude'):
                        row[k] = float(row[k])
                    if k in ('population', 'elevation'):
                        if row[k] is not None:
                            row[k] = int(row[k])

                # add geo_point location for spatial query
                row['location'] = {
                    'lon':  row['longitude'],
                    'lat':  row['latitude']
                }

                # add continent, adm1 and amd2 names
                row['continent_code'] = None
                row['continent_name'] = None
                row['country_name'] = row['country_code']
                row['admin1_name'] = None
                row['admin2_name'] = None
                if row['country_code'] is not None:
                    if row['country_code'] in cntries:
                        row['country_name'] = cntries[row['country_code']][3]
                        row['continent_code'] = cntries[row['country_code']][7]
                        row['continent_name'] = CONTINENT_CODES[row['continent_code']]
                    if row['admin1_code'] is not None:
                        adm1_code = '%s.%s' % (
                            row['country_code'], row['admin1_code'])
                        row['admin1_name'] = adm1.get(adm1_code, [None])[0]
                        if row['admin2_code'] is not None:
                            adm2_code = '%s.%s' % (
                                adm1_code, row['admin2_code'])
                            row['admin2_name'] = adm2.get(adm2_code, [None])[0]

                # index
                es.index(index=INDEX, id=row['geonameid'], body=row)
                if line_number % 10000 == 0:
                    print('%d documents ingested into %s' % (line_number, INDEX))
    except Exception as e:
        traceback.print_exc()
        print(("line_number: %d" % line_number))
        raise


if __name__ == "__main__":
    csv_file = 'allCountries.txt'
    parse(csv_file)
