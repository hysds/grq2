from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name='grq2',
    version='1.1.2',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=['Flask', 'gunicorn', 'eventlet', 'pymongo',
                      'elasticsearch>=1.0.0,<2.0.0', 'requests', 'pyshp',
                      'shapely>=1.5.15', 'Cython>=0.15.1', 'Cartopy>=0.13.1',
                      # TODO: remove installation of master branch after new release of
                      # flask-restx includes the fix referred to here:
                      # https://github.com/python-restx/flask-restx/issues/85
                      'flask-restx @ git+https://git@github.com/python-restx/flask-restx',
                      #'redis', 'flask-restx>0.1.1', 'future>=0.17.1']
                      'redis', 'future>=0.17.1']
)
