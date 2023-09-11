from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name='grq2',
    version='2.0.25',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'Flask<2.3.0',  # TODO: remove kluge when Flask-DebugToolbar fixes import error
        'flask-restx>=0.5.1',
        "elasticsearch>=7.0.0,<7.14.0",
        'shapely>=1.5.15',
        'Cython>=0.15.1',
        'Cartopy>=0.13.1',
        'future>=0.17.1',
        'gunicorn',
        'eventlet',
        'pymongo',
        'requests',
        'pyshp',
        'redis',
        "werkzeug>=2.2.0",  # TODO: remove this pin after fix has been made https://stackoverflow.com/a/73105878
    ]
)
