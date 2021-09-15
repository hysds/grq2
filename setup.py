from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name='grq2',
    version='2.0.13',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # TODO: remove this pin on click once this celery issue is resolved:
        # https://github.com/celery/celery/issues/6768
        # 'click>=7.0,<8.0',  # 'click>=7.0,<8.0',  # don't think we need click here because its nto used by mozart
        'Flask>2.0.0,<3.0.0',
        'flask-restx>=0.5.1',
        'elasticsearch>=7.0.0,<7.14.0',
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
    ]
)
