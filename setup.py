from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name='grq2',
    version='2.1.0',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.12',
    install_requires=[
        'Flask>=3.0.0,<4.0.0',
        'flask-restx>=1.1.0,<2.0.0',
        'elasticsearch>=8.11.0,<9.0.0',
        'opensearch-py>=2.3.0,<3.0.0',
        'shapely>=2.0.1',
        'Cython>=3.0.0',
        'Cartopy>=0.22.0',
        'gunicorn>=21.2.0',
        'eventlet>=0.33.3',
        'pymongo>=4.5.0',
        'requests>=2.31.0',
        'pyshp>=2.3.1',
        'redis>=5.0.0',
        'Werkzeug>=3.0.0,<4.0.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.4.0',
            'pytest-cov>=4.1.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'isort>=5.12.0',
        ]
    }
)
