from setuptools import setup, find_packages

setup(
    name='grq2',
    version='2.3.0',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    python_requires='>=3.12',
    install_requires=[
        'Flask<2.3.0',  # TODO: remove kluge when Flask-DebugToolbar fixes import error
        'flask-restx>=0.5.1',
        "elasticsearch>=7.0.0,<7.14.0",
        'opensearch-py>=2.3.0,<3.0.0',
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
        # TODO: remove this pin after fix has been made to resolve
        #  https://stackoverflow.com/questions/77213053/importerror-cannot-import-name-url-quote-from-werkzeug-urls
        "werkzeug<3.0.0",
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
