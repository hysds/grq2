from setuptools import setup, find_packages

setup(
    name='grq2',
    version='1.0.2',
    long_description='GeoRegionQuery REST API using ElasticSearch backend',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=['Flask', 'gunicorn', 'eventlet', 'supervisor', 'pymongo',
                      'elasticsearch>=1.0.0,<2.0.0', 'requests', 'pyshp',
                      'shapely==1.5.15', 'Cython', 'Cartopy==0.13.1', 'redis',
                      'flask-restplus>=0.9.2']
)
