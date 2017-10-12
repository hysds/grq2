grq2
====

GeoRegionQuery, REST API using ElasticSearch backend.

Create virtualenv using SciFlo system packages
----------------------------------------------
virtualenv --system-site-packages env

Install Dependencies via pip
----------------------------
pip install flask
pip install gunicorn
pip install gevent

To run in development mode
--------------------------
python run.py

To run in production mode
--------------------------
As a daemon:       gunicorn -w2 -b 0.0.0.0:8878 -k gevent --daemon -p grq2.pid grq2:app
In the foreground: gunicorn -w2 -b 0.0.0.0:8878 -k gevent -p grq2.pid grq2:app
