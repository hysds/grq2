from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from flask import Flask, jsonify
from flask_cors import CORS  # TODO: will remove this once we figure out the proper host for the UI

from hysds.es_util import get_grq_es, get_mozart_es


class ReverseProxied(object):
    """
    Wrap the application in this middleware and configure the
    front-end server to add these headers, to let you quietly bind 
    this to a URL other than / and to an HTTP scheme that is 
    different than what is used locally.

    In nginx:
        location /myprefix {
            proxy_pass http://127.0.0.1:8878;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Scheme $scheme;
            proxy_set_header X-Script-Name /myprefix;
        }

    In apache:
        RewriteEngine on
        RewriteRule "^/grq$" "/grq/" [R]
        SSLProxyEngine on
        ProxyRequests Off
        ProxyPreserveHost Off
        ProxyPass /grq/static !
        ProxyPass /grq/ http://localhost:8878/
        ProxyPassReverse /grq/ http://localhost:8878/
        <Location /grq>
            Header add "X-Script-Name" "/grq"
            RequestHeader set "X-Script-Name" "/grq"
            Header add "X-Scheme" "https"
            RequestHeader set "X-Scheme" "https"
        </Location>
        Alias /grq/static/ /home/ops/sciflo/ops/grq2/grq2/static/
        <Directory /home/ops/sciflo/ops/grq2/grq2/static>
            Options Indexes FollowSymLinks
            AllowOverride All
            Require all granted
        </Directory>

    :param app: the WSGI application
    """

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        x_forwarded_host = environ.get('HTTP_X_FORWARDED_HOST', '')
        if x_forwarded_host:
            environ['HTTP_HOST'] = x_forwarded_host
        return self.app(environ, start_response)


def resource_not_found(e):
    return jsonify({
        'status_code': 404,
        'message': str(e)
    }), 404


app = Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app)
app.config.from_pyfile('../settings.cfg')

# TODO: will remove this when ready for actual release, need to figure out the right host
CORS(app)

# handle global errors
app.register_error_handler(404, resource_not_found)

# initializing connection to GRQ's Elasticsearch
grq_es = get_grq_es()

# initializing connection to Mozart's Elasticsearch
mozart_es = get_mozart_es()

# services blueprints
from grq2.services.main import mod as main_module
app.register_blueprint(main_module)

from grq2.services.query import mod as query_module
app.register_blueprint(query_module)

from grq2.services.geonames import mod as geonames_module
app.register_blueprint(geonames_module)

# rest API blueprints
from grq2.services.api_v01.service import services as api_v01_services
app.register_blueprint(api_v01_services)

from grq2.services.api_v02.service import services as api_v02_services
app.register_blueprint(api_v02_services)


if __name__ != '__main__':
    import logging

    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
