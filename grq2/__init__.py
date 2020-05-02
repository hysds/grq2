from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from flask import Flask
from flask_cors import CORS  # TODO: will remove this once we figure out the proper host for the UI

from grq2.es_connection import get_grq_es, get_mozart_es


class ReverseProxied(object):
    '''Wrap the application in this middleware and configure the 
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
    '''

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


app = Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app)
app.config.from_pyfile('../settings.cfg')

# TODO: will remove this when ready for actual release, need to figure out the right host
CORS(app)

# initializing connection to GRQ's Elasticsearch
ES_HOST = app.config['ES_HOST']
ES_PORT = app.config['ES_PORT']
AWS_REGION = app.config['AWS_REGION']
AWS_ES = app.config['AWS_ES']

grq_es = get_grq_es(es_host=ES_HOST, port=ES_PORT, logger=app.logger, region=AWS_REGION, aws_es_service=AWS_ES)

# initializing connection to Mozart's Elasticsearch
MOZART_ES_URL = app.config['MOZART_ES_URL']
mozart_es = get_mozart_es(MOZART_ES_URL, app.logger)

# views blueprints
from grq2.views.main import mod as viewsModule
app.register_blueprint(viewsModule)

# services blueprints
from grq2.services.main import mod as mainModule
app.register_blueprint(mainModule)

from grq2.services.query import mod as queryModule
app.register_blueprint(queryModule)

from grq2.services.geonames import mod as geonamesModule
app.register_blueprint(geonamesModule)

# rest API blueprints
from grq2.services.api_v01 import services as api_v01Services
app.register_blueprint(api_v01Services)

from grq2.services.api_v02 import services as api_v02Services
app.register_blueprint(api_v02Services)
