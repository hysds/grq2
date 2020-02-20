from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from flask import Flask

from hysds_commons.elasticsearch_utils import ElasticsearchUtility


app = Flask(__name__)
app.config.from_pyfile('../settings.cfg')

# initializing connection to GRQ's Elasticsearch
grq_es = ElasticsearchUtility(app.config['ES_URL'], app.logger)

# initializing connection to Mozart's Elasticsearch
mozart_es = ElasticsearchUtility(app.config['MOZART_ES_URL'], app.logger)

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
