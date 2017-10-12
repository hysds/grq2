from flask import Flask

app = Flask(__name__)
app.config.from_pyfile('../settings.cfg')

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
