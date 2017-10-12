from datetime import datetime
from flask import render_template, Blueprint

from grq2 import app


mod = Blueprint('views/main', __name__)


@app.errorhandler(404)
def page_not_found(e):
    error_msg = """Error code 404: Page doesn't exist. Please check the URL. 
                   If you feel there is an issue with our application, 
                   please contact geraldjohn.m.manipon__at__jpl.nasa.gov."""
    return render_template('error.html',
                           title='GRQ: Encountered Error',
                           current_year=datetime.now().year,
                           error_msg=error_msg), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('error.html',
                           title='GRQ: Encountered Error',
                           current_year=datetime.now().year,
                           error_msg="Error code 500: " + str(e)), 500


@app.errorhandler(501)
def unimplemented(e):
    return render_template('error.html',
                           title='GRQ: Encountered Error',
                           current_year=datetime.now().year,
                           error_msg="Error code 501: " + str(e)), 501


@mod.route('/')
def index():
    #app.logger.debug("Got here")
    return render_template('grq.html',
                           title='GRQ: GeoRegionQuery REST Service',
                           current_year=datetime.now().year)
