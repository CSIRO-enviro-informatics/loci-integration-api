#!/bin/python3
# -*- coding: utf-8 -*-
"""
LOCI Integrator API
Copyright 2021 CSIRO Land and Water

@author Ashley Sommer <Ashley.Sommer@csiro.au>
@author Ben Leighton <Benjamin.Leighton@csiro.au>
"""
__authors__ = "Ashley Sommer, Ben Leighton"
__email__ = "Ashley.Sommer@csiro.au"
__maintainer__ = "Ashley Sommer <Ashley.Sommer@csiro.au>"
__copyright__ = "Copyright 2021 CSIRO Land and Water"
__license__ = "TBD"  # Open source or proprietary? Apache 2.0, or MIT?
__version__ = "0.0.1"

import os
from copy import copy
from sanic import Sanic
from sanic.log import LOGGING_CONFIG_DEFAULTS
from sanic.request import Request
from sanic.response import HTTPResponse
from spf import SanicPluginsFramework
from sanic_restplus.restplus import restplus
from sanic_cors.extension import cors
from api import api_v1
HERE_DIR = os.path.dirname(__file__)

import subprocess
gitlabel = subprocess.check_output(["git", "describe", "--always"]).strip().decode("utf-8")

def create_app():
    LOG_CONFIG = copy(LOGGING_CONFIG_DEFAULTS)
    LOG_CONFIG['loggers']['sanic.error']['level'] = "WARNING"  # Default is INFO
    LOG_CONFIG['loggers']['sanic.access']['level'] = "WARNING"  # Default is INFO
    app = Sanic(__name__, log_config=LOG_CONFIG, configure_logging=True)
    spf = SanicPluginsFramework(app)
    app.config['LOGO'] = r"""
     _     ___   ____ ___   ___ _   _ _____ _____ ____ ____     _  _____ ___  ____       _    ____ ___
    | |   / _ \ / ___|_ _| |_ _| \ | |_   _| ____/ ___|  _ \   / \|_   _/ _ \|  _ \     / \  |  _ |_ _|
    | |  | | | | |    | |   | ||  \| | | | |  _|| |  _| |_) | / _ \ | || | | | |_) |   / _ \ | |_) | |
    | |__| |_| | |___ | |   | || |\  | | | | |__| |_| |  _ < / ___ \| || |_| |  _ <   / ___ \|  __/| |
    |_____\___/ \____|___| |___|_| \_| |_| |_____\____|_| \_/_/   \_|_| \___/|_| \_\ /_/   \_|_|  |___|
    git commit: {}
    """.format(gitlabel)
    app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
    app.config.RESPONSE_TIMEOUT = 4800
    # Register/Activate Sanic-CORS plugin with allow all origins
    _ = spf.register_plugin(cors, origins=r".*", automatic_options=True)

    # Register/Activate Sanic-Restplus plugin
    restplus_associated = spf.register_plugin(restplus, _url_prefix="api")

    # Remove any previous apps from the api instance.
    # (this is needed during testing, the test runner does calls create_app many times)
    api_v1.spf_reg = None

    # Register our LOCI Api on the restplus plugin
    restplus_associated.api(api_v1)

    # Make the static directory available to be served via the /static/ route if needed
    # Note, it is preferred that something like apache or nginx does static file serving in production
    dir_loc = os.path.abspath(os.path.join(HERE_DIR, "static"))
    app.static(uri="/static/", file_or_directory=dir_loc, name="material_swagger")


    @app.route("/")
    def index(request):
        """
        Route function for the index route.
        Only exists to point a wayward user to the api swagger doc page.
        :param request:
        :type request: Request
        :return:
        :rtype: HTTPResponse
        """
        html = "<h1>LOCI Integration API</h1>\
        <a href=\"api/v1/doc\">Click here to go to the swaggerui doc page.</a>\
        <pre>Git commit: <a href=\"{prefix}{commit}\">{commit}</a></pre>".format(
              prefix="https://github.com/CSIRO-enviro-informatics/loci-integration-api/commit/",
              commit=str(gitlabel))
        return HTTPResponse(html, status=200, content_type="text/html")

    return app

if __name__ == "__main__":
    # Has run from the command line.
    # This section will not be called if run via Gunicorn or mod_wsgi
    LISTEN_HOST = "0.0.0.0"
    LISTEN_PORT = 8080
    app = create_app()
    app.run(LISTEN_HOST, LISTEN_PORT, debug=False, auto_reload=False)
