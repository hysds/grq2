#!/usr/bin/env python
import os
import json
import sysconfig
import elasticsearch.exceptions
import opensearchpy.exceptions

from hysds.es_util import get_mozart_es
from grq2 import app

mozart_es = get_mozart_es()
USER_RULES_INDEX = app.config['USER_RULES_INDEX']


def get_package_path(subdir, filename):
    """Get path to package resource for both PyPI and editable installs."""
    # Try PyPI shared-data location first
    pypi_path = os.path.join(sysconfig.get_path('data'), 'share', 'grq2', subdir, filename)
    if os.path.exists(pypi_path):
        return pypi_path
    
    # Fallback to editable install location (relative to script)
    current_directory = os.path.dirname(__file__)
    editable_path = os.path.join(current_directory, '..', subdir, filename)
    editable_path = os.path.abspath(editable_path)
    editable_path = os.path.normpath(editable_path)
    
    if os.path.exists(editable_path):
        return editable_path
    
    # Return PyPI path even if it doesn't exist (for error messages)
    return pypi_path


def create_user_rules_index():
    """Create user rules index applying percolator mapping."""
    mapping_file = get_package_path('config', 'user_rules_dataset.mapping')

    with open(mapping_file) as f:
        mapping = json.load(f)

    mozart_es.es.indices.create(USER_RULES_INDEX, mapping, ignore=400)


try:
    create_user_rules_index()
except (elasticsearch.exceptions.RequestError, opensearchpy.exceptions.RequestError) as e:
    pass
except Exception as e:
    raise e
