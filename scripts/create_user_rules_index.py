#!/usr/bin/env python
import os
import json
import elasticsearch.exceptions
import opensearchpy.exceptions

from hysds.es_util import get_mozart_es
from grq2 import app

mozart_es = get_mozart_es()
USER_RULES_INDEX = app.config['USER_RULES_INDEX']


def create_user_rules_index():
    """Create user rules index applying percolator mapping."""
    current_directory = os.path.dirname(__file__)
    mapping_file = os.path.join(current_directory, '..', 'config', 'user_rules_dataset.mapping')
    mapping_file = os.path.abspath(mapping_file)
    mapping_file = os.path.normpath(mapping_file)

    with open(mapping_file) as f:
        mapping = json.load(f)

    mozart_es.es.indices.create(USER_RULES_INDEX, mapping, ignore=400)


try:
    create_user_rules_index()
except (elasticsearch.exceptions.RequestError, opensearchpy.exceptions.RequestError) as e:
    pass
except Exception as e:
    raise e
