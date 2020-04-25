#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import os
import json
from grq2 import app, mozart_es

USER_RULES_INDEX = app.config['USER_RULES_INDEX']


# Create user rules index applying percolator mapping.
mapping_file = os.path.join(app.root_path, '..', 'config', 'user_rules_dataset.mapping')
mapping_file = os.path.normpath(mapping_file)

with open(mapping_file) as f:
    mapping = json.load(f)

mozart_es.es.indices.create(USER_RULES_INDEX, mapping, ignore=400)
