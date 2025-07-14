#!/usr/bin/env python
from future import standard_library
standard_library.install_aliases()
import sys

from hysds.es_util import get_grq_es

from grq2.lib.utils import parse_config


# get destination index and doctype
dest = sys.argv[1]
doctype = sys.argv[2]

# get connection and create destination index
grq_es = get_grq_es()
grq_es.es.indices.create(index=dest, ignore=400)
