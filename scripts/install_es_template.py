#!/usr/bin/env python
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import open
from future import standard_library
standard_library.install_aliases()
import os
from jinja2 import Template

from grq2 import grq_es


def write_template(prefix, alias, tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = Template(f.read()).render(prefix=prefix, alias=alias)

    # https://elasticsearch-py.readthedocs.io/en/1.3.0/api.html#elasticsearch.Elasticsearch.put_template
    grq_es.es.indices.put_template(name=alias, body=tmpl)
    print(("Successfully installed template %s" % alias))


if __name__ == "__main__":
    prefix = "grq"
    alias = "grq"

    current_file = os.path.dirname(__file__)
    tmpl_file = os.path.abspath(os.path.join(current_file, '..', 'config', 'es_template.json'))
    tmpl_file = os.path.normpath(tmpl_file)

    write_template(prefix, alias, tmpl_file)
