#!/usr/bin/env python
import os, sys, json, requests
from jinja2 import Template

from grq2 import app


def write_template(es_url, prefix, alias, tmpl_file):
    """Write template to ES."""

    with open(tmpl_file) as f:
        tmpl = Template(f.read()).render(prefix=prefix, alias=alias)
    tmpl_url = "%s/_template/%s" % (es_url, alias)
    r = requests.delete(tmpl_url)
    r = requests.put(tmpl_url, data=tmpl)
    r.raise_for_status()
    print r.json()
    print "Successfully installed template %s at %s." % (alias, tmpl_url)


if __name__ == "__main__":
    es_url = app.config['ES_URL']
    prefix = "grq"
    alias = "grq"
    tmpl_file = os.path.normpath(os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'config', 'es_template.json'
    )))
    write_template(es_url, prefix, alias, tmpl_file)
