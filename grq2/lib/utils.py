from future import standard_library
standard_library.install_aliases()
import os
import sys
import json

from grq2 import app


def parse_config(json_file):
    """Parse json config files sitting in config directory and return json."""

    path = os.path.join(app.root_path, '..', 'config', json_file)
    with open(path) as f:
        j = json.load(f)
    return j
