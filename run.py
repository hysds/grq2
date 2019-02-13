from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from grq2 import app

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8878, debug=True)
