from future import standard_library
standard_library.install_aliases()
from grq2 import app

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8868, debug=True)
