# passenger_wsgi.py — clean WSGI loader (no self-imports!)
import os, sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Import your Flask app and expose it as WSGI callable "application"
from chapaa import app as application