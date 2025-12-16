# install_requirements.py
import sys, subprocess, os
HERE = os.path.dirname(os.path.abspath(__file__))
req = os.path.join(HERE, "requirements.txt")

# fallback if you don't have a requirements.txt:
default = ["flask", "requests", "qrcode"]

if os.path.exists(req):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", req])
else:
    subprocess.check_call([sys.executable, "-m", "pip", "install", *default])

print("✅ Dependencies installed.")