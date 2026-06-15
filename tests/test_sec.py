import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys

print("Python version:", sys.version)
print("Pandas version:", pd.__version__)
print("Requests version:", requests.__version__)

url = "https://www.sec.gov/Archives/edgar/data/950170/000095017020012345/0000950170-20-012345-index.htm"
headers = {'User-Agent': 'UGA Finance Research teerth@uga.edu'}

print(f"Testing request to {url}...")
try:
    r = requests.get(url, headers=headers, timeout=10)
    print(f"Status Code: {r.status_code}")
    if r.status_code == 200:
        print("Successfully reached SEC EDGAR.")
except Exception as e:
    print(f"Request failed: {e}")
