import requests
headers = {'User-Agent': 'UGA Finance Research teerth@uga.edu'}
url = "https://www.sec.gov"
try:
    r = requests.get(url, headers=headers, timeout=10)
    print(f"SEC Home Status: {r.status_code}")
except Exception as e:
    print(f"SEC Home Failed: {e}")
