import requests, time, re
from bs4 import BeautifulSoup

def get_mercedes_collections(acc):
    cik = "1878122" # Mercedes-Benz 2021-1 example
    acc_clean = acc.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{acc}.txt"
    
    headers = {"User-Agent": "Teerth Patel (tmp00726@uga.edu)"}
    r = requests.get(url, headers=headers, timeout=20)
    if r.status_code != 200:
        return None
    
    txt = r.text
    # Finding the 10-D part or EX-99
    # Newer Mercedes filings are often in the main 10-D or EX-99
    # Let's search for "Available Funds" in the text
    
    # Absolute positioning parsing logic:
    # We look for DIVs with top/left
    soup = BeautifulSoup(txt, "lxml")
    
    # Check for Tables first just in case
    tables = soup.find_all("table")
    for t in tables:
        if "Available Funds" in t.get_text():
            return "MATCHED TABLE"
            
    # If no table, look for absolute positioning
    divs = soup.find_all("div", style=re.compile(r"position\s*:\s*absolute"))
    if not divs:
        return "NO ABS DIVS"
        
    # Find the DIV that says "Available Funds"
    target_div = None
    for d in divs:
        if "Available Funds" in d.get_text():
            target_div = d
            break
            
    if not target_div:
        return "NO TARGET DIV"
        
    # Get the 'top' of this div to find other elements on the same or nearby lines
    match = re.search(r"top\s*:\s*(\d+)", target_div.get("style", ""))
    if not match:
        return "NO TOP STYLE"
        
    target_top = int(match.group(1))
    
    # Find elements within a small vertical range of target_top
    results = []
    for d in divs:
        m = re.search(r"top\s*:\s*(\d+)", d.get("style", ""))
        if m:
            top = int(m.group(1))
            if abs(top - target_top) < 10:
                results.append((int(re.search(r"left\s*:\s*(\d+)", d.get("style", "")).group(1)), d.get_text(strip=True)))
                
    results.sort()
    return results

acc = "0001853620-21-000253"
print(f"Testing {acc}...")
res = get_mercedes_collections(acc)
print(res)
