import os, re, concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
import requests

OUTPUT_DIR = "output_hotfix"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Delete existing to start fresh
if os.path.exists(f"{OUTPUT_DIR}/table_2_available_funds.csv"):
    os.remove(f"{OUTPUT_DIR}/table_2_available_funds.csv")

def extract_tables(soup):
    master_list = []
    for t in soup.find_all('table'):
        try:
            text = t.get_text().lower()
            if not ('collection' in text or 'available' in text or 'deposit' in text or 'fund' in text):
                continue
            df = pd.read_html(str(t), flavor="bs4")[0].dropna(axis=1, how="all")
            # Only keep tables with more than 3 rows
            if len(df) > 3:
                # Rename columns to standard schema
                df.columns = ["label"] + [f"col_{i}" for i in range(1, len(df.columns))]
                master_list.append(df)
        except: pass
        
    for p in soup.find_all('pre'):
        text = p.get_text()
        rows = []
        for line in text.split("\n"):
            line = line.strip()
            if not line: continue
            match = re.match(r'^([A-Za-z\s]+)\s+([0-9\.,$\(\)\-]+)$', line)
            if match:
                rows.append({"label": match.group(1).strip(), "col_1": match.group(2).strip()})
        if rows:
            master_list.append(pd.DataFrame(rows))

    if master_list:
        return {"table_2_available_funds": pd.concat(master_list, ignore_index=True)}
    return {}

def scrape_one_url(url):
    try:
        acc = re.search(r'(\d{10}-\d{2}-\d{6})\.txt$', url).group(1)
        # Using requests with a highly aggressive 5 second socket read timeout
        resp = requests.get(url, headers={"User-Agent": "Teerth Patel teerth@uga.edu"}, timeout=(5, 5))
        doc_html = resp.text
        
        # Fast Regex Ex99 extraction
        ex99_match = re.search(r'<DOCUMENT>\s*<TYPE>EX-99.*?</HEADER>(.*?)</DOCUMENT>', doc_html, re.DOTALL | re.IGNORECASE)
        ex99 = ex99_match.group(1) if ex99_match else doc_html
        
        soup = BeautifulSoup(ex99, "lxml")
        tables = extract_tables(soup)
        
        cik_match = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', doc_html)
        name_match = re.search(r'COMPANY CONFORMED NAME:\s*(.*?)\n', doc_html)
        period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d+)', doc_html)
        
        meta = {
            "accession_number": acc, 
            "company_name": name_match.group(1).strip() if name_match else "", 
            "report_period": period_match.group(1) if period_match else "", 
            "cik": cik_match.group(1) if cik_match else ""
        }
        return {"url": url, "scraped": True, "meta": meta, "tables": tables}
    except Exception as e:
        return {"url": url, "scraped": False, "reason": str(e)}

if __name__ == "__main__":
    with open("remaining_repair_urls.txt", "r") as f:
        urls = [x.strip() for x in f.readlines() if x.strip()]

    print(f"Starting resilient scrape of {len(urls)} URLs...")
    
    metadata_rows = []
    table2_rows = []
    
    # Pre-warm an explicit requests session to bypass SEC dropping TLS handshakes
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=1)
    session.mount('https://', adapter)
    session.headers.update({"User-Agent": "Teerth Patel teerth@uga.edu"})
    
    # Overwrite scrape_one_url inside the loop scope to use the shared session
    def scrape_one_url_fast(url):
        try:
            acc = re.search(r'(\d{10}-\d{2}-\d{6})\.txt$', url).group(1)
            resp = session.get(url, timeout=(5, 5))
            doc_html = resp.text
            
            ex99_match = re.search(r'<DOCUMENT>\s*<TYPE>EX-99.*?</HEADER>(.*?)</DOCUMENT>', doc_html, re.DOTALL | re.IGNORECASE)
            ex99 = ex99_match.group(1) if ex99_match else doc_html
            
            soup = BeautifulSoup(ex99, "lxml")
            tables = extract_tables(soup)
            
            cik_match = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', doc_html)
            name_match = re.search(r'COMPANY CONFORMED NAME:\s*(.*?)\n', doc_html)
            period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d+)', doc_html)
            
            meta = {
                "accession_number": acc, 
                "company_name": name_match.group(1).strip() if name_match else "", 
                "report_period": period_match.group(1) if period_match else "", 
                "cik": cik_match.group(1) if cik_match else ""
            }
            return {"url": url, "scraped": True, "meta": meta, "tables": tables}
        except Exception as e:
            return {"url": url, "scraped": False, "reason": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for idx, result in enumerate(executor.map(scrape_one_url_fast, urls)):
            if result["scraped"]:
                metadata_rows.append(result["meta"])
                if "table_2_available_funds" in result["tables"]:
                    df = result["tables"]["table_2_available_funds"].copy()
                    df["accession_number"] = result["meta"]["accession_number"]
                    table2_rows.append(df)
            
            # Incremental save every 200 URLs
            if (idx + 1) % 200 == 0 or (idx + 1) == len(urls):
                print(f"Processed {idx + 1}/{len(urls)}")
                if metadata_rows:
                    mdf = pd.DataFrame(metadata_rows)
                    mode = 'a' if os.path.exists(f"{OUTPUT_DIR}/metadata.csv") else 'w'
                    mdf.to_csv(f"{OUTPUT_DIR}/metadata.csv", mode=mode, header=(mode=='w'), index=False)
                    metadata_rows = []
                    
                if table2_rows:
                    t2df = pd.concat(table2_rows, ignore_index=True)
                    mode = 'a' if os.path.exists(f"{OUTPUT_DIR}/table_2_available_funds.csv") else 'w'
                    t2df.to_csv(f"{OUTPUT_DIR}/table_2_available_funds.csv", mode=mode, header=(mode=='w'), index=False)
                    table2_rows = []
