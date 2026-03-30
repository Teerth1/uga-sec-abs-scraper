import os, re, urllib.request, concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
import scraper

OUTPUT_DIR = "output_hotfix"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_tables(soup):
    master_list = []
    
    # 1. Grab all standard HTML tables
    for t in soup.find_all('table'):
        df = scraper.extract_table(t, soup)
        if df is not None and not df.empty:
            master_list.append(df)
            
    # 2. Grab plain text PRE tables (for BMW)
    for p in soup.find_all('pre'):
        text = p.get_text()
        rows = []
        for line in text.split("\n"):
            line = line.strip()
            if not line: continue
            match = re.match(r'^((?:[A-Za-z]+\s*)+)\s+([0-9\.,$\(\) \-]+)$', line)
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
        req = urllib.request.Request(url, headers={"User-Agent": "Teerth Patel teerth@uga.edu"})
        doc_html = urllib.request.urlopen(req, timeout=10).read().decode("utf-8", errors="ignore")
        
        ex99 = scraper.extract_exhibit_99(doc_html)
        if ex99:
            soup = BeautifulSoup(ex99, "lxml")
            tables = extract_tables(soup)
            
            # Build metadata row
            cik_match = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', doc_html)
            cik = cik_match.group(1) if cik_match else ""
            name_match = re.search(r'COMPANY CONFORMED NAME:\s*(.*?)\n', doc_html)
            name = name_match.group(1).strip() if name_match else ""
            date_match = re.search(r'FILED AS OF DATE:\s*(\d+)', doc_html)
            date = date_match.group(1) if date_match else ""
            period_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d+)', doc_html)
            period = period_match.group(1) if period_match else ""
            
            meta = {"accession_number": acc, "company_name": name, "report_period": period, "filed_date": date, "cik": cik}
            return {"url": url, "scraped": True, "meta": meta, "tables": tables}
            
    except Exception as e:
        return {"url": url, "scraped": False, "reason": str(e)}
    return {"url": url, "scraped": False, "reason": "No Ex99"}

if __name__ == "__main__":
    with open("final_repair_urls.txt", "r") as f:
        urls = [x.strip() for x in f.readlines() if x.strip()]

    print(f"Starting hotfix scrape on {len(urls)} URLs...")

    # Create CSV headers if new
    if not os.path.exists(os.path.join(OUTPUT_DIR, "metadata.csv")):
        with open(os.path.join(OUTPUT_DIR, "metadata.csv"), "w") as f:
            f.write("accession_number,company_name,report_period,filed_date,cik,abs_class\n")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # SEC limit is 10/sec!
        results = list(executor.map(scrape_one_url, urls))

    # Save exactly like scraper.py's save_batch
    print("Saving results... ")
    scraper.OUTPUT_DIR = OUTPUT_DIR
    scraper.save_batch(results)
    print("Saved everything!")