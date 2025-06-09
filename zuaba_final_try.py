import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from datetime import datetime

class ZaubaCorpAddressScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://www.zaubacorp.com/companies-list"

    def fetch_html(self, url):
        api_url = "http://api.scrapestack.com/scrape"
        params = {
            "access_key": self.api_key,
            "url": url,
            "render_js": 1
        }
        print(f"[DEBUG] Fetching: {url}")
        resp = requests.get(api_url, params=params)
        if resp.status_code == 200:
            return resp.text
        else:
            print(f"[ERROR] ScrapeStack error {resp.status_code}: {resp.text[:200]}")
            return None

    def get_companies_by_address(self, address_keyword, max_pages=5):
        results = []
        for page in range(1, max_pages+1):
            url = f"{self.base_url}/p-{page}-company.html"
            html = self.fetch_html(url)
            if not html:
                break
            page_companies = self.parse_company_cards(html, address_keyword)
            print(f"[DEBUG] Page {page}: {len(page_companies)} companies matched.")
            if not page_companies:
                break
            results.extend(page_companies)
        print(f"[DEBUG] Total companies matched: {len(results)}")
        return results

    def parse_company_cards(self, html, address_keyword):
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        # Each company is inside a div with class 'col-lg-4 col-md-6 col-sm-6 col-xs-12'
        cards = soup.find_all('div', class_=re.compile(r'col-lg-4.*col-xs-12'))
        for card in cards:
            # Company Name and Link
            name_tag = card.find('a', href=True)
            name = name_tag.get_text(strip=True) if name_tag else ''
            link = "https://www.zaubacorp.com" + name_tag['href'] if name_tag else ''
            # CIN is usually in a <p> or <span> nearby
            cin = ''
            address = ''
            for p in card.find_all('p'):
                txt = p.get_text(strip=True)
                if txt.startswith('CIN'):
                    cin = txt.replace('CIN:', '').strip()
                elif txt.startswith('Address'):
                    address = txt.replace('Address:', '').strip()
            if not address:
                # Sometimes address is in a span or elsewhere
                for span in card.find_all('span'):
                    txt = span.get_text(strip=True)
                    if 'Address' in txt:
                        address = txt.replace('Address:', '').strip()
            # Address filter (case-insensitive substring match)
            if address_keyword.lower() in address.lower():
                companies.append({
                    'cin': cin,
                    'name': name,
                    'address': address,
                    'link': link
                })
        return companies

    def export(self, companies, address_keyword, fmt="csv"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_kw = re.sub(r'\W+', '_', address_keyword)
        if fmt == "csv":
            filename = f"zaubacorp_address_{safe_kw}_{timestamp}.csv"
            pd.DataFrame(companies).to_csv(filename, index=False)
            print(f"💾 Exported {len(companies)} companies to {filename}")
        elif fmt == "json":
            filename = f"zaubacorp_address_{safe_kw}_{timestamp}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(companies, f, indent=2, ensure_ascii=False)
            print(f"💾 Exported {len(companies)} companies to {filename}")
        else:
            print("[ERROR] Unknown export format.")

def main():
    api_key = input("Enter your ScrapeStack API key: ").strip()
    address_keyword = input("Enter address keyword to search: ").strip()
    max_pages = input("How many pages to scan? (default 5): ").strip()
    max_pages = int(max_pages) if max_pages.isdigit() else 5
    output_fmt = input("Export format? (csv/json, default csv): ").strip().lower() or "csv"

    scraper = ZaubaCorpAddressScraper(api_key)
    companies = scraper.get_companies_by_address(address_keyword, max_pages=max_pages)
    if companies:
        scraper.export(companies, address_keyword, fmt=output_fmt)
    else:
        print("No companies found with that address keyword.")

if __name__ == "__main__":
    main()
