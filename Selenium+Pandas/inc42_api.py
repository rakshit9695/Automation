import requests
import pandas as pd
import json
import re
from urllib.parse import urljoin

class Inc42APIScraper:
    def __init__(self):
        self.base_url = "https://inc42.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
        })
        self.build_id = None
        self.company_data = []

    def get_build_id(self):
        """Extract Next.js build ID from __NEXT_DATA__ script"""
        response = self.session.get(f"{self.base_url}/company/")
        if response.status_code == 200:
            match = re.search(r'"buildId":"([^"]+)"', response.text)
            if match:
                self.build_id = match.group(1)
                return True
        return False

    def fetch_company_json(self):
        """Fetch structured company data from Next.js JSON endpoint"""
        if not self.build_id:
            return False
            
        json_url = f"{self.base_url}/_next/data/{self.build_id}/company.json"
        response = self.session.get(json_url)
        
        if response.status_code == 200:
            try:
                data = response.json()
                return data.get('pageProps', {}).get('pageData', [])
            except json.JSONDecodeError:
                return False
        return False

    def parse_company_data(self, raw_data):
        """Transform API response into structured format"""
        structured_data = []
        for company in raw_data:
            # Extract key details
            parsed = {
                'name': company.get('title'),
                'url': urljoin(self.base_url, company.get('url')),
                'sector': company.get('industry'),
                'founded_year': company.get('yearFounded'),
                'headquarters': company.get('headquarters'),
                'founders': ', '.join(company.get('founders', [])),
                'funding_amount': company.get('totalFunding'),
                'latest_funding_type': company.get('latestFundingType'),
                'investors': ', '.join(company.get('investors', []))
            }
            
            # Add additional fields from search results
            if 'description' in company:
                parsed['description'] = company['description'][:500]  # Truncate long text
                
            structured_data.append(parsed)
        
        return structured_data

    def scrape(self):
        """Main scraping method"""
        if not self.get_build_id():
            print("Failed to get build ID")
            return False
            
        raw_data = self.fetch_company_json()
        if not raw_data:
            print("Failed to fetch company data")
            return False
            
        self.company_data = self.parse_company_data(raw_data)
        return True

    def save_to_excel(self, filename="inc42_companies_api.xlsx"):
        """Save data to Excel with formatting"""
        if not self.company_data:
            return False
            
        df = pd.DataFrame(self.company_data)
        
        # Clean and format columns
        df['funding_amount'] = df['funding_amount'].replace('[\$,]', '', regex=True).astype(float)
        df = df.dropna(how='all')  # Remove completely empty rows
        
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Companies')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Companies']
        for idx, col in enumerate(df.columns):
            max_len = max((
                df[col].astype(str).map(len).max(),
                len(str(col))
            )) + 2
            worksheet.set_column(idx, idx, min(max_len, 50))
            
        writer.close()
        return True

if __name__ == "__main__":
    scraper = Inc42APIScraper()
    if scraper.scrape():
        if scraper.save_to_excel():
            print(f"Success! Saved {len(scraper.company_data)} companies to inc42_companies_api.xlsx")
            print("Sample data:")
            print(pd.DataFrame(scraper.company_data).head())
        else:
            print("Failed to save data")
    else:
        print("Scraping failed")
