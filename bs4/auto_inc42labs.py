import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import time
import random
import json
import re
from urllib.parse import urljoin, urlparse
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Inc42Scraper:
    def __init__(self, use_selenium=True):
        """Initialize the scraper with Inc42-specific configurations."""
        self.base_url = "https://inc42.com"
        self.company_base_url = f"{self.base_url}/company"
        self.use_selenium = use_selenium
        self.session = requests.Session()
        self.driver = None
        self.setup_session()
        self.companies_data = []
        self.discovered_companies = set()
        
    def setup_session(self):
        """Configure session with appropriate headers for Inc42."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(headers)
        
    def setup_selenium(self):
        """Setup Selenium WebDriver for JavaScript-heavy pages."""
        if not self.use_selenium:
            return
            
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f'--user-agent={self.session.headers["User-Agent"]}')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Selenium WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Selenium: {e}")
            self.use_selenium = False
    
    def get_page_content(self, url, max_retries=3, use_selenium=False):
        """Fetch page content with multiple methods."""
        for attempt in range(max_retries):
            try:
                if use_selenium and self.driver:
                    logger.info(f"Fetching with Selenium: {url}")
                    self.driver.get(url)
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    time.sleep(random.uniform(2, 4))
                    return self.driver.page_source
                else:
                    logger.info(f"Fetching with requests: {url}")
                    response = self.session.get(url, timeout=30)
                    if response.status_code == 200:
                        return response.text
                    else:
                        logger.warning(f"HTTP {response.status_code} for {url}")
                        
            except Exception as e:
                logger.error(f"Request failed for {url}: {e}")
                if attempt < max_retries - 1:
                    delay = (attempt + 1) * 5
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    
        return None
    
    def discover_company_urls_from_search(self, search_terms=None):
        """Discover company URLs through Inc42's search functionality."""
        if not search_terms:
            search_terms = [
                "startup", "company", "unicorn", "funding", "series a", "series b",
                "fintech", "edtech", "healthtech", "ecommerce", "saas", "b2b"
            ]
        
        discovered_urls = set()
        
        for term in search_terms:
            try:
                search_url = f"{self.base_url}/search?q={term}"
                content = self.get_page_content(search_url)
                
                if content:
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # Find company profile links
                    links = soup.find_all('a', href=True)
                    for link in links:
                        href = link.get('href')
                        if href and '/company/' in href and href not in discovered_urls:
                            full_url = urljoin(self.base_url, href)
                            discovered_urls.add(full_url)
                            logger.info(f"Discovered company URL: {full_url}")
                
                # Add delay between searches
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                logger.error(f"Error searching for '{term}': {e}")
        
        return list(discovered_urls)
    
    def discover_company_urls_from_articles(self, max_pages=10):
        """Discover company URLs by scanning Inc42 articles and news."""
        discovered_urls = set()
        
        # Common Inc42 sections that mention companies
        sections = [
            '/buzz/', '/features/', '/startups/', '/funding/', 
            '/resources/', '/latest-news/'
        ]
        
        for section in sections:
            try:
                for page in range(1, max_pages + 1):
                    section_url = f"{self.base_url}{section}page/{page}/"
                    content = self.get_page_content(section_url)
                    
                    if content:
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # Extract all links that point to company pages
                        links = soup.find_all('a', href=True)
                        for link in links:
                            href = link.get('href')
                            if href and '/company/' in href:
                                full_url = urljoin(self.base_url, href)
                                if full_url not in discovered_urls:
                                    discovered_urls.add(full_url)
                                    logger.info(f"Found company URL in {section}: {full_url}")
                    
                    # Delay between page requests
                    time.sleep(random.uniform(2, 4))
                    
            except Exception as e:
                logger.error(f"Error scanning section {section}: {e}")
        
        return list(discovered_urls)
    
    def extract_company_data(self, company_url):
        """Extract detailed company information from individual company pages."""
        try:
            content = self.get_page_content(company_url, use_selenium=self.use_selenium)
            if not content:
                return None
                
            soup = BeautifulSoup(content, 'html.parser')
            
            # Initialize company data structure
            company_data = {
                'Company_URL': company_url,
                'Company_Name': '',
                'Company_Type': '',
                'Status': '',
                'Location': '',
                'Founded_Year': '',
                'Sector': '',
                'Description': '',
                'Funding_Status': '',
                'Total_Funding': '',
                'Latest_Funding_Round': '',
                'Employees': '',
                'Website': '',
                'Social_Media': {},
                'Key_People': [],
                'Last_Updated': ''
            }
            
            # Extract company name
            name_selectors = [
                'h1', 
                '.company-name', 
                '[data-testid="company-name"]',
                'title'
            ]
            
            for selector in name_selectors:
                name_elem = soup.select_one(selector)
                if name_elem and name_elem.get_text(strip=True):
                    company_data['Company_Name'] = name_elem.get_text(strip=True)
                    break
            
            # Extract company description
            desc_selectors = [
                '.company-description',
                '.about-company',
                'meta[name="description"]',
                'p'
            ]
            
            for selector in desc_selectors:
                if selector.startswith('meta'):
                    desc_elem = soup.select_one(selector)
                    if desc_elem:
                        company_data['Description'] = desc_elem.get('content', '')
                        break
                else:
                    desc_elem = soup.select_one(selector)
                    if desc_elem and len(desc_elem.get_text(strip=True)) > 50:
                        company_data['Description'] = desc_elem.get_text(strip=True)[:500]
                        break
            
            # Extract company details from structured data or text
            page_text = soup.get_text()
            
            # Extract location
            location_patterns = [
                r'based in ([^,\n]+)',
                r'headquartered in ([^,\n]+)',
                r'located in ([^,\n]+)'
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    company_data['Location'] = match.group(1).strip()
                    break
            
            # Extract founding year
            year_pattern = r'founded in (\d{4})|(\d{4})'
            year_match = re.search(year_pattern, page_text, re.IGNORECASE)
            if year_match:
                company_data['Founded_Year'] = year_match.group(1) or year_match.group(2)
            
            # Extract sector/industry
            sector_patterns = [
                r'(fintech|edtech|healthtech|agritech|legaltech|proptech|insurtech)',
                r'(ecommerce|e-commerce|marketplace)',
                r'(saas|b2b|b2c|d2c)',
                r'(logistics|transport|mobility)',
                r'(food|foodtech|delivery)'
            ]
            
            for pattern in sector_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    company_data['Sector'] = match.group(1).title()
                    break
            
            # Extract funding information
            funding_patterns = [
                r'raised ([\$₹\d\.,\s]+(?:million|mn|crore|cr|billion|bn))',
                r'funding of ([\$₹\d\.,\s]+(?:million|mn|crore|cr|billion|bn))',
                r'series [a-z] (?:funding )?of ([\$₹\d\.,\s]+(?:million|mn|crore|cr|billion|bn))'
            ]
            
            for pattern in funding_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    company_data['Latest_Funding_Round'] = match.group(1).strip()
                    break
            
            # Extract company type
            type_patterns = [
                r'(private limited|public limited|unicorn|startup)',
                r'(bootstrapped|funded|public)'
            ]
            
            for pattern in type_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    company_data['Company_Type'] = match.group(1).title()
                    break
            
            # Try to extract structured data from JSON-LD or other formats
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                try:
                    json_data = json.loads(script.string)
                    if isinstance(json_data, dict):
                        if json_data.get('@type') == 'Organization':
                            company_data['Company_Name'] = json_data.get('name', company_data['Company_Name'])
                            company_data['Description'] = json_data.get('description', company_data['Description'])
                            company_data['Website'] = json_data.get('url', company_data['Website'])
                except:
                    continue
            
            # Clean and validate data
            for key, value in company_data.items():
                if isinstance(value, str):
                    company_data[key] = value.strip()
            
            # Only return data if we have at least a company name
            if company_data['Company_Name']:
                logger.info(f"Extracted data for: {company_data['Company_Name']}")
                return company_data
            else:
                logger.warning(f"No valid company data found for {company_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error extracting data from {company_url}: {e}")
            return None
    
    def scrape_inc42_companies(self, max_companies=100, discover_method='both'):
        """Main method to scrape Inc42 company data."""
        logger.info("Starting Inc42 company data scraping...")
        
        if self.use_selenium:
            self.setup_selenium()
        
        # Discover company URLs
        company_urls = set()
        
        if discover_method in ['search', 'both']:
            logger.info("Discovering companies through search...")
            search_urls = self.discover_company_urls_from_search()
            company_urls.update(search_urls)
        
        if discover_method in ['articles', 'both']:
            logger.info("Discovering companies through articles...")
            article_urls = self.discover_company_urls_from_articles()
            company_urls.update(article_urls)
        
        # Add some known company URLs to ensure we have data
        known_companies = [
            '/company/ola/', '/company/ola-electric/', '/company/paytm/',
            '/company/flipkart/', '/company/zomato/', '/company/swiggy/',
            '/company/byju/', '/company/oyo/', '/company/freshworks/'
        ]
        
        for company_path in known_companies:
            company_urls.add(f"{self.base_url}{company_path}")
        
        logger.info(f"Found {len(company_urls)} unique company URLs")
        
        # Limit to max_companies if specified
        if max_companies and len(company_urls) > max_companies:
            company_urls = list(company_urls)[:max_companies]
        
        # Extract data from each company
        successful_extractions = 0
        for i, company_url in enumerate(company_urls, 1):
            try:
                logger.info(f"Processing company {i}/{len(company_urls)}: {company_url}")
                
                company_data = self.extract_company_data(company_url)
                if company_data:
                    self.companies_data.append(company_data)
                    successful_extractions += 1
                    
                    # Save progress periodically
                    if successful_extractions % 10 == 0:
                        self.save_to_csv(f"inc42_companies_progress_{successful_extractions}.csv")
                
                # Add delay between requests
                delay = random.uniform(3, 7)
                logger.info(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
                
            except KeyboardInterrupt:
                logger.info("Scraping interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error processing {company_url}: {e}")
                continue
        
        logger.info(f"Scraping completed. Successfully extracted data for {successful_extractions} companies")
        
        if self.driver:
            self.driver.quit()
        
        return self.companies_data
    
    def save_to_csv(self, filename="inc42_companies.csv"):
        """Save extracted data to CSV file."""
        if not self.companies_data:
            logger.warning("No data to save")
            return
        
        try:
            df = pd.DataFrame(self.companies_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Data saved to {filename}")
            logger.info(f"Total records: {len(df)}")
            
            # Print summary statistics
            logger.info("\nData Summary:")
            logger.info(f"Companies with funding info: {df['Latest_Funding_Round'].notna().sum()}")
            logger.info(f"Companies with location: {df['Location'].notna().sum()}")
            logger.info(f"Sectors represented: {df['Sector'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
    
    def save_to_excel(self, filename="inc42_companies.xlsx"):
        """Save extracted data to Excel file with formatting."""
        if not self.companies_data:
            logger.warning("No data to save")
            return
        
        try:
            df = pd.DataFrame(self.companies_data)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Inc42_Companies', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Inc42_Companies']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Data saved to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")

def main():
    """Main execution function with example usage."""
    # Initialize scraper with Selenium support for JavaScript content
    scraper = Inc42Scraper(use_selenium=True)
    
    try:
        # Scrape first 20 companies for testing
        companies = scraper.scrape_inc42_companies(
            max_companies=20, 
            discover_method='both'
        )
        
        if companies:
            # Save to both formats
            scraper.save_to_csv("inc42_companies_sample.csv")
            scraper.save_to_excel("inc42_companies_sample.xlsx")
            
            # Display sample data
            df = pd.DataFrame(companies)
            print("\nSample of extracted Inc42 company data:")
            print(df[['Company_Name', 'Sector', 'Location', 'Latest_Funding_Round']].head(10))
            
            print(f"\nTotal companies extracted: {len(companies)}")
            print(f"Companies with complete data: {df.dropna(subset=['Company_Name', 'Sector']).shape[0]}")
            
        else:
            logger.warning("No company data was extracted")
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        if scraper.companies_data:
            scraper.save_to_csv("inc42_companies_interrupted.csv")
            logger.info("Partial data saved")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
