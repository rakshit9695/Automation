import requests
from bs4 import BeautifulSoup
import pandas as pd
import csv
import time
import random
from urllib.parse import urljoin, urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ZaubaCorpScraper:
    def __init__(self):
        """Initialize the scraper with necessary configurations."""
        self.base_url = "https://www.zaubacorp.com"
        self.companies_url = f"{self.base_url}/companies-list"
        self.session = requests.Session()
        self.setup_session()
        self.all_companies = []
        
    def setup_session(self):
        """Configure session with appropriate headers to mimic browser behavior."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        self.session.headers.update(headers)
        
    def get_page_content(self, url, max_retries=3):
        """Fetch page content with retry mechanism and error handling."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching: {url} (Attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 403:
                    logger.warning(f"403 Forbidden - Possible bot detection. Waiting {(attempt + 1) * 10} seconds...")
                    time.sleep((attempt + 1) * 10)
                else:
                    logger.warning(f"HTTP {response.status_code} received")
                    
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 15))
                    
        return None
    
    def extract_companies_from_page(self, soup):
        """Extract company data from a single page."""
        companies = []
        
        # Find the main table containing company data
        table = soup.find('table', {'class': 'table table-striped'})
        if not table:
            logger.warning("No table found on page")
            return companies
            
        # Find table body
        tbody = table.find('tbody')
        if not tbody:
            logger.warning("No tbody found in table")
            return companies
            
        # Extract each row
        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} company rows")
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 5:  # Ensure we have all required columns
                try:
                    # Extract data from each cell
                    cin = cells[0].get_text(strip=True)
                    
                    # Company name might contain a link
                    name_cell = cells[1]
                    name_link = name_cell.find('a')
                    if name_link:
                        company_name = name_link.get_text(strip=True)
                        company_url = urljoin(self.base_url, name_link.get('href', ''))
                    else:
                        company_name = name_cell.get_text(strip=True)
                        company_url = ''
                    
                    status = cells[2].get_text(strip=True)
                    paid_up_capital = cells[3].get_text(strip=True)
                    address = cells[4].get_text(strip=True)
                    
                    company_data = {
                        'CIN': cin,
                        'Company_Name': company_name,
                        'Company_URL': company_url,
                        'Status': status,
                        'Paid_Up_Capital': paid_up_capital,
                        'Address': address
                    }
                    
                    companies.append(company_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting company data: {e}")
                    continue
                    
        return companies
    
    def get_total_pages(self, soup):
        """Extract total number of pages from pagination info."""
        try:
            # Look for pagination info
            page_info = soup.find('div', {'class': 'text-right'})
            if page_info:
                text = page_info.get_text()
                # Extract page numbers from text like "Page 1 of 90,769"
                if 'Page' in text and 'of' in text:
                    parts = text.split('of')
                    if len(parts) > 1:
                        total_str = parts[1].strip().replace(',', '')
                        return int(total_str)
            
            # Alternative method: look for pagination links
            pagination = soup.find('ul', {'class': 'pagination'})
            if pagination:
                links = pagination.find_all('a')
                page_numbers = []
                for link in links:
                    try:
                        page_num = int(link.get_text(strip=True))
                        page_numbers.append(page_num)
                    except ValueError:
                        continue
                if page_numbers:
                    return max(page_numbers)
                    
        except Exception as e:
            logger.error(f"Error extracting total pages: {e}")
            
        return 1  # Default to 1 page if unable to determine
    
    def scrape_companies(self, start_page=1, max_pages=None, delay_range=(2, 5)):
        """Main scraping method to extract company data from multiple pages."""
        logger.info("Starting ZaubaCorp scraping...")
        
        # Get first page to determine total pages
        first_page_url = f"{self.companies_url}/p-{start_page}-company.html"
        response = self.get_page_content(first_page_url)
        
        if not response:
            logger.error("Failed to fetch first page")
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        total_pages = self.get_total_pages(soup)
        
        if max_pages:
            total_pages = min(total_pages, max_pages)
            
        logger.info(f"Total pages to scrape: {total_pages}")
        
        # Extract companies from first page
        companies = self.extract_companies_from_page(soup)
        self.all_companies.extend(companies)
        logger.info(f"Extracted {len(companies)} companies from page {start_page}")
        
        # Scrape remaining pages
        for page_num in range(start_page + 1, total_pages + 1):
            try:
                # Random delay to avoid detection
                delay = random.uniform(delay_range[0], delay_range[1])
                logger.info(f"Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)
                
                # Construct page URL
                page_url = f"{self.companies_url}/p-{page_num}-company.html"
                
                # Fetch page content
                response = self.get_page_content(page_url)
                if not response:
                    logger.warning(f"Failed to fetch page {page_num}")
                    continue
                    
                # Parse and extract data
                soup = BeautifulSoup(response.text, 'html.parser')
                companies = self.extract_companies_from_page(soup)
                
                if companies:
                    self.all_companies.extend(companies)
                    logger.info(f"Extracted {len(companies)} companies from page {page_num}")
                else:
                    logger.warning(f"No companies found on page {page_num}")
                
                # Save progress periodically
                if page_num % 10 == 0:
                    self.save_to_csv(f"zaubacorp_companies_backup_page_{page_num}.csv")
                    logger.info(f"Backup saved at page {page_num}")
                    
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {e}")
                continue
                
        logger.info(f"Scraping completed. Total companies extracted: {len(self.all_companies)}")
        return self.all_companies
    
    def save_to_csv(self, filename="zaubacorp_companies.csv"):
        """Save extracted data to CSV file."""
        if not self.all_companies:
            logger.warning("No data to save")
            return
            
        try:
            df = pd.DataFrame(self.all_companies)
            df.to_csv(filename, index=False, encoding='utf-8')
            logger.info(f"Data saved to {filename}")
            logger.info(f"Total records: {len(df)}")
            
            # Print summary statistics
            logger.info("\nData Summary:")
            logger.info(f"Unique CINs: {df['CIN'].nunique()}")
            logger.info(f"Company Statuses: {df['Status'].value_counts().to_dict()}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
    
    def save_to_excel(self, filename="zaubacorp_companies.xlsx"):
        """Save extracted data to Excel file with formatting."""
        if not self.all_companies:
            logger.warning("No data to save")
            return
            
        try:
            df = pd.DataFrame(self.all_companies)
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Companies', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Companies']
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
    scraper = ZaubaCorpScraper()
    
    try:
        # Scrape first 5 pages as a test (remove max_pages for full scraping)
        companies = scraper.scrape_companies(start_page=1, max_pages=5, delay_range=(3, 7))
        
        if companies:
            # Save to both CSV and Excel
            scraper.save_to_csv("zaubacorp_companies_sample.csv")
            scraper.save_to_excel("zaubacorp_companies_sample.xlsx")
            
            # Display sample data
            df = pd.DataFrame(companies)
            print("\nSample of extracted data:")
            print(df.head(10).to_string())
            
            print(f"\nTotal companies extracted: {len(companies)}")
            print(f"Data types:\n{df.dtypes}")
            
        else:
            logger.warning("No data was extracted")
            
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        if scraper.all_companies:
            scraper.save_to_csv("zaubacorp_companies_interrupted.csv")
            logger.info("Partial data saved")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
