import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Inc42CompanyTableScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Selenium WebDriver."""
        self.headless = headless
        self.driver = None
        self.companies_data = []
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome WebDriver with optimized settings."""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        # Essential Chrome options for scraping
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-plugins')
        
        # User agent to appear as a real browser
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise
    
    def wait_for_table_to_load(self, timeout=30):
        """Wait for the company table to fully load."""
        try:
            logger.info("Waiting for page to load...")
            
            # Wait for the table to appear
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            logger.info("Table element found")
            
            # Wait for table rows to load
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
            )
            logger.info("Table rows found")
            
            # Additional wait for dynamic content to fully render
            time.sleep(5)
            return True
            
        except TimeoutException:
            logger.error("Timeout waiting for table to load")
            return False
    
    def scroll_and_load_more(self):
        """Scroll down to load more companies if pagination exists."""
        try:
            logger.info("Starting to scroll to load more companies...")
            
            # Get initial number of rows
            initial_rows = len(self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
            logger.info(f"Initial rows found: {initial_rows}")
            
            # Scroll down multiple times to load more content
            for i in range(10):  # Scroll 10 times
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                logger.info(f"Scroll {i+1}/10 completed")
                
                # Wait for new content to load
                time.sleep(3)
                
                # Check if new rows were added
                current_rows = len(self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
                if current_rows > initial_rows:
                    logger.info(f"New rows loaded: {current_rows} (was {initial_rows})")
                    initial_rows = current_rows
                else:
                    # No new content loaded, break early
                    logger.info("No new content loaded, stopping scroll")
                    break
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            final_rows = len(self.driver.find_elements(By.CSS_SELECTOR, "table tbody tr"))
            logger.info(f"Final number of rows after scrolling: {final_rows}")
            
        except Exception as e:
            logger.error(f"Error during scrolling: {e}")
    
    def extract_table_headers(self):
        """Extract table headers."""
        try:
            # Try to find header row
            header_row = self.driver.find_element(By.CSS_SELECTOR, "table thead tr")
            header_cells = header_row.find_elements(By.TAG_NAME, "th")
            
            headers = []
            for cell in header_cells:
                header_text = cell.get_attribute('textContent').strip()
                if header_text:  # Skip empty headers (like checkbox column)
                    headers.append(header_text)
                else:
                    headers.append(f"Column_{len(headers)+1}")
            
            logger.info(f"Extracted headers: {headers}")
            return headers
            
        except NoSuchElementException:
            # Fallback headers based on your screenshot
            default_headers = ['Checkbox', 'Company', 'Sector', 'Founded_Date', 'Amount_Raised', 'Headquarters', 'Founders']
            logger.info(f"Using default headers: {default_headers}")
            return default_headers
    
    def extract_table_data(self):
        """Extract company data from the loaded table."""
        try:
            # Find the table and all data rows
            table = self.driver.find_element(By.TAG_NAME, "table")
            tbody = table.find_element(By.TAG_NAME, "tbody")
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            
            logger.info(f"Found {len(rows)} data rows in table")
            
            if not rows:
                logger.error("No table rows found")
                return []
            
            # Extract headers
            headers = self.extract_table_headers()
            
            # Extract data from each row
            extracted_data = []
            
            for i, row in enumerate(rows):
                try:
                    # Find all cells in the row
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < 3:  # Skip rows with too few cells
                        continue
                    
                    row_data = {}
                    
                    # Extract data from each cell
                    for j, cell in enumerate(cells):
                        try:
                            # Get cell text content
                            cell_text = cell.get_attribute('textContent').strip()
                            
                            # Clean up the text
                            cell_text = re.sub(r'\s+', ' ', cell_text)
                            cell_text = cell_text.replace('\n', ' ').replace('\t', ' ')
                            
                            # Assign to appropriate column
                            if j < len(headers):
                                column_name = headers[j]
                            else:
                                column_name = f"Column_{j+1}"
                            
                            # Special handling for company column (might contain links)
                            if 'company' in column_name.lower() and j > 0:  # Skip checkbox column
                                # Try to find company link
                                try:
                                    link = cell.find_element(By.TAG_NAME, "a")
                                    company_url = link.get_attribute('href')
                                    row_data[f"{column_name}_URL"] = company_url
                                except NoSuchElementException:
                                    pass
                            
                            row_data[column_name] = cell_text
                            
                        except Exception as e:
                            logger.warning(f"Error extracting cell {j} from row {i}: {e}")
                            continue
                    
                    # Only add row if it has substantial data
                    if len([v for v in row_data.values() if v and v.strip()]) >= 3:
                        extracted_data.append(row_data)
                        
                        # Log company name if available
                        company_name = row_data.get('Company', row_data.get('Column_2', 'Unknown'))
                        logger.info(f"Extracted: {company_name}")
                
                except Exception as e:
                    logger.error(f"Error extracting row {i}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(extracted_data)} company records")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            return []
    
    def scrape_company_table(self, url="https://inc42.com/company/"):
        """Main method to scrape the company table."""
        try:
            logger.info(f"Starting to scrape: {url}")
            
            # Load the page
            self.driver.get(url)
            logger.info("Page loaded, waiting for content...")
            
            # Wait for the table to load
            if not self.wait_for_table_to_load():
                logger.error("Failed to load table content")
                return []
            
            # Scroll to load more content if needed
            self.scroll_and_load_more()
            
            # Extract the table data
            self.companies_data = self.extract_table_data()
            
            return self.companies_data
            
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return []
    
    def save_to_excel(self, filename="inc42_companies_table.xlsx"):
        """Save extracted data to Excel file."""
        if not self.companies_data:
            logger.warning("No data to save")
            return False
        
        try:
            df = pd.DataFrame(self.companies_data)
            
            # Clean up column names
            df.columns = [col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') for col in df.columns]
            
            # Remove empty columns and checkbox column
            df = df.loc[:, (df != '').any(axis=0)]  # Remove empty columns
            if 'Checkbox' in df.columns:
                df = df.drop('Checkbox', axis=1)
            
            # Create Excel writer with formatting
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
            
            logger.info(f"Data successfully saved to {filename}")
            logger.info(f"Total companies saved: {len(df)}")
            
            # Print sample data
            print("\n" + "="*80)
            print("EXTRACTION SUMMARY")
            print("="*80)
            print(f"Total companies extracted: {len(df)}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 10 companies:")
            print(df.head(10).to_string(max_cols=6))
            print("="*80)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            return False
    
    def close(self):
        """Close the WebDriver."""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")

def main():
    """Main execution function."""
    scraper = None
    try:
        # Initialize scraper (set headless=False to see browser window for debugging)
        scraper = Inc42CompanyTableScraper(headless=True)
        
        # Scrape the company table
        logger.info("Starting Inc42 company table extraction...")
        companies = scraper.scrape_company_table()
        
        if companies:
            # Save to Excel
            success = scraper.save_to_excel("inc42_companies_extracted.xlsx")
            
            if success:
                print(f"\n✅ SUCCESS: Extracted {len(companies)} companies and saved to Excel!")
                print("📁 File saved as: inc42_companies_extracted.xlsx")
                
                # Show sample of extracted data
                sample_company = companies[0] if companies else {}
                print(f"\nSample company data:")
                for key, value in sample_company.items():
                    print(f"  {key}: {value}")
                    
            else:
                print("❌ ERROR: Failed to save data to Excel")
        else:
            print("❌ ERROR: No company data was extracted")
            
            # Debug: Save page source for inspection
            try:
                with open("debug_page_source.html", "w", encoding="utf-8") as f:
                    f.write(scraper.driver.page_source)
                print("🔍 Debug: Page source saved to debug_page_source.html")
            except:
                pass
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()

if __name__ == "__main__":
    main()
