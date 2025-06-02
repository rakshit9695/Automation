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
        chrome_options.add_argument('--disable-images')  # Faster loading
        
        # User agent to appear as a real browser
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise
    
    def wait_for_table_to_load(self, timeout=20):
        """Wait for the company table to fully load."""
        try:
            # Wait for the main container
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CLASS_NAME, "MuiContainer-root"))
            )
            
            # Wait for table content - try multiple possible selectors
            table_selectors = [
                "table",
                "[class*='tableStyle__TableSectionLayout']",
                "[class*='MuiPaper-root']",
                ".MuiTableContainer-root",
                "[class*='Table']"
            ]
            
            table_found = False
            for selector in table_selectors:
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"Table found using selector: {selector}")
                    table_found = True
                    break
                except TimeoutException:
                    continue
            
            if not table_found:
                logger.warning("No table found with standard selectors, trying alternative approach")
            
            # Additional wait for dynamic content
            time.sleep(3)
            return True
            
        except TimeoutException:
            logger.error("Timeout waiting for table to load")
            return False
    
    def scroll_and_load_more(self):
        """Scroll down to load more companies if pagination exists."""
        try:
            # Get initial height
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Scroll down multiple times to load more content
            for i in range(5):  # Try scrolling 5 times
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Wait for new content to load
                time.sleep(2)
                
                # Calculate new height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                
                if new_height == last_height:
                    # No more content loaded
                    break
                    
                last_height = new_height
                logger.info(f"Scrolled {i+1} times, new height: {new_height}")
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error during scrolling: {e}")
    
    def extract_table_data(self):
        """Extract company data from the loaded table."""
        try:
            # Try multiple approaches to find table rows
            table_rows = []
            
            # Approach 1: Standard table structure
            try:
                table = self.driver.find_element(By.TAG_NAME, "table")
                table_rows = table.find_elements(By.TAG_NAME, "tr")
                logger.info(f"Found {len(table_rows)} rows using standard table approach")
            except NoSuchElementException:
                pass
            
            # Approach 2: Material-UI table structure
            if not table_rows:
                try:
                    table_body = self.driver.find_element(By.CSS_SELECTOR, "tbody, .MuiTableBody-root")
                    table_rows = table_body.find_elements(By.CSS_SELECTOR, "tr, .MuiTableRow-root")
                    logger.info(f"Found {len(table_rows)} rows using MUI table approach")
                except NoSuchElementException:
                    pass
            
            # Approach 3: Generic row detection
            if not table_rows:
                try:
                    table_rows = self.driver.find_elements(By.CSS_SELECTOR, "[class*='row'], [class*='Row']")
                    # Filter for actual data rows
                    table_rows = [row for row in table_rows if len(row.find_elements(By.TAG_NAME, "td")) > 3]
                    logger.info(f"Found {len(table_rows)} rows using generic approach")
                except NoSuchElementException:
                    pass
            
            if not table_rows:
                logger.error("No table rows found")
                return []
            
            # Extract header information
            headers = self.extract_headers()
            
            # Extract data from each row
            extracted_data = []
            for i, row in enumerate(table_rows):
                try:
                    # Skip header row if it's included
                    if i == 0 and self.is_header_row(row):
                        continue
                    
                    row_data = self.extract_row_data(row, headers)
                    if row_data and any(row_data.values()):  # Only add if row has actual data
                        extracted_data.append(row_data)
                        logger.info(f"Extracted data for company: {row_data.get('Company_Name', 'Unknown')}")
                
                except Exception as e:
                    logger.error(f"Error extracting row {i}: {e}")
                    continue
            
            logger.info(f"Successfully extracted {len(extracted_data)} company records")
            return extracted_data
            
        except Exception as e:
            logger.error(f"Error extracting table data: {e}")
            return []
    
    def extract_headers(self):
        """Extract table headers to understand column structure."""
        try:
            # Try to find header row
            header_selectors = [
                "thead tr th",
                ".MuiTableHead-root .MuiTableRow-root .MuiTableCell-root",
                "tr:first-child td",
                "tr:first-child th"
            ]
            
            headers = []
            for selector in header_selectors:
                try:
                    header_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if header_elements:
                        headers = [elem.get_attribute('textContent').strip() for elem in header_elements]
                        if headers and any(headers):
                            logger.info(f"Found headers: {headers}")
                            return headers
                except:
                    continue
            
            # Default headers based on your screenshot
            default_headers = [
                'Company_Name', 'Sector', 'Founded_Date', 'Amount_Raised', 
                'Headquarters', 'Founders', 'Country'
            ]
            logger.info(f"Using default headers: {default_headers}")
            return default_headers
            
        except Exception as e:
            logger.error(f"Error extracting headers: {e}")
            return ['Company_Name', 'Sector', 'Founded_Date', 'Amount_Raised', 'Headquarters', 'Founders', 'Country']
    
    def is_header_row(self, row):
        """Check if a row is a header row."""
        try:
            text = row.get_attribute('textContent').lower()
            header_indicators = ['company', 'sector', 'founded', 'amount', 'headquarters', 'founders']
            return any(indicator in text for indicator in header_indicators)
        except:
            return False
    
    def extract_row_data(self, row, headers):
        """Extract data from a single table row."""
        try:
            # Find all cells in the row
            cells = row.find_elements(By.CSS_SELECTOR, "td, .MuiTableCell-root, div[class*='cell']")
            
            if not cells:
                return None
            
            row_data = {}
            
            # Map cell data to headers
            for i, cell in enumerate(cells):
                if i < len(headers):
                    cell_text = cell.get_attribute('textContent').strip()
                    
                    # Clean up the cell text
                    cell_text = re.sub(r'\s+', ' ', cell_text)  # Replace multiple spaces with single space
                    cell_text = cell_text.replace('\n', ' ').replace('\t', ' ')
                    
                    # Check if cell contains a link (for company names)
                    link_element = cell.find_element(By.TAG_NAME, "a") if cell.find_elements(By.TAG_NAME, "a") else None
                    if link_element and headers[i] in ['Company_Name', 'company', 'Company']:
                        cell_text = link_element.get_attribute('textContent').strip()
                        # Also get the link URL
                        row_data[f"{headers[i]}_URL"] = link_element.get_attribute('href')
                    
                    row_data[headers[i]] = cell_text
                else:
                    # Handle extra columns
                    extra_key = f"Column_{i+1}"
                    cell_text = cell.get_attribute('textContent').strip()
                    row_data[extra_key] = cell_text
            
            return row_data
            
        except Exception as e:
            logger.error(f"Error extracting row data: {e}")
            return None
    
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
            df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]
            
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
            print("\n" + "="*50)
            print("EXTRACTION SUMMARY")
            print("="*50)
            print(f"Total companies extracted: {len(df)}")
            print(f"Columns: {list(df.columns)}")
            print("\nFirst 5 companies:")
            print(df.head().to_string())
            print("="*50)
            
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
        # Initialize scraper (set headless=False to see browser window)
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
