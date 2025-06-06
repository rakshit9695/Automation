import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from urllib.parse import quote, urljoin
import json
from datetime import datetime

class ZaubaCorpDetailedAnalyzer:
    def __init__(self):
        self.base_url = "https://www.zaubacorp.com"
        self.session = requests.Session()
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cache-Control': 'max-age=0'
        })
    
    def safe_slice(self, value, length=14, default='N/A'):
        if value is None:
            return default
        try:
            return str(value)[:length]
        except (TypeError, AttributeError):
            return str(default)[:length]
    
    def search_companies(self, company_name, max_results=50):
        clean_name = company_name.strip().upper()
        search_url = f"{self.base_url}/companysearchresults/{quote(clean_name)}"
        
        print(f"🔍 Searching for companies matching: '{company_name}'")
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.common.exceptions import TimeoutException
            
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            print("🚗 Starting Chrome browser...")
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            try:
                print(f"🌐 Navigating to: {search_url}")
                driver.get(search_url)
                
                wait = WebDriverWait(driver, 15)
                
                try:
                    wait.until(EC.presence_of_element_located((By.ID, "results")))
                    print("✅ Results table found")
                except TimeoutException:
                    print("⏰ Timeout waiting for results table, checking for any table...")
                    try:
                        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                        print("✅ Some table found")
                    except TimeoutException:
                        print("❌ No table found within timeout")
                        return []
                
                html = driver.page_source
                companies = self.parse_search_results(html, max_results)
                
                return companies
                
            except Exception as e:
                print(f"❌ Search failed: {e}")
                return []
            finally:
                driver.quit()
                
        except ImportError:
            print("❌ Selenium not installed. Install with: pip install selenium")
            return []
    
    def parse_search_results(self, html, max_results=50):
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        print("🔍 Analyzing HTML structure...")
        
        results_table = soup.find('table', {'id': 'results'})
        if not results_table:
            tables = soup.find_all('table')
            if tables:
                results_table = tables[0]
            else:
                print("❌ No tables found in HTML")
                return companies
        
        tbody = results_table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')
        else:
            all_rows = results_table.find_all('tr')
            rows = all_rows[1:] if all_rows else []
        
        print(f"📋 Processing {len(rows)} companies...")
        
        for i, row in enumerate(rows):
            if len(companies) >= max_results:
                break
                
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 3:
                company_data = self.extract_company_from_row(cells, i+1)
                if company_data:
                    companies.append(company_data)
                    print(f"   ✓ {i+1:2d}. {self.safe_slice(company_data.get('name'), 50)}...")
        
        print(f"✅ Successfully extracted {len(companies)} companies")
        return companies
    
    def extract_company_from_row(self, cells, row_number):
        try:
            company_data = {'row_number': row_number}
            
            if len(cells) >= 3:
                cin_cell = cells[0]
                name_cell = cells[1]
                address_cell = cells[2]
            else:
                return None
            
            cin_link = cin_cell.find('a')
            if cin_link:
                company_data['cin'] = cin_link.get_text().strip()
                href = cin_link.get('href', '')
                company_data['detail_url'] = urljoin(self.base_url, href) if href else None
            else:
                company_data['cin'] = cin_cell.get_text().strip()
                company_data['detail_url'] = None
            
            name_link = name_cell.find('a')
            if name_link:
                company_data['name'] = name_link.get_text().strip()
                if not company_data.get('detail_url'):
                    href = name_link.get('href', '')
                    company_data['detail_url'] = urljoin(self.base_url, href) if href else None
            else:
                company_data['name'] = name_cell.get_text().strip()
            
            address_text = address_cell.get_text().strip()
            company_data['address'] = re.sub(r'\s+', ' ', address_text) if address_text else 'N/A'
            
            address_details = self.parse_address_details(company_data['address'])
            company_data['city'] = address_details.get('city') or 'Unknown'
            company_data['state'] = address_details.get('state') or 'Unknown'
            company_data['state_code'] = address_details.get('state_code') or 'N/A'
            company_data['pin_code'] = address_details.get('pin_code') or 'N/A'
            company_data['company_type'] = self.classify_company_type(company_data.get('cin', ''))
            
            return company_data
            
        except Exception as e:
            print(f"⚠️  Error extracting row {row_number}: {e}")
            return None
    
    def parse_address_details(self, address):
        details = {'city': None, 'state': None, 'pin_code': None, 'state_code': None}
        
        try:
            if not address or address == 'N/A':
                return details
                
            state_codes = {
                'MH': 'Maharashtra', 'DL': 'Delhi', 'KA': 'Karnataka', 'TN': 'Tamil Nadu',
                'GJ': 'Gujarat', 'WB': 'West Bengal', 'UP': 'Uttar Pradesh', 'HR': 'Haryana',
                'PB': 'Punjab', 'RJ': 'Rajasthan', 'MP': 'Madhya Pradesh', 'AP': 'Andhra Pradesh',
                'TG': 'Telangana', 'KL': 'Kerala', 'OR': 'Odisha', 'BR': 'Bihar',
                'AS': 'Assam', 'HP': 'Himachal Pradesh', 'UR': 'Uttarakhand', 'CH': 'Chandigarh',
                'GA': 'Goa', 'JH': 'Jharkhand', 'CT': 'Chhattisgarh'
            }
            
            pin_match = re.search(r'\b(\d{6})\b', address)
            if pin_match:
                details['pin_code'] = pin_match.group(1)
            
            for code, full_name in state_codes.items():
                if f' {code} ' in address or address.endswith(f' {code}'):
                    details['state_code'] = code
                    details['state'] = full_name
                    break
            
            if details['state_code']:
                pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+' + details['state_code']
                city_match = re.search(pattern, address)
                if city_match:
                    details['city'] = city_match.group(1).strip()
            
        except Exception as e:
            print(f"⚠️  Error parsing address: {e}")
        
        return details
    
    def classify_company_type(self, cin):
        if not cin or len(cin) < 5:
            return 'Unknown'
        
        cin = str(cin).upper()
        if cin.startswith('U'):
            return 'Private Limited'
        elif cin.startswith('L'):
            return 'Public Limited'
        elif cin.startswith(('AAA', 'AAB', 'AAC')):
            return 'LLP'
        elif cin.startswith('F'):
            return 'Foreign Company'
        else:
            return 'Other'
    
    def get_detailed_company_info(self, company_url_or_details):
        if isinstance(company_url_or_details, dict):
            company_url = company_url_or_details.get('detail_url')
            company_name = company_url_or_details.get('name', 'Unknown')
        else:
            company_url = company_url_or_details
            company_name = 'Unknown'
        
        if not company_url:
            print("❌ No detail URL provided")
            return None
        
        print(f"🔍 Fetching detailed information for: {company_name}")
        print(f"📡 URL: {company_url}")
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            try:
                driver.get(company_url)
                wait = WebDriverWait(driver, 15)
                
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
                
                html = driver.page_source
                detailed_info = self.parse_detailed_company_page(html)
                
                return detailed_info
                
            except Exception as e:
                print(f"❌ Error fetching detailed page: {e}")
                return None
            finally:
                driver.quit()
                
        except ImportError:
            print("❌ Selenium not installed. Install with: pip install selenium")
            return None
    
    def parse_detailed_company_page(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        company_details = {}
        
        try:
            title_h1 = soup.find('h1', id='title')
            if title_h1:
                company_details['company_name'] = title_h1.get_text().strip()
            
            last_updated = soup.find('span', id='last_updated')
            if last_updated:
                company_details['last_updated'] = last_updated.get_text().strip()
            
            about_para = soup.find('p', id='about')
            if about_para:
                company_details['description'] = about_para.get_text().strip()
            
            basic_info = {}
            tables = soup.find_all('table', class_='table table-striped')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].get_text().strip()
                        value = cells[1].get_text().strip()
                        
                        value = re.sub(r'\s+', ' ', value)
                        basic_info[key] = value
            
            company_details['basic_info'] = basic_info
            
            directors = []
            director_tables = soup.find_all('table', class_='table table-striped table-hover table-condensed')
            
            for table in director_tables:
                caption = table.find('caption')
                if caption and 'Current Directors' in caption.get_text():
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')[1:]
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 4:
                            director = {
                                'din': cells[0].get_text().strip(),
                                'name': cells[1].get_text().strip(),
                                'designation': cells[2].get_text().strip(),
                                'appointment_date': cells[3].get_text().strip()
                            }
                            
                            din_link = cells[0].find('a')
                            if din_link:
                                director['din_url'] = urljoin(self.base_url, din_link.get('href', ''))
                            
                            directors.append(director)
            
            company_details['directors'] = directors
            
            contact_info = {}
            contact_section = soup.find('div', id='contact-details-content')
            if contact_section:
                email_link = contact_section.find('a', class_='__cf_email__')
                if email_link:
                    contact_info['email'] = email_link.get('data-cfemail', 'Protected Email')
                
                spans = contact_section.find_all('span')
                for span in spans:
                    text = span.get_text().strip()
                    if 'Address:' in text:
                        next_span = span.find_next_sibling('span')
                        if next_span:
                            contact_info['address'] = next_span.get_text().strip()
                    elif 'Website:' in text:
                        contact_info['website'] = text.replace('Website:', '').strip()
            
            company_details['contact_info'] = contact_info
            
            charges = []
            charges_section = soup.find('div', id='charges-content')
            if charges_section:
                charges_table = charges_section.find('table')
                if charges_table:
                    tbody = charges_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows:
                            cells = row.find_all('td')
                            if len(cells) >= 7:
                                if not cells[0].find('i', class_='lock'):
                                    charge = {
                                        'charge_id': cells[0].get_text().strip(),
                                        'creation_date': cells[1].get_text().strip(),
                                        'modification_date': cells[2].get_text().strip(),
                                        'closure_date': cells[3].get_text().strip(),
                                        'assets_under_charge': cells[4].get_text().strip(),
                                        'amount': cells[5].get_text().strip(),
                                        'charge_holder': cells[6].get_text().strip()
                                    }
                                    charges.append(charge)
            
            company_details['charges'] = charges
            
            similar_companies = []
            similar_section = soup.find('div', id='similar-address-content')
            if similar_section:
                similar_table = similar_section.find('table')
                if similar_table:
                    tbody = similar_table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        for row in rows[:10]:
                            cells = row.find_all('td')
                            if len(cells) >= 3:
                                similar_company = {
                                    'cin': cells[0].get_text().strip(),
                                    'name': cells[1].get_text().strip(),
                                    'address': cells[2].get_text().strip()
                                }
                                
                                cin_link = cells[0].find('a')
                                if cin_link:
                                    similar_company['cin_url'] = urljoin(self.base_url, cin_link.get('href', ''))
                                
                                name_link = cells[1].find('a')
                                if name_link:
                                    similar_company['name_url'] = urljoin(self.base_url, name_link.get('href', ''))
                                
                                similar_companies.append(similar_company)
            
            company_details['similar_companies'] = similar_companies
            
            financial_info = {}
            key_numbers_tables = soup.find_all('table', class_='table table-striped table-hover')
            
            for table in key_numbers_tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].get_text().strip()
                        value_cell = cells[1]
                        
                        if not value_cell.find('i', class_='lock'):
                            value = value_cell.get_text().strip()
                            if value and value != '':
                                financial_info[key] = value
            
            company_details['financial_info'] = financial_info
            
            print(f"✅ Successfully extracted detailed information")
            return company_details
            
        except Exception as e:
            print(f"❌ Error parsing detailed company page: {e}")
            return None
    
    def display_search_results(self, companies):
        if not companies:
            print("❌ No companies found")
            return
        
        print(f"\n{'='*120}")
        print(f"🏢 COMPANY SEARCH RESULTS ({len(companies)} companies)")
        print(f"{'='*120}")
        
        print(f"\n📊 SUMMARY TABLE:")
        print(f"{'No.':<4} {'Company Name':<45} {'CIN':<25} {'City':<15} {'State':<12}")
        print(f"{'-'*4} {'-'*45} {'-'*25} {'-'*15} {'-'*12}")
        
        for i, company in enumerate(companies, 1):
            name = self.safe_slice(company.get('name'), 44)
            cin = self.safe_slice(company.get('cin'), 24)
            city = self.safe_slice(company.get('city'), 14)
            state = self.safe_slice(company.get('state_code'), 11)
            
            print(f"{i:<4} {name:<45} {cin:<25} {city:<15} {state:<12}")
        
        return companies
    
    def display_detailed_info(self, detailed_info):
        if not detailed_info:
            print("❌ No detailed information available")
            return
        
        print(f"\n{'='*120}")
        print(f"🏢 DETAILED COMPANY INFORMATION")
        print(f"{'='*120}")
        
        if 'company_name' in detailed_info:
            print(f"\n📋 COMPANY: {detailed_info['company_name']}")
        
        if 'last_updated' in detailed_info:
            print(f"📅 Last Updated: {detailed_info['last_updated']}")
        
        if 'basic_info' in detailed_info and detailed_info['basic_info']:
            print(f"\n📊 BASIC INFORMATION:")
            for key, value in detailed_info['basic_info'].items():
                if value and value.strip():
                    print(f"   • {key}: {value}")
        
        if 'directors' in detailed_info and detailed_info['directors']:
            print(f"\n👥 DIRECTORS ({len(detailed_info['directors'])} total):")
            for i, director in enumerate(detailed_info['directors'], 1):
                print(f"   {i}. {director.get('name', 'N/A')} (DIN: {director.get('din', 'N/A')})")
                print(f"      Designation: {director.get('designation', 'N/A')}")
                print(f"      Appointed: {director.get('appointment_date', 'N/A')}")
        
        if 'contact_info' in detailed_info and detailed_info['contact_info']:
            print(f"\n📞 CONTACT INFORMATION:")
            for key, value in detailed_info['contact_info'].items():
                if value and value.strip():
                    print(f"   • {key.title()}: {value}")
        
        if 'financial_info' in detailed_info and detailed_info['financial_info']:
            print(f"\n💰 FINANCIAL INFORMATION:")
            for key, value in detailed_info['financial_info'].items():
                if value and value.strip():
                    print(f"   • {key}: {value}")
        
        if 'charges' in detailed_info and detailed_info['charges']:
            print(f"\n🏦 CHARGES/LOANS ({len(detailed_info['charges'])} total):")
            for i, charge in enumerate(detailed_info['charges'], 1):
                print(f"   {i}. Charge ID: {charge.get('charge_id', 'N/A')}")
                print(f"      Amount: {charge.get('amount', 'N/A')}")
                print(f"      Holder: {charge.get('charge_holder', 'N/A')}")
                print(f"      Status: {charge.get('closure_date', 'Ongoing') if charge.get('closure_date') != '-' else 'Ongoing'}")
        
        if 'similar_companies' in detailed_info and detailed_info['similar_companies']:
            print(f"\n🏘️  COMPANIES AT SIMILAR ADDRESS ({len(detailed_info['similar_companies'])} shown):")
            for i, similar in enumerate(detailed_info['similar_companies'][:5], 1):
                print(f"   {i}. {similar.get('name', 'N/A')} (CIN: {similar.get('cin', 'N/A')})")
        
        if 'description' in detailed_info and detailed_info['description']:
            print(f"\n📝 COMPANY DESCRIPTION:")
            description = detailed_info['description']
            if len(description) > 200:
                description = description[:200] + "..."
            print(f"   {description}")
    
    def export_detailed_info(self, detailed_info, company_name="company"):
        if not detailed_info:
            print("❌ No data to export")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r'[^\w\-_\.]', '_', company_name)
        
        json_filename = f"company_details_{safe_name}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(detailed_info, f, indent=2, ensure_ascii=False)
        print(f"💾 Detailed information exported to: {json_filename}")
        
        csv_data = []
        
        basic_info = detailed_info.get('basic_info', {})
        row = {'type': 'basic_info', 'company_name': detailed_info.get('company_name', 'N/A')}
        row.update(basic_info)
        csv_data.append(row)
        
        for director in detailed_info.get('directors', []):
            director_row = {'type': 'director', 'company_name': detailed_info.get('company_name', 'N/A')}
            director_row.update(director)
            csv_data.append(director_row)
        
        for charge in detailed_info.get('charges', []):
            charge_row = {'type': 'charge', 'company_name': detailed_info.get('company_name', 'N/A')}
            charge_row.update(charge)
            csv_data.append(charge_row)
        
        if csv_data:
            df = pd.DataFrame(csv_data)
            csv_filename = f"company_details_{safe_name}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)
            print(f"💾 Data also exported to CSV: {csv_filename}")
        
        return json_filename

def main():
    analyzer = ZaubaCorpDetailedAnalyzer()
    
    print("🏢 ZaubaCorp Advanced Company Analysis Tool")
    print("=" * 60)
    
    while True:
        print("\n🛠️  AVAILABLE OPTIONS:")
        print("   1. Search companies and get detailed information")
        print("   2. Exit")
        
        choice = input("\n👉 Choose option (1-2): ").strip()
        
        if choice == '1':
            company_name = input("\n🔍 Enter company name to search: ").strip()
            if not company_name:
                print("❌ Please enter a valid company name")
                continue
            
            max_results = input(f"📊 Maximum results to show (default: 10): ").strip()
            try:
                max_results = int(max_results) if max_results else 10
            except ValueError:
                max_results = 10
            
            print(f"\n🚀 Starting search...")
            companies = analyzer.search_companies(company_name, max_results)
            
            if companies:
                displayed_companies = analyzer.display_search_results(companies)
                
                while True:
                    detail_choice = input(f"\n🔍 Enter company number for detailed info (1-{len(companies)}) or 'q' to quit: ").strip()
                    
                    if detail_choice.lower() == 'q':
                        break
                    
                    try:
                        company_index = int(detail_choice) - 1
                        if 0 <= company_index < len(companies):
                            selected_company = companies[company_index]
                            print(f"\n🔍 Getting detailed information for: {selected_company['name']}")
                            
                            detailed_info = analyzer.get_detailed_company_info(selected_company)
                            
                            if detailed_info:
                                analyzer.display_detailed_info(detailed_info)
                                
                                export_choice = input(f"\n💾 Export detailed information? (y/n): ").strip().lower()
                                if export_choice == 'y':
                                    analyzer.export_detailed_info(detailed_info, selected_company['name'])
                            else:
                                print("❌ Could not fetch detailed information")
                        else:
                            print(f"❌ Please enter a number between 1 and {len(companies)}")
                    except ValueError:
                        print("❌ Please enter a valid number or 'q' to quit")
            else:
                print("❌ No companies found")
        
        elif choice == '2':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1 or 2.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("💡 Please check your input and try again")
