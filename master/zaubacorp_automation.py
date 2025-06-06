import pandas as pd
import json
import time
import random
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import defaultdict, Counter
import re
import glob
import os
from zaubacorp_scraper import ZaubaCorpDetailedAnalyzer

class ZaubaCorpMassAnalyzer:
    def __init__(self):
        self.analyzer = ZaubaCorpDetailedAnalyzer()
        self.all_companies_data = []
        self.detailed_companies_data = []
        self.financial_summary = {}
        self.processing_stats = {
            'total_searched': 0,
            'successful_searches': 0,
            'detailed_extractions': 0,
            'failed_extractions': 0,
            'start_time': None,
            'end_time': None
        }
    
    def load_existing_data(self):
        json_files = glob.glob("zaubacorp_comprehensive_*.json")
        
        if not json_files:
            print("ℹ️  No previous data files found")
            return False
        
        latest_file = max(json_files, key=os.path.getctime)
        print(f"📂 Loading data from: {latest_file}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.detailed_companies_data = data.get('companies', [])
            metadata = data.get('metadata', {})
            self.processing_stats = metadata.get('processing_stats', {})
            self.financial_summary = metadata.get('financial_summary', {})
            
            print(f"✅ Loaded {len(self.detailed_companies_data)} companies from saved data")
            return True
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def batch_search_companies(self, search_terms, max_results_per_term=20):
        print("🚀 Starting batch company search...")
        self.processing_stats['start_time'] = datetime.now()
        
        all_companies = []
        
        for i, term in enumerate(search_terms, 1):
            print(f"\n📍 Processing search term {i}/{len(search_terms)}: '{term}'")
            self.processing_stats['total_searched'] += 1
            
            try:
                companies = self.analyzer.search_companies(term, max_results_per_term)
                if companies:
                    for company in companies:
                        company['search_term'] = term
                        company['search_timestamp'] = datetime.now().isoformat()
                    all_companies.extend(companies)
                    self.processing_stats['successful_searches'] += 1
                    print(f"✅ Found {len(companies)} companies for '{term}'")
                else:
                    print(f"❌ No companies found for '{term}'")
                
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"❌ Error searching for '{term}': {e}")
        
        self.all_companies_data = all_companies
        print(f"\n🎯 Batch search complete: {len(all_companies)} total companies found")
        return all_companies
    
    def extract_all_detailed_info(self, companies=None, max_companies=50):
        if companies is None:
            companies = self.all_companies_data
        
        if not companies:
            print("❌ No companies to process")
            return []
        
        companies_to_process = companies[:max_companies]
        print(f"🔍 Extracting detailed information for {len(companies_to_process)} companies...")
        
        detailed_data = []
        
        for i, company in enumerate(companies_to_process, 1):
            print(f"\n📊 Processing {i}/{len(companies_to_process)}: {company.get('name', 'Unknown')}")
            
            try:
                detailed_info = self.analyzer.get_detailed_company_info(company)
                if detailed_info:
                    detailed_info['basic_company_data'] = company
                    detailed_info['extraction_timestamp'] = datetime.now().isoformat()
                    detailed_data.append(detailed_info)
                    self.processing_stats['detailed_extractions'] += 1
                    print(f"✅ Successfully extracted detailed info")
                else:
                    self.processing_stats['failed_extractions'] += 1
                    print(f"❌ Failed to extract detailed info")
                
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                print(f"❌ Error extracting details for {company.get('name', 'Unknown')}: {e}")
                self.processing_stats['failed_extractions'] += 1
        
        self.detailed_companies_data.extend(detailed_data)
        self.processing_stats['end_time'] = datetime.now()
        
        print(f"\n🎯 Detailed extraction complete: {len(detailed_data)} companies processed")
        print(f"📊 Total companies in database: {len(self.detailed_companies_data)}")
        return detailed_data
    
    def analyze_financial_patterns(self):
        print("💰 Analyzing financial patterns...")
        
        if not self.detailed_companies_data:
            print("❌ No detailed data available for financial analysis")
            print(f"📊 Current data status:")
            print(f"   • All companies data: {len(self.all_companies_data)} companies")
            print(f"   • Detailed companies data: {len(self.detailed_companies_data)} companies")
            return {}
        
        print(f"📊 Analyzing {len(self.detailed_companies_data)} companies...")
        
        financial_analysis = {
            'capital_analysis': {},
            'director_analysis': {},
            'charges_analysis': {},
            'geographic_analysis': {},
            'sector_analysis': {}
        }
        
        authorized_capitals = []
        paid_up_capitals = []
        director_counts = []
        charge_amounts = []
        states = []
        cities = []
        incorporation_years = []
        
        for company in self.detailed_companies_data:
            basic_info = company.get('basic_info', {})
            
            for key, value in basic_info.items():
                if 'authorized capital' in key.lower() and value:
                    capital_amount = self.extract_amount_from_text(value)
                    if capital_amount:
                        authorized_capitals.append(capital_amount)
                
                if 'paid up capital' in key.lower() and value:
                    capital_amount = self.extract_amount_from_text(value)
                    if capital_amount:
                        paid_up_capitals.append(capital_amount)
                
                if 'date of incorporation' in key.lower() and value:
                    year = self.extract_year_from_text(value)
                    if year:
                        incorporation_years.append(year)
            
            directors = company.get('directors', [])
            director_counts.append(len(directors))
            
            charges = company.get('charges', [])
            for charge in charges:
                amount = self.extract_amount_from_text(charge.get('amount', ''))
                if amount:
                    charge_amounts.append(amount)
            
            basic_company_data = company.get('basic_company_data', {})
            if basic_company_data.get('state'):
                states.append(basic_company_data['state'])
            if basic_company_data.get('city'):
                cities.append(basic_company_data['city'])
        
        financial_analysis['capital_analysis'] = {
            'authorized_capital_stats': self.calculate_stats(authorized_capitals),
            'paid_up_capital_stats': self.calculate_stats(paid_up_capitals),
            'total_companies_with_capital_data': len(authorized_capitals)
        }
        
        financial_analysis['director_analysis'] = {
            'director_count_stats': self.calculate_stats(director_counts),
            'average_directors_per_company': np.mean(director_counts) if director_counts else 0
        }
        
        financial_analysis['charges_analysis'] = {
            'charge_amount_stats': self.calculate_stats(charge_amounts),
            'companies_with_charges': len([c for c in self.detailed_companies_data if c.get('charges')]),
            'total_charges': sum(len(c.get('charges', [])) for c in self.detailed_companies_data)
        }
        
        financial_analysis['geographic_analysis'] = {
            'state_distribution': dict(Counter(states)),
            'city_distribution': dict(Counter(cities)),
            'geographic_diversity': len(set(states))
        }
        
        financial_analysis['temporal_analysis'] = {
            'incorporation_years': dict(Counter(incorporation_years)),
            'year_range': (min(incorporation_years), max(incorporation_years)) if incorporation_years else (0, 0)
        }
        
        self.financial_summary = financial_analysis
        print("✅ Financial analysis complete")
        
        print(f"\n📊 FINANCIAL ANALYSIS SUMMARY:")
        print(f"   • Companies with capital data: {len(authorized_capitals)}")
        print(f"   • Average directors per company: {np.mean(director_counts):.1f}")
        print(f"   • Companies with charges: {len([c for c in self.detailed_companies_data if c.get('charges')])}")
        print(f"   • Geographic diversity: {len(set(states))} states")
        print(f"   • Incorporation year range: {min(incorporation_years) if incorporation_years else 'N/A'} - {max(incorporation_years) if incorporation_years else 'N/A'}")
        
        return financial_analysis
    
    def extract_amount_from_text(self, text):
        if not text:
            return None
        
        amount_patterns = [
            r'₹\s*([\d,]+(?:\.\d+)?)\s*(crore|cr|lakh|lakhs?|thousand)?',
            r'Rs\.?\s*([\d,]+(?:\.\d+)?)\s*(crore|cr|lakh|lakhs?|thousand)?',
            r'INR\s*([\d,]+(?:\.\d+)?)\s*(crore|cr|lakh|lakhs?|thousand)?',
            r'([\d,]+(?:\.\d+)?)\s*(crore|cr|lakh|lakhs?|thousand)?'
        ]
        
        for pattern in amount_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    amount = float(amount_str)
                    unit = match.group(2).lower() if match.group(2) else ''
                    
                    if 'crore' in unit or 'cr' in unit:
                        amount *= 10000000
                    elif 'lakh' in unit:
                        amount *= 100000
                    elif 'thousand' in unit:
                        amount *= 1000
                    
                    return amount
                except ValueError:
                    continue
        
        return None
    
    def extract_year_from_text(self, text):
        if not text:
            return None
        
        year_match = re.search(r'(\d{4})', text)
        if year_match:
            year = int(year_match.group(1))
            if 1950 <= year <= 2024:
                return year
        return None
    
    def calculate_stats(self, values):
        if not values:
            return {'count': 0, 'mean': 0, 'median': 0, 'min': 0, 'max': 0, 'std': 0}
        
        return {
            'count': len(values),
            'mean': np.mean(values),
            'median': np.median(values),
            'min': np.min(values),
            'max': np.max(values),
            'std': np.std(values)
        }
    
    def create_comprehensive_analysis_dashboard(self):
        if not self.detailed_companies_data:
            print("❌ No data available for dashboard creation")
            return
        
        print("📊 Creating comprehensive analysis dashboard...")
        
        fig, axes = plt.subplots(3, 3, figsize=(20, 15))
        fig.suptitle('ZaubaCorp Comprehensive Financial Analysis Dashboard', fontsize=16, fontweight='bold')
        
        authorized_capitals = []
        director_counts = []
        states = []
        company_types = []
        incorporation_years = []
        charge_counts = []
        
        for company in self.detailed_companies_data:
            basic_info = company.get('basic_info', {})
            
            for key, value in basic_info.items():
                if 'authorized capital' in key.lower() and value:
                    amount = self.extract_amount_from_text(value)
                    if amount:
                        authorized_capitals.append(amount / 100000)
                
                if 'date of incorporation' in key.lower() and value:
                    year = self.extract_year_from_text(value)
                    if year:
                        incorporation_years.append(year)
            
            directors = company.get('directors', [])
            director_counts.append(len(directors))
            
            charges = company.get('charges', [])
            charge_counts.append(len(charges))
            
            basic_company_data = company.get('basic_company_data', {})
            if basic_company_data.get('state'):
                states.append(basic_company_data['state'])
            if basic_company_data.get('company_type'):
                company_types.append(basic_company_data['company_type'])
        
        # Plot 1: Capital Distribution
        if authorized_capitals:
            axes[0, 0].hist(authorized_capitals, bins=20, alpha=0.7, color='skyblue', edgecolor='navy')
            axes[0, 0].set_title('Authorized Capital Distribution (Lakhs INR)')
            axes[0, 0].set_xlabel('Amount (Lakhs)')
            axes[0, 0].set_ylabel('Number of Companies')
            axes[0, 0].grid(True, alpha=0.3)
        else:
            axes[0, 0].text(0.5, 0.5, 'No Capital Data\nAvailable', ha='center', va='center', transform=axes[0, 0].transAxes)
            axes[0, 0].set_title('Authorized Capital Distribution')
        
        # Plot 2: Director Distribution
        if director_counts:
            axes[0, 1].hist(director_counts, bins=15, alpha=0.7, color='lightgreen', edgecolor='darkgreen')
            axes[0, 1].set_title('Director Count Distribution')
            axes[0, 1].set_xlabel('Number of Directors')
            axes[0, 1].set_ylabel('Number of Companies')
            axes[0, 1].grid(True, alpha=0.3)
        
        # Plot 3: State Distribution
        if states:
            state_counts = Counter(states)
            top_states = dict(state_counts.most_common(8))
            bars = axes[0, 2].bar(top_states.keys(), top_states.values(), color='lightcoral', alpha=0.7)
            axes[0, 2].set_title('Top States by Company Count')
            axes[0, 2].tick_params(axis='x', rotation=45)
            axes[0, 2].grid(True, alpha=0.3)
            
            for bar in bars:
                height = bar.get_height()
                axes[0, 2].text(bar.get_x() + bar.get_width()/2., height,
                               f'{int(height)}', ha='center', va='bottom')
        
        # Plot 4: Company Types
        if company_types:
            type_counts = Counter(company_types)
            wedges, texts, autotexts = axes[1, 0].pie(type_counts.values(), labels=type_counts.keys(), autopct='%1.1f%%')
            axes[1, 0].set_title('Company Type Distribution')
        
        # Plot 5: Incorporation Years Trend
        if incorporation_years:
            year_counts = Counter(incorporation_years)
            years = sorted(year_counts.keys())
            counts = [year_counts[year] for year in years]
            axes[1, 1].plot(years, counts, marker='o', color='navy', linewidth=2, markersize=4)
            axes[1, 1].set_title('Company Incorporation Trend')
            axes[1, 1].set_xlabel('Year')
            axes[1, 1].set_ylabel('Number of Companies')
            axes[1, 1].grid(True, alpha=0.3)
        
        # Plot 6: Charges Distribution
        if charge_counts:
            axes[1, 2].hist(charge_counts, bins=10, alpha=0.7, color='gold', edgecolor='orange')
            axes[1, 2].set_title('Charges per Company Distribution')
            axes[1, 2].set_xlabel('Number of Charges')
            axes[1, 2].set_ylabel('Number of Companies')
            axes[1, 2].grid(True, alpha=0.3)
        
        # Plot 7: Capital vs Directors Scatter
        if authorized_capitals and director_counts:
            min_length = min(len(authorized_capitals), len(director_counts))
            capital_sample = authorized_capitals[:min_length]
            director_sample = director_counts[:min_length]
            
            axes[2, 0].scatter(capital_sample, director_sample, alpha=0.6, color='purple')
            axes[2, 0].set_title('Capital vs Directors Relationship')
            axes[2, 0].set_xlabel('Authorized Capital (Lakhs)')
            axes[2, 0].set_ylabel('Number of Directors')
            axes[2, 0].grid(True, alpha=0.3)
        
        # Plot 8: Processing Statistics
        processing_stats = [
            self.processing_stats.get('successful_searches', 0),
            self.processing_stats.get('detailed_extractions', 0),
            self.processing_stats.get('failed_extractions', 0)
        ]
        
        bars = axes[2, 1].bar(['Successful\nSearches', 'Detailed\nExtractions', 'Failed\nExtractions'], 
                             processing_stats, color=['green', 'blue', 'red'], alpha=0.7)
        axes[2, 1].set_title('Processing Statistics')
        axes[2, 1].set_ylabel('Count')
        
        for bar in bars:
            height = bar.get_height()
            axes[2, 1].text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(height)}', ha='center', va='bottom')
        
        # Plot 9: Summary Statistics
        processing_time = None
        if self.processing_stats.get('start_time') and self.processing_stats.get('end_time'):
            start_time = self.processing_stats['start_time']
            end_time = self.processing_stats['end_time']
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)
            processing_time = (end_time - start_time).total_seconds() / 60
        
        total_extractions = self.processing_stats.get('detailed_extractions', 0) + self.processing_stats.get('failed_extractions', 0)
        success_rate = (self.processing_stats.get('detailed_extractions', 0) / total_extractions * 100) if total_extractions > 0 else 0
        
        summary_text = f"""
COMPREHENSIVE ANALYSIS SUMMARY

📊 Dataset Overview:
   • Total Companies: {len(self.detailed_companies_data)}
   • Processing Time: {processing_time:.1f} min
   • Success Rate: {success_rate:.1f}%

💰 Financial Insights:
   • Capital Data: {len(authorized_capitals)} companies
   • Avg. Directors: {np.mean(director_counts):.1f}
   • Companies w/ Charges: {len([c for c in charge_counts if c > 0])}

🗺️  Geographic Coverage:
   • States: {len(set(states))}
   • Top State: {Counter(states).most_common(1)[0][0] if states else 'N/A'}

⏰ Temporal Insights:
   • Year Range: {min(incorporation_years) if incorporation_years else 'N/A'}-{max(incorporation_years) if incorporation_years else 'N/A'}
   • Peak Year: {Counter(incorporation_years).most_common(1)[0][0] if incorporation_years else 'N/A'}
        """
        
        axes[2, 2].text(0.05, 0.95, summary_text, transform=axes[2, 2].transAxes, 
                        fontsize=9, verticalalignment='top', fontfamily='monospace',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.5))
        axes[2, 2].axis('off')
        
        plt.tight_layout()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'comprehensive_financial_dashboard_{timestamp}.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Dashboard saved as: {filename}")
    
    def save_data(self, prefix="zaubacorp_comprehensive"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        all_data = {
            'metadata': {
                'extraction_date': datetime.now().isoformat(),
                'total_companies': len(self.detailed_companies_data),
                'processing_stats': self.processing_stats,
                'financial_summary': self.financial_summary
            },
            'companies': self.detailed_companies_data
        }
        
        json_filename = f"{prefix}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False, default=str)
        print(f"💾 Data saved to: {json_filename}")
        
        flattened_data = []
        for company in self.detailed_companies_data:
            base_row = {
                'company_name': company.get('company_name', 'N/A'),
                'extraction_timestamp': company.get('extraction_timestamp', ''),
                'last_updated': company.get('last_updated', ''),
            }
            
            basic_company_data = company.get('basic_company_data', {})
            base_row.update({
                'cin': basic_company_data.get('cin', ''),
                'search_term': basic_company_data.get('search_term', ''),
                'city': basic_company_data.get('city', ''),
                'state': basic_company_data.get('state', ''),
                'company_type': basic_company_data.get('company_type', '')
            })
            
            basic_info = company.get('basic_info', {})
            for key, value in basic_info.items():
                base_row[f"basic_{key.replace(' ', '_').lower()}"] = value
            
            base_row['director_count'] = len(company.get('directors', []))
            base_row['charges_count'] = len(company.get('charges', []))
            base_row['similar_companies_count'] = len(company.get('similar_companies', []))
            
            contact_info = company.get('contact_info', {})
            for key, value in contact_info.items():
                base_row[f"contact_{key}"] = value
            
            financial_info = company.get('financial_info', {})
            for key, value in financial_info.items():
                base_row[f"financial_{key.replace(' ', '_').lower()}"] = value
            
            flattened_data.append(base_row)
        
        if flattened_data:
            df = pd.DataFrame(flattened_data)
            csv_filename = f"{prefix}_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)
            print(f"💾 Flattened data exported to: {csv_filename}")
        
        return json_filename

def predefined_search_terms():
    return {
        'tech_companies': [
            'technology', 'software', 'tech', 'digital', 'it services',
            'artificial intelligence', 'machine learning', 'data analytics'
        ],
        'financial_companies': [
            'financial', 'fintech', 'banking', 'insurance', 'payment',
            'lending', 'credit', 'investment'
        ],
        'ecommerce_companies': [
            'ecommerce', 'e-commerce', 'marketplace', 'retail', 'shopping',
            'online', 'delivery', 'logistics'
        ],
        'healthcare_companies': [
            'healthcare', 'medical', 'pharma', 'health', 'diagnostic',
            'hospital', 'biotech', 'telemedicine'
        ],
        'education_companies': [
            'education', 'edtech', 'learning', 'training', 'academy',
            'skill development', 'online education'
        ]
    }

global_analyzer = ZaubaCorpMassAnalyzer()

def main():
    global global_analyzer
    
    print("🚀 ZaubaCorp Complete Mass Data Analysis & Automation Tool")
    print("=" * 70)
    
    global_analyzer.load_existing_data()
    
    while True:
        print(f"\n📊 Current Status:")
        print(f"   • Basic companies data: {len(global_analyzer.all_companies_data)} companies")
        print(f"   • Detailed companies data: {len(global_analyzer.detailed_companies_data)} companies")
        
        print("\n🎯 AUTOMATION OPTIONS:")
        print("   1. Quick sector analysis (predefined terms)")
        print("   2. Custom bulk analysis")
        print("   3. Comprehensive financial analysis")
        print("   4. Load existing data")
        print("   5. Save current data")
        print("   6. Export detailed analysis")
        print("   7. Exit")
        
        choice = input("\n👉 Choose option (1-7): ").strip()
        
        if choice == '1':
            sectors = predefined_search_terms()
            print("\n📋 Available sectors:")
            sector_list = list(sectors.keys())
            for i, sector in enumerate(sector_list, 1):
                print(f"   {i}. {sector.replace('_', ' ').title()}")
            
            sector_choice = input(f"\n🎯 Choose sector (1-{len(sector_list)}): ").strip()
            try:
                sector_index = int(sector_choice) - 1
                if 0 <= sector_index < len(sector_list):
                    selected_sector = sector_list[sector_index]
                    search_terms = sectors[selected_sector]
                    
                    max_per_term = input("📊 Max companies per search term (default: 15): ").strip()
                    max_per_term = int(max_per_term) if max_per_term else 15
                    
                    max_detailed = input("🔍 Max companies for detailed analysis (default: 30): ").strip()
                    max_detailed = int(max_detailed) if max_detailed else 30
                    
                    companies = global_analyzer.batch_search_companies(search_terms, max_per_term)
                    if companies:
                        detailed_data = global_analyzer.extract_all_detailed_info(companies, max_detailed)
                        if detailed_data:
                            print(f"\n✅ {selected_sector.replace('_', ' ')} sector analysis complete!")
                            global_analyzer.save_data(f"sector_{selected_sector}")
                        else:
                            print("❌ No detailed data extracted")
                    else:
                        print("❌ No companies found")
                else:
                    print("❌ Invalid sector choice")
            except ValueError:
                print("❌ Invalid input")
        
        elif choice == '2':
            custom_terms = input("\n📝 Enter search terms (comma-separated): ").strip()
            if custom_terms:
                search_terms = [term.strip() for term in custom_terms.split(',')]
                
                max_per_term = input("📊 Max companies per search term (default: 20): ").strip()
                max_per_term = int(max_per_term) if max_per_term else 20
                
                max_detailed = input("🔍 Max companies for detailed analysis (default: 50): ").strip()
                max_detailed = int(max_detailed) if max_detailed else 50
                
                companies = global_analyzer.batch_search_companies(search_terms, max_per_term)
                if companies:
                    detailed_data = global_analyzer.extract_all_detailed_info(companies, max_detailed)
                    if detailed_data:
                        print(f"\n✅ Custom bulk analysis complete!")
                        global_analyzer.save_data("custom_analysis")
                    else:
                        print("❌ No detailed data extracted")
                else:
                    print("❌ No companies found")
            else:
                print("❌ No search terms provided")
        
        elif choice == '3':
            if global_analyzer.detailed_companies_data:
                print(f"\n💰 Running financial analysis on {len(global_analyzer.detailed_companies_data)} companies...")
                financial_analysis = global_analyzer.analyze_financial_patterns()
                global_analyzer.create_comprehensive_analysis_dashboard()
                print("✅ Financial analysis complete!")
            else:
                print("❌ No detailed data available. Run option 1 or 2 first to collect data.")
        
        elif choice == '4':
            success = global_analyzer.load_existing_data()
            if success:
                print("✅ Data loaded successfully!")
            else:
                print("❌ Could not load existing data")
        
        elif choice == '5':
            if global_analyzer.detailed_companies_data:
                filename = global_analyzer.save_data()
                print(f"✅ Data saved successfully!")
            else:
                print("❌ No data to save")
        
        elif choice == '6':
            if global_analyzer.detailed_companies_data:
                print("📊 Creating comprehensive export...")
                global_analyzer.create_comprehensive_analysis_dashboard()
                filename = global_analyzer.save_data("comprehensive_export")
                print("✅ Comprehensive analysis exported!")
            else:
                print("❌ No data available for export")
        
        elif choice == '7':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice")

if __name__ == "__main__":
    main()
