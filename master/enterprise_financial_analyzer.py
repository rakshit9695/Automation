import pandas as pd
import json
import time
import random
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import defaultdict, Counter
import re
import glob
import os
import warnings
warnings.filterwarnings('ignore')

# Financial Analysis Libraries
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import yfinance as yf

# Gemini AI Integration
import google.generativeai as genai
from typing import Dict, List, Optional, Tuple, Any

# Professional Reporting
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.io as pio

from zaubacorp_scraper import ZaubaCorpDetailedAnalyzer

class EnterpriseFinancialAnalyzer:
    def __init__(self, gemini_api_key: str):
        self.analyzer = ZaubaCorpDetailedAnalyzer()
        self.gemini_api_key = gemini_api_key
        self.setup_gemini()
        
        # Enterprise Data Storage
        self.companies_data = []
        self.financial_metrics = {}
        self.risk_assessment = {}
        self.market_analysis = {}
        self.compliance_data = {}
        self.ai_insights = {}
        
        # Financial Constants
        self.risk_free_rate = 0.065  # Current India risk-free rate
        self.market_risk_premium = 0.08
        self.corporate_tax_rate = 0.30
        
        # Industry Benchmarks
        self.industry_benchmarks = {
            'technology': {'roe': 0.15, 'debt_equity': 0.3, 'current_ratio': 1.8},
            'financial': {'roe': 0.12, 'debt_equity': 0.8, 'current_ratio': 1.2},
            'healthcare': {'roe': 0.14, 'debt_equity': 0.4, 'current_ratio': 2.0},
            'manufacturing': {'roe': 0.10, 'debt_equity': 0.6, 'current_ratio': 1.5}
        }
        
        self.processing_stats = {
            'companies_analyzed': 0,
            'ai_insights_generated': 0,
            'risk_assessments_completed': 0,
            'compliance_checks_performed': 0,
            'start_time': None,
            'end_time': None
        }
    
    def setup_gemini(self):
        """Initialize Gemini AI for intelligent analysis"""
        try:
            genai.configure(api_key=self.gemini_api_key)
            self.model = genai.GenerativeModel('gemini-pro')
            print("✅ Gemini AI initialized successfully")
        except Exception as e:
            print(f"❌ Failed to initialize Gemini AI: {e}")
            self.model = None
    
    def batch_enterprise_analysis(self, search_terms: List[str], max_companies: int = 100):
        """Perform comprehensive enterprise-level analysis"""
        print("🏢 Starting Enterprise-Level Financial Analysis...")
        self.processing_stats['start_time'] = datetime.now()
        
        # Phase 1: Data Collection
        print("\n📊 Phase 1: Data Collection & Extraction")
        all_companies = self.collect_comprehensive_data(search_terms, max_companies)
        
        if not all_companies:
            print("❌ No data collected for analysis")
            return
        
        # Phase 2: Financial Metrics Calculation
        print("\n💰 Phase 2: Advanced Financial Metrics Calculation")
        self.calculate_enterprise_metrics(all_companies)
        
        # Phase 3: Risk Assessment
        print("\n⚠️  Phase 3: Comprehensive Risk Assessment")
        self.perform_risk_analysis(all_companies)
        
        # Phase 4: Market Analysis
        print("\n📈 Phase 4: Market & Industry Analysis")
        self.conduct_market_analysis(all_companies)
        
        # Phase 5: Compliance Analysis
        print("\n📋 Phase 5: Regulatory & Compliance Analysis")
        self.assess_compliance(all_companies)
        
        # Phase 6: AI-Powered Insights
        print("\n🤖 Phase 6: AI-Powered Investment Insights")
        self.generate_ai_insights(all_companies)
        
        # Phase 7: Professional Reporting
        print("\n📊 Phase 7: Professional Report Generation")
        self.create_enterprise_dashboard()
        self.generate_executive_summary()
        
        self.processing_stats['end_time'] = datetime.now()
        print("\n✅ Enterprise Analysis Complete!")
    
    def collect_comprehensive_data(self, search_terms: List[str], max_companies: int) -> List[Dict]:
        """Collect comprehensive company data"""
        all_companies = []
        
        for i, term in enumerate(search_terms, 1):
            print(f"🔍 Processing search term {i}/{len(search_terms)}: '{term}'")
            
            try:
                companies = self.analyzer.search_companies(term, max_companies // len(search_terms))
                if companies:
                    for company in companies:
                        detailed_info = self.analyzer.get_detailed_company_info(company)
                        if detailed_info:
                            # Enhance with additional data
                            enhanced_company = self.enhance_company_data(company, detailed_info)
                            all_companies.append(enhanced_company)
                            self.processing_stats['companies_analyzed'] += 1
                
                time.sleep(random.uniform(2, 4))
                
            except Exception as e:
                print(f"❌ Error processing '{term}': {e}")
        
        self.companies_data = all_companies
        return all_companies
    
    def enhance_company_data(self, basic_data: Dict, detailed_data: Dict) -> Dict:
        """Enhance company data with calculated fields"""
        enhanced = {
            **basic_data,
            **detailed_data,
            'enhancement_timestamp': datetime.now().isoformat()
        }
        
        # Calculate financial ratios
        enhanced['financial_ratios'] = self.calculate_financial_ratios(detailed_data)
        
        # Determine industry classification
        enhanced['industry_classification'] = self.classify_industry(basic_data, detailed_data)
        
        # Calculate company age
        enhanced['company_age'] = self.calculate_company_age(detailed_data)
        
        # Risk indicators
        enhanced['risk_indicators'] = self.extract_risk_indicators(detailed_data)
        
        return enhanced
    
    def calculate_financial_ratios(self, company_data: Dict) -> Dict:
        """Calculate comprehensive financial ratios"""
        ratios = {}
        basic_info = company_data.get('basic_info', {})
        
        # Extract financial numbers
        authorized_capital = self.extract_financial_value(basic_info.get('Authorised Capital', ''))
        paid_up_capital = self.extract_financial_value(basic_info.get('Paid up capital', ''))
        
        # Capital adequacy ratios
        if authorized_capital and paid_up_capital:
            ratios['capital_utilization'] = paid_up_capital / authorized_capital
            ratios['capital_buffer'] = (authorized_capital - paid_up_capital) / authorized_capital
        
        # Director efficiency metrics
        directors_count = len(company_data.get('directors', []))
        if directors_count > 0:
            ratios['director_efficiency'] = paid_up_capital / directors_count if paid_up_capital else 0
        
        # Charge analysis
        charges = company_data.get('charges', [])
        total_charge_amount = sum(self.extract_financial_value(charge.get('amount', '')) or 0 for charge in charges)
        
        if paid_up_capital and total_charge_amount:
            ratios['debt_to_equity'] = total_charge_amount / paid_up_capital
            ratios['financial_leverage'] = (paid_up_capital + total_charge_amount) / paid_up_capital
        
        # Governance metrics
        ratios['governance_score'] = self.calculate_governance_score(company_data)
        
        return ratios
    
    def calculate_governance_score(self, company_data: Dict) -> float:
        """Calculate corporate governance score"""
        score = 0.0
        
        # Director diversity and count
        directors = company_data.get('directors', [])
        if 3 <= len(directors) <= 8:  # Optimal board size
            score += 20
        
        # Independent directors (estimated from names)
        unique_surnames = len(set(d.get('name', '').split()[-1] for d in directors if d.get('name')))
        independence_ratio = unique_surnames / len(directors) if directors else 0
        score += independence_ratio * 25
        
        # Compliance indicators
        if company_data.get('last_updated'):
            # Recent updates indicate active compliance
            score += 15
        
        # Charge management
        charges = company_data.get('charges', [])
        closed_charges = sum(1 for c in charges if c.get('closure_date') and c.get('closure_date') != '-')
        if charges:
            charge_management = closed_charges / len(charges)
            score += charge_management * 20
        else:
            score += 20  # No charges is good
        
        # Information transparency
        basic_info = company_data.get('basic_info', {})
        info_completeness = len([v for v in basic_info.values() if v and v.strip()]) / max(len(basic_info), 1)
        score += info_completeness * 20
        
        return min(score, 100)  # Cap at 100
    
    def classify_industry(self, basic_data: Dict, detailed_data: Dict) -> str:
        """Classify company into industry sector"""
        name = basic_data.get('name', '').lower()
        description = detailed_data.get('description', '').lower()
        
        industry_keywords = {
            'technology': ['tech', 'software', 'digital', 'it', 'computer', 'data', 'ai', 'ml'],
            'financial': ['financial', 'bank', 'insurance', 'investment', 'credit', 'loan'],
            'healthcare': ['health', 'medical', 'pharma', 'hospital', 'diagnostic', 'biotech'],
            'manufacturing': ['manufacturing', 'industrial', 'factory', 'production', 'steel'],
            'retail': ['retail', 'shopping', 'ecommerce', 'marketplace', 'store'],
            'energy': ['energy', 'oil', 'gas', 'renewable', 'solar', 'power'],
            'real_estate': ['real estate', 'property', 'construction', 'builder'],
            'telecommunications': ['telecom', 'communication', 'network', 'mobile']
        }
        
        text_to_analyze = f"{name} {description}"
        
        for industry, keywords in industry_keywords.items():
            if any(keyword in text_to_analyze for keyword in keywords):
                return industry
        
        return 'diversified'
    
    def calculate_company_age(self, company_data: Dict) -> Optional[int]:
        """Calculate company age from incorporation date"""
        basic_info = company_data.get('basic_info', {})
        
        for key, value in basic_info.items():
            if 'incorporation' in key.lower() or 'date' in key.lower():
                year_match = re.search(r'(\d{4})', str(value))
                if year_match:
                    inc_year = int(year_match.group(1))
                    if 1950 <= inc_year <= datetime.now().year:
                        return datetime.now().year - inc_year
        
        return None
    
    def extract_risk_indicators(self, company_data: Dict) -> Dict:
        """Extract risk indicators from company data"""
        indicators = {}
        
        # Charge-based risks
        charges = company_data.get('charges', [])
        indicators['total_charges'] = len(charges)
        indicators['active_charges'] = len([c for c in charges if not c.get('closure_date') or c.get('closure_date') == '-'])
        
        # Director stability
        directors = company_data.get('directors', [])
        if directors:
            appointment_dates = []
            for director in directors:
                date_str = director.get('appointment_date', '')
                year_match = re.search(r'(\d{4})', date_str)
                if year_match:
                    appointment_dates.append(int(year_match.group(1)))
            
            if appointment_dates:
                indicators['director_turnover_risk'] = len(set(appointment_dates)) / len(appointment_dates)
                indicators['recent_director_changes'] = len([d for d in appointment_dates if d >= datetime.now().year - 2])
        
        # Compliance risk
        last_updated = company_data.get('last_updated', '')
        if last_updated:
            # Estimate days since last update
            indicators['compliance_staleness'] = 'recent' if 'as on' in last_updated.lower() else 'outdated'
        
        return indicators
    
    def calculate_enterprise_metrics(self, companies: List[Dict]):
        """Calculate enterprise-level financial metrics"""
        print("💰 Calculating advanced financial metrics...")
        
        metrics = {
            'portfolio_summary': {},
            'sector_analysis': {},
            'risk_metrics': {},
            'valuation_metrics': {},
            'efficiency_metrics': {}
        }
        
        # Portfolio Summary
        total_authorized_capital = 0
        total_paid_up_capital = 0
        sector_distribution = Counter()
        age_distribution = []
        governance_scores = []
        
        for company in companies:
            # Capital aggregation
            auth_cap = self.extract_financial_value(company.get('basic_info', {}).get('Authorised Capital', ''))
            paid_cap = self.extract_financial_value(company.get('basic_info', {}).get('Paid up capital', ''))
            
            if auth_cap:
                total_authorized_capital += auth_cap
            if paid_cap:
                total_paid_up_capital += paid_cap
            
            # Sector distribution
            sector = company.get('industry_classification', 'unknown')
            sector_distribution[sector] += 1
            
            # Age analysis
            age = company.get('company_age')
            if age:
                age_distribution.append(age)
            
            # Governance
            governance = company.get('financial_ratios', {}).get('governance_score', 0)
            governance_scores.append(governance)
        
        metrics['portfolio_summary'] = {
            'total_companies': len(companies),
            'total_authorized_capital_cr': total_authorized_capital / 10000000,  # Convert to crores
            'total_paid_up_capital_cr': total_paid_up_capital / 10000000,
            'average_company_age': np.mean(age_distribution) if age_distribution else 0,
            'average_governance_score': np.mean(governance_scores) if governance_scores else 0,
            'sector_diversification': len(sector_distribution),
            'largest_sector': sector_distribution.most_common(1)[0] if sector_distribution else ('unknown', 0)
        }
        
        # Sector Analysis
        for sector, count in sector_distribution.items():
            sector_companies = [c for c in companies if c.get('industry_classification') == sector]
            
            sector_metrics = self.calculate_sector_metrics(sector_companies)
            metrics['sector_analysis'][sector] = {
                'company_count': count,
                'percentage_of_portfolio': (count / len(companies)) * 100,
                **sector_metrics
            }
        
        # Risk Metrics
        metrics['risk_metrics'] = self.calculate_portfolio_risk(companies)
        
        # Valuation Metrics
        metrics['valuation_metrics'] = self.calculate_valuation_metrics(companies)
        
        self.financial_metrics = metrics
        return metrics
    
    def calculate_sector_metrics(self, sector_companies: List[Dict]) -> Dict:
        """Calculate metrics for a specific sector"""
        if not sector_companies:
            return {}
        
        # Capital efficiency
        auth_capitals = []
        paid_capitals = []
        governance_scores = []
        ages = []
        
        for company in sector_companies:
            auth_cap = self.extract_financial_value(company.get('basic_info', {}).get('Authorised Capital', ''))
            paid_cap = self.extract_financial_value(company.get('basic_info', {}).get('Paid up capital', ''))
            
            if auth_cap:
                auth_capitals.append(auth_cap)
            if paid_cap:
                paid_capitals.append(paid_cap)
            
            governance = company.get('financial_ratios', {}).get('governance_score', 0)
            governance_scores.append(governance)
            
            age = company.get('company_age')
            if age:
                ages.append(age)
        
        return {
            'avg_authorized_capital_cr': np.mean(auth_capitals) / 10000000 if auth_capitals else 0,
            'avg_paid_up_capital_cr': np.mean(paid_capitals) / 10000000 if paid_capitals else 0,
            'avg_governance_score': np.mean(governance_scores) if governance_scores else 0,
            'avg_company_age': np.mean(ages) if ages else 0,
            'capital_efficiency': np.mean(paid_capitals) / np.mean(auth_capitals) if auth_capitals and paid_capitals else 0
        }
    
    def calculate_portfolio_risk(self, companies: List[Dict]) -> Dict:
        """Calculate comprehensive portfolio risk metrics"""
        risk_metrics = {}
        
        # Concentration Risk
        sector_distribution = Counter(c.get('industry_classification', 'unknown') for c in companies)
        sector_weights = [count / len(companies) for count in sector_distribution.values()]
        herfindahl_index = sum(w**2 for w in sector_weights)
        
        risk_metrics['sector_concentration_risk'] = herfindahl_index
        risk_metrics['diversification_ratio'] = 1 - herfindahl_index
        
        # Governance Risk
        governance_scores = [c.get('financial_ratios', {}).get('governance_score', 0) for c in companies]
        risk_metrics['portfolio_governance_risk'] = 100 - np.mean(governance_scores)
        risk_metrics['governance_volatility'] = np.std(governance_scores)
        
        # Age Risk (business maturity)
        ages = [c.get('company_age', 0) for c in companies if c.get('company_age')]
        if ages:
            risk_metrics['maturity_risk'] = np.std(ages) / np.mean(ages)  # Coefficient of variation
        
        # Charge Risk
        charge_risks = []
        for company in companies:
            risk_indicators = company.get('risk_indicators', {})
            active_charges = risk_indicators.get('active_charges', 0)
            total_charges = risk_indicators.get('total_charges', 0)
            
            if total_charges > 0:
                charge_risk = active_charges / total_charges
            else:
                charge_risk = 0
            
            charge_risks.append(charge_risk)
        
        risk_metrics['portfolio_charge_risk'] = np.mean(charge_risks) if charge_risks else 0
        
        # Overall Risk Score (0-100, where 100 is highest risk)
        risk_components = [
            risk_metrics['sector_concentration_risk'] * 30,
            risk_metrics['portfolio_governance_risk'] * 0.4,
            risk_metrics.get('maturity_risk', 0) * 20,
            risk_metrics['portfolio_charge_risk'] * 50
        ]
        
        risk_metrics['overall_risk_score'] = min(sum(risk_components), 100)
        
        return risk_metrics
    
    def calculate_valuation_metrics(self, companies: List[Dict]) -> Dict:
        """Calculate portfolio valuation metrics"""
        valuation_metrics = {}
        
        # Asset-based valuation approximation
        total_assets = 0
        total_equity = 0
        
        for company in companies:
            paid_cap = self.extract_financial_value(company.get('basic_info', {}).get('Paid up capital', ''))
            charges = company.get('charges', [])
            total_debt = sum(self.extract_financial_value(c.get('amount', '')) or 0 for c in charges)
            
            if paid_cap:
                total_equity += paid_cap
                total_assets += paid_cap + total_debt
        
        valuation_metrics['total_portfolio_equity_cr'] = total_equity / 10000000
        valuation_metrics['total_portfolio_assets_cr'] = total_assets / 10000000
        valuation_metrics['portfolio_leverage'] = total_assets / total_equity if total_equity > 0 else 0
        
        # Industry premium/discount analysis
        sector_premiums = {}
        for sector, benchmark in self.industry_benchmarks.items():
            sector_companies = [c for c in companies if c.get('industry_classification') == sector]
            if sector_companies:
                avg_governance = np.mean([c.get('financial_ratios', {}).get('governance_score', 0) for c in sector_companies])
                sector_premiums[sector] = (avg_governance - 50) / 50  # Relative to 50% baseline
        
        valuation_metrics['sector_premiums'] = sector_premiums
        
        return valuation_metrics
    
    def perform_risk_analysis(self, companies: List[Dict]):
        """Perform comprehensive risk analysis"""
        print("⚠️  Performing enterprise risk assessment...")
        
        risk_analysis = {
            'credit_risk': {},
            'operational_risk': {},
            'market_risk': {},
            'regulatory_risk': {},
            'strategic_risk': {}
        }
        
        # Credit Risk Analysis
        risk_analysis['credit_risk'] = self.assess_credit_risk(companies)
        
        # Operational Risk Analysis
        risk_analysis['operational_risk'] = self.assess_operational_risk(companies)
        
        # Market Risk Analysis
        risk_analysis['market_risk'] = self.assess_market_risk(companies)
        
        # Regulatory Risk Analysis
        risk_analysis['regulatory_risk'] = self.assess_regulatory_risk(companies)
        
        # Strategic Risk Analysis
        risk_analysis['strategic_risk'] = self.assess_strategic_risk(companies)
        
        self.risk_assessment = risk_analysis
        self.processing_stats['risk_assessments_completed'] = len(companies)
        
        return risk_analysis
    
    def assess_credit_risk(self, companies: List[Dict]) -> Dict:
        """Assess credit risk across portfolio"""
        credit_metrics = {}
        
        debt_ratios = []
        charge_histories = []
        
        for company in companies:
            # Debt-to-equity analysis
            financial_ratios = company.get('financial_ratios', {})
            debt_equity = financial_ratios.get('debt_to_equity', 0)
            debt_ratios.append(debt_equity)
            
            # Charge history analysis
            charges = company.get('charges', [])
            closed_charges = len([c for c in charges if c.get('closure_date') and c.get('closure_date') != '-'])
            total_charges = len(charges)
            
            if total_charges > 0:
                charge_closure_rate = closed_charges / total_charges
            else:
                charge_closure_rate = 1.0  # No charges is good
            
            charge_histories.append(charge_closure_rate)
        
        credit_metrics['average_debt_equity_ratio'] = np.mean(debt_ratios) if debt_ratios else 0
        credit_metrics['debt_ratio_volatility'] = np.std(debt_ratios) if debt_ratios else 0
        credit_metrics['average_charge_closure_rate'] = np.mean(charge_histories) if charge_histories else 1.0
        
        # Credit score calculation (0-100, higher is better)
        avg_debt_equity = credit_metrics['average_debt_equity_ratio']
        avg_closure_rate = credit_metrics['average_charge_closure_rate']
        
        # Lower debt-equity is better, higher closure rate is better
        debt_score = max(0, 100 - (avg_debt_equity * 50))  # Penalty for high debt
        closure_score = avg_closure_rate * 100
        
        credit_metrics['portfolio_credit_score'] = (debt_score + closure_score) / 2
        
        return credit_metrics
    
    def assess_operational_risk(self, companies: List[Dict]) -> Dict:
        """Assess operational risk factors"""
        operational_metrics = {}
        
        governance_scores = []
        director_stability = []
        compliance_scores = []
        
        for company in companies:
            # Governance risk
            governance = company.get('financial_ratios', {}).get('governance_score', 0)
            governance_scores.append(governance)
            
            # Director stability
            risk_indicators = company.get('risk_indicators', {})
            turnover_risk = risk_indicators.get('director_turnover_risk', 0)
            director_stability.append(1 - turnover_risk)  # Invert for stability
            
            # Compliance freshness
            compliance_staleness = risk_indicators.get('compliance_staleness', 'outdated')
            compliance_score = 100 if compliance_staleness == 'recent' else 50
            compliance_scores.append(compliance_score)
        
        operational_metrics['average_governance_score'] = np.mean(governance_scores)
        operational_metrics['average_director_stability'] = np.mean(director_stability)
        operational_metrics['average_compliance_score'] = np.mean(compliance_scores)
        
        # Overall operational risk score
        operational_risk = 100 - np.mean([
            operational_metrics['average_governance_score'],
            operational_metrics['average_director_stability'] * 100,
            operational_metrics['average_compliance_score']
        ])
        
        operational_metrics['operational_risk_score'] = operational_risk
        
        return operational_metrics
    
    def assess_market_risk(self, companies: List[Dict]) -> Dict:
        """Assess market and industry risk"""
        market_metrics = {}
        
        # Sector concentration analysis
        sectors = [c.get('industry_classification', 'unknown') for c in companies]
        sector_counts = Counter(sectors)
        
        market_metrics['sector_concentration'] = sector_counts
        market_metrics['most_concentrated_sector'] = sector_counts.most_common(1)[0]
        
        # Industry lifecycle analysis
        age_by_sector = {}
        for company in companies:
            sector = company.get('industry_classification', 'unknown')
            age = company.get('company_age')
            if age and sector:
                if sector not in age_by_sector:
                    age_by_sector[sector] = []
                age_by_sector[sector].append(age)
        
        sector_maturity = {}
        for sector, ages in age_by_sector.items():
            avg_age = np.mean(ages)
            if avg_age < 10:
                maturity = 'emerging'
            elif avg_age < 20:
                maturity = 'growth'
            else:
                maturity = 'mature'
            sector_maturity[sector] = {'avg_age': avg_age, 'maturity_stage': maturity}
        
        market_metrics['sector_maturity_analysis'] = sector_maturity
        
        # Market risk score (based on concentration and sector risks)
        concentration_risk = max(sector_counts.values()) / len(companies) * 100
        emerging_sector_exposure = len([s for s, data in sector_maturity.items() 
                                      if data['maturity_stage'] == 'emerging']) / len(sector_maturity) * 100
        
        market_risk_score = (concentration_risk * 0.6) + (emerging_sector_exposure * 0.4)
        market_metrics['market_risk_score'] = market_risk_score
        
        return market_metrics
    
    def assess_regulatory_risk(self, companies: List[Dict]) -> Dict:
        """Assess regulatory and compliance risks"""
        regulatory_metrics = {}
        
        # Compliance tracking
        compliance_statuses = []
        charge_compliance = []
        
        for company in companies:
            # Last updated compliance
            last_updated = company.get('last_updated', '')
            if 'as on' in last_updated.lower():
                compliance_statuses.append(1)  # Recent
            else:
                compliance_statuses.append(0)  # Outdated
            
            # Charge management compliance
            charges = company.get('charges', [])
            if not charges:
                charge_compliance.append(1)  # No charges is compliant
            else:
                closed_charges = len([c for c in charges if c.get('closure_date') and c.get('closure_date') != '-'])
                charge_compliance.append(closed_charges / len(charges))
        
        regulatory_metrics['compliance_rate'] = np.mean(compliance_statuses) * 100
        regulatory_metrics['charge_management_compliance'] = np.mean(charge_compliance) * 100
        
        # Regulatory risk score (lower is better)
        regulatory_risk = 100 - np.mean([
            regulatory_metrics['compliance_rate'],
            regulatory_metrics['charge_management_compliance']
        ])
        
        regulatory_metrics['regulatory_risk_score'] = regulatory_risk
        
        return regulatory_metrics
    
    def assess_strategic_risk(self, companies: List[Dict]) -> Dict:
        """Assess strategic business risks"""
        strategic_metrics = {}
        
        # Business model diversity
        sectors = [c.get('industry_classification') for c in companies]
        unique_sectors = len(set(sectors))
        diversity_score = min(unique_sectors / 8 * 100, 100)  # Max 8 sectors
        
        # Capital efficiency analysis
        capital_efficiencies = []
        for company in companies:
            ratios = company.get('financial_ratios', {})
            cap_util = ratios.get('capital_utilization', 0)
            capital_efficiencies.append(cap_util)
        
        avg_capital_efficiency = np.mean(capital_efficiencies) * 100 if capital_efficiencies else 0
        
        # Innovation proxy (younger companies in tech sectors)
        tech_companies = [c for c in companies if c.get('industry_classification') == 'technology']
        young_tech_companies = [c for c in tech_companies if c.get('company_age', 100) < 15]
        innovation_score = len(young_tech_companies) / len(companies) * 100 if companies else 0
        
        strategic_metrics['business_diversity_score'] = diversity_score
        strategic_metrics['capital_efficiency_score'] = avg_capital_efficiency
        strategic_metrics['innovation_score'] = innovation_score
        
        # Strategic risk (lower diversity and efficiency = higher risk)
        strategic_risk = 100 - np.mean([diversity_score, avg_capital_efficiency, innovation_score])
        strategic_metrics['strategic_risk_score'] = strategic_risk
        
        return strategic_metrics
    
    def conduct_market_analysis(self, companies: List[Dict]):
        """Conduct comprehensive market analysis"""
        print("📈 Conducting market and industry analysis...")
        
        market_analysis = {
            'industry_trends': {},
            'competitive_landscape': {},
            'market_opportunities': {},
            'macroeconomic_factors': {}
        }
        
        # Industry trends analysis
        market_analysis['industry_trends'] = self.analyze_industry_trends(companies)
        
        # Competitive landscape
        market_analysis['competitive_landscape'] = self.analyze_competitive_landscape(companies)
        
        # Market opportunities
        market_analysis['market_opportunities'] = self.identify_market_opportunities(companies)
        
        # Macroeconomic factors
        market_analysis['macroeconomic_factors'] = self.assess_macro_factors(companies)
        
        self.market_analysis = market_analysis
        return market_analysis
    
    def analyze_industry_trends(self, companies: List[Dict]) -> Dict:
        """Analyze industry trends and lifecycle stages"""
        trends = {}
        
        # Incorporation trend analysis
        incorporation_by_year = defaultdict(int)
        incorporation_by_sector = defaultdict(lambda: defaultdict(int))
        
        for company in companies:
            year = None
            sector = company.get('industry_classification', 'unknown')
            
            # Extract incorporation year
            basic_info = company.get('basic_info', {})
            for key, value in basic_info.items():
                if 'incorporation' in key.lower():
                    year_match = re.search(r'(\d{4})', str(value))
                    if year_match:
                        year = int(year_match.group(1))
                        break
            
            if year and 2000 <= year <= datetime.now().year:
                incorporation_by_year[year] += 1
                incorporation_by_sector[sector][year] += 1
        
        # Calculate growth trends
        recent_years = [y for y in incorporation_by_year.keys() if y >= datetime.now().year - 5]
        older_years = [y for y in incorporation_by_year.keys() if y < datetime.now().year - 5]
        
        recent_incorporations = sum(incorporation_by_year[y] for y in recent_years)
        older_incorporations = sum(incorporation_by_year[y] for y in older_years)
        
        if older_incorporations > 0:
            growth_rate = ((recent_incorporations / len(recent_years)) - 
                          (older_incorporations / len(older_years))) / (older_incorporations / len(older_years)) * 100
        else:
            growth_rate = 0
        
        trends['overall_incorporation_trend'] = {
            'growth_rate_5yr': growth_rate,
            'recent_incorporations': recent_incorporations,
            'historical_incorporations': older_incorporations
        }
        
        # Sector-wise trends
        sector_trends = {}
        for sector, year_data in incorporation_by_sector.items():
            if len(year_data) > 1:
                years = sorted(year_data.keys())
                values = [year_data[y] for y in years]
                
                # Simple trend calculation
                if len(values) > 2:
                    slope, _, r_value, _, _ = stats.linregress(years, values)
                    sector_trends[sector] = {
                        'trend_slope': slope,
                        'trend_strength': r_value ** 2,
                        'trend_direction': 'increasing' if slope > 0 else 'decreasing'
                    }
        
        trends['sector_trends'] = sector_trends
        
        return trends
    
    def analyze_competitive_landscape(self, companies: List[Dict]) -> Dict:
        """Analyze competitive landscape and market structure"""
        landscape = {}
        
        # Market share approximation (by sector and capital)
        sector_capitals = defaultdict(list)
        
        for company in companies:
            sector = company.get('industry_classification', 'unknown')
            paid_cap = self.extract_financial_value(company.get('basic_info', {}).get('Paid up capital', ''))
            
            if paid_cap:
                sector_capitals[sector].append({
                    'company': company.get('name', 'Unknown'),
                    'capital': paid_cap
                })
        
        # Calculate market concentration for each sector
        for sector, companies_data in sector_capitals.items():
            if len(companies_data) > 1:
                total_capital = sum(c['capital'] for c in companies_data)
                market_shares = [(c['capital'] / total_capital) * 100 for c in companies_data]
                
                # Herfindahl-Hirschman Index
                hhi = sum(share ** 2 for share in market_shares)
                
                # Market structure classification
                if hhi < 1500:
                    structure = 'competitive'
                elif hhi < 2500:
                    structure = 'moderately_concentrated'
                else:
                    structure = 'highly_concentrated'
                
                # Top players
                companies_data.sort(key=lambda x: x['capital'], reverse=True)
                top_players = companies_data[:3]
                
                landscape[sector] = {
                    'market_structure': structure,
                    'hhi_index': hhi,
                    'number_of_players': len(companies_data),
                    'top_players': [{'name': p['company'], 
                                   'market_share': (p['capital']/total_capital)*100} 
                                  for p in top_players]
                }
        
        return landscape
    
    def identify_market_opportunities(self, companies: List[Dict]) -> Dict:
        """Identify market opportunities and investment themes"""
        opportunities = {}
        
        # Underrepresented sectors
        sector_counts = Counter(c.get('industry_classification') for c in companies)
        total_companies = len(companies)
        
        # Expected sector distribution (benchmark)
        expected_distribution = {
            'technology': 0.25,
            'financial': 0.15,
            'healthcare': 0.12,
            'manufacturing': 0.20,
            'retail': 0.10,
            'energy': 0.08,
            'real_estate': 0.05,
            'telecommunications': 0.05
        }
        
        underrepresented_sectors = {}
        for sector, expected_pct in expected_distribution.items():
            actual_count = sector_counts.get(sector, 0)
            actual_pct = actual_count / total_companies
            
            if actual_pct < expected_pct * 0.7:  # 30% below expected
                gap = (expected_pct - actual_pct) * 100
                underrepresented_sectors[sector] = {
                    'current_representation': actual_pct * 100,
                    'expected_representation': expected_pct * 100,
                    'opportunity_gap': gap
                }
        
        opportunities['sector_gaps'] = underrepresented_sectors
        
        # High-growth potential companies (young companies in growing sectors)
        growth_companies = []
        for company in companies:
            age = company.get('company_age', 100)
            sector = company.get('industry_classification')
            governance = company.get('financial_ratios', {}).get('governance_score', 0)
            
            # Young companies with good governance in tech/healthcare
            if (age < 15 and 
                sector in ['technology', 'healthcare', 'financial'] and 
                governance > 70):
                
                growth_companies.append({
                    'name': company.get('name'),
                    'sector': sector,
                    'age': age,
                    'governance_score': governance
                })
        
        opportunities['high_growth_potential'] = growth_companies
        
        # Capital efficiency opportunities
        capital_efficient_companies = []
        for company in companies:
            ratios = company.get('financial_ratios', {})
            cap_utilization = ratios.get('capital_utilization', 0)
            governance = ratios.get('governance_score', 0)
            
            if cap_utilization > 0.8 and governance > 75:  # High efficiency + governance
                capital_efficient_companies.append({
                    'name': company.get('name'),
                    'capital_utilization': cap_utilization,
                    'governance_score': governance,
                    'sector': company.get('industry_classification')
                })
        
        opportunities['capital_efficient_targets'] = capital_efficient_companies
        
        return opportunities
    
    def assess_macro_factors(self, companies: List[Dict]) -> Dict:
        """Assess macroeconomic factors affecting the portfolio"""
        macro_factors = {}
        
        # Interest rate sensitivity (companies with high debt)
        high_debt_companies = []
        for company in companies:
            debt_equity = company.get('financial_ratios', {}).get('debt_to_equity', 0)
            if debt_equity > 0.5:  # High debt-to-equity ratio
                high_debt_companies.append(company.get('name'))
        
        interest_rate_sensitivity = len(high_debt_companies) / len(companies) * 100
        
        # Regulatory exposure (sectors with high regulatory oversight)
        regulated_sectors = ['financial', 'healthcare', 'energy', 'telecommunications']
        regulated_companies = [c for c in companies if c.get('industry_classification') in regulated_sectors]
        regulatory_exposure = len(regulated_companies) / len(companies) * 100
        
        # Economic cycle sensitivity
        cyclical_sectors = ['manufacturing', 'real_estate', 'retail']
        cyclical_companies = [c for c in companies if c.get('industry_classification') in cyclical_sectors]
        cyclical_exposure = len(cyclical_companies) / len(companies) * 100
        
        # Technology disruption risk
        traditional_sectors = ['manufacturing', 'retail', 'financial']
        disruption_vulnerable = [c for c in companies if c.get('industry_classification') in traditional_sectors]
        disruption_risk = len(disruption_vulnerable) / len(companies) * 100
        
        macro_factors = {
            'interest_rate_sensitivity': interest_rate_sensitivity,
            'regulatory_exposure': regulatory_exposure,
            'economic_cycle_sensitivity': cyclical_exposure,
            'technology_disruption_risk': disruption_risk,
            'high_debt_companies': high_debt_companies[:10]  # Top 10
        }
        
        return macro_factors
    
    def assess_compliance(self, companies: List[Dict]):
        """Assess regulatory compliance across portfolio"""
        print("📋 Performing regulatory compliance assessment...")
        
        compliance_data = {
            'roc_compliance': {},
            'director_compliance': {},
            'charge_compliance': {},
            'transparency_compliance': {},
            'overall_compliance_score': 0
        }
        
        # ROC (Registrar of Companies) Compliance
        compliance_data['roc_compliance'] = self.assess_roc_compliance(companies)
        
        # Director Compliance
        compliance_data['director_compliance'] = self.assess_director_compliance(companies)
        
        # Charge Compliance
        compliance_data['charge_compliance'] = self.assess_charge_compliance(companies)
        
        # Transparency Compliance
        compliance_data['transparency_compliance'] = self.assess_transparency_compliance(companies)
        
        # Overall compliance score
        compliance_scores = [
            compliance_data['roc_compliance'].get('compliance_score', 0),
            compliance_data['director_compliance'].get('compliance_score', 0),
            compliance_data['charge_compliance'].get('compliance_score', 0),
            compliance_data['transparency_compliance'].get('compliance_score', 0)
        ]
        
        compliance_data['overall_compliance_score'] = np.mean(compliance_scores)
        
        self.compliance_data = compliance_data
        self.processing_stats['compliance_checks_performed'] = len(companies)
        
        return compliance_data
    
    def assess_roc_compliance(self, companies: List[Dict]) -> Dict:
        """Assess ROC filing compliance"""
        roc_metrics = {}
        
        # Check for recent updates (proxy for ROC filing compliance)
        recent_updates = 0
        total_companies = len(companies)
        
        for company in companies:
            last_updated = company.get('last_updated', '')
            if 'as on' in last_updated.lower():
                # Check if update is recent (within last year)
                current_year = datetime.now().year
                if str(current_year) in last_updated or str(current_year - 1) in last_updated:
                    recent_updates += 1
        
        roc_compliance_rate = (recent_updates / total_companies) * 100 if total_companies > 0 else 0
        
        roc_metrics['recent_updates_count'] = recent_updates
        roc_metrics['compliance_rate'] = roc_compliance_rate
        roc_metrics['compliance_score'] = roc_compliance_rate
        
        # Compliance categories
        if roc_compliance_rate >= 80:
            roc_metrics['compliance_category'] = 'excellent'
        elif roc_compliance_rate >= 60:
            roc_metrics['compliance_category'] = 'good'
        elif roc_compliance_rate >= 40:
            roc_metrics['compliance_category'] = 'fair'
        else:
            roc_metrics['compliance_category'] = 'poor'
        
        return roc_metrics
    
    def assess_director_compliance(self, companies: List[Dict]) -> Dict:
        """Assess director-related compliance"""
        director_metrics = {}
        
        compliant_companies = 0
        director_issues = []
        
        for company in companies:
            directors = company.get('directors', [])
            company_name = company.get('name', 'Unknown')
            
            # Check minimum director requirement (at least 2 for private companies)
            if len(directors) < 2:
                director_issues.append(f"{company_name}: Insufficient directors ({len(directors)})")
                continue
            
            # Check maximum director limit (15 for most companies)
            if len(directors) > 15:
                director_issues.append(f"{company_name}: Too many directors ({len(directors)})")
                continue
            
            # Check for DIN (Director Identification Number)
            directors_with_din = sum(1 for d in directors if d.get('din') and len(d.get('din', '')) > 5)
            
            if directors_with_din == len(directors):
                compliant_companies += 1
            else:
                director_issues.append(f"{company_name}: Missing DIN for some directors")
        
        compliance_rate = (compliant_companies / len(companies)) * 100 if companies else 0
        
        director_metrics['compliant_companies'] = compliant_companies
        director_metrics['compliance_rate'] = compliance_rate
        director_metrics['compliance_score'] = compliance_rate
        director_metrics['director_issues'] = director_issues[:10]  # Top 10 issues
        
        return director_metrics
    
    def assess_charge_compliance(self, companies: List[Dict]) -> Dict:
        """Assess charge-related compliance"""
        charge_metrics = {}
        
        total_charges = 0
        properly_documented_charges = 0
        companies_with_charges = 0
        
        for company in companies:
            charges = company.get('charges', [])
            
            if charges:
                companies_with_charges += 1
                total_charges += len(charges)
                
                for charge in charges:
                    # Check if charge has proper documentation
                    required_fields = ['charge_id', 'creation_date', 'assets_under_charge', 'charge_holder']
                    documented_fields = sum(1 for field in required_fields if charge.get(field))
                    
                    if documented_fields >= 3:  # At least 3 out of 4 fields
                        properly_documented_charges += 1
        
        documentation_rate = (properly_documented_charges / total_charges) * 100 if total_charges > 0 else 100
        
        charge_metrics['companies_with_charges'] = companies_with_charges
        charge_metrics['total_charges'] = total_charges
        charge_metrics['documentation_rate'] = documentation_rate
        charge_metrics['compliance_score'] = documentation_rate
        
        return charge_metrics
    
    def assess_transparency_compliance(self, companies: List[Dict]) -> Dict:
        """Assess information transparency compliance"""
        transparency_metrics = {}
        
        transparency_scores = []
        
        for company in companies:
            score = 0
            
            # Basic information completeness
            basic_info = company.get('basic_info', {})
            required_fields = ['Authorised Capital', 'Paid up capital', 'Company Category', 'Company Sub Category']
            available_fields = sum(1 for field in required_fields if basic_info.get(field))
            score += (available_fields / len(required_fields)) * 30
            
            # Director information completeness
            directors = company.get('directors', [])
            if directors:
                complete_directors = sum(1 for d in directors if d.get('name') and d.get('din') and d.get('designation'))
                score += (complete_directors / len(directors)) * 25
            else:
                score += 25  # No directors reported might be compliant for some company types
            
            # Contact information availability
            contact_info = company.get('contact_info', {})
            if contact_info:
                score += 20
            
            # Charge information transparency
            charges = company.get('charges', [])
            if not charges:
                score += 25  # No charges is transparent
            else:
                documented_charges = sum(1 for c in charges if c.get('amount') and c.get('charge_holder'))
                score += (documented_charges / len(charges)) * 25
            
            transparency_scores.append(score)
        
        avg_transparency = np.mean(transparency_scores) if transparency_scores else 0
        
        transparency_metrics['average_transparency_score'] = avg_transparency
        transparency_metrics['compliance_score'] = avg_transparency
        transparency_metrics['companies_above_80_percent'] = sum(1 for s in transparency_scores if s >= 80)
        transparency_metrics['companies_below_50_percent'] = sum(1 for s in transparency_scores if s < 50)
        
        return transparency_metrics
    
    def generate_ai_insights(self, companies: List[Dict]):
        """Generate AI-powered insights using Gemini"""
        print("🤖 Generating AI-powered investment insights...")
        
        if not self.model:
            print("❌ Gemini AI not available. Skipping AI insights.")
            return {}
        
        ai_insights = {}
        
        try:
            # Prepare data summary for AI analysis
            portfolio_summary = self.prepare_portfolio_summary(companies)
            
            # Generate different types of insights
            ai_insights['investment_recommendations'] = self.get_investment_recommendations(portfolio_summary)
            ai_insights['risk_assessment'] = self.get_ai_risk_assessment(portfolio_summary)
            ai_insights['market_opportunities'] = self.get_market_opportunity_insights(portfolio_summary)
            ai_insights['strategic_recommendations'] = self.get_strategic_recommendations(portfolio_summary)
            
            self.ai_insights = ai_insights
            self.processing_stats['ai_insights_generated'] = len(ai_insights)
            
        except Exception as e:
            print(f"❌ Error generating AI insights: {e}")
            ai_insights['error'] = str(e)
        
        return ai_insights
    
    def prepare_portfolio_summary(self, companies: List[Dict]) -> str:
        """Prepare portfolio summary for AI analysis"""
        portfolio_metrics = self.financial_metrics.get('portfolio_summary', {})
        risk_metrics = self.risk_assessment.get('credit_risk', {})
        market_analysis = self.market_analysis.get('industry_trends', {})
        
        summary = f"""
PORTFOLIO ANALYSIS SUMMARY:

Portfolio Overview:
- Total Companies: {portfolio_metrics.get('total_companies', 0)}
- Total Authorized Capital: ₹{portfolio_metrics.get('total_authorized_capital_cr', 0):.1f} Crores
- Total Paid-up Capital: ₹{portfolio_metrics.get('total_paid_up_capital_cr', 0):.1f} Crores
- Average Company Age: {portfolio_metrics.get('average_company_age', 0):.1f} years
- Average Governance Score: {portfolio_metrics.get('average_governance_score', 0):.1f}/100

Sector Distribution:
{self.format_sector_distribution(companies)}

Risk Profile:
- Portfolio Credit Score: {risk_metrics.get('portfolio_credit_score', 0):.1f}/100
- Average Debt-to-Equity: {risk_metrics.get('average_debt_equity_ratio', 0):.2f}
- Charge Closure Rate: {risk_metrics.get('average_charge_closure_rate', 0):.2f}

Compliance Status:
- Overall Compliance Score: {self.compliance_data.get('overall_compliance_score', 0):.1f}/100
- ROC Compliance Rate: {self.compliance_data.get('roc_compliance', {}).get('compliance_rate', 0):.1f}%

Key Concerns:
{self.identify_key_concerns(companies)}
        """
        
        return summary
    
    def format_sector_distribution(self, companies: List[Dict]) -> str:
        """Format sector distribution for AI analysis"""
        sectors = Counter(c.get('industry_classification', 'unknown') for c in companies)
        total = len(companies)
        
        distribution = []
        for sector, count in sectors.most_common(5):
            percentage = (count / total) * 100
            distribution.append(f"- {sector.title()}: {count} companies ({percentage:.1f}%)")
        
        return '\n'.join(distribution)
    
    def identify_key_concerns(self, companies: List[Dict]) -> str:
        """Identify key concerns for AI analysis"""
        concerns = []
        
        # High-risk companies
        high_risk_companies = []
        for company in companies:
            risk_indicators = company.get('risk_indicators', {})
            governance_score = company.get('financial_ratios', {}).get('governance_score', 0)
            
            if (risk_indicators.get('active_charges', 0) > 2 or 
                governance_score < 40):
                high_risk_companies.append(company.get('name', 'Unknown'))
        
        if high_risk_companies:
            concerns.append(f"- {len(high_risk_companies)} companies with high risk indicators")
        
        # Compliance issues
        compliance_score = self.compliance_data.get('overall_compliance_score', 0)
        if compliance_score < 70:
            concerns.append(f"- Overall compliance score below 70% ({compliance_score:.1f}%)")
        
        # Concentration risk
        sectors = Counter(c.get('industry_classification', 'unknown') for c in companies)
        max_concentration = max(sectors.values()) / len(companies) * 100
        if max_concentration > 40:
            concerns.append(f"- High sector concentration ({max_concentration:.1f}% in one sector)")
        
        return '\n'.join(concerns) if concerns else "- No major concerns identified"
    
    def get_investment_recommendations(self, portfolio_summary: str) -> str:
        """Get AI-powered investment recommendations"""
        prompt = f"""
As a senior investment analyst at JP Morgan, analyze this Indian company portfolio and provide specific investment recommendations:

{portfolio_summary}

Please provide:
1. Top 3 investment recommendations with rationale
2. Specific sectors to increase/decrease exposure
3. Risk-adjusted return opportunities
4. Portfolio optimization strategies

Focus on actionable insights for institutional investors.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error generating investment recommendations: {str(e)}"
    
    def get_ai_risk_assessment(self, portfolio_summary: str) -> str:
        """Get AI-powered risk assessment"""
        prompt = f"""
As a chief risk officer at a major investment bank, analyze this portfolio's risk profile:

{portfolio_summary}

Please provide:
1. Primary risk factors and their potential impact
2. Risk mitigation strategies
3. Scenario analysis (best/worst case outcomes)
4. Recommended risk limits and controls
5. Early warning indicators to monitor

Focus on quantifiable risks and mitigation strategies.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error generating risk assessment: {str(e)}"
    
    def get_market_opportunity_insights(self, portfolio_summary: str) -> str:
        """Get AI insights on market opportunities"""
        prompt = f"""
As a market strategist analyzing the Indian market, identify opportunities in this portfolio:

{portfolio_summary}

Please provide:
1. Emerging market trends that benefit this portfolio
2. Undervalued sectors with growth potential
3. M&A opportunities and consolidation plays
4. Technology disruption opportunities
5. ESG (Environmental, Social, Governance) investment themes

Focus on opportunities in the next 12-24 months.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error generating market insights: {str(e)}"
    
    def get_strategic_recommendations(self, portfolio_summary: str) -> str:
        """Get AI-powered strategic recommendations"""
        prompt = f"""
As a strategic advisor to institutional investors, provide strategic recommendations for this Indian company portfolio:

{portfolio_summary}

Please provide:
1. Portfolio rebalancing strategy
2. Due diligence priorities for high-potential companies
3. Exit strategy recommendations for underperforming assets
4. Value creation opportunities through active ownership
5. Long-term strategic positioning (3-5 years)

Focus on actionable strategies that maximize risk-adjusted returns.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error generating strategic recommendations: {str(e)}"
    
    def create_enterprise_dashboard(self):
        """Create comprehensive enterprise-grade dashboard"""
        print("📊 Creating enterprise-grade analytical dashboard...")
        
        if not self.companies_data:
            print("❌ No data available for dashboard creation")
            return
        
        # Create interactive Plotly dashboard
        fig = make_subplots(
            rows=4, cols=3,
            subplot_titles=[
                'Portfolio Capital Distribution', 'Sector Allocation', 'Risk Heatmap',
                'Governance Score Distribution', 'Company Age vs Capital', 'Geographic Distribution',
                'Financial Leverage Analysis', 'Compliance Scorecard', 'Market Concentration',
                'Investment Opportunities', 'Risk-Return Profile', 'Executive Summary'
            ],
            specs=[
                [{"type": "histogram"}, {"type": "pie"}, {"type": "heatmap"}],
                [{"type": "histogram"}, {"type": "scatter"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}, {"type": "bar"}],
                [{"type": "table"}, {"type": "scatter"}, {"type": "table"}]
            ]
        )
        
        # Extract data for visualizations
        paid_capitals = []
        sectors = []
        governance_scores = []
        company_ages = []
        states = []
        debt_ratios = []
        company_names = []
        
        for company in self.companies_data:
            # Capital data
            paid_cap = self.extract_financial_value(company.get('basic_info', {}).get('Paid up capital', ''))
            if paid_cap:
                paid_capitals.append(paid_cap / 100000)  # Convert to lakhs
            
            # Other data
            sectors.append(company.get('industry_classification', 'unknown'))
            governance_scores.append(company.get('financial_ratios', {}).get('governance_score', 0))
            
            age = company.get('company_age')
            if age:
                company_ages.append(age)
            
            state = company.get('basic_company_data', {}).get('state', 'Unknown')
            states.append(state)
            
            debt_ratio = company.get('financial_ratios', {}).get('debt_to_equity', 0)
            debt_ratios.append(debt_ratio)
            
            company_names.append(company.get('name', 'Unknown')[:20])
        
        # 1. Portfolio Capital Distribution
        if paid_capitals:
            fig.add_trace(
                go.Histogram(x=paid_capitals, nbinsx=20, name="Capital Distribution"),
                row=1, col=1
            )
        
        # 2. Sector Allocation
        if sectors:
            sector_counts = Counter(sectors)
            fig.add_trace(
                go.Pie(labels=list(sector_counts.keys()), values=list(sector_counts.values()), name="Sectors"),
                row=1, col=2
            )
        
        # 3. Risk Heatmap (Risk metrics by sector)
        if sectors and governance_scores:
            risk_data = self.prepare_risk_heatmap_data()
            fig.add_trace(
                go.Heatmap(z=risk_data['z'], x=risk_data['x'], y=risk_data['y'], colorscale='RdYlGn_r'),
                row=1, col=3
            )
        
        # 4. Governance Score Distribution
        if governance_scores:
            fig.add_trace(
                go.Histogram(x=governance_scores, nbinsx=15, name="Governance Scores"),
                row=2, col=1
            )
        
        # 5. Company Age vs Capital
        if company_ages and paid_capitals:
            min_length = min(len(company_ages), len(paid_capitals))
            fig.add_trace(
                go.Scatter(
                    x=company_ages[:min_length], 
                    y=paid_capitals[:min_length],
                    mode='markers',
                    name="Age vs Capital",
                    text=company_names[:min_length]
                ),
                row=2, col=2
            )
        
        # 6. Geographic Distribution
        if states:
            state_counts = Counter(states)
            top_states = dict(state_counts.most_common(8))
            fig.add_trace(
                go.Bar(x=list(top_states.keys()), y=list(top_states.values()), name="States"),
                row=2, col=3
            )
        
        # 7. Financial Leverage Analysis
        if debt_ratios:
            leverage_ranges = ['0-0.5', '0.5-1.0', '1.0-2.0', '2.0+']
            leverage_counts = [
                sum(1 for r in debt_ratios if 0 <= r < 0.5),
                sum(1 for r in debt_ratios if 0.5 <= r < 1.0),
                sum(1 for r in debt_ratios if 1.0 <= r < 2.0),
                sum(1 for r in debt_ratios if r >= 2.0)
            ]
            fig.add_trace(
                go.Bar(x=leverage_ranges, y=leverage_counts, name="Leverage"),
                row=3, col=1
            )
        
        # 8. Compliance Scorecard
        compliance_metrics = self.prepare_compliance_scorecard()
        fig.add_trace(
            go.Bar(x=list(compliance_metrics.keys()), y=list(compliance_metrics.values()), name="Compliance"),
            row=3, col=2
        )
        
        # 9. Market Concentration
        sector_concentration = self.calculate_market_concentration()
        fig.add_trace(
            go.Bar(x=list(sector_concentration.keys()), y=list(sector_concentration.values()), name="Concentration"),
            row=3, col=3
        )
        
        # 10. Investment Opportunities Table
        opportunities = self.prepare_opportunities_table()
        fig.add_trace(
            go.Table(
                header=dict(values=list(opportunities.keys())),
                cells=dict(values=list(opportunities.values()))
            ),
            row=4, col=1
        )
        
        # 11. Risk-Return Profile
        risk_return_data = self.prepare_risk_return_data()
        fig.add_trace(
            go.Scatter(
                x=risk_return_data['risk'],
                y=risk_return_data['return'],
                mode='markers+text',
                text=risk_return_data['labels'],
                name="Risk-Return"
            ),
            row=4, col=2
        )
        
        # 12. Executive Summary Table
        exec_summary = self.prepare_executive_summary_table()
        fig.add_trace(
            go.Table(
                header=dict(values=['Metric', 'Value', 'Assessment']),
                cells=dict(values=[exec_summary['metrics'], exec_summary['values'], exec_summary['assessments']])
            ),
            row=4, col=3
        )
        
        # Update layout
        fig.update_layout(
            height=1600,
            showlegend=False,
            title_text="Enterprise Financial Analysis Dashboard - JP Morgan Style",
            title_x=0.5,
            font=dict(size=10)
        )
        
        # Save and display
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'enterprise_dashboard_{timestamp}.html'
        fig.write_html(filename)
        
        print(f"📊 Enterprise dashboard saved as: {filename}")
        fig.show()
    
    def prepare_risk_heatmap_data(self) -> Dict:
        """Prepare data for risk heatmap"""
        sectors = list(set(c.get('industry_classification', 'unknown') for c in self.companies_data))
        risk_types = ['Credit Risk', 'Operational Risk', 'Market Risk', 'Regulatory Risk']
        
        # Calculate risk scores for each sector-risk type combination
        z_data = []
        for risk_type in risk_types:
            row = []
            for sector in sectors:
                sector_companies = [c for c in self.companies_data if c.get('industry_classification') == sector]
                
                if risk_type == 'Credit Risk':
                    avg_debt = np.mean([c.get('financial_ratios', {}).get('debt_to_equity', 0) for c in sector_companies])
                    risk_score = min(avg_debt * 50, 100)
                elif risk_type == 'Operational Risk':
                    avg_governance = np.mean([c.get('financial_ratios', {}).get('governance_score', 0) for c in sector_companies])
                    risk_score = 100 - avg_governance
                elif risk_type == 'Market Risk':
                    risk_score = random.uniform(20, 80)  # Placeholder
                else:  # Regulatory Risk
                    compliance_score = np.mean([70, 80, 60])  # Placeholder
                    risk_score = 100 - compliance_score
                
                row.append(risk_score)
            z_data.append(row)
        
        return {'z': z_data, 'x': sectors, 'y': risk_types}
    
    def prepare_compliance_scorecard(self) -> Dict:
        """Prepare compliance scorecard data"""
        compliance_data = self.compliance_data
        
        return {
            'ROC Compliance': compliance_data.get('roc_compliance', {}).get('compliance_score', 0),
            'Director Compliance': compliance_data.get('director_compliance', {}).get('compliance_score', 0),
            'Charge Compliance': compliance_data.get('charge_compliance', {}).get('compliance_score', 0),
            'Transparency': compliance_data.get('transparency_compliance', {}).get('compliance_score', 0)
        }
    
    def calculate_market_concentration(self) -> Dict:
        """Calculate market concentration by sector"""
        sectors = Counter(c.get('industry_classification', 'unknown') for c in self.companies_data)
        total_companies = len(self.companies_data)
        
        concentration = {}
        for sector, count in sectors.most_common(6):
            concentration[sector] = (count / total_companies) * 100
        
        return concentration
    
    def prepare_opportunities_table(self) -> Dict:
        """Prepare investment opportunities table"""
        opportunities = self.market_analysis.get('market_opportunities', {})
        growth_companies = opportunities.get('high_growth_potential', [])
        
        if growth_companies:
            return {
                'Company': [c['name'][:20] for c in growth_companies[:5]],
                'Sector': [c['sector'] for c in growth_companies[:5]],
                'Age': [f"{c['age']} years" for c in growth_companies[:5]],
                'Governance': [f"{c['governance_score']:.0f}" for c in growth_companies[:5]]
            }
        else:
            return {
                'Company': ['No opportunities', 'identified', 'in current', 'dataset', ''],
                'Sector': ['', '', '', '', ''],
                'Age': ['', '', '', '', ''],
                'Governance': ['', '', '', '', '']
            }
    
    def prepare_risk_return_data(self) -> Dict:
        """Prepare risk-return scatter plot data"""
        sectors = list(set(c.get('industry_classification', 'unknown') for c in self.companies_data))
        
        risk_scores = []
        return_scores = []
        labels = []
        
        for sector in sectors:
            sector_companies = [c for c in self.companies_data if c.get('industry_classification') == sector]
            
            # Calculate average risk (higher is riskier)
            avg_governance = np.mean([c.get('financial_ratios', {}).get('governance_score', 0) for c in sector_companies])
            avg_debt = np.mean([c.get('financial_ratios', {}).get('debt_to_equity', 0) for c in sector_companies])
            risk_score = (100 - avg_governance) + (avg_debt * 20)
            
            # Calculate return proxy (capital efficiency)
            avg_cap_util = np.mean([c.get('financial_ratios', {}).get('capital_utilization', 0) for c in sector_companies])
            return_score = avg_cap_util * 100
            
            risk_scores.append(risk_score)
            return_scores.append(return_score)
            labels.append(sector)
        
        return {'risk': risk_scores, 'return': return_scores, 'labels': labels}
    
    def prepare_executive_summary_table(self) -> Dict:
        """Prepare executive summary table"""
        portfolio_metrics = self.financial_metrics.get('portfolio_summary', {})
        risk_metrics = self.risk_assessment.get('credit_risk', {})
        
        metrics = [
            'Total Companies',
            'Total Capital (Cr)',
            'Avg Governance Score',
            'Portfolio Credit Score',
            'Compliance Score',
            'Sector Diversification'
        ]
        
        values = [
            str(portfolio_metrics.get('total_companies', 0)),
            f"₹{portfolio_metrics.get('total_paid_up_capital_cr', 0):.1f}",
            f"{portfolio_metrics.get('average_governance_score', 0):.1f}/100",
            f"{risk_metrics.get('portfolio_credit_score', 0):.1f}/100",
            f"{self.compliance_data.get('overall_compliance_score', 0):.1f}/100",
            f"{portfolio_metrics.get('sector_diversification', 0)} sectors"
        ]
        
        assessments = []
        for i, value in enumerate(values):
            if i == 0:  # Total companies
                assessments.append('Good' if int(value) > 20 else 'Limited')
            elif i in [2, 3, 4]:  # Score-based metrics
                score = float(value.split('/')[0])
                if score >= 80:
                    assessments.append('Excellent')
                elif score >= 60:
                    assessments.append('Good')
                elif score >= 40:
                    assessments.append('Fair')
                else:
                    assessments.append('Poor')
            else:
                assessments.append('Adequate')
        
        return {'metrics': metrics, 'values': values, 'assessments': assessments}
    
    def generate_executive_summary(self):
        """Generate comprehensive executive summary report"""
        print("📋 Generating executive summary report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"executive_summary_{timestamp}.txt"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("EXECUTIVE SUMMARY - INDIAN COMPANY PORTFOLIO ANALYSIS\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Analyst: Enterprise Financial Analyzer with Gemini AI\n")
            f.write(f"Report Type: Comprehensive Investment Analysis\n\n")
            
            # Portfolio Overview
            portfolio_metrics = self.financial_metrics.get('portfolio_summary', {})
            f.write("PORTFOLIO OVERVIEW:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Companies Analyzed: {portfolio_metrics.get('total_companies', 0)}\n")
            f.write(f"Total Authorized Capital: ₹{portfolio_metrics.get('total_authorized_capital_cr', 0):.1f} Crores\n")
            f.write(f"Total Paid-up Capital: ₹{portfolio_metrics.get('total_paid_up_capital_cr', 0):.1f} Crores\n")
            f.write(f"Average Company Age: {portfolio_metrics.get('average_company_age', 0):.1f} years\n")
            f.write(f"Average Governance Score: {portfolio_metrics.get('average_governance_score', 0):.1f}/100\n")
            f.write(f"Sector Diversification: {portfolio_metrics.get('sector_diversification', 0)} sectors\n\n")
            
            # Key Findings
            f.write("KEY FINDINGS:\n")
            f.write("-" * 20 + "\n")
            
            # Risk Assessment Summary
            overall_risk = self.risk_assessment.get('credit_risk', {}).get('portfolio_credit_score', 0)
            f.write(f"• Portfolio Credit Score: {overall_risk:.1f}/100 ")
            if overall_risk >= 80:
                f.write("(Excellent)\n")
            elif overall_risk >= 60:
                f.write("(Good)\n")
            elif overall_risk >= 40:
                f.write("(Fair)\n")
            else:
                f.write("(Poor)\n")
            
            # Compliance Summary
            compliance_score = self.compliance_data.get('overall_compliance_score', 0)
            f.write(f"• Overall Compliance Score: {compliance_score:.1f}/100\n")
            
            # Sector Concentration
            largest_sector = portfolio_metrics.get('largest_sector', ('unknown', 0))
            concentration = (largest_sector[1] / portfolio_metrics.get('total_companies', 1)) * 100
            f.write(f"• Largest Sector Exposure: {largest_sector[0].title()} ({concentration:.1f}%)\n")
            
            # AI Insights Summary
            f.write("\nAI-POWERED INSIGHTS:\n")
            f.write("-" * 20 + "\n")
            
            if self.ai_insights:
                for insight_type, insight_content in self.ai_insights.items():
                    if insight_type != 'error':
                        f.write(f"\n{insight_type.replace('_', ' ').title()}:\n")
                        # Extract first paragraph of AI insight
                        first_paragraph = insight_content.split('\n\n')[0] if isinstance(insight_content, str) else str(insight_content)
                        f.write(f"{first_paragraph[:300]}...\n")
            else:
                f.write("AI insights not available in this analysis.\n")
            
            # Recommendations
            f.write("\nKEY RECOMMENDATIONS:\n")
            f.write("-" * 20 + "\n")
            
            recommendations = []
            
            # Risk-based recommendations
            if overall_risk < 60:
                recommendations.append("• Improve portfolio credit quality through enhanced due diligence")
            
            # Compliance recommendations
            if compliance_score < 70:
                recommendations.append("• Strengthen compliance monitoring and reporting processes")
            
            # Diversification recommendations
            if concentration > 40:
                recommendations.append(f"• Reduce concentration in {largest_sector[0]} sector")
            
            # Governance recommendations
            avg_governance = portfolio_metrics.get('average_governance_score', 0)
            if avg_governance < 70:
                recommendations.append("• Focus on companies with stronger governance practices")
            
            # Default recommendations if none triggered
            if not recommendations:
                recommendations = [
                    "• Continue monitoring portfolio performance and risk metrics",
                    "• Evaluate opportunities for portfolio optimization",
                    "• Maintain current diversification strategy"
                ]
            
            for rec in recommendations:
                f.write(f"{rec}\n")
            
            # Processing Statistics
            f.write("\nANALYSIS STATISTICS:\n")
            f.write("-" * 20 + "\n")
            f.write(f"Companies Analyzed: {self.processing_stats['companies_analyzed']}\n")
            f.write(f"Risk Assessments Completed: {self.processing_stats['risk_assessments_completed']}\n")
            f.write(f"Compliance Checks Performed: {self.processing_stats['compliance_checks_performed']}\n")
            f.write(f"AI Insights Generated: {self.processing_stats['ai_insights_generated']}\n")
            
            if self.processing_stats['start_time'] and self.processing_stats['end_time']:
                duration = self.processing_stats['end_time'] - self.processing_stats['start_time']
                f.write(f"Total Processing Time: {duration.total_seconds() / 60:.1f} minutes\n")
            
            f.write("\n" + "=" * 60 + "\n")
            f.write("This analysis was generated by Enterprise Financial Analyzer\n")
            f.write("with Gemini AI integration for institutional investment purposes.\n")
        
        print(f"📋 Executive summary saved as: {filename}")
    
    def extract_financial_value(self, value_str: str) -> Optional[float]:
        """Extract numerical value from financial strings"""
        if not value_str or not isinstance(value_str, str):
            return None
        
        # Remove currency symbols and clean up
        clean_str = re.sub(r'[₹$,\s]', '', value_str.upper())
        
        # Extract number and unit
        match = re.search(r'([\d.]+)\s*(CRORE|CR|LAKH|LAKHS?|THOUSAND|K)?', clean_str)
        
        if not match:
            return None
        
        try:
            number = float(match.group(1))
            unit = match.group(2) if match.group(2) else ''
            
            # Convert to rupees
            if 'CRORE' in unit or 'CR' in unit:
                return number * 10000000
            elif 'LAKH' in unit:
                return number * 100000
            elif 'THOUSAND' in unit or 'K' in unit:
                return number * 1000
            else:
                return number
                
        except ValueError:
            return None
    
    def save_comprehensive_analysis(self, prefix="enterprise_analysis"):
        """Save all analysis data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        comprehensive_data = {
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'total_companies': len(self.companies_data),
                'processing_stats': self.processing_stats,
                'analysis_type': 'enterprise_financial_analysis'
            },
            'companies_data': self.companies_data,
            'financial_metrics': self.financial_metrics,
            'risk_assessment': self.risk_assessment,
            'market_analysis': self.market_analysis,
            'compliance_data': self.compliance_data,
            'ai_insights': self.ai_insights
        }
        
        filename = f"{prefix}_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"💾 Comprehensive analysis saved as: {filename}")
        return filename

def main():
    print("🏢 Enterprise Financial Analyzer with Gemini AI")
    print("=" * 70)
    print("Professional-grade analysis for institutional investors")
    
    # Get Gemini API key
    gemini_api_key = "AIzaSyC4m9TI1nqpSLL3myiwiE2TKeIti1pjWQ8"
    
    if not gemini_api_key:
        print("❌ Gemini API key is required for AI insights")
        return
    
    analyzer = EnterpriseFinancialAnalyzer(gemini_api_key)
    
    print("\n🎯 ENTERPRISE ANALYSIS OPTIONS:")
    print("   1. Technology Sector Deep Dive")
    print("   2. Financial Services Analysis")
    print("   3. Custom Multi-Sector Analysis")
    print("   4. Exit")
    
    choice = input("\n👉 Choose analysis type (1-4): ").strip()
    
    if choice == '1':
        search_terms = ['technology', 'software', 'fintech', 'ai', 'digital']
        print("\n🚀 Starting Technology Sector Analysis...")
        analyzer.batch_enterprise_analysis(search_terms, max_companies=50)
        
    elif choice == '2':
        search_terms = ['financial', 'banking', 'insurance', 'investment', 'credit']
        print("\n🚀 Starting Financial Services Analysis...")
        analyzer.batch_enterprise_analysis(search_terms, max_companies=50)
        
    elif choice == '3':
        custom_terms = input("\n📝 Enter search terms (comma-separated): ").strip()
        if custom_terms:
            search_terms = [term.strip() for term in custom_terms.split(',')]
            max_companies = input("📊 Maximum companies to analyze (default: 100): ").strip()
            max_companies = int(max_companies) if max_companies else 100
            
            print(f"\n🚀 Starting Custom Analysis...")
            analyzer.batch_enterprise_analysis(search_terms, max_companies)
        else:
            print("❌ No search terms provided")
    
    elif choice == '4':
        print("👋 Goodbye!")
        return
    
    else:
        print("❌ Invalid choice")
        return
    
    # Save comprehensive analysis
    analyzer.save_comprehensive_analysis()
    
    print("\n✅ Enterprise analysis complete!")
    print("📊 Dashboard, reports, and data files have been generated.")
    print("🤖 AI insights included for strategic decision making.")

if __name__ == "__main__":
    # Required packages
    required_packages = [
        'pandas', 'numpy', 'matplotlib', 'seaborn', 'plotly', 
        'scikit-learn', 'scipy', 'yfinance', 'google-generativeai',
        'requests', 'beautifulsoup4', 'selenium'
    ]
    
    print("📦 Required packages:")
    for package in required_packages:
        print(f"   • {package}")
    print(f"\n💡 Install with: pip install {' '.join(required_packages)}")
    print("\n" + "="*70)
    
    main()
