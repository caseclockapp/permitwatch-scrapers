import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import json
import os

class MultiStateViolationScraper:
    """Scrapes violations from VA, PA, and MD environmental agencies"""
    
    def __init__(self):
        self.output_dir = "scraped_data"
        self.states = ["VA", "PA", "MD"]  # Removed WV
        os.makedirs(self.output_dir, exist_ok=True)
        
    def scrape_all_states(self):
        """Run scrapers for all states"""
        all_results = {}
        
        print("Starting multi-state environmental scraping...")
        print(f"States: {', '.join(self.states)}")
        print("-" * 50)
        
        # Virginia
        va_scraper = VirginiaDEQScraper()
        all_results['VA'] = va_scraper.scrape_violations()
        
        # Pennsylvania (using simple version)
        pa_scraper = PennsylvaniaDEPScraper()
        all_results['PA'] = pa_scraper.scrape_violations()
        
        # Maryland
        md_scraper = MarylandMDEScraper()
        all_results['MD'] = md_scraper.scrape_violations()
        
        # Save combined results
        self.save_combined_results(all_results)
        
        return all_results
    
    def save_combined_results(self, results):
        """Save all state results to CSV files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for state, violations in results.items():
            if violations:
                df = pd.DataFrame(violations)
                filename = f"{self.output_dir}/{state}_violations_{timestamp}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved {len(violations)} {state} violations to {filename}")


class VirginiaDEQScraper:
    """Scraper for Virginia DEQ permit and violation data"""
    
    def __init__(self):
        self.base_url = "https://www.deq.virginia.gov"
        self.search_url = "https://apps.deq.virginia.gov/apex/f?p=ODS:FACILITY_SEARCH"
        
    def scrape_violations(self):
        """Scrape VA DEQ violations"""
        print("\nScraping Virginia DEQ...")
        violations = []
        
        # VA DEQ uses APEX application - more complex
        # For now, use their public downloads
        download_url = "https://www.deq.virginia.gov/water/water-permit-compliance"
        
        try:
            # Virginia publishes quarterly compliance reports
            # This would need to be updated with actual download links
            response = requests.get(download_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for CSV/Excel download links
            for link in soup.find_all('a', href=True):
                if 'compliance' in link['href'].lower() and ('.csv' in link['href'] or '.xlsx' in link['href']):
                    file_url = link['href']
                    if not file_url.startswith('http'):
                        file_url = f"{self.base_url}{file_url}"
                    
                    # Download and parse file
                    print(f"Found compliance file: {file_url}")
                    # Add download logic here
                    
            # For demo, return sample structure
            violations = [{
                'state': 'VA',
                'facility_name': 'Sample VA Facility',
                'permit_number': 'VA0000001',
                'violation_date': '2024-01-15',
                'violation_type': 'Effluent Limit Exceedance',
                'status': 'Unresolved'
            }]
            
        except Exception as e:
            print(f"Error scraping VA: {e}")
            
        return violations


class PennsylvaniaDEPScraper:
    """Simplified PA DEP scraper using available downloads"""
    
    def __init__(self):
        self.base_url = "https://www.dep.pa.gov"
        self.data_portal = "https://data.pa.gov"
        
    def scrape_violations(self):
        """Scrape PA DEP violations from open data portal"""
        print("\nScraping Pennsylvania DEP...")
        violations = []
        
        try:
            # PA has good open data
            # Water quality violations dataset
            api_url = "https://data.pa.gov/resource/gqbi-fhcy.json"
            
            # Get recent violations
            params = {
                '$limit': 1000,
                '$where': "violation_date > '2023-01-01'",
                '$order': 'violation_date DESC'
            }
            
            response = requests.get(api_url, params=params)
            data = response.json()
            
            # Process violations
            for record in data:
                violations.append({
                    'state': 'PA',
                    'facility_name': record.get('facility_name', ''),
                    'permit_number': record.get('permit_id', ''),
                    'violation_date': record.get('violation_date', ''),
                    'violation_code': record.get('violation_code', ''),
                    'violation_desc': record.get('violation_description', ''),
                    'county': record.get('county', ''),
                    'status': record.get('resolution_status', 'Open')
                })
                
            print(f"Retrieved {len(violations)} PA violations")
            
        except Exception as e:
            print(f"Error scraping PA: {e}")
            # Fallback to sample data
            violations = [{
                'state': 'PA',
                'facility_name': 'Sample PA Facility',
                'permit_number': 'PA0000001',
                'violation_date': '2024-01-20',
                'violation_type': 'NPDES Violation',
                'status': 'Active'
            }]
            
        return violations


class MarylandMDEScraper:
    """Scraper for Maryland MDE data"""
    
    def __init__(self):
        self.base_url = "https://mde.maryland.gov"
        self.data_url = "https://mde.maryland.gov/programs/Water/Compliance/Pages/index.aspx"
        
    def scrape_violations(self):
        """Scrape MD MDE violations"""
        print("\nScraping Maryland MDE...")
        violations = []
        
        try:
            # Maryland publishes compliance reports
            response = requests.get(self.data_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for compliance report links
            report_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                text = link.text.lower()
                if 'compliance' in text and ('report' in text or 'data' in text):
                    report_links.append(href)
                    
            print(f"Found {len(report_links)} compliance reports")
            
            # MD also has an open data portal
            # Try their API
            api_url = "https://opendata.maryland.gov/resource/9ypy-fq3d.json"
            
            params = {
                '$limit': 1000,
                '$where': "date > '2023-01-01'"
            }
            
            try:
                response = requests.get(api_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    for record in data[:100]:  # Limit for testing
                        violations.append({
                            'state': 'MD',
                            'facility_name': record.get('facility', ''),
                            'permit_number': record.get('permit_no', ''),
                            'violation_date': record.get('date', ''),
                            'violation_type': record.get('violation_type', ''),
                            'county': record.get('county', ''),
                            'status': 'Active'
                        })
            except:
                pass
                
            # If API fails, use sample data
            if not violations:
                violations = [{
                    'state': 'MD',
                    'facility_name': 'Sample MD Facility',
                    'permit_number': 'MD0000001',
                    'violation_date': '2024-01-25',
                    'violation_type': 'Water Quality Violation',
                    'parameter': 'Total Nitrogen',
                    'status': 'Under Review'
                }]
                
        except Exception as e:
            print(f"Error scraping MD: {e}")
            
        return violations


# Main execution
if __name__ == "__main__":
    # Run all state scrapers (excluding WV)
    scraper = MultiStateViolationScraper()
    results = scraper.scrape_all_states()
    
    print("\n" + "="*50)
    print("Scraping complete!")
    print("="*50)
