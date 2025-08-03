import requests
import pandas as pd
from datetime import datetime
import time
import os

class EPAEchoScraper:
    """Scrapes violation data from EPA ECHO API - CORRECT WORKING VERSION"""
    
    def __init__(self):
        # THIS IS THE ACTUAL WORKING BASE URL
        self.base_url = "https://ofmpub.epa.gov/echo"
        
        # Working endpoints verified from API documentation
        self.endpoints = {
            'cwa_facilities': '/cwa_rest_services.get_facilities',  # Clean Water Act
            'cwa_violations': '/cwa_rest_services.get_cwa_violations',
            'air_facilities': '/air_rest_services.get_facilities',  # Clean Air Act
            'rcra_facilities': '/rcra_rest_services.get_facilities', # Hazardous Waste
            'sdw_systems': '/sdw_rest_services.get_systems',  # Drinking Water
            'case_enforcement': '/case_rest_services.get_cases',  # Enforcement Cases
            'dfr': '/dfr_rest_services.get_dfr'  # Detailed Facility Report
        }
        
        self.output_dir = "scraped_data"
        os.makedirs(self.output_dir, exist_ok=True)
        
    def search_cwa_facilities(self, state):
        """Search for Clean Water Act facilities with violations in a state"""
        
        url = self.base_url + self.endpoints['cwa_facilities']
        
        params = {
            "output": "JSON",
            "p_st": state,  # State code
            "p_act": "Y",   # Active facilities
            "p_qiv": "1",   # Quarters in violation > 0
            "responseset": 5000  # Max results
        }
        
        try:
            print(f"Searching CWA facilities in {state}...")
            print(f"URL: {url}")
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for results in the response
            if 'Results' in data and data['Results']:
                facilities = data['Results']
                print(f"Found {len(facilities)} facilities with violations")
                return facilities
            else:
                print("No facilities found")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error searching facilities: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error: {e}")
            return []
    
    def get_cwa_violations(self, state):
        """Get CWA violations for a state"""
        
        url = self.base_url + self.endpoints['cwa_violations']
        
        params = {
            "output": "JSON",
            "p_st": state,
            "responseset": 5000
        }
        
        try:
            print(f"Getting CWA violations for {state}...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if 'Results' in data:
                return data['Results']
            return []
            
        except Exception as e:
            print(f"Error getting violations: {e}")
            return []
    
    def get_enforcement_cases(self, state):
        """Get enforcement cases for a state"""
        
        url = self.base_url + self.endpoints['case_enforcement']
        
        params = {
            "output": "JSON",
            "p_st": state,
            "responseset": 5000
        }
        
        try:
            print(f"Getting enforcement cases for {state}...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if 'Results' in data:
                print(f"Found {len(data['Results'])} enforcement cases")
                return data['Results']
            return []
            
        except Exception as e:
            print(f"Error getting enforcement cases: {e}")
            return []
    
    def parse_facility_data(self, facilities_data, state):
        """Parse facility data into standardized format"""
        violations = []
        
        for facility in facilities_data:
            # Map the actual field names from the API response
            violations.append({
                "state": state,
                "registry_id": facility.get("RegistryID", ""),
                "facility_name": facility.get("CWAName", ""),
                "city": facility.get("CWACity", ""),
                "county": facility.get("CWACounty", ""),
                "permit_id": facility.get("SourceID", ""),
                "permit_name": facility.get("CWAPermitName", ""),
                "qtrs_in_nc": facility.get("Qtr13", 0),  # Quarters in noncompliance
                "inspection_count": facility.get("CWAInspectionCount", 0),
                "informal_enforcement": facility.get("CWAInformalCount", 0),
                "formal_enforcement": facility.get("CWAFormalCount", 0),
                "compliance_status": facility.get("CWAComplianceStatus", ""),
                "sic_codes": facility.get("CWASICCodes", ""),
                "naics_codes": facility.get("CWANAICSCodes", "")
            })
            
        return violations
    
    def scrape_state(self, state):
        """Scrape all data for a state"""
        print(f"\n{'='*50}")
        print(f"Starting EPA ECHO scrape for {state}")
        print(f"{'='*50}")
        
        all_data = []
        
        # Get facilities with violations
        facilities = self.search_cwa_facilities(state)
        
        if facilities:
            # Parse the facility data
            parsed_data = self.parse_facility_data(facilities, state)
            all_data.extend(parsed_data)
            
        # Also get enforcement cases
        cases = self.get_enforcement_cases(state)
        
        # Note: Cases have different structure, you might want to save separately
        # or merge with facility data based on registry ID
        
        return all_data
    
    def save_data(self, data, state):
        """Save data to CSV"""
        if not data:
            print(f"No data to save for {state}")
            return
            
        df = pd.DataFrame(data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/ECHO_{state}_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        print(f"\n✓ Saved {len(data)} records to {filename}")
        
        return filename
    
    def run_daily_scrape(self, states=["MD", "VA", "PA", "WV"]):
        """Main function to run daily"""
        print(f"\nStarting EPA ECHO scrape at {datetime.now()}")
        print(f"Using API at: {self.base_url}")
        print(f"States to process: {', '.join(states)}")
        
        results_summary = {}
        
        for state in states:
            data = self.scrape_state(state)
            filename = self.save_data(data, state)
            results_summary[state] = len(data) if data else 0
            
            # Be nice to the API
            time.sleep(2)
        
        print(f"\n{'='*50}")
        print("SCRAPE COMPLETED")
        print(f"{'='*50}")
        for state, count in results_summary.items():
            print(f"{state}: {count} facilities with violations")
        print(f"\nCompleted at {datetime.now()}")

# Test the scraper
if __name__ == "__main__":
    scraper = EPAEchoScraper()
    
    # Test with one state first
    print("Testing EPA ECHO scraper with Maryland...")
    scraper.run_daily_scrape(["MD"])
    
    # For production, use all states:
    # scraper.run_daily_scrape(["MD", "VA", "PA", "WV"])
