import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

class EPAEchoScraper:
    """Scrapes violation data from EPA ECHO API"""
    
    def __init__(self):
        self.base_url = "https://echo.epa.gov/tools/web-services/detailed-facility-report"
        self.search_url = "https://echo.epa.gov/tools/web-services/facility-search"
        self.output_dir = "scraped_data"
        os.makedirs(self.output_dir, exist_ok=True)
        
    def search_facilities(self, state, program="CWA"):
        """Search for facilities with violations in a state"""
        params = {
            "output": "JSON",
            "p_st": state,
            "p_ptype": program,  # CWA = Clean Water Act
            "p_qiv": "1",  # In violation
            "p_fac_ico": "Y",  # Active facilities
            "responseset": "5000"  # Max results
        }
        
        try:
            response = requests.get(self.search_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            facilities = data.get("Results", {}).get("Facilities", [])
            print(f"Found {len(facilities)} facilities in {state}")
            return facilities
            
        except Exception as e:
            print(f"Error searching facilities: {e}")
            return []
    
    def get_violations(self, registry_id):
        """Get detailed violations for a facility"""
        params = {
            "output": "JSON",
            "p_id": registry_id
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"Error getting violations for {registry_id}: {e}")
            return None
    
    def parse_violations(self, facility_data):
        """Extract violation details from facility data"""
        violations = []
        
        if not facility_data:
            return violations
            
        # Extract CWA violations
        cwa_data = facility_data.get("CWACSData", {})
        viol_data = cwa_data.get("Violations", [])
        
        for v in viol_data:
            violations.append({
                "registry_id": facility_data.get("RegistryId"),
                "facility_name": facility_data.get("FacilityName"),
                "city": facility_data.get("FacilityCity"),
                "state": facility_data.get("FacilityState"),
                "violation_date": v.get("ViolationDate"),
                "violation_code": v.get("ViolationCode"),
                "violation_desc": v.get("ViolationDesc"),
                "rnc_status": v.get("RNCStatus"),
                "enforcement_action": v.get("EnforcementAction")
            })
            
        return violations
    
    def scrape_state(self, state):
        """Scrape all violations for a state"""
        print(f"\nStarting scrape for {state}...")
        all_violations = []
        
        # Get facilities
        facilities = self.search_facilities(state)
        
        # Get violations for each facility
        for i, facility in enumerate(facilities[:100]):  # Limit for testing
            registry_id = facility.get("RegistryId")
            if not registry_id:
                continue
                
            print(f"Processing {i+1}/{len(facilities)}: {registry_id}")
            
            # Get detailed data
            facility_data = self.get_violations(registry_id)
            violations = self.parse_violations(facility_data)
            all_violations.extend(violations)
            
            # Be nice to the API
            time.sleep(0.5)
        
        return all_violations
    
    def save_data(self, violations, state):
        """Save violations to CSV"""
        if not violations:
            print(f"No violations found for {state}")
            return
            
        df = pd.DataFrame(violations)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_dir}/violations_{state}_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        print(f"Saved {len(violations)} violations to {filename}")
        
        return filename
    
    def run_daily_scrape(self, states=["MD", "VA", "PA", "WV"]):
        """Main function to run daily"""
        print(f"Starting EPA ECHO scrape at {datetime.now()}")
        
        for state in states:
            violations = self.scrape_state(state)
            self.save_data(violations, state)
            
        print(f"Scrape completed at {datetime.now()}")

# Usage
if __name__ == "__main__":
    scraper = EPAEchoScraper()
    
    # Test with one state
    scraper.run_daily_scrape(["MD"])
    
    # For production, use all states:
    # scraper.run_daily_scrape(["MD", "VA", "PA", "WV", "DE", "DC"])
