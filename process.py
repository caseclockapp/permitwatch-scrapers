import requests
import zipfile
import pandas as pd
import os
from datetime import datetime

def download_and_extract_echo_data():
    """Download ECHO bulk data files and extract them"""
    
    # Create directories
    os.makedirs('monthly_echo_downloads', exist_ok=True)
    os.makedirs('outputs', exist_ok=True)
    
    # Download ECHO_EXPORTER.zip
    print("Downloading ECHO_EXPORTER.zip...")
    url = "https://echo.epa.gov/files/echodownloads/ECHO_EXPORTER.zip"
    response = requests.get(url, stream=True)
    
    zip_path = 'monthly_echo_downloads/ECHO_EXPORTER.zip'
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    # Extract the zip file
    print("Extracting files...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall('monthly_echo_downloads/')
    
    return True

def process_water_violations():
    """Process ECHO data to extract water violations"""
    
    print("Processing water violation data...")
    
    # Look for NPDES files (water permits)
    files_to_process = [
        'NPDES_QNCR_HISTORY.csv',  # Quarterly non-compliance reports
        'NPDES_EFF_VIOLATIONS.csv',  # Effluent violations
        'NPDES_FORMAL_ENFORCEMENT_ACTIONS.csv',  # Formal actions
        'NPDES_INFORMAL_ENFORCEMENT_ACTIONS.csv',  # Informal actions
        'NPDES_FACILITIES.csv'  # Facility information
    ]
    
    combined_data = []
    
    for filename in files_to_process:
        filepath = f'monthly_echo_downloads/{filename}'
        if os.path.exists(filepath):
            print(f"Processing {filename}...")
            try:
                # Read CSV with error handling
                df = pd.read_csv(filepath, low_memory=False, encoding='latin-1')
                df['source_file'] = filename
                combined_data.append(df)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    
    if combined_data:
        # Combine all data
        print("Combining data...")
        result = pd.concat(combined_data, ignore_index=True)
        
        # Save processed data
        output_file = f'outputs/water_violations_{datetime.now().strftime("%Y%m%d")}.csv'
        result.to_csv(output_file, index=False)
        print(f"Saved processed data to {output_file}")
        
        # Create summary
        print(f"\nSummary:")
        print(f"Total records processed: {len(result)}")
        print(f"Files processed: {len(combined_data)}")
        
        return output_file
    else:
        print("No data files found to process")
        return None

def main():
    """Main function to run the entire process"""
    
    print("Starting ECHO data processing for PermitWatch...")
    print("=" * 50)
    
    # Download and extract data
    if download_and_extract_echo_data():
        # Process the data
        output_file = process_water_violations()
        
        if output_file:
            print("\n✅ Success! Data processed and saved.")
        else:
            print("\n❌ Error: No data was processed.")
    else:
        print("\n❌ Error: Failed to download data.")

if __name__ == "__main__":
    main()
