import os
import pandas as pd
from datetime import datetime

def process_echo_data():
    """Process existing ECHO data files in scraped_data folder"""
    
    print("Starting ECHO data processing...")
    
    # Create output directory
    os.makedirs('outputs', exist_ok=True)
    
    # Look for CSV files in scraped_data folder
    scraped_data_dir = 'scraped_data'
    
    if not os.path.exists(scraped_data_dir):
        print(f"Error: {scraped_data_dir} folder not found!")
        return False
    
    # Find all CSV files
    csv_files = [f for f in os.listdir(scraped_data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print("No CSV files found in scraped_data folder!")
        return False
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process water violation files
    water_files = []
    for filename in csv_files:
        if any(term in filename.lower() for term in ['violation', 'water', 'cwa', 'npdes']):
            filepath = os.path.join(scraped_data_dir, filename)
            print(f"Processing {filename}...")
            
            try:
                df = pd.read_csv(filepath, low_memory=False, encoding='latin-1')
                water_files.append({
                    'filename': filename,
                    'data': df,
                    'rows': len(df)
                })
                print(f"  - Loaded {len(df)} rows")
            except Exception as e:
                print(f"  - Error loading {filename}: {e}")
    
    if water_files:
        # Create summary
        print("\n=== SUMMARY ===")
        print(f"Successfully processed {len(water_files)} water-related files:")
        
        for file_info in water_files:
            print(f"  - {file_info['filename']}: {file_info['rows']} rows")
        
        # Save a combined summary file
        summary_data = []
        for file_info in water_files:
            summary_data.append({
                'source_file': file_info['filename'],
                'record_count': file_info['rows'],
                'processed_date': datetime.now().strftime("%Y-%m-%d")
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_file = f'outputs/processing_summary_{datetime.now().strftime("%Y%m%d")}.csv'
        summary_df.to_csv(summary_file, index=False)
        print(f"\nSaved summary to: {summary_file}")
        
        return True
    else:
        print("No water violation files found to process!")
        return False

def main():
    """Main function"""
    try:
        success = process_echo_data()
        if success:
            print("\n✅ Processing completed successfully!")
            exit(0)
        else:
            print("\n❌ Processing failed!")
            exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
