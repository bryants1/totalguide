#!/usr/bin/env python3
"""
Golf Course Pars and Tees CSV Upload Script
Reads CSV files and uploads to database via API endpoints
"""

import pandas as pd
import requests
import json
import argparse
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Configuration
API_BASE_URL = 'http://localhost:3000'
FILES_CONFIG = {
    'pars': {
        'filename': 'pars_export_with_course_numbers_20250801_163153.csv',
        'endpoint': '/api/import/pars'
    },
    'tees': {
        'filename': 'tees_export_with_course_numbers_20250801_163153.csv',
        'endpoint': '/api/import/tees'
    }
}

def make_api_request(endpoint: str, data: List[Dict]) -> Dict:
    """Make API request to upload data"""
    try:
        url = f"{API_BASE_URL}{endpoint}"

        # Additional JSON serialization check
        try:
            json_data = json.dumps({"data": data})
        except (ValueError, TypeError) as e:
            print(f"❌ JSON serialization error: {str(e)}")
            # Try to identify problematic records
            for i, record in enumerate(data):
                try:
                    json.dumps(record)
                except (ValueError, TypeError):
                    print(f"   Problematic record {i}: {record}")
                    # Clean the problematic record
                    for key, value in record.items():
                        if isinstance(value, float):
                            if pd.isna(value) or not (-1e308 <= value <= 1e308):
                                print(f"     Fixing problematic value {key}={value}")
                                record[key] = None
            raise

        payload = {"data": data}

        print(f"📤 Making API request to {endpoint}")
        response = requests.post(
            url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=300  # 5 minute timeout
        )

        if not response.ok:
            error_text = response.text
            try:
                error_json = response.json()
                error_msg = error_json.get('error', 'Unknown error')
            except:
                error_msg = error_text

            raise Exception(f"HTTP {response.status_code}: {error_msg}")

        return response.json()

    except requests.exceptions.Timeout:
        raise Exception("Request timed out after 5 minutes")
    except requests.exceptions.ConnectionError:
        raise Exception("Connection failed - is the server running?")
    except Exception as e:
        print(f"❌ API request failed for {endpoint}: {str(e)}")
        raise

def read_csv_file(filename: str) -> pd.DataFrame:
    """Read and parse CSV file"""
    try:
        print(f"📖 Reading file: {filename}")

        file_path = Path(filename)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path.absolute()}")

        # Read CSV with pandas
        df = pd.read_csv(filename)

        # Clean column names (strip whitespace)
        df.columns = df.columns.str.strip()

        print(f"✅ Successfully read {filename}: {len(df)} rows, {len(df.columns)} columns")
        print(f"   Columns: {list(df.columns)}")

        return df

    except Exception as e:
        print(f"❌ Error reading {filename}: {str(e)}")
        raise

def clean_data(df: pd.DataFrame, data_type: str) -> List[Dict]:
    """Clean and transform data (removes id and course_id columns)"""
    print(f"🧹 Cleaning {data_type} data...")

    # Drop id and course_id columns if they exist
    columns_to_drop = ['id', 'course_id']
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]

    if existing_columns_to_drop:
        df_cleaned = df.drop(columns=existing_columns_to_drop)
        print(f"   Dropped columns: {existing_columns_to_drop}")
    else:
        df_cleaned = df.copy()
        print(f"   No id/course_id columns found to drop")

    # Check for problematic values
    print(f"🔍 Checking for problematic values...")

    # Replace infinite values with None
    df_cleaned = df_cleaned.replace([float('inf'), float('-inf')], None)

    # Check for NaN values
    nan_counts = df_cleaned.isnull().sum()
    total_nans = nan_counts.sum()
    if total_nans > 0:
        print(f"   Found {total_nans} NaN values across columns")
        # Show columns with most NaNs
        top_nan_cols = nan_counts[nan_counts > 0].head()
        for col, count in top_nan_cols.items():
            print(f"     {col}: {count} NaN values")

    # Convert to list of dictionaries and handle NaN/None values properly
    records = []
    for _, row in df_cleaned.iterrows():
        record = {}
        for col, value in row.items():
            if pd.isna(value) or value in [float('inf'), float('-inf')]:
                record[col] = None
            elif isinstance(value, float):
                # Check if it's a whole number that should be an int
                if value.is_integer():
                    record[col] = int(value)
                else:
                    record[col] = value
            else:
                record[col] = value
        records.append(record)

    print(f"✅ Cleaned {data_type} data: {len(records)} records")

    # Show a sample of the first record for debugging
    if records:
        print(f"🔍 Sample cleaned record keys: {list(records[0].keys())}")
        # Check for any remaining problematic values
        sample_record = records[0]
        problematic_values = []
        for key, value in sample_record.items():
            if value is not None and isinstance(value, float):
                if not (-1.7976931348623157e+308 <= value <= 1.7976931348623157e+308):
                    problematic_values.append(f"{key}={value}")
        if problematic_values:
            print(f"⚠️ Still found problematic float values: {problematic_values}")

    return records

def upload_in_batches(data: List[Dict], endpoint: str, batch_size: int = 50) -> Dict:
    """Upload data in batches"""
    print(f"📤 Uploading {len(data)} records in batches of {batch_size}...")

    total_imported = 0
    total_errors = 0
    all_error_details = []

    # Split data into batches
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        batch_number = (i // batch_size) + 1
        total_batches = (len(data) + batch_size - 1) // batch_size

        print(f"📦 Processing batch {batch_number}/{total_batches} ({len(batch)} records)...")

        try:
            result = make_api_request(endpoint, batch)

            print(f"✅ Batch {batch_number} completed:")
            print(f"   - Imported: {result['stats']['imported']}")
            print(f"   - Errors: {result['stats']['errors']}")

            total_imported += result['stats']['imported']
            total_errors += result['stats']['errors']

            if result.get('errorDetails'):
                all_error_details.extend(result['errorDetails'])

            # Small delay between batches
            time.sleep(0.1)

        except Exception as e:
            print(f"❌ Batch {batch_number} failed: {str(e)}")
            total_errors += len(batch)

            # Add all records in this batch to error details
            for record in batch:
                all_error_details.append({
                    'course': record.get('course_number') or record.get('course_name') or 'Unknown',
                    'error': f"Batch upload failed: {str(e)}"
                })

    return {
        'total': len(data),
        'imported': total_imported,
        'errors': total_errors,
        'errorDetails': all_error_details
    }

def check_server_health() -> bool:
    """Check if server is healthy and ready"""
    try:
        print('🏥 Checking server health...')

        response = requests.get(f"{API_BASE_URL}/api/health", timeout=10)

        if not response.ok:
            raise Exception('Server health check failed')

        result = response.json()
        if not result.get('success'):
            raise Exception('Server reported unhealthy status')

        print('✅ Server is healthy and ready')
        print(f"   - Supabase connected: {result.get('supabaseConnected', 'Unknown')}")

        return True

    except requests.exceptions.ConnectionError:
        print('❌ Connection failed - is the server running on http://localhost:3000?')
        return False
    except Exception as e:
        print(f'❌ Server health check failed: {str(e)}')
        return False

def show_sample_data(data: List[Dict], data_type: str, num_samples: int = 2):
    """Show sample records for verification"""
    if not data:
        print(f"   No {data_type} data to show")
        return

    print(f"📊 Sample {data_type} records:")
    for i, record in enumerate(data[:num_samples]):
        print(f"   Record {i+1}:")
        for key, value in record.items():
            if value is not None:
                print(f"     {key}: {value}")
        print()

def upload_pars_and_tees(pars_only: bool = False, tees_only: bool = False, batch_size: int = 50):
    """Main upload function"""
    print('🚀 Starting Pars and Tees CSV Upload Process')
    print('===============================================')

    try:
        # Check server health first
        if not check_server_health():
            print('❌ Cannot proceed without healthy server connection')
            sys.exit(1)

        # Filter files based on options
        files_to_process = FILES_CONFIG.copy()
        if pars_only:
            files_to_process = {k: v for k, v in files_to_process.items() if k == 'pars'}
            print('📋 Running in PARS-ONLY mode')
        elif tees_only:
            files_to_process = {k: v for k, v in files_to_process.items() if k == 'tees'}
            print('📋 Running in TEES-ONLY mode')

        results = {}

        # Process each file type
        for data_type, config in files_to_process.items():
            print(f"\n📋 Processing {data_type.upper()} data")
            print('================================')

            try:
                # Read and parse CSV
                df = read_csv_file(config['filename'])

                if len(df) == 0:
                    print(f"⚠️ No data found in {config['filename']}")
                    continue

                # Clean data (remove id and course_id columns)
                cleaned_data = clean_data(df, data_type)

                # Show sample of cleaned data
                show_sample_data(cleaned_data, data_type)

                # Upload data
                upload_result = upload_in_batches(cleaned_data, config['endpoint'], batch_size)

                results[data_type] = upload_result

                print(f"\n✅ {data_type.upper()} upload completed:")
                print(f"   - Total records: {upload_result['total']}")
                print(f"   - Successfully imported: {upload_result['imported']}")
                print(f"   - Errors: {upload_result['errors']}")

                if upload_result['errorDetails']:
                    print(f"\n⚠️ Error details for {data_type}:")
                    for error in upload_result['errorDetails'][:5]:  # Show first 5 errors
                        print(f"   - {error['course']}: {error['error']}")

                    if len(upload_result['errorDetails']) > 5:
                        print(f"   ... and {len(upload_result['errorDetails']) - 5} more errors")

            except Exception as e:
                print(f"❌ Failed to process {data_type}: {str(e)}")
                results[data_type] = {
                    'total': 0,
                    'imported': 0,
                    'errors': 0,
                    'errorDetails': [{'course': 'All', 'error': str(e)}]
                }

        # Final summary
        print('\n🎯 FINAL SUMMARY')
        print('================')

        grand_total_imported = 0
        grand_total_errors = 0

        for data_type, result in results.items():
            print(f"{data_type.upper()}:")
            print(f"  ✅ Imported: {result['imported']}")
            print(f"  ❌ Errors: {result['errors']}")

            grand_total_imported += result['imported']
            grand_total_errors += result['errors']

        print(f"\nGRAND TOTAL:")
        print(f"  ✅ Total imported: {grand_total_imported}")
        print(f"  ❌ Total errors: {grand_total_errors}")

        if grand_total_errors == 0:
            print('\n🎉 All data uploaded successfully!')
        else:
            print('\n⚠️ Some errors occurred during upload. Check logs above for details.')

        return results

    except Exception as e:
        print(f'\n💥 Fatal error: {str(e)}')
        sys.exit(1)

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description='Upload golf course pars and tees CSV data to database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_pars_tees.py                    # Upload both pars and tees
  python upload_pars_tees.py --pars-only        # Upload only pars
  python upload_pars_tees.py --tees-only        # Upload only tees
  python upload_pars_tees.py --batch-size 25    # Use smaller batches

Files expected in same directory:
  - pars_export_with_course_numbers_20250801_163153.csv
  - tees_export_with_course_numbers_20250801_163153.csv

Server must be running on http://localhost:3000
        """
    )

    parser.add_argument('--pars-only', action='store_true',
                      help='Upload only pars data')
    parser.add_argument('--tees-only', action='store_true',
                      help='Upload only tees data')
    parser.add_argument('--batch-size', type=int, default=50,
                      help='Batch size for uploads (default: 50)')

    args = parser.parse_args()

    # Validate arguments
    if args.pars_only and args.tees_only:
        print("❌ Cannot specify both --pars-only and --tees-only")
        sys.exit(1)

    if args.batch_size < 1:
        print("❌ Batch size must be at least 1")
        sys.exit(1)

    # Run upload
    upload_pars_and_tees(
        pars_only=args.pars_only,
        tees_only=args.tees_only,
        batch_size=args.batch_size
    )

if __name__ == '__main__':
    main()
