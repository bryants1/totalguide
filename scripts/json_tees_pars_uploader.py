#!/usr/bin/env python3
"""
Minimal Working JSON Uploader
"""

import os
import json
import requests
import re
import sys
from pathlib import Path

def check_existing_data(course_numbers):
    """Check for existing course_scraping_data"""
    supabase_url = "https://pmpymmdayzqsxrbymxvh.supabase.co/rest/v1"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y"

    headers = {
        'apikey': supabase_key,
        'Authorization': f'Bearer {supabase_key}',
        'Content-Type': 'application/json'
    }

    existing_tees = set()
    existing_pars = set()

    if course_numbers:
        course_filter = ','.join([f'"{num}"' for num in course_numbers])

        # Check tees
        try:
            response = requests.get(
                f"{supabase_url}/course_tees",
                headers=headers,
                params={
                    'select': 'course_number',
                    'course_number': f'in.({course_filter})',
                    'data_group': 'eq.course_scraping_data'
                }
            )
            if response.status_code == 200:
                existing_tees = {row['course_number'] for row in response.json()}
        except:
            pass

        # Check pars
        try:
            response = requests.get(
                f"{supabase_url}/course_pars",
                headers=headers,
                params={
                    'select': 'course_number',
                    'course_number': f'in.({course_filter})',
                    'data_group': 'eq.course_scraping_data'
                }
            )
            if response.status_code == 200:
                existing_pars = {row['course_number'] for row in response.json()}
        except:
            pass

    return existing_tees, existing_pars

def main():
    # Get directory from command line
    if len(sys.argv) > 1:
        json_dir = sys.argv[1]
    else:
        json_dir = "tees_pars"

    print("🚀 Minimal JSON Uploader")
    print("=" * 30)

    # Find JSON files
    json_path = Path(json_dir)
    if not json_path.exists():
        print(f"❌ Directory {json_dir} not found!")
        return

    json_files = list(json_path.glob("*.json"))
    print(f"📁 Found {len(json_files)} JSON files")

    tees_data = []
    pars_data = []

    # Process each file
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            # Extract course info from filename
            filename = json_file.name.replace('.json', '')

            # Pattern: MA-XXX-X_Name_scorecard_type (handles suffixes like -1, -2)
            match = re.match(r'^(MA-\d+(?:-\d+)?)_(.+?)_scorecard_(.+)$', filename)

            if not match:
                print(f"   ⚠️  Skipped {json_file.name} - couldn't parse")
                continue

            course_number = match.group(1)
            course_name = match.group(2).replace('_', ' ')
            data_type = match.group(3)

            print(f"   📄 {course_number} - {course_name} ({data_type})")

            if data_type == 'pars':
                # Par record
                par_record = {
                    'course_name': course_name,
                    'course_number': course_number,
                    'data_group': 'course_scraping_data',
                    'hole_1': data.get('par_1'),
                    'hole_2': data.get('par_2'),
                    'hole_3': data.get('par_3'),
                    'hole_4': data.get('par_4'),
                    'hole_5': data.get('par_5'),
                    'hole_6': data.get('par_6'),
                    'hole_7': data.get('par_7'),
                    'hole_8': data.get('par_8'),
                    'hole_9': data.get('par_9'),
                    'out_9': data.get('out_par'),
                    'hole_10': data.get('par_10'),
                    'hole_11': data.get('par_11'),
                    'hole_12': data.get('par_12'),
                    'hole_13': data.get('par_13'),
                    'hole_14': data.get('par_14'),
                    'hole_15': data.get('par_15'),
                    'hole_16': data.get('par_16'),
                    'hole_17': data.get('par_17'),
                    'hole_18': data.get('par_18'),
                    'in_9': data.get('in_par'),
                    'total_par': data.get('total_par'),
                    'verified': True
                }
                pars_data.append(par_record)
                print(f"      ✅ Par data (Total: {data.get('total_par')})")

            else:
                # Tee record - use tee_name from JSON if available, otherwise parse from filename
                if 'tee_name' in data and data['tee_name']:
                    tee_name = data['tee_name']  # Use the tee_name from JSON (e.g., "RED", "WHITE", "BLUE")
                else:
                    # Fallback to filename parsing
                    tee_name = data_type.upper()
                    if 'ladies' in data_type.lower():
                        tee_name = 'LADIES YELLOW'
                    elif 'junior' in data_type.lower():
                        tee_name = 'JUNIOR RED'

                tee_record = {
                    'course_name': course_name,
                    'course_number': course_number,
                    'data_group': 'course_scraping_data',
                    'tee_number': data.get('course_id'),
                    'tee_name': tee_name,
                    'total_yardage': data.get('total_yardage'),
                    'rating': data.get('rating'),
                    'slope': data.get('slope'),
                    'hole_1': data.get('hole_1'),
                    'hole_2': data.get('hole_2'),
                    'hole_3': data.get('hole_3'),
                    'hole_4': data.get('hole_4'),
                    'hole_5': data.get('hole_5'),
                    'hole_6': data.get('hole_6'),
                    'hole_7': data.get('hole_7'),
                    'hole_8': data.get('hole_8'),
                    'hole_9': data.get('hole_9'),
                    'out_9': data.get('out_9'),
                    'hole_10': data.get('hole_10'),
                    'hole_11': data.get('hole_11'),
                    'hole_12': data.get('hole_12'),
                    'hole_13': data.get('hole_13'),
                    'hole_14': data.get('hole_14'),
                    'hole_15': data.get('hole_15'),
                    'hole_16': data.get('hole_16'),
                    'hole_17': data.get('hole_17'),
                    'hole_18': data.get('hole_18'),
                    'in_9': data.get('in_9'),
                    'scrape_source': 'scorecard_scraping',
                    'verified': True
                }
                tees_data.append(tee_record)
                print(f"      ✅ Tee data ({tee_name}, {data.get('total_yardage')} yards)")

        except Exception as e:
            print(f"   ❌ Error with {json_file.name}: {e}")

    print(f"\n📊 Summary: {len(tees_data)} tees, {len(pars_data)} pars")

    # Check for existing data
    all_course_numbers = list(set([t['course_number'] for t in tees_data] + [p['course_number'] for p in pars_data]))
    existing_tees, existing_pars = check_existing_data(all_course_numbers)

    # Filter out existing data
    tees_to_upload = [t for t in tees_data if t['course_number'] not in existing_tees]
    pars_to_upload = [p for p in pars_data if p['course_number'] not in existing_pars]

    print(f"🔍 Existing data check:")
    print(f"   • Tees to upload: {len(tees_to_upload)} (skipping {len(tees_data) - len(tees_to_upload)} existing)")
    print(f"   • Pars to upload: {len(pars_to_upload)} (skipping {len(pars_data) - len(pars_to_upload)} existing)")

    # Upload tees
    if tees_to_upload:
        print(f"\n📤 Uploading {len(tees_to_upload)} tee records...")
        try:
            response = requests.post(
                "http://localhost:3000/api/import/tees",
                json={'data': tees_to_upload},
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Tees: {result.get('stats', {}).get('imported', 0)} imported")
            else:
                print(f"   ❌ Tees failed: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Tees error: {e}")
    else:
        print(f"\n⏭️  Skipping tees (no new data)")

    # Upload pars
    if pars_to_upload:
        print(f"\n📤 Uploading {len(pars_to_upload)} par records...")
        try:
            response = requests.post(
                "http://localhost:3000/api/import/pars",
                json={'data': pars_to_upload},
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Pars: {result.get('stats', {}).get('imported', 0)} imported")
            else:
                print(f"   ❌ Pars failed: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Pars error: {e}")
    else:
        print(f"\n⏭️  Skipping pars (no new data)")

    print("\n✅ Done!")

if __name__ == "__main__":
    main()

# Usage:
# python minimal_json_uploader.py "states/ma/website_data/tees_pars"
# python minimal_json_uploader.py "tees_pars"
