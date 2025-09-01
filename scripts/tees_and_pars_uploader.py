#!/usr/bin/env python3
"""
Tees and Pars Data Uploader Script
Uploads both tee data and par data from Excel file to Supabase database using course name matching
"""

import pandas as pd
import requests
import re
from typing import Dict, Set, List, Tuple
from difflib import SequenceMatcher
import json

# Configuration - UPDATE THESE VALUES
EXCEL_FILE = "USGolfDataMassGolfGuide03232025.xlsx"
SUPABASE_URL = "https://pmpymmdayzqsxrbymxvh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y"
API_BASE_URL = "http://localhost:3000/api"  # Your server API

class TeesAndParsUploader:
    def __init__(self, excel_file: str, supabase_url: str, supabase_key: str, api_base: str):
        self.excel_file = excel_file
        self.supabase_url = supabase_url.rstrip('/') + '/rest/v1'
        self.supabase_key = supabase_key
        self.api_base = api_base.rstrip('/')

        # Supabase headers
        self.headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }

    def normalize_name(self, name: str) -> str:
        """Normalize course name for better matching"""
        if not name:
            return ""

        normalized = str(name).lower().strip()

        # Replace common variations
        normalized = re.sub(r'\b(golf\s+club|country\s+club|golf\s+course|golf\s+links|golf)\b', 'gc', normalized)
        normalized = re.sub(r'\b(the\s+)', '', normalized)
        normalized = re.sub(r'\b(at\s+)', '', normalized)
        normalized = re.sub(r'\b(and|&)\b', 'and', normalized)

        # Remove special characters and extra spaces
        normalized = re.sub(r'[^\w\s]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)

        return normalized.strip()

    def load_excel_data(self) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
        """Load both tee and par data from Excel file"""
        print("📊 Loading Excel tee and par data...")

        df = pd.read_excel(self.excel_file, sheet_name=0)

        print(f"   • Total Excel rows: {len(df):,}")
        print(f"   • Columns: {list(df.columns)}")

        tee_data_by_course = {}
        par_data_by_course = {}

        for _, row in df.iterrows():
            course_number = str(row['cCourseNumber']).strip()
            course_name = str(row['CoursesMasterT::CourseName']).strip()

            if pd.isna(course_number) or pd.isna(course_name):
                continue

            normalized_name = self.normalize_name(course_name)

            # Extract tee data
            tee_record = {
                'course_name': course_name,
                'course_number': course_number,  # Will be updated to DB number later
                'data_group': 'initial_upload_data',
                'tee_number': self._safe_int(row.get('TeeNumber')),
                'tee_name': self._safe_str(row.get('TeeName')),
                'total_yardage': self._safe_int(row.get('Length')),
                'rating': self._safe_float(row.get('Rating')),
                'slope': self._safe_int(row.get('Slope')),
                'hole_1': self._safe_int(row.get('Hole_1')),
                'hole_2': self._safe_int(row.get('Hole_2')),
                'hole_3': self._safe_int(row.get('Hole_3')),
                'hole_4': self._safe_int(row.get('Hole_4')),
                'hole_5': self._safe_int(row.get('Hole_5')),
                'hole_6': self._safe_int(row.get('Hole_6')),
                'hole_7': self._safe_int(row.get('Hole_7')),
                'hole_8': self._safe_int(row.get('Hole_8')),
                'hole_9': self._safe_int(row.get('Hole_9')),
                'out_9': self._safe_int(row.get('Tot_Out Ydg')),
                'hole_10': self._safe_int(row.get('Hole_10')),
                'hole_11': self._safe_int(row.get('Hole_11')),
                'hole_12': self._safe_int(row.get('Hole_12')),
                'hole_13': self._safe_int(row.get('Hole_13')),
                'hole_14': self._safe_int(row.get('Hole_14')),
                'hole_15': self._safe_int(row.get('Hole_15')),
                'hole_16': self._safe_int(row.get('Hole_16')),
                'hole_17': self._safe_int(row.get('Hole_17')),
                'hole_18': self._safe_int(row.get('Hole_18')),
                'in_9': self._safe_int(row.get('Tot_In Ydg')),
                'scrape_source': 'excel_import',
                'verified': False
            }

            # Group tee data by normalized course name
            if normalized_name not in tee_data_by_course:
                tee_data_by_course[normalized_name] = {
                    'course_info': {
                        'original_name': course_name,
                        'excel_number': course_number,
                        'normalized_name': normalized_name
                    },
                    'tees': []
                }

            tee_data_by_course[normalized_name]['tees'].append(tee_record)

            # Extract par data (only once per course, not per tee)
            if normalized_name not in par_data_by_course:
                par_record = {
                    'course_name': course_name,
                    'course_number': course_number,  # Will be updated to DB number later
                    'data_group': 'initial_upload_data',
                    'hole_1': self._safe_int(row.get('Par_1')),
                    'hole_2': self._safe_int(row.get('Par_2')),
                    'hole_3': self._safe_int(row.get('Par_3')),
                    'hole_4': self._safe_int(row.get('Par_4')),
                    'hole_5': self._safe_int(row.get('Par_5')),
                    'hole_6': self._safe_int(row.get('Par_6')),
                    'hole_7': self._safe_int(row.get('Par_7')),
                    'hole_8': self._safe_int(row.get('Par_8')),
                    'hole_9': self._safe_int(row.get('Par_9')),
                    'out_9': self._safe_int(row.get('Tot_Out Par')),
                    'hole_10': self._safe_int(row.get('Par_10')),
                    'hole_11': self._safe_int(row.get('Par_11')),
                    'hole_12': self._safe_int(row.get('Par_12')),
                    'hole_13': self._safe_int(row.get('Par_13')),
                    'hole_14': self._safe_int(row.get('Par_14')),
                    'hole_15': self._safe_int(row.get('Par_15')),
                    'hole_16': self._safe_int(row.get('Par_16')),
                    'hole_17': self._safe_int(row.get('Par_17')),
                    'hole_18': self._safe_int(row.get('Par_18')),
                    'in_9': self._safe_int(row.get('Tot_In Par')),
                    'total_par': self._safe_int(row.get('Par')),  # Total par for the course
                    'verified': False
                }

                par_data_by_course[normalized_name] = {
                    'course_info': {
                        'original_name': course_name,
                        'excel_number': course_number,
                        'normalized_name': normalized_name
                    },
                    'pars': par_record
                }

        print(f"   • Unique courses with tee data: {len(tee_data_by_course):,}")
        print(f"   • Unique courses with par data: {len(par_data_by_course):,}")

        # Show summary by course
        for norm_name in list(tee_data_by_course.keys())[:3]:
            course_info = tee_data_by_course[norm_name]['course_info']
            tee_count = len(tee_data_by_course[norm_name]['tees'])
            has_pars = norm_name in par_data_by_course
            print(f"     - '{course_info['original_name']}' ({course_info['excel_number']}): {tee_count} tees, pars: {has_pars}")

        return tee_data_by_course, par_data_by_course

    def _safe_str(self, value) -> str:
        """Safely convert to string"""
        if pd.isna(value) or value is None:
            return None
        return str(value).strip() if str(value).strip() else None

    def _safe_int(self, value) -> int:
        """Safely convert to integer"""
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value) -> float:
        """Safely convert to float"""
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def load_db_courses(self) -> Dict[str, Dict]:
        """Load courses from database"""
        print("\n🗄️  Loading database courses...")

        # Query database
        response = requests.get(
            f"{self.supabase_url}/initial_course_upload",
            headers=self.headers,
            params={'select': 'course_number,course_name'}
        )

        if response.status_code != 200:
            raise Exception(f"Database error: {response.status_code} - {response.text}")

        data = response.json()
        db_courses = {}

        for row in data:
            course_number = row.get('course_number', '').strip()
            course_name = row.get('course_name', '').strip()

            if not course_number or not course_name:
                continue

            normalized_name = self.normalize_name(course_name)

            db_courses[normalized_name] = {
                'course_number': course_number,
                'course_name': course_name,
                'normalized_name': normalized_name
            }

        print(f"   • Database courses loaded: {len(db_courses):,}")
        return db_courses

    def check_existing_data(self, course_numbers: List[str]) -> Tuple[Set[str], Set[str]]:
        """Check which courses already have tee and par data"""
        print("\n🔍 Checking for existing data...")

        if not course_numbers:
            return set(), set()

        course_filter = ','.join([f'"{num}"' for num in course_numbers])

        # Check existing tee data
        tee_response = requests.get(
            f"{self.supabase_url}/course_tees",
            headers=self.headers,
            params={
                'select': 'course_number',
                'course_number': f'in.({course_filter})',
                'data_group': 'eq.initial_upload_data'
            }
        )

        # Check existing par data
        par_response = requests.get(
            f"{self.supabase_url}/course_pars",
            headers=self.headers,
            params={
                'select': 'course_number',
                'course_number': f'in.({course_filter})',
                'data_group': 'eq.initial_upload_data'
            }
        )

        existing_tees = set()
        existing_pars = set()

        if tee_response.status_code == 200:
            tee_data = tee_response.json()
            existing_tees = {row['course_number'] for row in tee_data}

        if par_response.status_code == 200:
            par_data = par_response.json()
            existing_pars = {row['course_number'] for row in par_data}

        print(f"   • Courses with existing tee data: {len(existing_tees):,}")
        print(f"   • Courses with existing par data: {len(existing_pars):,}")

        return existing_tees, existing_pars

    def match_courses(self, excel_tee_data: Dict, excel_par_data: Dict, db_courses: Dict) -> Tuple[List[Dict], List[Dict]]:
        """Match Excel courses with database courses"""
        print("\n🔗 Matching Excel courses with database courses...")

        # Use tee data as the primary source since it has more records
        matched_courses = []
        unmatched_courses = []

        all_excel_courses = set(excel_tee_data.keys()) | set(excel_par_data.keys())

        for norm_name in all_excel_courses:
            # Get course info from whichever source has it
            if norm_name in excel_tee_data:
                excel_info = excel_tee_data[norm_name]['course_info']
            else:
                excel_info = excel_par_data[norm_name]['course_info']

            if norm_name in db_courses:
                # Exact match found
                db_course = db_courses[norm_name]

                matched_courses.append({
                    'excel_tee_data': excel_tee_data.get(norm_name),
                    'excel_par_data': excel_par_data.get(norm_name),
                    'db_course': db_course,
                    'match_type': 'exact',
                    'db_course_number': db_course['course_number']
                })
            else:
                # Try fuzzy matching
                best_match = None
                best_similarity = 0.0

                for db_norm_name, db_course in db_courses.items():
                    similarity = SequenceMatcher(None, norm_name, db_norm_name).ratio()

                    if similarity > best_similarity and similarity >= 0.8:
                        best_similarity = similarity
                        best_match = db_course

                if best_match:
                    matched_courses.append({
                        'excel_tee_data': excel_tee_data.get(norm_name),
                        'excel_par_data': excel_par_data.get(norm_name),
                        'db_course': best_match,
                        'match_type': 'fuzzy',
                        'similarity': best_similarity,
                        'db_course_number': best_match['course_number']
                    })
                else:
                    unmatched_courses.append({
                        'excel_info': excel_info,
                        'has_tees': norm_name in excel_tee_data,
                        'has_pars': norm_name in excel_par_data,
                        'match_type': 'no_match'
                    })

        exact_matches = [m for m in matched_courses if m['match_type'] == 'exact']
        fuzzy_matches = [m for m in matched_courses if m['match_type'] == 'fuzzy']

        print(f"   • Exact matches: {len(exact_matches):,}")
        print(f"   • Fuzzy matches: {len(fuzzy_matches):,}")
        print(f"   • Unmatched: {len(unmatched_courses):,}")

        return matched_courses, unmatched_courses

    def upload_tee_data(self, matched_courses: List[Dict], existing_tees: Set[str]) -> Dict:
        """Upload tee data to database"""
        print("\n📤 Uploading tee data...")

        # Filter courses that have tee data and don't already exist
        courses_to_upload = [
            match for match in matched_courses
            if match['excel_tee_data'] is not None and match['db_course_number'] not in existing_tees
        ]

        skipped_count = len([m for m in matched_courses if m['db_course_number'] in existing_tees])

        print(f"   • Courses to upload: {len(courses_to_upload):,}")
        print(f"   • Courses skipped (already have tee data): {skipped_count:,}")

        if not courses_to_upload:
            return {
                'uploaded': 0,
                'skipped': skipped_count,
                'errors': 0,
                'error_details': []
            }

        # Prepare tee data for upload
        all_tee_records = []

        for match in courses_to_upload:
            db_course_number = match['db_course_number']
            db_course_name = match['db_course']['course_name']
            excel_tees = match['excel_tee_data']['tees']

            for tee in excel_tees:
                # Update course number to database number
                tee_record = tee.copy()
                tee_record['course_number'] = db_course_number
                tee_record['course_name'] = db_course_name

                all_tee_records.append(tee_record)

        print(f"   • Total tee records to upload: {len(all_tee_records):,}")

        # Upload via API
        try:
            upload_data = {'data': all_tee_records}

            response = requests.post(
                f"{self.api_base}/import/tees",
                json=upload_data,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Tee upload successful!")
                print(f"   • Imported: {result.get('stats', {}).get('imported', 0):,}")
                print(f"   • Errors: {result.get('stats', {}).get('errors', 0):,}")

                return {
                    'uploaded': len(courses_to_upload),
                    'skipped': skipped_count,
                    'total_records': len(all_tee_records),
                    'imported_records': result.get('stats', {}).get('imported', 0),
                    'errors': result.get('stats', {}).get('errors', 0),
                    'error_details': result.get('errorDetails', [])
                }
            else:
                error_msg = f"API error: {response.status_code} - {response.text}"
                print(f"   ❌ Tee upload failed: {error_msg}")
                return {
                    'uploaded': 0,
                    'skipped': skipped_count,
                    'errors': len(courses_to_upload),
                    'error_details': [error_msg]
                }

        except Exception as e:
            error_msg = f"Upload exception: {str(e)}"
            print(f"   ❌ Tee upload failed: {error_msg}")
            return {
                'uploaded': 0,
                'skipped': skipped_count,
                'errors': len(courses_to_upload),
                'error_details': [error_msg]
            }

    def upload_par_data(self, matched_courses: List[Dict], existing_pars: Set[str]) -> Dict:
        """Upload par data to database"""
        print("\n📤 Uploading par data...")

        # Filter courses that have par data and don't already exist
        courses_to_upload = [
            match for match in matched_courses
            if match['excel_par_data'] is not None and match['db_course_number'] not in existing_pars
        ]

        skipped_count = len([m for m in matched_courses if m['db_course_number'] in existing_pars])

        print(f"   • Courses to upload: {len(courses_to_upload):,}")
        print(f"   • Courses skipped (already have par data): {skipped_count:,}")

        if not courses_to_upload:
            return {
                'uploaded': 0,
                'skipped': skipped_count,
                'errors': 0,
                'error_details': []
            }

        # Prepare par data for upload
        all_par_records = []

        for match in courses_to_upload:
            db_course_number = match['db_course_number']
            db_course_name = match['db_course']['course_name']
            excel_par = match['excel_par_data']['pars']

            # Update course number to database number
            par_record = excel_par.copy()
            par_record['course_number'] = db_course_number
            par_record['course_name'] = db_course_name

            all_par_records.append(par_record)

        print(f"   • Total par records to upload: {len(all_par_records):,}")

        # Upload via API
        try:
            upload_data = {'data': all_par_records}

            response = requests.post(
                f"{self.api_base}/import/pars",
                json=upload_data,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Par upload successful!")
                print(f"   • Imported: {result.get('stats', {}).get('imported', 0):,}")
                print(f"   • Errors: {result.get('stats', {}).get('errors', 0):,}")

                return {
                    'uploaded': len(courses_to_upload),
                    'skipped': skipped_count,
                    'total_records': len(all_par_records),
                    'imported_records': result.get('stats', {}).get('imported', 0),
                    'errors': result.get('stats', {}).get('errors', 0),
                    'error_details': result.get('errorDetails', [])
                }
            else:
                error_msg = f"API error: {response.status_code} - {response.text}"
                print(f"   ❌ Par upload failed: {error_msg}")
                return {
                    'uploaded': 0,
                    'skipped': skipped_count,
                    'errors': len(courses_to_upload),
                    'error_details': [error_msg]
                }

        except Exception as e:
            error_msg = f"Upload exception: {str(e)}"
            print(f"   ❌ Par upload failed: {error_msg}")
            return {
                'uploaded': 0,
                'skipped': skipped_count,
                'errors': len(courses_to_upload),
                'error_details': [error_msg]
            }

    def print_summary_report(self, matched_courses: List[Dict], unmatched_courses: List[Dict],
                           tee_results: Dict, par_results: Dict):
        """Print summary report"""
        print("\n" + "="*80)
        print("📋 TEES AND PARS UPLOAD SUMMARY REPORT")
        print("="*80)

        print(f"\n📊 MATCHING SUMMARY:")
        exact_matches = [m for m in matched_courses if m['match_type'] == 'exact']
        fuzzy_matches = [m for m in matched_courses if m['match_type'] == 'fuzzy']

        print(f"   • Total Excel courses: {len(matched_courses) + len(unmatched_courses):,}")
        print(f"   • Exact name matches: {len(exact_matches):,}")
        print(f"   • Fuzzy name matches: {len(fuzzy_matches):,}")
        print(f"   • Unmatched courses: {len(unmatched_courses):,}")

        print(f"\n📤 TEE DATA UPLOAD:")
        print(f"   • Courses uploaded: {tee_results['uploaded']:,}")
        print(f"   • Courses skipped: {tee_results['skipped']:,}")
        print(f"   • Total tee records: {tee_results.get('total_records', 0):,}")
        print(f"   • Successfully imported: {tee_results.get('imported_records', 0):,}")
        print(f"   • Upload errors: {tee_results['errors']:,}")

        print(f"\n📤 PAR DATA UPLOAD:")
        print(f"   • Courses uploaded: {par_results['uploaded']:,}")
        print(f"   • Courses skipped: {par_results['skipped']:,}")
        print(f"   • Total par records: {par_results.get('total_records', 0):,}")
        print(f"   • Successfully imported: {par_results.get('imported_records', 0):,}")
        print(f"   • Upload errors: {par_results['errors']:,}")

        # Show fuzzy matches
        if fuzzy_matches:
            print(f"\n🔍 FUZZY MATCHES (first 5):")
            for i, match in enumerate(fuzzy_matches[:5], 1):
                if match['excel_tee_data']:
                    excel_name = match['excel_tee_data']['course_info']['original_name']
                    excel_number = match['excel_tee_data']['course_info']['excel_number']
                elif match['excel_par_data']:
                    excel_name = match['excel_par_data']['course_info']['original_name']
                    excel_number = match['excel_par_data']['course_info']['excel_number']

                db_name = match['db_course']['course_name']
                similarity = match.get('similarity', 0)
                print(f"   {i:2d}. Similarity: {similarity:.3f}")
                print(f"       Excel: '{excel_name}' ({excel_number})")
                print(f"       DB:    '{db_name}' ({match['db_course_number']})")

    def run_upload(self) -> Dict:
        """Run the complete upload process"""
        print("🚀 Starting Tees and Pars Upload Process...")
        print("-" * 60)

        try:
            # Load data
            excel_tee_data, excel_par_data = self.load_excel_data()
            db_courses = self.load_db_courses()

            # Match courses
            matched_courses, unmatched_courses = self.match_courses(excel_tee_data, excel_par_data, db_courses)

            # Check existing data
            db_course_numbers = [match['db_course_number'] for match in matched_courses]
            existing_tees, existing_pars = self.check_existing_data(db_course_numbers)

            # Upload data
            tee_results = self.upload_tee_data(matched_courses, existing_tees)
            par_results = self.upload_par_data(matched_courses, existing_pars)

            # Print summary
            self.print_summary_report(matched_courses, unmatched_courses, tee_results, par_results)

            return {
                'success': True,
                'matched_courses': len(matched_courses),
                'unmatched_courses': len(unmatched_courses),
                'tee_results': tee_results,
                'par_results': par_results
            }

        except Exception as e:
            print(f"❌ Error during upload process: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

def main():
    """Main function"""
    print("🏌️  Tees and Pars Upload Tool")
    print("=" * 50)

    # Update these configuration values
    uploader = TeesAndParsUploader(
        excel_file=EXCEL_FILE,
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        api_base=API_BASE_URL
    )

    results = uploader.run_upload()

    if results['success']:
        print(f"\n✅ Upload process completed successfully!")
        print(f"   • Tees uploaded: {results['tee_results']['uploaded']:,}")
        print(f"   • Pars uploaded: {results['par_results']['uploaded']:,}")
    else:
        print(f"\n❌ Upload process failed: {results['error']}")

    return results

if __name__ == "__main__":
    results = main()

# Example configuration:
"""
# Update these values before running:
EXCEL_FILE = "USGolfDataMassGolfGuide03232025.xlsx"
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-supabase-anon-key"
API_BASE_URL = "http://localhost:3000/api"

# Make sure your server is running first:
# node server.js

# Then run:
python tees_and_pars_uploader.py
"""
