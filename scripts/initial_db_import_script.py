

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class GolfDataImporter:
    def __init__(self, api_base_url: str = "http://localhost:3000", data_directory: str = "./states/ma"):
        self.api_base_url = api_base_url
        self.data_directory = Path(data_directory)
        self.supabase_url = os.getenv('SUPABASE_URL', 'https://pmpymmdayzqsxrbymxvh.supabase.co')
        self.supabase_key = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y')

        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # File pattern mappings - each pattern must be a complete regex string
        self.file_patterns = {
            'vector_attributes': {
                'pattern': re.compile(r'image_elevation/MA-\d+[^/]*/course_vector_attributes\.json$'),
                'endpoint': '/api/import/vector-attributes',
                'description': 'Vector Attributes'
            },
            'comprehensive_analysis': {
                'pattern': re.compile(r'image_elevation/MA-\d+[^/]*/comprehensive_analysis\.json$'),
                'endpoint': '/api/import/comprehensive-analysis',
                'description': 'Comprehensive Analysis'
            },
            'review_summaries': {
                'pattern': re.compile(r'reviews/.*MA-\d+.*_reviews_summary\.json$'),
                'endpoint': '/api/import/reviews',
                'description': 'Review Summaries'
            },
            'course_scores': {
                'pattern': re.compile(r'course_scores/.*MA-\d+.*_rubric\.json$'),
                'endpoint': '/api/import/scores',
                'description': 'Course Scores (Rubrics)'
            },
            'course_scraping_data': {
                'pattern': re.compile(r'website_data/.*MA-\d+.*_structure\.json$'),
                'endpoint': '/api/import/course-scraping',
                'description': 'Course Scraping Data'
            }
        }

        # Statistics
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'successful_imports': 0,
            'failed_imports': 0,
            'errors': []
        }

        self.is_connected = False

    def connect_to_supabase(self) -> bool:
        """Connect to Supabase database"""
        if not self.supabase_url or not self.supabase_key:
            print("❌ Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY environment variables.")
            return False

        try:
            response = self.session.post(
                f"{self.api_base_url}/api/connect",
                json={
                    'supabaseUrl': self.supabase_url,
                    'supabaseKey': self.supabase_key
                },
                timeout=30
            )

            if response.status_code == 200 and response.json().get('success'):
                print("✅ Connected to Supabase successfully")
                self.is_connected = True
                return True
            else:
                error_msg = response.json().get('error', 'Unknown error')
                print(f"❌ Failed to connect to Supabase: {error_msg}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to connect to Supabase: {e}")
            return False

    def find_matching_files(self) -> Dict[str, List[Path]]:
        """Recursively find all JSON files matching patterns"""
        matched_files = {key: [] for key in self.file_patterns.keys()}

        if not self.data_directory.exists():
            print(f"❌ Data directory does not exist: {self.data_directory}")
            return matched_files

        print(f"🔍 Scanning directory: {self.data_directory}")

        # Walk through all files in the directory
        all_json_files = []
        for json_file in self.data_directory.rglob("*.json"):
            all_json_files.append(json_file)
            relative_path = json_file.relative_to(self.data_directory)

            # Check against all patterns
            matched = False
            for key, config in self.file_patterns.items():
                if config['pattern'].search(str(relative_path)):
                    matched_files[key].append(json_file)
                    matched = True
                    print(f"  ✓ Matched {key}: {relative_path}")
                    break  # File matches one pattern, don't check others

            if not matched:
                print(f"  ⚠️  No pattern match: {relative_path}")

        print(f"\n📊 Found {len(all_json_files)} total JSON files")

        # Print summary
        total_files = sum(len(files) for files in matched_files.values())
        print(f"📊 Matched {total_files} files by type:")
        for key, files in matched_files.items():
            if files:
                print(f"  • {self.file_patterns[key]['description']}: {len(files)} files")
                for file in files:
                    print(f"    - {file.name}")

        self.stats['total_files'] = total_files
        return matched_files

    def load_json_file(self, file_path: Path) -> Optional[dict]:
        """Load and validate JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            error_msg = f"Failed to load {file_path.name}: {e}"
            self.stats['errors'].append(error_msg)
            print(f"  ❌ {error_msg}")
            return None

    def send_batch_to_api(self, data_array: List[dict], endpoint: str, description: str) -> bool:
        """Send a batch of data to the API"""
        if not data_array:
            return True

        try:
            print(f"  📤 Sending {len(data_array)} records to API...")

            response = self.session.post(
                f"{self.api_base_url}{endpoint}",
                json={'data': data_array},
                timeout=300  # 5 minutes for large batches
            )

            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    stats = result.get('stats', {})
                    print(f"  ✅ {description} import completed:")
                    print(f"     • Total: {stats.get('total', 0)}")
                    print(f"     • Imported: {stats.get('imported', 0)}")
                    print(f"     • Errors: {stats.get('errors', 0)}")

                    # Show error details if any
                    if stats.get('errors', 0) > 0 and result.get('errorDetails'):
                        print("     • Error details:")
                        for error in result['errorDetails'][:5]:  # Show first 5 errors
                            print(f"       - {error.get('course', 'Unknown')}: {error.get('error', 'Unknown error')}")
                        if len(result['errorDetails']) > 5:
                            print(f"       - ... and {len(result['errorDetails']) - 5} more errors")

                    self.stats['successful_imports'] += stats.get('imported', 0)
                    return True
                else:
                    error_msg = result.get('error', 'Unknown API error')
                    print(f"  ❌ API returned error: {error_msg}")
                    return False
            else:
                print(f"  ❌ HTTP {response.status_code}: {response.text}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")
            return False

    def process_file_batch(self, files: List[Path], file_type: str) -> bool:
        """Process a batch of files of the same type"""
        if not files:
            print(f"ℹ️  No {self.file_patterns[file_type]['description']} files found")
            return True

        print(f"\n📂 Processing {len(files)} {self.file_patterns[file_type]['description']} files...")

        # Load all files in the batch
        data_array = []
        successful_loads = 0

        for file_path in files:
            data = self.load_json_file(file_path)
            if data is not None:
                data_array.append(data)
                successful_loads += 1
                print(f"  ✓ Loaded: {file_path.name}")
            else:
                self.stats['failed_imports'] += 1

        self.stats['processed_files'] += len(files)

        if not data_array:
            print(f"  ⚠️  No valid {self.file_patterns[file_type]['description']} data to import")
            return False

        # Send batch to API
        endpoint = self.file_patterns[file_type]['endpoint']
        description = self.file_patterns[file_type]['description']

        return self.send_batch_to_api(data_array, endpoint, description)

    def import_all_data(self) -> bool:
        """Main method to import all data"""
        print("🚀 Starting Golf Course Data Import")
        print("=" * 50)

        # Check API connectivity
        try:
            response = self.session.get(f"{self.api_base_url}/api/health", timeout=10)
            if response.status_code != 200:
                print(f"❌ API server is not responding at {self.api_base_url}")
                return False
        except requests.exceptions.RequestException:
            print(f"❌ Cannot connect to API server at {self.api_base_url}")
            return False

        # Connect to Supabase
        if not self.connect_to_supabase():
            return False

        # Find all matching files
        matched_files = self.find_matching_files()

        if self.stats['total_files'] == 0:
            print("ℹ️  No matching files found to import")
            return True

        # Process each file type
        success = True
        for file_type, files in matched_files.items():
            if not self.process_file_batch(files, file_type):
                success = False

            # Small delay between batches
            if files:
                time.sleep(1)

        # Print final statistics
        self.print_final_stats()
        return success

    def print_final_stats(self):
        """Print final import statistics"""
        print("\n" + "=" * 50)
        print("📊 IMPORT SUMMARY")
        print("=" * 50)
        print(f"Total files found: {self.stats['total_files']}")
        print(f"Files processed: {self.stats['processed_files']}")
        print(f"Successful imports: {self.stats['successful_imports']}")
        print(f"Failed imports: {self.stats['failed_imports']}")

        if self.stats['errors']:
            print(f"\n❌ Errors encountered: {len(self.stats['errors'])}")
            for i, error in enumerate(self.stats['errors'][:5], 1):
                print(f"  {i}. {error}")
            if len(self.stats['errors']) > 5:
                print(f"  ... and {len(self.stats['errors']) - 5} more errors")

        success_rate = (self.stats['successful_imports'] / max(self.stats['total_files'], 1)) * 100
        print(f"\n✨ Success rate: {success_rate:.1f}%")

def main():
    """Main entry point"""
    # Parse command line arguments
    api_url = "http://localhost:3000"
    data_dir = "./states/ma"

    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    if len(sys.argv) > 2:
        api_url = sys.argv[2]

    # Check if data directory exists
    if not Path(data_dir).exists():
        print(f"❌ Data directory does not exist: {data_dir}")
        print("Usage: python golf_import.py [data_directory] [api_url]")
        print("Example: python golf_import.py ./states/ma http://localhost:3000")
        sys.exit(1)

    # Create importer and run
    importer = GolfDataImporter(api_url, data_dir)

    try:
        success = importer.import_all_data()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Import interrupted by user")
        importer.print_final_stats()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        importer.print_final_stats()
        sys.exit(1)

if __name__ == "__main__":
    main()
