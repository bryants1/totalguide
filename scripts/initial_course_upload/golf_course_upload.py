#!/usr/bin/env python3
"""
Golf Course Excel Upload Script
Uploads course data from Excel file to initial_course_upload table via API
Handles duplicate entries by updating existing records
"""

import pandas as pd
import requests
import json
import sys
from typing import Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GolfCourseUploader:
    def __init__(self, api_base_url: str = "http://localhost:3000"):
        self.api_base_url = api_base_url.rstrip('/')
        self.upload_endpoint = f"{self.api_base_url}/api/initial-course-upload"

        # Field mapping from Excel columns to database columns
        self.field_mapping = {
            'coursenumber': 'course_number',
            'course_name': 'course_name',
            'address': 'street_address',
            'city': 'city',
            'state': 'state_or_region',
            'zip_code': 'zip_code',
            'phone': 'phone_number',
            'website': 'website_url',
            'architect': 'architect',
            'year_opened': 'year_built_founded',
            'main_email': 'email_address',
            'par': 'total_par',
            'holes': 'total_holes',
            'rating': 'course_rating',
            'slope': 'slope_rating',
            'yardage': 'total_length'
        }

        self.stats = {
            'total_processed': 0,
            'successful_uploads': 0,
            'failed_uploads': 0,
            'skipped': 0
        }

    def read_excel_file(self, file_path: str) -> pd.DataFrame:
        """Read the Excel file and return a DataFrame"""
        try:
            # Read Excel file, skip first row if it contains metadata
            df = pd.read_excel(file_path, sheet_name=0)

            # Check if first row contains actual headers or metadata
            if df.columns[0] == 'unmatched_courses_20250801_174900':
                # Skip the metadata row and use second row as headers
                df = pd.read_excel(file_path, sheet_name=0, skiprows=1)

            logger.info(f"Successfully read Excel file: {len(df)} rows, {len(df.columns)} columns")
            return df

        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise

    def clean_value(self, value: Any) -> Any:
        """Clean and convert values for database insertion"""
        if pd.isna(value) or value == 'undefined' or value == '':
            return None

        # Convert numeric strings to appropriate types
        if isinstance(value, str):
            value = value.strip()

            # Handle zip codes - convert to integer if possible
            if value.isdigit():
                return int(value)

            # Handle decimal numbers
            try:
                if '.' in value:
                    return float(value)
            except ValueError:
                pass

        return value

    def map_course_data(self, row: pd.Series) -> Dict[str, Any]:
        """Map Excel row data to database field structure"""
        course_data = {}

        for excel_col, db_col in self.field_mapping.items():
            if excel_col in row.index:
                raw_value = row[excel_col]
                cleaned_value = self.clean_value(raw_value)
                course_data[db_col] = cleaned_value

        # Ensure course_number is always present
        if not course_data.get('course_number'):
            return None

        # Set process flag to False for new uploads
        course_data['process'] = False

        return course_data

    def upload_course(self, course_data: Dict[str, Any]) -> bool:
        """Upload a single course to the API with upsert behavior"""
        try:
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            # Use POST with upsert behavior (update if exists, insert if not)
            response = requests.post(
                self.upload_endpoint,
                json=course_data,
                headers=headers,
                timeout=30
            )

            if response.status_code in [200, 201]:
                action = "Updated" if response.status_code == 200 else "Created"
                logger.info(f"✅ {action} course: {course_data['course_number']} - {course_data.get('course_name', 'N/A')}")
                return True
            else:
                logger.error(f"❌ Failed to upload {course_data['course_number']}: HTTP {response.status_code}")
                try:
                    error_detail = response.json() if response.text else "No error details"
                    logger.error(f"   Error details: {error_detail}")
                except:
                    logger.error(f"   Response text: {response.text[:200]}")
                return False

        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ Connection error for {course_data['course_number']}: {e}")
            logger.error("   Make sure your server.js is running on the correct port")
            return False
        except requests.exceptions.Timeout as e:
            logger.error(f"❌ Timeout error for {course_data['course_number']}: {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Network error uploading {course_data['course_number']}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error uploading {course_data['course_number']}: {e}")
            return False

    def test_api_connection(self) -> bool:
        """Test if the API is accessible"""
        try:
            # Try to ping a health endpoint or the base API
            test_url = f"{self.api_base_url}/api/health"
            response = requests.get(test_url, timeout=10)

            if response.status_code == 404:
                # Health endpoint might not exist, try the upload endpoint with GET
                response = requests.get(self.upload_endpoint, timeout=10)

            logger.info(f"API connection test: HTTP {response.status_code}")
            return True

        except Exception as e:
            logger.warning(f"API connection test failed: {e}")
            logger.info("Proceeding anyway - the upload endpoint might still work")
            return True

    def upload_all_courses(self, file_path: str) -> Dict[str, int]:
        """Main method to upload all courses from Excel file"""
        logger.info("🚀 Starting golf course upload process")

        # Test API connection
        self.test_api_connection()

        # Read Excel file
        df = self.read_excel_file(file_path)

        # Process each course
        for index, row in df.iterrows():
            self.stats['total_processed'] += 1

            # Map course data
            course_data = self.map_course_data(row)

            if not course_data:
                logger.warning(f"⚠️  Skipping row {index + 1}: Missing course number")
                self.stats['skipped'] += 1
                continue

            # Upload course
            if self.upload_course(course_data):
                self.stats['successful_uploads'] += 1
            else:
                self.stats['failed_uploads'] += 1

        # Print summary
        self.print_summary()
        return self.stats

    def print_summary(self):
        """Print upload summary statistics"""
        logger.info("\n" + "="*50)
        logger.info("📊 UPLOAD SUMMARY")
        logger.info("="*50)
        logger.info(f"Total rows processed: {self.stats['total_processed']}")
        logger.info(f"✅ Successful uploads: {self.stats['successful_uploads']}")
        logger.info(f"❌ Failed uploads: {self.stats['failed_uploads']}")
        logger.info(f"⚠️  Skipped rows: {self.stats['skipped']}")

        success_rate = (self.stats['successful_uploads'] / self.stats['total_processed'] * 100) if self.stats['total_processed'] > 0 else 0
        logger.info(f"📈 Success rate: {success_rate:.1f}%")
        logger.info("="*50)


def main():
    """Main execution function"""
    if len(sys.argv) != 2:
        print("Usage: python golf_course_upload.py <excel_file_path>")
        print("Example: python golf_course_upload.py non_matching.xlsx")
        sys.exit(1)

    excel_file_path = sys.argv[1]

    # You can modify the API URL here if your server runs on a different port/host
    api_url = "http://localhost:3000"

    uploader = GolfCourseUploader(api_url)

    try:
        stats = uploader.upload_all_courses(excel_file_path)

        # Exit with error code if there were failures
        if stats['failed_uploads'] > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
