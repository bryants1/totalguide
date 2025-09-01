#!/usr/bin/env python3
"""
Smart Google Places Data Enrichment Script
Handles targeting (single course, state, or all) AND processing.
API-driven architecture with consistent argument handling.
"""
import argparse
import requests
import time
import logging
from datetime import datetime
import sys
import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from one directory up (single script in scripts/)
script_dir = Path(__file__).parent
env_file = script_dir.parent / '.env'
load_dotenv(env_file)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_api_url():
    """Get API URL from environment variable - REQUIRED"""
    api_url = os.getenv('API_URL')
    if not api_url:
        raise ValueError("API_URL environment variable is required but not set")
    return api_url

def get_google_places_api_key():
    """Get Google Places API key from environment variable - REQUIRED"""
    api_key = os.getenv('GOOGLE_PLACES_API_KEY')
    if not api_key:
        raise ValueError("GOOGLE_PLACES_API_KEY environment variable is required but not set")
    return api_key

class SmartGooglePlacesEnricher:
    def __init__(self, course=None, state=None, force=False, limit=None):
        self.course = course
        self.state = state
        self.force = force
        self.limit = limit

        # API configuration
        self.api_url = get_api_url()
        self.api_key = get_google_places_api_key()
        self.session = requests.Session()
        self.db_timeout = 30

        logger.info(f"🔧 Using API URL: {self.api_url}")
        logger.info(f"🔑 Google Places API Key: {'✅ Set' if self.api_key else '❌ Missing'}")

        # Log targeting mode
        if course:
            logger.info(f"🎯 Single course mode: {course}")
        elif state:
            logger.info(f"🗺️ State mode: {state}")
        else:
            logger.info(f"🌍 All courses mode")

        if force:
            logger.info("🔥 FORCE MODE: Will reprocess existing data")
        if limit:
            logger.info(f"📊 Limit: {limit} courses")

    def verify_api_connection(self) -> bool:
        """Verify that the API is accessible"""
        try:
            response = self.session.get(f"{self.api_url}/api/health", timeout=10)
            if response.status_code == 200:
                logger.info("✅ API connection verified")
                return True
            else:
                logger.error(f"❌ API health check failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Could not connect to API: {e}")
            return False

    def get_single_course(self, course_number: str) -> list:
        """Get a single course by course number"""
        try:
            # Try primary_data table first
            response = self.session.get(
                f"{self.api_url}/api/courses/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found course {course_number} in primary_data")
                return [course_data]

            # If not found in primary_data, try initial_course_upload
            response = self.session.get(
                f"{self.api_url}/api/initial-courses/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found course {course_number} in initial_course_upload")
                return [course_data]

            logger.error(f"❌ Course {course_number} not found in either table")
            return []

        except Exception as e:
            logger.error(f"❌ Error fetching course {course_number}: {e}")
            return []

    def get_target_courses(self) -> tuple:
        """Get list of courses to process based on targeting mode"""
        try:
            if self.course:
                # Single course mode
                logger.info(f"🎯 Getting single course: {self.course}")
                courses = self.get_single_course(self.course)
                return courses, 0  # No skipped count for single course mode

            else:
                # Multiple courses mode
                params = {}
                if self.state:
                    params['state'] = self.state
                if self.limit:
                    params['limit'] = self.limit

                # Get all courses from primary_data
                response = self.session.get(
                    f"{self.api_url}/api/courses",
                    params=params,
                    timeout=self.db_timeout
                )
                response.raise_for_status()

                all_courses = response.json()
                logger.info(f"📋 Found {len(all_courses)} total courses")

                if not self.force:
                    # Filter out courses that already have Google Places data
                    courses_to_process = []
                    skipped_courses = []

                    for course in all_courses:
                        if self.check_existing_data(course['course_number']):
                            skipped_courses.append(course)
                        else:
                            courses_to_process.append(course)

                    # ✅ FIXED: Explicitly log the skip count
                    if len(skipped_courses) > 0:
                        logger.info(f"⏭️ Skipped {len(skipped_courses)} courses (already have Google Places data)")

                    logger.info(f"🔄 {len(courses_to_process)} courses need processing")

                    if len(courses_to_process) == 0:
                        logger.info("✅ No courses need processing")

                    return courses_to_process, len(skipped_courses)
                else:
                    logger.info(f"🔥 Force mode: processing all {len(all_courses)} courses")
                    return all_courses, 0

        except Exception as e:
            logger.error(f"❌ Error getting target courses: {e}")
            return [], 0

    def check_existing_data(self, course_number: str) -> bool:
        """Check if course already has Google Places data"""
        try:
            response = self.session.get(
                f"{self.api_url}/api/google-places-data/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    logger.debug(f"📄 Course {course_number} has existing Google Places data")
                    return True

            return False

        except Exception as e:
            logger.debug(f"⚠️ Error checking existing data for {course_number}: {e}")
            return False

    def parse_address_components(self, address_components):
        """Parse Google Places address components into structured data"""
        parsed = {
            'street_number': None,
            'route': None,
            'city': None,
            'state': None,
            'zip_code': None,
            'county': None,
            'country': None
        }

        if not address_components:
            return parsed

        for component in address_components:
            types = component.get('types', [])
            long_text = component.get('longText')
            short_text = component.get('shortText')

            if 'street_number' in types:
                parsed['street_number'] = long_text
            elif 'route' in types:
                parsed['route'] = long_text
            elif 'locality' in types:
                parsed['city'] = long_text
            elif 'administrative_area_level_2' in types:
                parsed['county'] = long_text
            elif 'administrative_area_level_1' in types:
                parsed['state'] = short_text
            elif 'postal_code' in types:
                parsed['zip_code'] = long_text
            elif 'country' in types:
                parsed['country'] = long_text

        return parsed

    def search_google_places(self, course):
        """Search Google Places API for course information"""
        # Build search query
        query = f"{course.get('course_name', course.get('name', ''))}"
        if course.get('street_address'):
            query += f", {course['street_address']}"
        if course.get('city'):
            query += f", {course['city']}"
        if course.get('state'):
            query += f", {course['state']}"

        url = f"https://places.googleapis.com/v1/places:searchText?key={self.api_key}"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-FieldMask": (
                "places.id,"
                "places.displayName,"
                "places.formattedAddress,"
                "places.addressComponents,"
                "places.location,"
                "places.primaryType,"
                "places.websiteUri,"
                "places.nationalPhoneNumber,"
                "places.regularOpeningHours.weekdayDescriptions,"
                "places.rating,"
                "places.userRatingCount,"
                "places.photos"
            )
        }
        payload = {"textQuery": query}

        logger.debug(f"🔍 Searching Google Places for: {query}")

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get("places"):
                place = data["places"][0]
                logger.debug(f"✅ Found place: {place.get('id')}")
                return place
            else:
                logger.debug("❌ No places found")
                return None

        except Exception as e:
            logger.error(f"⚠️ Google Places API Error: {e}")
            return None

    def prepare_place_data(self, course, place_data):
        """Prepare place data for API submission"""
        course_number = course.get('course_number', '')

        if not place_data:
            return {
                'course_number': course_number,
                'place_id': None,
                'display_name': None,
                'formatted_address': None,
                'street_number': None,
                'route': None,
                'street_address': None,
                'city': None,
                'state': None,
                'county': None,
                'country': None,
                'latitude': None,
                'longitude': None,
                'primary_type': None,
                'website': None,
                'phone': None,
                'opening_hours': None,
                'user_rating_count': None,
                'photo_reference': None,
                'google_maps_link': None,
                'zip_code': None
            }

        # Parse address components
        address_components = place_data.get("addressComponents", [])
        parsed_address = self.parse_address_components(address_components)

        # Combine street number and route for full street address
        street_address = None
        if parsed_address['street_number'] and parsed_address['route']:
            street_address = f"{parsed_address['street_number']} {parsed_address['route']}"
        elif parsed_address['route']:
            street_address = parsed_address['route']
        elif parsed_address['street_number']:
            street_address = parsed_address['street_number']

        return {
            'course_number': course_number,
            'place_id': place_data.get("id"),
            'display_name': place_data.get("displayName", {}).get("text"),
            'formatted_address': place_data.get("formattedAddress"),
            'street_number': parsed_address['street_number'],
            'route': parsed_address['route'],
            'street_address': street_address,
            'city': parsed_address['city'],
            'state': parsed_address['state'],
            'county': parsed_address['county'],
            'country': parsed_address['country'],
            'latitude': place_data.get("location", {}).get("latitude"),
            'longitude': place_data.get("location", {}).get("longitude"),
            'primary_type': place_data.get("primaryType"),
            'website': place_data.get("websiteUri"),
            'phone': place_data.get("nationalPhoneNumber"),
            'opening_hours': "; ".join(
                place_data.get("regularOpeningHours", {}).get("weekdayDescriptions", [])
            ) if place_data.get("regularOpeningHours", {}).get("weekdayDescriptions") else None,
            'user_rating_count': place_data.get("userRatingCount"),
            'photo_reference': (
                place_data.get("photos", [{}])[0].get("name") if place_data.get("photos") else None
            ),
            'google_maps_link': f"https://www.google.com/maps/place/?q=place_id:{place_data.get('id')}" if place_data.get('id') else None,
            'zip_code': parsed_address['zip_code']
        }

    def save_place_data_via_api(self, place_data):
        """Save place data via API"""
        try:
            payload = {
                'course_number': place_data['course_number'],
                'google_places_data': place_data
            }

            response = self.session.post(
                f"{self.api_url}/api/google-places-data",
                json=payload,
                timeout=self.db_timeout
            )

            if response.status_code in [200, 201]:
                logger.debug(f"✅ Saved Google Places data for course: {place_data['course_number']}")
                return True
            else:
                logger.error(f"❌ API error saving course {place_data['course_number']}: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"❌ Failed to save data for course {place_data['course_number']}: {e}")
            return False

    def update_pipeline_status(self, course_number: str, status: str, step_details: dict = None):
        """Update pipeline status via API"""
        try:
            payload = {
                'course_number': course_number,
                'current_step': 2,  # Google Places enrichment step
                'status': status,
                'step_details': step_details or {},
                'progress_percent': 100 if status == 'completed' else 50
            }

            response = self.session.post(
                f"{self.api_url}/api/pipeline/update",
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                logger.debug(f"📊 Updated pipeline status for {course_number}: {status}")

        except Exception as e:
            logger.debug(f"⚠️ Error updating pipeline status: {e}")

    def process_single_course(self, course: dict) -> dict:
        """Process a single course and return results"""
        course_number = course.get('course_number', '')
        course_name = course.get('course_name', course.get('name', ''))

        result = {
            'course_number': course_number,
            'course_name': course_name,
            'success': False,
            'has_data': False,
            'database_updated': False,
            'skipped': False,
            'error': None
        }

        try:
            # Check if we should skip (existing data + no force)
            if not self.force and self.check_existing_data(course_number):
                logger.info(f"⏭️ Skipping {course_number}: Data already exists (use --force to override)")
                result['skipped'] = True
                return result

            # Validate required fields
            if not course_number or not course_name:
                error_msg = f"Missing required data: number={bool(course_number)}, name={bool(course_name)}"
                logger.warning(f"⚠️ {course_number}: {error_msg}")
                result['error'] = error_msg
                return result

            # Update pipeline status
            self.update_pipeline_status(course_number, 'in_progress', {'step': 'google_places_search'})

            # Search Google Places
            place_data = self.search_google_places(course)

            # Prepare data for API
            api_data = self.prepare_place_data(course, place_data)

            # Check if we found useful data
            if place_data:
                result['has_data'] = True

            # Save to database (even if no data found, to record the attempt)
            database_updated = self.save_place_data_via_api(api_data)
            result['database_updated'] = database_updated

            if database_updated:
                result['success'] = True
                status_details = {'step': 'google_places_saved'}
                if place_data:
                    status_details['place_found'] = True
                    status_details['place_id'] = place_data.get('id')
                else:
                    status_details['place_found'] = False

                self.update_pipeline_status(course_number, 'completed', status_details)
            else:
                result['error'] = 'Database save failed'
                self.update_pipeline_status(course_number, 'failed', {'step': 'database_save_failed'})

            return result

        except Exception as e:
            error_msg = f"Processing error: {e}"
            logger.error(f"❌ {course_number}: {error_msg}")
            result['error'] = error_msg
            self.update_pipeline_status(course_number, 'failed',
                                      {'step': 'processing_error', 'error': str(e)})
            return result

    def run(self) -> dict:
        """Main execution method"""
        try:
            logger.info("🚀 Starting Smart Google Places Enrichment")

            # Verify API connection
            if not self.verify_api_connection():
                raise Exception("Cannot proceed without API connection")

            # ✅ FIXED: Get target courses AND skip count
            courses, total_skipped = self.get_target_courses()

            if not courses:
                logger.info("✅ No courses need processing")
                return {
                    'total_courses': total_skipped,  # ✅ Show total courses found
                    'processed': 0,
                    'successful': 0,
                    'failed': 0,
                    'skipped': total_skipped,  # ✅ Show skipped count
                    'found_data': 0,
                    'success_rate': 0
                }

            logger.info(f"📋 Processing {len(courses)} courses")

            # Process each course
            results = {
                'total_courses': len(courses) + total_skipped,  # ✅ Include skipped in total
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'skipped': total_skipped,  # ✅ Start with pre-skipped count
                'found_data': 0,
                'courses': []
            }

            for i, course in enumerate(courses, 1):
                course_number = course.get('course_number', 'Unknown')
                course_name = course.get('course_name', course.get('name', 'Unknown'))

                logger.info(f"📍 [{i}/{len(courses)}] Processing {course_number}: {course_name}")

                # Process single course
                course_result = self.process_single_course(course)
                results['courses'].append(course_result)
                results['processed'] += 1

                # Update counters
                if course_result['skipped']:
                    results['skipped'] += 1  # Additional skips during processing
                elif course_result['success']:
                    results['successful'] += 1
                    if course_result['has_data']:
                        results['found_data'] += 1
                        logger.info(f"✅ {course_number}: Found and saved Google Places data")
                    else:
                        logger.info(f"✅ {course_number}: No data found but recorded attempt")
                else:
                    results['failed'] += 1
                    logger.error(f"❌ {course_number}: {course_result.get('error', 'Unknown error')}")

                # Rate limiting - be nice to Google's API
                if i < len(courses):
                    time.sleep(0.5)

            # Calculate success rate
            processed_count = results['processed']
            results['success_rate'] = (results['successful'] / processed_count * 100) if processed_count > 0 else 0

            return results

        except Exception as e:
            logger.error(f"💥 Critical error: {e}")
            raise

def parse_args():
    """Parse command line arguments with standard patterns"""
    parser = argparse.ArgumentParser(description="Smart Google Places Enrichment - handles targeting and processing")

    # Standard targeting arguments
    parser.add_argument('--course', help='Process single course by course number')
    parser.add_argument('--state', help='Process all courses in specific state')
    parser.add_argument('--force', action='store_true', help='Force reprocess courses with existing data')

    # Additional options
    parser.add_argument('--limit', type=int, help='Maximum number of courses to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without running')
    parser.add_argument('--status', action='store_true', help='Show processing status')

    return parser.parse_args()

def show_status(enricher: SmartGooglePlacesEnricher):
    """Show processing status"""
    try:
        # Get status via API
        params = {}
        if enricher.state:
            params['state'] = enricher.state

        response = enricher.session.get(
            f"{enricher.api_url}/api/google-places-status",
            params=params,
            timeout=10
        )
        response.raise_for_status()

        status_data = response.json()

        if enricher.course:
            # Single course status
            course_response = enricher.session.get(
                f"{enricher.api_url}/api/google-places-data/{enricher.course}",
                timeout=10
            )

            print(f"\nGoogle Places Data Status for Course: {enricher.course.upper()}")
            print("=" * 60)

            if course_response.status_code == 200:
                course_data = course_response.json()
                data = course_data.get('data', {})
                print(f"Course: {enricher.course.upper()}")
                print(f"Status: ✅ Processed")
                print(f"Display Name: {data.get('display_name', 'N/A')}")
                print(f"Address: {data.get('formatted_address', 'N/A')}")
                print(f"Last Updated: {data.get('updated_at', 'N/A')}")
            else:
                print(f"Course: {enricher.course.upper()}")
                print(f"Status: ❌ Not processed")

        else:
            # State or all courses status
            if status_data.get('success'):
                stats = status_data.get('statistics', {})
                recent = status_data.get('recent_entries', [])

                print("\nGoogle Places Data Processing Status:")
                print("=" * 60)

                for state_name, state_stats in stats.items():
                    print(f"State: {state_name}")
                    print(f"  Total Courses: {state_stats['total']}")
                    print(f"  Processed: {state_stats['processed']}")
                    print(f"  Remaining: {state_stats['remaining']}")
                    print(f"  Completion: {state_stats['percentage']}%")
                    print("-" * 40)

                if recent:
                    print(f"\nRecent Entries ({len(recent)}):")
                    for entry in recent[:5]:
                        print(f"  - {entry['course_number']}: {entry['display_name']}")

        return True

    except Exception as e:
        logger.error(f"❌ Error getting status: {e}")
        return False

def main():
    """Main execution function"""
    args = parse_args()

    # Validate arguments
    if args.course and args.state:
        logger.error("❌ Cannot specify both --course and --state")
        return 1

    try:
        # Create enricher instance
        enricher = SmartGooglePlacesEnricher(
            course=args.course,
            state=args.state,
            force=args.force,
            limit=args.limit
        )

        # Status mode
        if args.status:
            success = show_status(enricher)
            return 0 if success else 1

        # Dry run mode
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - showing what would be processed:")
            courses, skipped_count = enricher.get_target_courses()
            for course in courses:
                course_name = course.get('course_name', course.get('name', 'Unknown'))
                logger.info(f"  - {course['course_number']}: {course_name}")
            logger.info(f"📊 Total courses found: {len(courses) + skipped_count}")
            logger.info(f"⏭️ Would skip: {skipped_count}")
            logger.info(f"🔄 Would process: {len(courses)}")
            return 0

        # Run the enrichment
        results = enricher.run()

        # ✅ FIXED: Enhanced final summary with better breakdown
        logger.info("=" * 60)
        logger.info("📊 FINAL SUMMARY")
        logger.info(f"📋 Total courses found: {results['total_courses']}")
        logger.info(f"⏭️ Courses skipped: {results['skipped']}")
        logger.info(f"🔄 Courses processed: {results['processed']}")
        logger.info(f"✅ Successful: {results['successful']}")
        logger.info(f"🔍 Found data: {results['found_data']}")
        logger.info(f"❌ Failed: {results['failed']}")
        if results['processed'] > 0:
            logger.info(f"📈 Success rate: {results['success_rate']:.1f}%")
        logger.info("=" * 60)

        # Return appropriate exit code
        return 0 if results['failed'] == 0 else 1

    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)
