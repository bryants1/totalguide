#!/usr/bin/env python3
"""
Rubric Runner Script
Reads course numbers from Excel file and creates rubrics for each course
Usage: python run_rubrics.py [--start-from MA-111] [--limit 10] [--force]
"""

import pandas as pd
import subprocess
import sys
import os
import time
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RubricRunner:
    def __init__(self):
        self.excel_path = Path("../states/ma/course_list")
        self.rubric_script = "fixed_rubric_script.py"
        self.results = {
            'processed': [],
            'skipped': [],
            'failed': [],
            'total_processed': 0,
            'total_skipped': 0,
            'total_failed': 0
        }

    def find_excel_file(self):
        """Find the Excel file in the course_list directory"""
        possible_files = [
            "USGolfDataWithPlaceDetails_with_urls.xlsx",
            "USGolfData-WithPlaceDetails_with_urls.xlsx",
            "USGolfData_WithPlaceDetails_with_urls.xlsx"
        ]

        for filename in possible_files:
            file_path = self.excel_path / filename
            if file_path.exists():
                logger.info(f"✅ Found Excel file: {file_path}")
                return file_path

        # If exact files not found, look for any Excel file
        excel_files = list(self.excel_path.glob("*.xlsx"))
        if excel_files:
            non_old_files = [f for f in excel_files if "old" not in f.name.lower()]
            if non_old_files:
                logger.info(f"✅ Found Excel file: {non_old_files[0]}")
                return non_old_files[0]
            else:
                logger.info(f"⚠️  Using Excel file: {excel_files[0]}")
                return excel_files[0]

        logger.error(f"❌ No Excel file found in {self.excel_path}")
        return None

    def load_course_numbers(self):
        """Load course numbers from the Excel file"""
        excel_file = self.find_excel_file()
        if not excel_file:
            return []

        try:
            df = pd.read_excel(excel_file)
            logger.info(f"📊 Loaded Excel file with {len(df)} rows and {len(df.columns)} columns")

            # Look for course number column
            course_col = None
            possible_names = ['coursenumber', 'course_number', 'CourseNumber', 'Course Number', 'ID', 'id']

            for col_name in possible_names:
                if col_name in df.columns:
                    course_col = col_name
                    break

            if course_col is None:
                for col in df.columns:
                    if 'course' in col.lower() and 'number' in col.lower():
                        course_col = col
                        break
                    elif 'course' in col.lower() and 'id' in col.lower():
                        course_col = col
                        break

            if course_col is None:
                course_col = df.columns[0]
                logger.warning(f"⚠️  Could not find course number column, using first column: {course_col}")
            else:
                logger.info(f"✅ Using course number column: {course_col}")

            course_numbers = df[course_col].dropna().astype(str).tolist()

            valid_courses = []
            for course in course_numbers:
                course = course.strip()
                if course and course != 'nan' and len(course) > 2:
                    if '-' in course or course.startswith(('MA', 'CA', 'FL', 'TX', 'NY')):
                        valid_courses.append(course)

            logger.info(f"📋 Found {len(valid_courses)} valid course numbers")
            logger.info(f"📋 Sample courses: {valid_courses[:5]}")

            return valid_courses

        except Exception as e:
            logger.error(f"❌ Error loading Excel file: {e}")
            return []

    def check_existing_rubric(self, course_id):
        """Check if rubric already exists for this course"""
        import glob
        course_scores_pattern = f"../states/ma/course_scores/{course_id}_*/"
        course_dirs = glob.glob(course_scores_pattern)

        if course_dirs:
            rubric_file = Path(course_dirs[0]) / f"{course_id}_rubric.json"
            return rubric_file.exists()
        return False

    def run_rubric_script(self, course_id):
        """Run the rubric creation script for a course"""
        try:
            logger.info(f"📊 Creating rubric for {course_id}...")
            result = subprocess.run(
                [sys.executable, self.rubric_script, "single", course_id],
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout for AI processing
            )

            if result.returncode == 0:
                logger.info(f"✅ Rubric created successfully for {course_id}")
                return True, result.stdout
            elif result.returncode == 2:
                logger.warning(f"⏭️  Rubric skipped for {course_id} - missing required files")
                return "skipped", result.stderr
            else:
                logger.error(f"❌ Rubric creation failed for {course_id}")
                logger.error(f"Error output: {result.stderr}")
                return False, result.stderr

        except subprocess.TimeoutExpired:
            logger.error(f"⏰ Rubric creation timed out for {course_id}")
            return False, "Timeout"
        except Exception as e:
            logger.error(f"❌ Exception running rubric script for {course_id}: {e}")
            return False, str(e)

    def process_course(self, course_id, force=False):
        """Process a single course"""
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Processing Rubric: {course_id}")
        logger.info(f"{'='*60}")

        # Check existing rubric
        has_rubric = self.check_existing_rubric(course_id)

        if not force and has_rubric:
            logger.info(f"⏭️  Skipping {course_id} - rubric already exists")
            self.results['skipped'].append(course_id)
            self.results['total_skipped'] += 1
            return True

        # Create rubric
        rubric_result, rubric_msg = self.run_rubric_script(course_id)

        if rubric_result == "skipped":
            logger.warning(f"⏭️  Skipped {course_id} - missing required files")
            self.results['skipped'].append(course_id)
            self.results['total_skipped'] += 1
            return True
        elif rubric_result:
            logger.info(f"✅ Successfully processed {course_id}")
            self.results['processed'].append(course_id)
            self.results['total_processed'] += 1
            return True
        else:
            logger.error(f"❌ Failed to process {course_id}")
            self.results['failed'].append((course_id, rubric_msg))
            self.results['total_failed'] += 1
            return False

    def run(self, start_from=None, limit=None, force=False):
        """Run the rubric processing for all courses"""
        logger.info("🚀 Starting Rubric Runner")

        # Check if rubric script exists
        if not Path(self.rubric_script).exists():
            logger.error(f"❌ Rubric script not found: {self.rubric_script}")
            return

        # Load course numbers
        course_numbers = self.load_course_numbers()
        if not course_numbers:
            logger.error("❌ No course numbers found")
            return

        # Apply start_from filter
        if start_from:
            try:
                start_index = course_numbers.index(start_from)
                course_numbers = course_numbers[start_index:]
                logger.info(f"🎯 Starting from course {start_from} (index {start_index})")
            except ValueError:
                logger.warning(f"⚠️  Start course {start_from} not found, starting from beginning")

        # Apply limit
        if limit:
            course_numbers = course_numbers[:limit]
            logger.info(f"🔢 Limited to {limit} courses")

        logger.info(f"📋 Processing rubrics for {len(course_numbers)} courses...")

        # Process each course
        start_time = time.time()

        for i, course_id in enumerate(course_numbers, 1):
            logger.info(f"\n📊 Progress: {i}/{len(course_numbers)} courses")

            try:
                self.process_course(course_id, force)
            except KeyboardInterrupt:
                logger.info("\n⚠️  Processing interrupted by user")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error processing {course_id}: {e}")
                self.results['failed'].append((course_id, f"Unexpected error: {e}"))
                self.results['total_failed'] += 1

            time.sleep(1)  # Small delay between courses

        # Print final summary
        elapsed_time = time.time() - start_time
        self.print_summary(elapsed_time)

    def print_summary(self, elapsed_time):
        """Print processing summary"""
        logger.info(f"\n{'='*60}")
        logger.info("📊 RUBRIC PROCESSING SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {elapsed_time:.1f} seconds")
        logger.info(f"✅ Successfully processed: {self.results['total_processed']}")
        logger.info(f"⏭️  Skipped (already exist or missing files): {self.results['total_skipped']}")
        logger.info(f"❌ Failed: {self.results['total_failed']}")

        if self.results['failed']:
            logger.info(f"\n❌ Failed courses:")
            for course_id, reason in self.results['failed']:
                logger.info(f"  • {course_id}: {reason}")


def main():
    parser = argparse.ArgumentParser(description='Create rubrics for golf courses from Excel file')
    parser.add_argument('--start-from', help='Course ID to start from (e.g., MA-111)')
    parser.add_argument('--limit', type=int, help='Maximum number of courses to process')
    parser.add_argument('--force', action='store_true', help='Force recreation even if rubrics exist')

    args = parser.parse_args()

    runner = RubricRunner()
    runner.run(
        start_from=args.start_from,
        limit=args.limit,
        force=args.force
    )


if __name__ == "__main__":
    main()
