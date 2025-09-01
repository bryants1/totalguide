#!/usr/bin/env python3
"""
Course Text and Form Summary Generator
Processes individual course text insight files and generates summary scores.
"""

import json
import pandas as pd
import os
import re
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

CATEGORY_MAP = {
    # GolfNow mappings
    "18 Hole Pace of Play": "Pace",
    "Course Conditions": "Conditions",
    "Staff Friendliness": "Friendliness",
    "Value for the Money": "Value",
    "Off-Course Amenities": "Amenities",
    "Course Difficulty": "Difficulty",

    # GolfPass mappings (direct mappings)
    "Conditions": "Conditions",
    "Value": "Value",
    "Layout": "Layout",
    "Friendliness": "Friendliness",
    "Pace": "Pace",
    "Amenities": "Amenities",
    "Difficulty": "Difficulty",

    # Legacy/backup mappings
    "Pace": "Pace", "Conditions": "Conditions", "Friendliness": "Friendliness",
    "Value": "Value", "Amenities": "Amenities", "Difficulty": "Difficulty",
    "Layout": "Layout"
}

TEXT_INSIGHT_CATEGORIES = [
    "Fairways",
    "Greens",
    "Bunkers",
    "Tee Boxes",
    "Shot Variety / Hole Uniqueness",
    "Signature Holes / Quirky/Fun Design Features",
    "Water & OB Placement",
    "Overall feel / Scenery",
    "Green Complexity",
    "Driving Range",
    "Putting & Short Game Areas",
    "Availability",
    "Snack Bar-1, Snack Bar w/ Alcohol-2, Grill w/ Alcohol-3, Full Bar & Lounge-4, Full Service Restaurant-5",
    "Locker room & Showers",
    "Pro-shop",
    "Staff Friendliness, After-Round Experience",
    "Eco-friendless and sustainability",
    "Course History",
    "Architect",
    "Green Fees vs. Quality",
    "Replay Value",
    "Ease of Walking",
    "Pace of Play"
]

TEXT_TO_NUM = {
    "Excellent": 5,
    "Good": 4,
    "Average": 3,
    "Fair": 2,
    "Poor": 2,
    "Very Poor": 1,
    "Moderate": 3,
    "Somewhat Challenging": 4,
    "Extremely Challenging": 5,
    None: None
}

def detect_review_type(review_item):
    """Detect whether this is a Google review, GolfNow review, or GolfPass review"""
    if isinstance(review_item, dict):
        # Check for GolfPass review indicators first (most specific)
        if 'secondary_ratings' in review_item or 'course' in review_item or 'played_on' in review_item:
            return 'golfpass'

        # Check for Google review indicators
        if 'place_id' in review_item or 'reviewer_name' in review_item or 'review_rating' in review_item:
            return 'google'

        # Check for GolfNow review indicators (review_number + ratings structure)
        if 'review_number' in review_item and 'ratings' in review_item:
            return 'golfnow'

    return 'unknown'

def parse_review_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%Y - Verified Purchase"]:
        try:
            return datetime.strptime(date_str.strip().split()[0], fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def parse_google_review(review_item):
    """Parse a single Google review"""
    return {
        "review_id": review_item.get('review_id'),
        "source": "Google",
        "category_ratings": {
            "Conditions": None, "Value": None, "Layout": None,
            "Friendliness": None, "Pace": None, "Amenities": None, "Difficulty": None
        },
        "text_review": review_item.get('review'),
        "text_insights": review_item.get("text_insights", None),
        "recommend": None
    }

def parse_golfnow_review(review_item):
    """Parse a single GolfNow review"""
    ratings = review_item.get("ratings", {})
    cat_ratings = {}

    # Map GolfNow-specific ratings using CATEGORY_MAP
    for k, v in ratings.items():
        mapped_key = CATEGORY_MAP.get(k)
        if mapped_key and v != "n/a":
            cat_ratings[mapped_key] = v

    return {
        "review_id": str(review_item.get('review_number')),
        "source": "GolfNow",
        "category_ratings": {
            "Conditions": cat_ratings.get("Conditions"),
            "Value": cat_ratings.get("Value"),
            "Layout": cat_ratings.get("Layout"),  # May be None for GolfNow
            "Friendliness": cat_ratings.get("Friendliness"),
            "Pace": cat_ratings.get("Pace"),
            "Amenities": cat_ratings.get("Amenities"),
            "Difficulty": cat_ratings.get("Difficulty")
        },
        "text_review": review_item.get("comment") or review_item.get("raw_text"),
        "text_insights": review_item.get("text_insights", None),
        "recommend": review_item.get("recommend")
    }

def parse_golfpass_review(review_item):
    """Parse a single GolfPass review"""
    sec = review_item.get('secondary_ratings', {}) or {}

    # Generate a unique review ID
    review_id = f"gp_{review_item.get('page', 'unknown')}_{hash(str(review_item.get('text', ''))[:50]) % 10000}"

    return {
        "review_id": review_id,
        "source": "GolfPass",
        "category_ratings": {
            "Conditions": sec.get('Conditions'),
            "Value": sec.get('Value'),
            "Layout": sec.get('Layout'),
            "Friendliness": sec.get('Friendliness'),
            "Pace": sec.get('Pace'),
            "Amenities": sec.get('Amenities'),
            "Difficulty": sec.get('Difficulty')
        },
        "text_review": review_item.get('text'),
        "text_insights": review_item.get("text_insights", None),
        "recommend": review_item.get("recommend")
    }

def parse_mixed_reviews(data):
    """Parse mixed review data that could contain Google, GolfNow, and GolfPass reviews"""
    result = []
    type_counts = {"google": 0, "golfnow": 0, "golfpass": 0, "unknown": 0}

    # Handle if data is a list directly
    if isinstance(data, list):
        reviews_list = data
    # Handle if data is wrapped in a 'reviews' key
    elif isinstance(data, dict) and 'reviews' in data:
        reviews_list = data['reviews']
    else:
        reviews_list = [data]

    for i, review_item in enumerate(reviews_list):
        review_type = detect_review_type(review_item)
        type_counts[review_type] += 1

        try:
            if review_type == 'google':
                parsed_review = parse_google_review(review_item)
            elif review_type == 'golfnow':
                parsed_review = parse_golfnow_review(review_item)
            elif review_type == 'golfpass':
                parsed_review = parse_golfpass_review(review_item)
            else:
                print(f"Warning: Unknown review type for item {i}: {list(review_item.keys())[:5]}")
                continue

            result.append(parsed_review)
        except Exception as e:
            print(f"Error parsing review {i} (type: {review_type}): {e}")
            continue

    print(f"Detected review types: {dict(type_counts)}")
    return result

def aggregate_form_categories(reviews):
    sums = defaultdict(float)
    counts = defaultdict(int)
    for r in reviews:
        for cat, val in r.get("category_ratings", {}).items():
            num = TEXT_TO_NUM.get(val, None)
            if num is not None and cat:
                sums[cat] += num
                counts[cat] += 1
    avg = {cat: round(sums[cat]/counts[cat], 2) if counts[cat] else None for cat in sums}
    return avg

def aggregate_text_insights(reviews):
    cat_totals = {cat: 0.0 for cat in TEXT_INSIGHT_CATEGORIES}
    cat_counts = {cat: 0 for cat in TEXT_INSIGHT_CATEGORIES}
    all_themes = []

    for r in reviews:
        ti = r.get("text_insights", {})
        sentiment = ti.get("sentiment", {}) if ti else {}
        for cat in TEXT_INSIGHT_CATEGORIES:
            val = sentiment.get(cat)
            if val is not None:
                try:
                    cat_totals[cat] += float(val)
                    cat_counts[cat] += 1
                except Exception:
                    pass
        themes = ti.get("extra_themes", []) if ti else []
        if isinstance(themes, list):
            all_themes.extend([str(t) for t in themes])

    summary = {cat: (round(cat_totals[cat]/cat_counts[cat], 3) if cat_counts[cat] else None)
               for cat in TEXT_INSIGHT_CATEGORIES}
    theme_counts = Counter(all_themes)
    return summary, theme_counts.most_common(20)

def recommend_rate(reviews):
    count = 0
    total = 0
    for r in reviews:
        val = r.get("recommend", None)
        if val is not None:
            total += 1
            if val is True or (isinstance(val, str) and val.lower() == "yes"):
                count += 1
    return round(100 * count / total, 1) if total else None

def extract_course_info(filename):
    """Extract course prefix and name from filename."""
    # Pattern: MA-XX_CourseName_textInsights.json or MA-XX_CourseName_textInsights_recent.json
    # Note: _provenance files are excluded before this function is called
    match = re.match(r'^(MA-\d+(?:-\d+)?)_(.+?)_textInsights(?:_recent)?\.json$', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def find_insights_directory():
    """Find the insights directory."""
    possible_paths = [
        "../states/ma/reviews/insights/",
        "./states/ma/reviews/insights/",
        "./insights/",
        "../insights/"
    ]

    for path in possible_paths:
        insights_path = Path(path)
        if insights_path.exists() and insights_path.is_dir():
            print(f"Found insights directory: {insights_path.absolute()}")
            return str(insights_path)

    print("Warning: Could not find insights directory")
    return None

def ensure_scores_directory():
    """Ensure the scores directory exists."""
    possible_paths = [
        "../states/ma/reviews/scores/",
        "./states/ma/reviews/scores/",
        "./scores/",
        "../scores/"
    ]

    # Try to find existing scores directory
    for path in possible_paths:
        scores_path = Path(path)
        if scores_path.exists() and scores_path.is_dir():
            print(f"Found scores directory: {scores_path.absolute()}")
            return str(scores_path)

    # If not found, create it relative to insights
    insights_dir = find_insights_directory()
    if insights_dir:
        scores_path = Path(insights_dir).parent / "scores"
        scores_path.mkdir(exist_ok=True)
        print(f"Created scores directory: {scores_path.absolute()}")
        return str(scores_path)

    # Fallback to current directory
    scores_path = Path("./scores")
    scores_path.mkdir(exist_ok=True)
    print(f"Created scores directory in current location: {scores_path.absolute()}")
    return str(scores_path)

def prioritize_course_files(text_insight_files):
    """
    When multiple files exist for the same course, prioritize _recent over base files.
    Returns a list with one file per course.
    """
    # Group files by course prefix
    course_files = defaultdict(list)

    for file_path in text_insight_files:
        course_prefix, course_name = extract_course_info(file_path.name)
        if course_prefix:
            course_files[course_prefix].append(file_path)

    # For each course, pick the best version
    prioritized_files = []
    duplicates_info = []

    for course_prefix, files in course_files.items():
        if len(files) > 1:
            # Sort by preference: _recent > base
            def file_priority(file_path):
                name = file_path.name
                if '_recent' in name:
                    return 0  # Highest priority
                else:
                    return 1  # Lower priority (base file)

            sorted_files = sorted(files, key=file_priority)
            best_file = sorted_files[0]
            other_files = sorted_files[1:]

            prioritized_files.append(best_file)
            duplicates_info.append({
                'course': course_prefix,
                'selected': best_file.name,
                'skipped': [f.name for f in other_files]
            })
        else:
            prioritized_files.append(files[0])

    return prioritized_files, duplicates_info

def process_single_course(input_file, output_dir, skip_existing=True):
    """Process a single course text insights file."""
    course_prefix, course_name = extract_course_info(Path(input_file).name)
    if not course_prefix:
        print(f"Warning: Could not extract course info from {input_file}")
        return False

    # Check if output file already exists
    output_filename = f"{course_prefix}_{course_name}_summary.json"
    output_path = Path(output_dir) / output_filename

    if skip_existing and output_path.exists():
        print(f"⚠️  Summary already exists, skipping {course_prefix} - {course_name}")
        print(f"    Existing: {output_path.name}")
        print(f"    Use --overwrite to reprocess existing files")
        return True  # Return True since it's not really a failure
    elif output_path.exists():
        print(f"🔄 Overwriting existing summary for {course_prefix} - {course_name}")

    print(f"\n=== Processing {course_prefix} - {course_name} ===")

    try:
        with open(input_file) as f:
            data = json.load(f)

        # Parse mixed reviews from this file
        parsed_reviews = parse_mixed_reviews(data)
        print(f"Processed {len(parsed_reviews)} reviews from {Path(input_file).name}")

        if not parsed_reviews:
            print(f"No reviews found in {input_file}")
            return False

        # Aggregate numeric stats
        form_avg = aggregate_form_categories(parsed_reviews)
        text_avg, top_themes = aggregate_text_insights(parsed_reviews)

        # Compute overall average if available (category "overall" or mean of form categories)
        overall_vals = [v for v in form_avg.values() if v is not None]
        overall_rating = round(sum(overall_vals) / len(overall_vals), 2) if overall_vals else None
        recommend = recommend_rate(parsed_reviews)

        # Count reviews by source
        source_counts = Counter([r.get('source', 'Unknown') for r in parsed_reviews])

        summary = {
            "course_prefix": course_prefix,
            "course_name": course_name,
            "total_reviews": len(parsed_reviews),
            "reviews_by_source": dict(source_counts),
            "overall_rating": overall_rating,
            "recommend_percent": recommend,
            "form_category_averages": form_avg,
            "text_insight_averages": text_avg,
            "top_text_themes": top_themes
        }

        # Save summary JSON
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"✓ Saved summary as {output_path}")

        return True

    except Exception as e:
        print(f"Error processing {input_file}: {e}")
        return False

def main(insights_dir_override=None, scores_dir_override=None, overwrite=False):
    """Main function to process all text insight files."""
    print('Course Text and Form Summary Generator')
    print('======================================')

    # Find insights directory
    if insights_dir_override:
        insights_dir = insights_dir_override
        print(f"Using specified insights directory: {insights_dir}")
    else:
        insights_dir = find_insights_directory()
        if not insights_dir:
            print("Error: Could not find insights directory")
            return

    # Ensure scores directory exists
    if scores_dir_override:
        scores_dir = scores_dir_override
        print(f"Using specified scores directory: {scores_dir}")
        Path(scores_dir).mkdir(parents=True, exist_ok=True)
    else:
        scores_dir = ensure_scores_directory()

    # Find all text insights files
    insights_path = Path(insights_dir)
    all_files = list(insights_path.glob("*_textInsights*.json"))

    # Filter out _provenance files
    text_insight_files = [f for f in all_files if '_provenance' not in f.name]
    provenance_files = [f for f in all_files if '_provenance' in f.name]

    print(f"Found {len(all_files)} total text insight files")
    print(f"Excluding {len(provenance_files)} _provenance files")

    if not text_insight_files:
        print(f"No processable *_textInsights*.json files found in {insights_dir}")
        return

    # Handle duplicate courses (prioritize _recent over base)
    text_insight_files, duplicate_info = prioritize_course_files(text_insight_files)

    if duplicate_info:
        print(f"Found {len(duplicate_info)} courses with multiple versions (prioritizing _recent):")
        for info in duplicate_info:
            print(f"  {info['course']}: Using {info['selected']}")

    print(f"Processing {len(text_insight_files)} unique courses:")
    for file_path in text_insight_files:
        print(f"  {file_path.name}")

    # Process each file
    successful = 0
    skipped = 0
    failed = 0

    for file_path in text_insight_files:
        course_prefix, course_name = extract_course_info(file_path.name)
        if course_prefix:
            output_filename = f"{course_prefix}_{course_name}_summary.json"
            output_path = Path(scores_dir) / output_filename

            if not overwrite and output_path.exists():
                skipped += 1
                print(f"⚠️  Summary already exists, skipping {course_prefix} - {course_name}")
            else:
                if process_single_course(file_path, scores_dir, skip_existing=not overwrite):
                    successful += 1
                else:
                    failed += 1
        else:
            failed += 1
            print(f"❌ Could not extract course info from {file_path.name}")

    print(f"\n=== Summary ===")
    print(f"Successfully processed: {successful}")
    print(f"Skipped (already exist): {skipped}")
    print(f"Failed: {failed}")
    print(f"Excluded _provenance files: {len(provenance_files)}")
    print(f"Output directory: {scores_dir}")
    if not overwrite and skipped > 0:
        print("Use --overwrite to reprocess existing summary files")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Process course text insight files and generate summary scores."
    )
    parser.add_argument(
        "--insights_dir",
        help="Directory containing *_textInsights.json files (auto-detected if not specified)"
    )
    parser.add_argument(
        "--scores_dir",
        help="Output directory for summary files (auto-detected if not specified)"
    )
    parser.add_argument(
        "--single_file",
        help="Process a single text insights file instead of scanning directory"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing summary files (default: skip existing files)"
    )

    args = parser.parse_args()

    if args.single_file:
        # Process single file mode
        if not Path(args.single_file).exists():
            print(f"Error: File {args.single_file} does not exist")
            exit(1)

        scores_dir = args.scores_dir or ensure_scores_directory()
        success = process_single_course(args.single_file, scores_dir, skip_existing=not args.overwrite)
        if success:
            print("✓ Successfully processed single file")
        else:
            print("✗ Failed to process single file")
    else:
        # Directory processing mode (default)
        main(insights_dir_override=args.insights_dir, scores_dir_override=args.scores_dir, overwrite=args.overwrite)
