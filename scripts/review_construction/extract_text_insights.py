#!/usr/bin/env python3
"""
Golf Course Text Insights Processor
Processes review files from golfnow, golfpass, and google folders,
groups them by course prefix, and outputs text insights.
"""

import json
import openai
import time
import pandas as pd
import os
import re
from pathlib import Path
from collections import defaultdict

client = openai.OpenAI(api_key="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")  # <-- PUT YOUR OPENAI API KEY HERE

# Use cheaper model - gpt-3.5-turbo is even cheaper and works well for sentiment analysis
MODEL_NAME = "gpt-3.5-turbo"  # Cheapest option that works well for this task

# Date filtering - only process reviews from last 3 years
from datetime import datetime, timedelta
CUTOFF_DATE = datetime.now() - timedelta(days=3*365)  # 3 years ago

# These will be updated based on command line arguments
def update_globals(model_name, years_back):
    global MODEL_NAME, CUTOFF_DATE
    MODEL_NAME = model_name
    CUTOFF_DATE = datetime.now() - timedelta(days=years_back*365)

CATEGORIES = [
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

def extract_text_insights(text, categories):
    prompt = (
        "For the following golf course review text, analyze and extract a sentiment score for each category below (from -1 to +1, null if not mentioned). "
        "Also list any extra themes or repeated topics in the text. Output valid JSON as shown.\n\n"
        "Categories:\n- " + "\n- ".join(categories) + "\n\n"
        f"Review text:\n{text}\n\n"
        "Output format:\n"
        "{\n"
        "  \"sentiment\": {\"Fairways\": ..., ...},\n"
        "  \"extra_themes\": [ ... ]\n"
        "}\n"
        "Only output valid JSON."
    )
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,  # Using cheaper model
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=500,  # Restored token count
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.strip("`").replace("json", "").strip()
        data = json.loads(content)
        # Ensure all categories are present
        s = data.get("sentiment", {})
        out_sentiment = {cat: s.get(cat, None) for cat in categories}
        data["sentiment"] = out_sentiment
        return data
    except Exception as e:
        print(f"OpenAI extraction failed: {e}")
        return {"sentiment": {cat: None for cat in categories}, "extra_themes": []}

def parse_review_date(review):
    """Parse date from review and return datetime object if valid and recent."""
    # Try different date fields that might exist in reviews
    date_fields = [
        'date', 'review_date', 'created_date', 'posted_date',
        'played_on', 'date_played', 'review_time', 'timestamp'
    ]

    for field in date_fields:
        date_str = review.get(field)
        if not date_str:
            continue

        try:
            # Handle different date formats
            if isinstance(date_str, str):
                # Remove extra text like "Verified Purchase"
                date_str = date_str.split(' - ')[0].strip()

                # Try common date formats
                date_formats = [
                    "%Y-%m-%d",           # 2024-01-15
                    "%m/%d/%Y",           # 01/15/2024
                    "%d/%m/%Y",           # 15/01/2024
                    "%Y-%m-%d %H:%M:%S",  # 2024-01-15 10:30:00
                    "%B %d, %Y",          # January 15, 2024
                    "%b %d, %Y",          # Jan 15, 2024
                    "%d %B %Y",           # 15 January 2024
                    "%d %b %Y"            # 15 Jan 2024
                ]

                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date
                    except ValueError:
                        continue
            elif isinstance(date_str, (int, float)):
                # Handle timestamp (seconds since epoch)
                try:
                    parsed_date = datetime.fromtimestamp(date_str)
                    return parsed_date
                except (ValueError, OSError):
                    continue
        except Exception:
            continue

    return None

def is_recent_review(review, cutoff_date=CUTOFF_DATE):
    """Check if review is from the last 3 years."""
    review_date = parse_review_date(review)
    if review_date:
        return review_date >= cutoff_date

    # If no date found, include the review (assume it's recent)
    return True

def load_reviews(input_file):
    """Load reviews from a single file (JSON or CSV)."""
    ext = os.path.splitext(input_file)[-1].lower()
    if ext == ".csv":
        df = pd.read_csv(input_file)
        records = []
        for _, row in df.iterrows():
            review = row.to_dict()
            review["comment"] = row.get("review") or row.get("text_review") or row.get("text") or ""
            records.append(review)
        return records
    elif ext == ".json":
        with open(input_file, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            if "reviews" in data and isinstance(data["reviews"], list):
                return data["reviews"]
            else:
                return [data]
        else:
            raise ValueError("Unknown JSON format")
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def find_insights_directory():
    """Find or create the insights directory for output."""
    possible_paths = [
        "../states/ma/reviews/insights/",
        "./states/ma/reviews/insights/",
        "./insights/",
        "../insights/"
    ]

    # Try to find existing insights directory
    for path in possible_paths:
        insights_path = Path(path)
        if insights_path.exists() and insights_path.is_dir():
            print(f"Found insights directory: {insights_path.absolute()}")
            return str(insights_path)

    # If not found, try to create it relative to reviews directory
    reviews_dir = find_reviews_directory()
    if reviews_dir and reviews_dir != "./":
        insights_path = Path(reviews_dir) / "insights"
        insights_path.mkdir(exist_ok=True)
        print(f"Created insights directory: {insights_path.absolute()}")
        return str(insights_path)

    # Fallback to current directory
    insights_path = Path("./insights")
    insights_path.mkdir(exist_ok=True)
    print(f"Created insights directory in current location: {insights_path.absolute()}")
    return str(insights_path)

def find_reviews_directory():
    """Auto-detect the reviews directory."""
    possible_paths = [
        "../states/ma/reviews/",  # From review_construction to states/ma/reviews
        "./",
        "./states/ma/reviews/",
        "./States/ma/reviews/",
        "./reviews/",
        "../reviews/",
        "./ma/reviews/"
    ]

    print("Searching for reviews directory...")
    for path in possible_paths:
        base_path = Path(path)
        print(f"  Checking: {base_path.absolute()}")
        if base_path.exists():
            # Check if it contains any of our target folders
            has_folders = any((base_path / folder).exists() for folder in ["golfnow", "golfpass", "google"])
            print(f"    Directory exists. Has target folders: {has_folders}")
            if has_folders:
                print(f"Auto-detected reviews directory: {base_path.absolute()}")
                return str(base_path)
        else:
            print(f"    Directory does not exist")

    print("No reviews directory found, using current directory")
    return "./"

def find_course_files(base_dir="./"):
    """
    Find all course files in golfnow, golfpass, and google directories.
    Group them by course prefix (e.g., MA-93).
    """
    folders = ["golfnow", "golfpass", "google"]
    course_groups = defaultdict(list)

    # Debug: Show what we're looking for
    print(f"Looking for folders in base directory: {Path(base_dir).absolute()}")

    # List all items in base directory for debugging
    base_path = Path(base_dir)
    if base_path.exists():
        print(f"Contents of {base_path}:")
        for item in base_path.iterdir():
            print(f"  {'[DIR]' if item.is_dir() else '[FILE]'} {item.name}")
    else:
        print(f"Base directory {base_path} does not exist!")
        return course_groups

    for folder in folders:
        folder_path = base_path / folder
        print(f"Checking: {folder_path.absolute()}")
        if not folder_path.exists():
            print(f"Warning: {folder_path} does not exist, skipping...")
            continue

        print(f"Found folder: {folder_path}")
        # Find all files in the folder
        for file_path in folder_path.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in ['.json', '.csv']:
                filename = file_path.name
                print(f"  Found file: {filename}")

                # Extract course prefix (MA-XX format)
                match = re.match(r'^(MA-\d+(?:-\d+)?)', filename)
                if match:
                    course_prefix = match.group(1)
                    course_groups[course_prefix].append(file_path)
                    print(f"    → Grouped under {course_prefix}")
                else:
                    print(f"    → Warning: Could not extract course prefix from {filename}")

    return course_groups

def extract_course_name(files):
    """Extract course name from the filename (part between course number and source)."""
    for file_path in files:
        filename = file_path.stem  # filename without extension

        # Pattern: MA-XX_CourseName_source or MA-XX_CourseName
        # Remove course prefix
        match = re.match(r'^MA-\d+(?:-\d+)?_(.+?)(?:_(?:golfnow|golfpass|google))?$', filename)
        if match:
            course_name = match.group(1)
            return course_name

    # Fallback: use the first filename without extensions
    return files[0].stem if files else "Unknown"

def process_course_group(course_prefix, file_paths, output_dir="./", skip_existing=True):
    """Process all files for a single course and generate text insights."""
    print(f"\n=== Processing {course_prefix} ===")
    print(f"Files: {[f.name for f in file_paths]}")
    print(f"Output directory: {Path(output_dir).absolute()}")

    # Generate output filenames first to check if they exist
    course_name = extract_course_name(file_paths)
    output_base = f"{course_prefix}_{course_name}_textInsights_recent"

    output_file = Path(output_dir) / f"{output_base}.json"
    provenance_file = Path(output_dir) / f"{output_base}_provenance.json"

    print(f"Target output file: {output_file.absolute()}")

    # Check if files already exist
    if skip_existing and output_file.exists() and provenance_file.exists():
        print(f"  ⚠️  Output files already exist, skipping {course_prefix}")
        print(f"    Existing: {output_file.name}")
        print(f"    Use --overwrite to reprocess existing files")
        return
    elif output_file.exists() or provenance_file.exists():
        print(f"  🔄 Overwriting existing files for {course_prefix}")

    # Load all reviews from all files for this course
    all_reviews = []
    total_loaded = 0
    filtered_out = 0

    for file_path in file_paths:
        try:
            reviews = load_reviews(file_path)
            total_loaded += len(reviews)

            # Filter reviews by date (last 3 years only)
            recent_reviews = []
            for review in reviews:
                if is_recent_review(review):
                    # Add source information to each review
                    source = "unknown"
                    if "golfnow" in file_path.name:
                        source = "golfnow"
                    elif "golfpass" in file_path.name:
                        source = "golfpass"
                    elif "google" in file_path.name:
                        source = "google"

                    review["source_file"] = file_path.name
                    review["source_platform"] = source
                    recent_reviews.append(review)
                else:
                    filtered_out += 1

            all_reviews.extend(recent_reviews)
            print(f"  Loaded {len(recent_reviews)}/{len(reviews)} recent reviews from {file_path.name}")
        except Exception as e:
            print(f"  Error loading {file_path.name}: {e}")

    if not all_reviews:
        print(f"  No recent reviews found for {course_prefix}")
        return

    print(f"  Total recent reviews: {len(all_reviews)} (filtered out {filtered_out} old reviews)")
    print(f"  Using model: {MODEL_NAME}")

    # Process reviews with text insights
    cat_provenance = {cat: {"count": 0, "values": [], "review_texts": []} for cat in CATEGORIES}

    for idx, review in enumerate(all_reviews):
        text = (
            review.get("comment")
            or review.get("text_review")
            or review.get("raw_text")
            or review.get("text")
            or review.get("review")
        )
        if not text or not isinstance(text, str) or not text.strip():
            review["text_insights"] = {"sentiment": {cat: None for cat in CATEGORIES}, "extra_themes": []}
            continue

        print(f"  Processing review {idx+1}/{len(all_reviews)}...")
        insights = extract_text_insights(text, CATEGORIES)
        review["text_insights"] = insights

        for cat, val in insights.get("sentiment", {}).items():
            if val is not None:
                cat_provenance[cat]["count"] += 1
                cat_provenance[cat]["values"].append(val)
                cat_provenance[cat]["review_texts"].append(text.strip())

        time.sleep(0.3)  # Reduced API rate limit for cheaper model

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Write output files
    try:
        with open(output_file, "w") as f:
            json.dump(all_reviews, f, indent=2)
        print(f"  ✓ Wrote {len(all_reviews)} recent reviews to {output_file.absolute()}")

        with open(provenance_file, "w") as f:
            json.dump(cat_provenance, f, indent=2)
        print(f"  ✓ Wrote provenance to {provenance_file.absolute()}")
    except Exception as e:
        print(f"  ✗ Error writing files: {e}")
        print(f"    Attempted to write to: {output_file.absolute()}")

def main():
    """Main function to process all course groups."""
    print('Golf Course Text Insights Processor (Recent Reviews Only)')
    print('=========================================================')
    print(f'Using model: {MODEL_NAME}')
    print(f'Processing reviews from: {CUTOFF_DATE.strftime("%Y-%m-%d")} onwards')

    # Auto-detect reviews directory
    base_dir = find_reviews_directory()

    # Auto-detect or create insights directory for output
    output_dir = find_insights_directory()

    # Find all course file groups
    print('Scanning for course files...')
    course_groups = find_course_files(base_dir)

    if not course_groups:
        print("No course files found in golfnow, golfpass, or google directories.")
        return

    print(f"Found {len(course_groups)} course groups:")
    for course_prefix, files in course_groups.items():
        print(f"  {course_prefix}: {len(files)} files")

    # Process each course group
    total_processed = 0
    total_skipped = 0

    for course_prefix, file_paths in course_groups.items():
        try:
            # Check if files exist first
            course_name = extract_course_name(file_paths)
            output_base = f"{course_prefix}_{course_name}_textInsights_recent"
            output_file = Path(output_dir) / f"{output_base}.json"

            if output_file.exists():
                total_skipped += 1
            else:
                total_processed += 1

            process_course_group(course_prefix, file_paths, output_dir, skip_existing=True)
        except Exception as e:
            print(f"Error processing {course_prefix}: {e}")

    print('\n=== Processing Summary ===')
    print(f'Successfully processed: {total_processed} courses')
    print(f'Skipped (already exist): {total_skipped} courses')
    print(f'Model used: {MODEL_NAME}')
    print(f'Date filter: Reviews from {CUTOFF_DATE.strftime("%Y-%m-%d")} onwards only')
    print(f'Output directory: {output_dir}')
    print('Processing complete!')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Process golf course reviews from golfnow, golfpass, and google folders, "
                   "group by course prefix, and generate text insights. Only processes recent reviews. "
                   "Outputs to ../states/ma/reviews/insights/ by default."
    )
    parser.add_argument(
        "--base_dir",
        default="./",
        help="Base directory containing golfnow, golfpass, and google folders (default: current directory)"
    )
    parser.add_argument(
        "--output_dir",
        default=None,
        help="Output directory for text insight files (default: auto-detect insights directory)"
    )
    parser.add_argument(
        "--years_back",
        type=int,
        default=3,
        help="Number of years back to include reviews (default: 3)"
    )
    parser.add_argument(
        "--model",
        default="gpt-3.5-turbo",
        choices=["gpt-3.5-turbo", "gpt-4o-mini", "gpt-4o"],
        help="OpenAI model to use (default: gpt-3.5-turbo for lowest cost)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing text insight files (default: skip existing files)"
    )

    args = parser.parse_args()

    # Update global variables based on arguments
    update_globals(args.model, args.years_back)

    # Main execution
    if args.base_dir == "./" and args.output_dir is None:
        # Use the simplified main function with auto-detection
        # But we need to handle overwrite flag, so call the custom version
        print('Golf Course Text Insights Processor (Recent Reviews Only)')
        print('=========================================================')
        print(f'Using model: {MODEL_NAME}')
        print(f'Processing reviews from: {CUTOFF_DATE.strftime("%Y-%m-%d")} onwards')

        base_dir = find_reviews_directory()
        output_dir = find_insights_directory() if args.output_dir is None else args.output_dir

        print('Scanning for course files...')
        course_groups = find_course_files(base_dir)

        if not course_groups:
            print("No course files found in golfnow, golfpass, or google directories.")
        else:
            print(f"Found {len(course_groups)} course groups:")
            for course_prefix, files in course_groups.items():
                print(f"  {course_prefix}: {len(files)} files")

            total_processed = 0
            total_skipped = 0

            for course_prefix, file_paths in course_groups.items():
                try:
                    # Check if files exist first (unless overwriting)
                    course_name = extract_course_name(file_paths)
                    output_base = f"{course_prefix}_{course_name}_textInsights_recent"
                    output_file = Path(output_dir) / f"{output_base}.json"

                    if output_file.exists() and not args.overwrite:
                        total_skipped += 1
                    else:
                        total_processed += 1

                    process_course_group(course_prefix, file_paths, output_dir, skip_existing=not args.overwrite)
                except Exception as e:
                    print(f"Error processing {course_prefix}: {e}")

            print('\n=== Processing Summary ===')
            print(f'Successfully processed: {total_processed} courses')
            print(f'Skipped (already exist): {total_skipped} courses')
            print(f'Model used: {MODEL_NAME}')
            print(f'Date filter: Reviews from {CUTOFF_DATE.strftime("%Y-%m-%d")} onwards only')
            print(f'Output directory: {output_dir}')
            print('Processing complete!')
    else:
        # Use custom directories
        print('Golf Course Text Insights Processor (Recent Reviews Only)')
        print('=========================================================')
        print(f'Using model: {MODEL_NAME}')
        print(f'Processing reviews from: {CUTOFF_DATE.strftime("%Y-%m-%d")} onwards')

        base_dir = args.base_dir
        if base_dir == "./":
            base_dir = find_reviews_directory()

        output_dir = args.output_dir
        if output_dir is None:
            output_dir = find_insights_directory()

        print(f'Scanning for course files in {base_dir}...')
        print(f'Output directory: {output_dir}')
        course_groups = find_course_files(base_dir)

        if not course_groups:
            print("No course files found in golfnow, golfpass, or google directories.")
        else:
            print(f"Found {len(course_groups)} course groups:")
            for course_prefix, files in course_groups.items():
                print(f"  {course_prefix}: {len(files)} files")

            for course_prefix, file_paths in course_groups.items():
                try:
                    process_course_group(course_prefix, file_paths, output_dir, skip_existing=not args.overwrite)
                except Exception as e:
                    print(f"Error processing {course_prefix}: {e}")

        print('\nProcessing complete!')
