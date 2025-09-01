
import pandas as pd
import requests
import re
from difflib import SequenceMatcher

# Configuration - UPDATE THESE VALUES
EXCEL_FILE = "USGolfDataMassGolfGuide03232025.xlsx"
SUPABASE_URL = "https://pmpymmdayzqsxrbymxvh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y"

def normalize_name(name):
    """Normalize course name for better matching"""
    if not name:
        return ""

    # Convert to lowercase and strip
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

def load_excel_courses():
    """Load courses from Excel file"""
    print("📊 Loading Excel courses...")

    df = pd.read_excel(EXCEL_FILE, sheet_name=0)

    # Get unique courses (remove duplicate tee entries)
    course_cols = ['cCourseNumber', 'CoursesMasterT::CourseName', 'CoursesMasterT::City']
    unique_courses = df[course_cols].drop_duplicates()

    courses = {}

    for _, row in unique_courses.iterrows():
        course_number = str(row['cCourseNumber']).strip()
        course_name = str(row['CoursesMasterT::CourseName']).strip()
        city = str(row['CoursesMasterT::City']) if not pd.isna(row['CoursesMasterT::City']) else ''

        if pd.isna(course_number) or pd.isna(course_name):
            continue

        normalized = normalize_name(course_name)

        courses[normalized] = {
            'number': course_number,
            'name': course_name,
            'city': city,
            'source': 'excel'
        }

    print(f"   • Loaded {len(courses):,} unique Excel courses")
    return courses

def load_db_courses():
    """Load courses from Supabase database"""
    print("🗄️  Loading database courses...")

    # Ensure URL has correct endpoint
    url = SUPABASE_URL.rstrip('/') + '/rest/v1'

    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }

    # Query database
    response = requests.get(
        f"{url}/initial_course_upload",
        headers=headers,
        params={'select': 'course_number,course_name,city'}
    )

    if response.status_code != 200:
        raise Exception(f"Database error: {response.status_code} - {response.text}")

    data = response.json()
    courses = {}

    for row in data:
        course_number = row.get('course_number', '').strip()
        course_name = row.get('course_name', '').strip()
        city = row.get('city', '') or ''

        if not course_number or not course_name:
            continue

        normalized = normalize_name(course_name)

        courses[normalized] = {
            'number': course_number,
            'name': course_name,
            'city': city,
            'source': 'database'
        }

    print(f"   • Loaded {len(courses):,} unique database courses")
    return courses

def find_number_mismatches(excel_courses, db_courses):
    """Find courses with same name but different numbers"""
    print("\n🔍 Finding courses with same names but different numbers...")

    mismatches = []
    exact_matches = 0

    for norm_name in excel_courses:
        if norm_name in db_courses:
            excel_course = excel_courses[norm_name]
            db_course = db_courses[norm_name]

            if excel_course['number'] != db_course['number']:
                mismatches.append({
                    'name': excel_course['name'],
                    'excel_number': excel_course['number'],
                    'db_number': db_course['number'],
                    'excel_city': excel_course['city'],
                    'db_city': db_course['city']
                })
            else:
                exact_matches += 1

    print(f"   • Exact matches (name + number): {exact_matches:,}")
    print(f"   • Number mismatches found: {len(mismatches):,}")

    return mismatches, exact_matches

def find_fuzzy_matches(excel_courses, db_courses, threshold=0.8):
    """Find potential fuzzy matches between unmatched courses"""
    print(f"\n🔍 Finding fuzzy matches (similarity >= {threshold})...")

    # Get unmatched courses
    excel_unmatched = {name: info for name, info in excel_courses.items() if name not in db_courses}
    db_unmatched = {name: info for name, info in db_courses.items() if name not in excel_courses}

    fuzzy_matches = []

    for excel_name, excel_info in excel_unmatched.items():
        for db_name, db_info in db_unmatched.items():
            # Calculate similarity between normalized names
            norm_similarity = SequenceMatcher(None, excel_name, db_name).ratio()

            # Calculate similarity between original names
            orig_similarity = SequenceMatcher(None,
                excel_info['name'].lower(),
                db_info['name'].lower()
            ).ratio()

            # Use the higher similarity
            similarity = max(norm_similarity, orig_similarity)

            if similarity >= threshold:
                fuzzy_matches.append({
                    'excel': excel_info,
                    'database': db_info,
                    'similarity': similarity
                })

    # Sort by similarity (highest first)
    fuzzy_matches.sort(key=lambda x: x['similarity'], reverse=True)

    print(f"   • Found {len(fuzzy_matches):,} potential fuzzy matches")
    return fuzzy_matches

def print_results(mismatches, exact_matches, fuzzy_matches, excel_total, db_total):
    """Print analysis results"""
    print("\n" + "="*80)
    print("📋 COURSE NAME MATCHING RESULTS")
    print("="*80)

    print(f"\n📊 SUMMARY:")
    print(f"   • Excel courses:           {excel_total:,}")
    print(f"   • Database courses:        {db_total:,}")
    print(f"   • Exact matches:           {exact_matches:,}")
    print(f"   • Number mismatches:       {len(mismatches):,}")
    print(f"   • Potential fuzzy matches: {len(fuzzy_matches):,}")

    # Show number mismatches
    if mismatches:
        print(f"\n❌ COURSES WITH SAME NAME BUT DIFFERENT NUMBERS ({len(mismatches):,}):")
        print("-" * 80)

        for i, mismatch in enumerate(mismatches[:20], 1):
            print(f"{i:2d}. {mismatch['name']}")
            print(f"    Excel:    {mismatch['excel_number']} | {mismatch['excel_city']}")
            print(f"    Database: {mismatch['db_number']} | {mismatch['db_city']}")
            print()

        if len(mismatches) > 20:
            print(f"    ... and {len(mismatches) - 20:,} more")

    # Show fuzzy matches
    if fuzzy_matches:
        print(f"\n🔍 POTENTIAL FUZZY MATCHES ({len(fuzzy_matches):,}):")
        print("-" * 80)

        for i, match in enumerate(fuzzy_matches[:15], 1):
            excel = match['excel']
            db = match['database']
            print(f"{i:2d}. Similarity: {match['similarity']:.3f}")
            print(f"    Excel:    '{excel['name']}' ({excel['number']}) - {excel['city']}")
            print(f"    Database: '{db['name']}' ({db['number']}) - {db['city']}")
            print()

        if len(fuzzy_matches) > 15:
            print(f"    ... and {len(fuzzy_matches) - 15:,} more")

    if not mismatches and not fuzzy_matches:
        print("\n✅ No significant discrepancies found!")

def save_results(mismatches, fuzzy_matches, filename="course_name_discrepancies.txt"):
    """Save results to file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("COURSE NAME MATCHING DISCREPANCIES\n")
            f.write("="*50 + "\n\n")
            f.write(f"Generated: {pd.Timestamp.now()}\n\n")

            # Number mismatches
            if mismatches:
                f.write(f"COURSES WITH SAME NAME BUT DIFFERENT NUMBERS ({len(mismatches):,})\n")
                f.write("-" * 50 + "\n")
                for i, mismatch in enumerate(mismatches, 1):
                    f.write(f"{i:3d}. {mismatch['name']}\n")
                    f.write(f"     Excel:    {mismatch['excel_number']} | {mismatch['excel_city']}\n")
                    f.write(f"     Database: {mismatch['db_number']} | {mismatch['db_city']}\n\n")

            # Fuzzy matches
            if fuzzy_matches:
                f.write(f"POTENTIAL FUZZY MATCHES ({len(fuzzy_matches):,})\n")
                f.write("-" * 30 + "\n")
                for i, match in enumerate(fuzzy_matches, 1):
                    excel = match['excel']
                    db = match['database']
                    f.write(f"{i:3d}. Similarity: {match['similarity']:.3f}\n")
                    f.write(f"     Excel:    '{excel['name']}' ({excel['number']}) - {excel['city']}\n")
                    f.write(f"     Database: '{db['name']}' ({db['number']}) - {db['city']}\n\n")

        print(f"\n💾 Results saved to: {filename}")

    except Exception as e:
        print(f"❌ Error saving results: {str(e)}")

def main():
    """Main function"""
    print("🚀 Starting Course Name Analysis...")
    print("-" * 50)

    try:
        # Load courses from both sources
        excel_courses = load_excel_courses()
        db_courses = load_db_courses()

        # Find mismatches and fuzzy matches
        mismatches, exact_matches = find_number_mismatches(excel_courses, db_courses)
        fuzzy_matches = find_fuzzy_matches(excel_courses, db_courses)

        # Print results
        print_results(mismatches, exact_matches, fuzzy_matches,
                     len(excel_courses), len(db_courses))

        # Save results
        if mismatches or fuzzy_matches:
            save_results(mismatches, fuzzy_matches)

        return {
            'mismatches': mismatches,
            'exact_matches': exact_matches,
            'fuzzy_matches': fuzzy_matches
        }

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None

if __name__ == "__main__":
    results = main()

# Manual step-by-step execution
"""
# Run step by step for debugging:

excel_courses = load_excel_courses()
db_courses = load_db_courses()
mismatches, exact_matches = find_number_mismatches(excel_courses, db_courses)
fuzzy_matches = find_fuzzy_matches(excel_courses, db_courses)

print(f"Number mismatches: {len(mismatches)}")
print(f"Fuzzy matches: {len(fuzzy_matches)}")
"""
