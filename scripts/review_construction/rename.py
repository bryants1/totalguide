#!/usr/bin/env python3
"""
Golf Course File Renamer
Renames golf course files with course numbers and appropriate suffixes.
"""

import os
import csv
import re
from pathlib import Path


def load_course_mapping(csv_file='course_numbers_and_names.csv'):
    """Load course mapping from CSV file."""
    mapping = {}

    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['course_name'] and row['course_number']:
                    # Create simplified name for matching (remove spaces, special chars, make lowercase)
                    simplified_name = re.sub(r'[^a-z0-9]', '', row['course_name'].lower())
                    simplified_name = re.sub(r'country|club|golf|course|municipal|gc|links', '', simplified_name)

                    mapping[simplified_name] = {
                        'course_number': row['course_number'],
                        'full_name': row['course_name']
                    }
    except FileNotFoundError:
        print(f"Error: {csv_file} not found!")
        return {}
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return {}

    return mapping


def find_course_match(filename, mapping):
    """Find the best course match for a filename."""
    # Remove file extension and common suffixes
    base_name = re.sub(r'\.(json|csv|txt)$', '', filename, flags=re.IGNORECASE)
    base_name = re.sub(r'_reviews?$', '', base_name, flags=re.IGNORECASE)
    base_name = base_name.lower()

    # Try exact match first
    exact_key = re.sub(r'[^a-z0-9]', '', base_name)
    if exact_key in mapping:
        return mapping[exact_key]

    # Try partial matches
    for key, value in mapping.items():
        if key in exact_key or exact_key in key:
            return value

    # Manual mappings for specific cases
    manual_mappings = {
        'chicopee': 'MA-93',
        'cherryhill': 'MA-90',
        'chemawa': 'MA-89',
        'chelmsford': 'MA-87',
        'chathamseasidelinks': 'MA-86',
        'charteroak': 'MA-85',
        'cyprian': 'MA-82-1',  # Default to Championship course
        'cypriankeyes': 'MA-82-1',
        'agawam': 'MA-8',
        'cedarhill': 'MA-77'
    }

    if exact_key in manual_mappings:
        return {'course_number': manual_mappings[exact_key]}

    return None


def rename_files_in_directory(dir_path, suffix, mapping):
    """Rename files in a specific directory."""
    dir_path = Path(dir_path)

    if not dir_path.exists():
        print(f"Directory {dir_path} does not exist, skipping...")
        return

    if not dir_path.is_dir():
        print(f"{dir_path} is not a directory, skipping...")
        return

    files = list(dir_path.iterdir())
    print(f"\nProcessing {dir_path} directory:")

    for file_path in files:
        # Skip directories
        if file_path.is_dir():
            continue

        filename = file_path.name

        # Skip if already renamed (starts with course number pattern)
        if re.match(r'^MA-\d+', filename):
            print(f"  Skipping {filename} (already renamed)")
            continue

        match = find_course_match(filename, mapping)

        if match:
            file_ext = file_path.suffix
            base_name = file_path.stem
            new_filename = f"{match['course_number']}_{base_name}{suffix}{file_ext}"
            new_file_path = file_path.parent / new_filename

            try:
                file_path.rename(new_file_path)
                print(f"  ✓ {filename} → {new_filename}")
            except Exception as error:
                print(f"  ✗ Error renaming {filename}: {error}")
        else:
            print(f"  ? No match found for: {filename}")


def main():
    """Main function to run the file renamer."""
    print('Golf Course File Renamer')
    print('========================')

    try:
        # Load course mapping from CSV
        print('Loading course mapping from CSV...')
        mapping = load_course_mapping()

        if not mapping:
            print("No course mappings loaded. Please check your CSV file.")
            return

        print(f"Loaded {len(mapping)} course mappings")

        # Process golfpass directory
        rename_files_in_directory('./golfpass', '_golfpass', mapping)

        # Process golfnow directory
        rename_files_in_directory('./golfnow', '_golfnow', mapping)

        print('\nRenaming complete!')

    except Exception as error:
        print(f'Error: {error}')


if __name__ == '__main__':
    main()
