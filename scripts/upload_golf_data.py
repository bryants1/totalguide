import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import sys
import os
from typing import Optional, Dict, Any, List
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase configuration - using the anon key
SUPABASE_URL = 'https://pmpymmdayzqsxrbymxvh.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBtcHltbWRheXpxc3hyYnlteHZoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTM2MjkxNDAsImV4cCI6MjA2OTIwNTE0MH0.WgvFI9kfeqZPzdxF5JcQLt8xq-JtoX8E_pzblVxNv0Y'

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_numeric(value):
    """Clean numeric values, handling NaN and None."""
    if pd.isna(value) or value is None:
        return None
    return float(value)

def clean_integer(value):
    """Clean integer values, handling NaN and None."""
    if pd.isna(value) or value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        # If conversion fails, return None
        return None

def clean_year(value):
    """Clean year values, handling multiple years like '1892/1913/1955'."""
    if pd.isna(value) or value is None:
        return None

    # Convert to string first
    value_str = str(value)

    # If it contains a slash, take the first year
    if '/' in value_str:
        parts = value_str.split('/')
        # Try to get the first valid year
        for part in parts:
            try:
                year = int(part.strip())
                return year
            except (ValueError, TypeError):
                continue
        return None

    # If it contains a dash (range like 1892-1913), take the first year
    if '-' in value_str and len(value_str) > 4:  # Avoid negative numbers
        parts = value_str.split('-')
        try:
            return int(parts[0].strip())
        except (ValueError, TypeError):
            return None

    # Otherwise try to convert directly
    try:
        # Handle float years (like 1892.0)
        if '.' in value_str:
            return int(float(value_str))
        return int(value_str)
    except (ValueError, TypeError):
        return None

def clean_text(value):
    """Clean text values, handling NaN and None."""
    if pd.isna(value) or value is None:
        return None
    return str(value).strip()

def upload_batch(table_name: str, data: List[Dict], batch_size: int = 100):
    """Upload data in batches to handle large datasets."""
    total_records = len(data)
    uploaded = 0

    for i in range(0, total_records, batch_size):
        batch = data[i:i + batch_size]
        try:
            # Use upsert to handle conflicts
            response = supabase.table(table_name).upsert(batch).execute()
            uploaded += len(batch)
            logger.info(f"Uploaded batch {i//batch_size + 1}: {uploaded}/{total_records} records")
        except Exception as e:
            logger.error(f"Error uploading batch to {table_name}: {e}")
            # Continue with next batch instead of failing completely
            continue

    return uploaded

def upload_initial_course_data(filepath: str):
    """Upload USGolfData.xlsx to initial_course_upload table."""
    logger.info(f"Reading {filepath}...")
    df = pd.read_excel(filepath, sheet_name='Sheet1')

    # Rename columns to match database schema
    column_mapping = {
        'cCourseNumber': 'course_number',
        'CoursesMasterT::CourseName': 'course_name',
        'CourseName': 'course_name_alt',  # Alternative if first doesn't exist
        'ClubNumber': 'club_number',  # NEW FIELD
        'CourseNumber': 'course_number_only',  # NEW FIELD
        'CoursesMasterT::StreetAddress': 'street_address',
        'CoursesMasterT::City': 'city',
        'CoursesMasterT::County': 'county',
        'CoursesMasterT::StateorRegion': 'state_or_region',
        'CoursesMasterT::Zip': 'zip_code',
        'CoursesMasterT::PhoneNumber': 'phone_number',
        'CoursesMasterT::URL    4598': 'website_url',
        'CoursesMasterT::TotalHoles': 'total_holes_str',  # Note: this is a string in the data
        'CoursesMasterT::Architect': 'architect',
        'CoursesMasterT::YearBuiltFounded': 'year_built_founded',
        'CoursesMasterT::StatusPublicPrivateResort': 'status_type',
        'CoursesMasterT::GuestPolicy': 'guest_policy',  # NEW FIELD
        'CoursesMasterT::EmailAddress': 'email_address',
        'Par': 'total_par',
        'Holes': 'total_holes',
        'Rating': 'course_rating',
        'Slope': 'slope_rating',
        'Length': 'total_length'
    }

    df = df.rename(columns=column_mapping)

    # Clean data
    for col in df.columns:
        if col in ['total_par', 'total_holes', 'club_number', 'course_number_only']:
            df[col] = df[col].apply(clean_integer)
        elif col == 'year_built_founded':
            df[col] = df[col].apply(clean_year)  # Use special handler for years
        elif col in ['course_rating', 'slope_rating']:
            df[col] = df[col].apply(clean_numeric)
        elif col == 'total_length':
            df[col] = df[col].apply(clean_integer)
        elif col not in ['created_at']:  # Don't clean timestamp columns
            df[col] = df[col].apply(clean_text)

    # Handle the total_holes_str field (it's stored as string like "18" in the Excel)
    if 'total_holes_str' in df.columns and 'total_holes' not in df.columns:
        df['total_holes'] = df['total_holes_str'].apply(lambda x: clean_integer(x) if pd.notna(x) else None)

    # Add created_at timestamp
    df['created_at'] = datetime.now().isoformat()

    # Prepare data for insertion - including new fields
    columns = ['course_number', 'course_name', 'club_number', 'course_number_only',
               'street_address', 'city', 'county', 'state_or_region', 'zip_code',
               'phone_number', 'website_url', 'architect', 'year_built_founded',
               'status_type', 'guest_policy', 'email_address', 'total_par',
               'total_holes', 'course_rating', 'slope_rating', 'total_length', 'created_at']

    # Filter to only columns that exist in the dataframe
    available_columns = [col for col in columns if col in df.columns]

    # Convert DataFrame to list of dictionaries
    data = df[available_columns].to_dict('records')

    # Convert any NaN values to None for JSON serialization
    for record in data:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    # Upload data
    logger.info(f"Uploading {len(data)} records to initial_course_upload...")
    uploaded = upload_batch('initial_course_upload', data)
    logger.info(f"Successfully uploaded {uploaded} records to initial_course_upload")

def upload_google_places_data(filepath: str):
    """Upload google_places_data.xlsx to google_places_data table."""
    logger.info(f"Reading {filepath}...")
    df = pd.read_excel(filepath, sheet_name='Sheet1')

    # Rename columns to match database schema
    column_mapping = {
        'cCourseNumber': 'course_number',
        'PlaceID': 'place_id',
        'DisplayName': 'display_name',
        'FormattedAddress': 'formatted_address',
        'StreetNumber': 'street_number',
        'Route': 'route',
        'StreetAddress': 'street_address',
        'City': 'city',
        'State': 'state',
        'County': 'county',
        'Country': 'country',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'PrimaryType': 'primary_type',
        'Website': 'website',
        'Phone': 'phone',
        'OpeningHours': 'opening_hours',
        'UserRatingCount': 'user_rating_count',
        'PhotoRef': 'photo_reference',
        'GoogleMapsLink': 'google_maps_link',
        'ZipCode': 'zip_code'
    }

    df = df.rename(columns=column_mapping)

    # Clean data
    for col in df.columns:
        if col in ['latitude', 'longitude']:
            df[col] = df[col].apply(clean_numeric)
        elif col == 'user_rating_count':
            df[col] = df[col].apply(clean_integer)
        elif col not in ['created_at', 'updated_at']:  # Don't clean timestamp columns
            df[col] = df[col].apply(clean_text)

    # Add timestamps
    df['created_at'] = datetime.now().isoformat()
    df['updated_at'] = datetime.now().isoformat()

    # Prepare data for insertion
    columns = ['course_number', 'place_id', 'display_name', 'formatted_address',
               'street_number', 'route', 'street_address', 'city', 'state', 'county',
               'country', 'latitude', 'longitude', 'primary_type', 'website', 'phone',
               'opening_hours', 'user_rating_count', 'photo_reference', 'google_maps_link',
               'zip_code', 'created_at', 'updated_at']

    # Filter to only columns that exist in the dataframe
    available_columns = [col for col in columns if col in df.columns]

    # Convert DataFrame to list of dictionaries
    data = df[available_columns].to_dict('records')

    # Convert any NaN values to None for JSON serialization
    for record in data:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    # Upload data
    logger.info(f"Uploading {len(data)} records to google_places_data...")
    uploaded = upload_batch('google_places_data', data)
    logger.info(f"Successfully uploaded {uploaded} records to google_places_data")

def upload_review_urls(filepath: str):
    """Upload review_urls.xlsx to review_urls table."""
    logger.info(f"Reading {filepath}...")
    df = pd.read_excel(filepath, sheet_name='Sheet1')

    # Rename columns to match database schema
    column_mapping = {
        'cCourseNumber': 'course_number',
        'GolfNow URL': 'golf_now_url',
        'GolfPassURL': 'golf_pass_url'
    }

    df = df.rename(columns=column_mapping)

    # Clean data
    for col in df.columns:
        if col not in ['created_at', 'updated_at']:
            df[col] = df[col].apply(clean_text)

    # Add timestamps
    df['created_at'] = datetime.now().isoformat()
    df['updated_at'] = datetime.now().isoformat()

    # Prepare data for insertion
    columns = ['course_number', 'golf_now_url', 'golf_pass_url', 'created_at', 'updated_at']

    # Filter to only columns that exist in the dataframe
    available_columns = [col for col in columns if col in df.columns]

    # Convert DataFrame to list of dictionaries
    data = df[available_columns].to_dict('records')

    # Convert any NaN values to None for JSON serialization
    for record in data:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    # Upload data
    logger.info(f"Uploading {len(data)} records to review_urls...")
    uploaded = upload_batch('review_urls', data)
    logger.info(f"Successfully uploaded {uploaded} records to review_urls")

def verify_uploads():
    """Verify that data was uploaded successfully."""
    tables = [
        'initial_course_upload',
        'google_places_data',
        'review_urls'
    ]

    logger.info("\n=== Upload Verification ===")
    for table in tables:
        try:
            # Get count of records
            response = supabase.table(table).select('course_number', count='exact').execute()
            count = response.count if hasattr(response, 'count') else len(response.data)
            logger.info(f"{table}: {count} records uploaded")
        except Exception as e:
            logger.warning(f"Could not verify {table}: {e}")

def main():
    """Main function to orchestrate the upload process."""
    # File paths - update these to match your actual file locations
    files = {
        'initial_course': 'USGolfData.xlsx',
        'google_places': 'google_places_data.xlsx',
        'review_urls': 'review_urls.xlsx'
    }

    # Check if files exist
    for name, filepath in files.items():
        if not os.path.exists(filepath):
            logger.error(f"File not found: {filepath}")
            sys.exit(1)

    try:
        logger.info("Connecting to Supabase...")

        # Test connection by trying to fetch from a table
        test = supabase.table('initial_course_upload').select('course_number').limit(1).execute()
        logger.info("Connected successfully to Supabase!")

        # Upload data in order (initial_course_upload first due to foreign key constraints)
        logger.info("\n=== Starting data upload ===")

        # 1. Upload initial course data first (parent table)
        upload_initial_course_data(files['initial_course'])

        # 2. Upload Google Places data
        upload_google_places_data(files['google_places'])

        # 3. Upload review URLs
        upload_review_urls(files['review_urls'])

        # Verify uploads
        verify_uploads()

        logger.info("\n=== All uploads completed successfully! ===")

    except Exception as e:
        logger.error(f"Error: {e}")
        logger.info("\nTroubleshooting tips:")
        logger.info("1. Check if your Supabase URL and anon key are correct")
        logger.info("2. Verify that RLS (Row Level Security) policies allow inserts")
        logger.info("3. Make sure the tables exist in your database")
        logger.info("4. Check that the anon key has sufficient permissions")
        sys.exit(1)

if __name__ == "__main__":
    main()

# Installation requirements:
"""
pip install supabase pandas openpyxl

# If you need to disable RLS for initial upload (run in Supabase SQL editor):
ALTER TABLE initial_course_upload DISABLE ROW LEVEL SECURITY;
ALTER TABLE google_places_data DISABLE ROW LEVEL SECURITY;
ALTER TABLE review_urls DISABLE ROW LEVEL SECURITY;

# Or create a policy to allow inserts (more secure):
CREATE POLICY "Allow anonymous inserts" ON initial_course_upload
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Allow anonymous inserts" ON google_places_data
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Allow anonymous inserts" ON review_urls
    FOR INSERT WITH CHECK (true);
"""
