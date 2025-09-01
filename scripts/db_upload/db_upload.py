#!/usr/bin/env python3

"""
Golf Course Update Script (Python)
Updates golf course data via API using JSON input file with proper text formatting

Usage: python scripts/update_course_from_json.py <json_file_path> <course_number> [options]

Options:
  --dry-run    Show what would be updated without making changes
  --host       API host (default: https://mass-golf-resource-bryants3.replit.app)
  --help       Show this help message

Example:
  python scripts/update_course_from_json.py data/butternut_farm.json MA-66 --dry-run
"""

import argparse
import json
import sys
import requests
import base64
import re
from pathlib import Path

# Configuration
DEFAULT_HOST = 'https://mass-golf-resource-bryants3.replit.app'
ADMIN_USERNAME = 'admin'
ADMIN_PASSWORD = 'golfadmin2025'

def format_pricing_table(pricing_text):
    """Convert pipe-separated pricing data into formatted HTML table"""
    if not pricing_text or '|' not in pricing_text:
        return pricing_text

    # Split by lines and process each line
    lines = pricing_text.strip().split('|')

    # Clean up the lines
    cleaned_lines = [line.strip() for line in lines if line.strip()]

    # Try to identify table structure
    formatted_text = []
    current_section = None

    for line in cleaned_lines:
        line = line.strip()
        if not line:
            continue

        # Check if this looks like a section header
        if any(keyword in line.lower() for keyword in ['rates', 'fees', 'weekdays', 'weekends', 'holidays', 'carts']):
            if formatted_text and not formatted_text[-1].endswith(':'):
                formatted_text.append('')  # Add spacing
            formatted_text.append(f"<strong>{line}</strong>")
        # Check if this looks like a price line (contains $ or price pattern)
        elif '$' in line or re.search(r'\d+\.\d{2}', line):
            # Format as a pricing line
            formatted_text.append(f"• {line}")
        # Regular text
        else:
            if line not in formatted_text:  # Avoid duplicates
                formatted_text.append(line)

    return '<br>'.join(formatted_text)

def format_description_array(description_array):
    """Format description arrays into proper paragraphs"""
    if isinstance(description_array, list):
        # Join array items as separate paragraphs
        return '</p><p>'.join([item.strip() for item in description_array if item.strip()])
    return description_array

def format_amenities_description(amenities_desc_list):
    """Format amenities descriptions with proper formatting"""
    if not amenities_desc_list:
        return ""

    # Join with line breaks for better readability
    formatted_desc = '<br>'.join([desc.strip() for desc in amenities_desc_list if desc.strip()])
    return formatted_desc

def format_policies_text(policies_text):
    """Format policies text for better web display"""
    if not policies_text:
        return policies_text

    # Split on common separators and format as bullet points
    sentences = re.split(r'[.!]\s+', policies_text)
    formatted_policies = []

    for sentence in sentences:
        sentence = sentence.strip()
        if sentence and not sentence.endswith('.'):
            sentence += '.'
        if sentence:
            formatted_policies.append(f"• {sentence}")

    return '<br>'.join(formatted_policies)

def format_contact_info(contact_text):
    """Format contact information for better display"""
    if not contact_text:
        return contact_text

    # Make phone numbers and extensions more readable
    formatted_text = re.sub(r'\((\d{3})\)\s*(\d{3})-(\d{4})', r'(\1) \2-\3', contact_text)
    formatted_text = re.sub(r'ext\.?\s*(\d+)', r'ext. \1', formatted_text)

    return formatted_text

def clean_and_format_text(text):
    """General text cleaning and formatting"""
    if not text:
        return text

    # Remove excessive whitespace
    text = ' '.join(text.split())

    # Ensure proper sentence spacing
    text = re.sub(r'\.(\w)', r'. \1', text)

    return text

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Update golf course data via API using JSON input',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/update_course_from_json.py data/butternut_farm.json MA-66
  python scripts/update_course_from_json.py data/butternut_farm.json MA-66 --dry-run
  python scripts/update_course_from_json.py data/butternut_farm.json MA-66 --host https://mass-golf-resource-bryants3.replit.app
        """
    )

    parser.add_argument('json_file_path', help='Path to JSON file containing course data')
    parser.add_argument('course_number', help='Course number (e.g., MA-66) of the course to update')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')
    parser.add_argument('--host', default=DEFAULT_HOST, help=f'API host (default: {DEFAULT_HOST})')
    parser.add_argument('--auto-confirm', action='store_true', help='Skip confirmation prompt')

    return parser.parse_args()

def get_nested_value(data, key, default=None):
    """Safely get value from nested structure, handling both dict and direct values"""
    if key not in data:
        return default

    value = data[key]

    # If it's a dictionary with 'value' key, return the nested value
    if isinstance(value, dict) and 'value' in value:
        return value['value']

    # Otherwise return the value directly
    return value

def map_json_to_database(json_data):
    """Map JSON structure to database fields with proper text formatting"""
    course_data = {}

    # Basic course information
    if 'general_info' in json_data:
        info = json_data['general_info']

        name_value = get_nested_value(info, 'name')
        if name_value:
            course_data['course_name'] = clean_and_format_text(name_value)

        address_value = get_nested_value(info, 'address')
        if address_value:
            course_data['address'] = clean_and_format_text(address_value)

        phone_value = get_nested_value(info, 'phone')
        if phone_value:
            course_data['phone'] = format_contact_info(phone_value)

        email_value = get_nested_value(info, 'email')
        if email_value:
            course_data['email'] = email_value.strip()

        website_value = get_nested_value(info, 'website')
        if website_value:
            course_data['website'] = website_value.strip()

        course_type_value = get_nested_value(info, 'course_type')
        if course_type_value:
            course_data['course_type'] = course_type_value

        # Course description (handle array format with proper paragraph formatting)
        description_value = get_nested_value(info, 'course_description')
        if description_value:
            if isinstance(description_value, list):
                formatted_description = format_description_array(description_value)
                course_data['course_description'] = f"<p>{formatted_description}</p>"
            else:
                course_data['course_description'] = f"<p>{clean_and_format_text(description_value)}</p>"

        # URL fields
        url_mappings = {
            'scorecard_url': 'scorecard_url',
            'about_url': 'about_url',
            'membership_url': 'membership_url',
            'tee_time_url': 'tee_times_url',
            'rates_url': 'rates_url'
        }

        for json_field, db_field in url_mappings.items():
            url_value = get_nested_value(info, json_field)
            if url_value and url_value.strip():  # Only add non-empty URLs
                course_data[db_field] = url_value.strip()

        # Course types (boolean fields)
        par_3_value = get_nested_value(info, 'par_3_course')
        if par_3_value is not None:
            course_data['par_3'] = par_3_value

        # Pricing information with proper formatting
        pricing_value = get_nested_value(info, 'pricing_information')
        if pricing_value:
            course_data['rates_and_pricing'] = format_pricing_table(pricing_value)

    # Amenities mapping
    if 'amenities' in json_data:
        amenities = json_data['amenities']

        amenity_mappings = {
            'pro_shop': 'pro_shop',
            'driving_range': 'driving_range',
            'putting_green': 'putting_green',
            'clubhouse': 'club_house'
        }

        for json_field, db_field in amenity_mappings.items():
            if json_field in amenities:
                amenity_data = amenities[json_field]

                # Handle different data structures
                if isinstance(amenity_data, dict):
                    # Check if it has an 'available' field
                    if 'available' in amenity_data:
                        available_value = amenity_data['available']
                        if isinstance(available_value, bool):
                            course_data[db_field] = available_value
                        else:
                            # If 'available' is not a boolean, skip it
                            print(f"⚠️  {json_field}: 'available' field is not boolean: {available_value}")
                    else:
                        # If no 'available' field, skip
                        print(f"⚠️  {json_field}: No 'available' field found in data structure")
                elif isinstance(amenity_data, bool):
                    # Direct boolean value
                    course_data[db_field] = amenity_data
                else:
                    # Unknown data type
                    print(f"⚠️  {json_field}: Unknown data type: {type(amenity_data)}")

        # Combine amenities descriptions with better formatting
        amenities_desc = []

        # Food & beverage options
        food_beverage = amenities.get('food_beverage_options', {})
        if isinstance(food_beverage, dict):
            if food_beverage.get('available'):
                food_value = food_beverage.get('value')
                if food_value:
                    amenities_desc.append(f"<strong>Food & Beverage:</strong> {food_value}")

        # Food & beverage description
        food_desc = amenities.get('food_beverage_options_description', {})
        if isinstance(food_desc, dict):
            if food_desc.get('available'):
                desc_value = food_desc.get('value')
                if desc_value:
                    amenities_desc.append(desc_value)

        # Banquet facilities
        banquet = amenities.get('banquet_facilities', {})
        if isinstance(banquet, dict):
            if 'available' in banquet and isinstance(banquet['available'], bool) and banquet['available']:
                amenities_desc.append('<strong>Banquet facilities:</strong> Available for events and gatherings')
        elif isinstance(banquet, bool) and banquet:
            amenities_desc.append('<strong>Banquet facilities:</strong> Available for events and gatherings')

        if amenities_desc:
            course_data['ammenties'] = format_amenities_description(amenities_desc)

    # Course history with formatting
    if 'course_history' in json_data:
        history = json_data['course_history']

        architect_value = get_nested_value(history, 'architect')
        if architect_value:
            course_data['architect'] = clean_and_format_text(architect_value)

        year_value = get_nested_value(history, 'year_built')
        if year_value:
            course_data['year_opened'] = year_value

        # Combine general history information with proper formatting
        general_value = get_nested_value(history, 'general')
        if general_value:
            if isinstance(general_value, list):
                history_text = format_description_array(general_value)
                course_data['difficulty_summary'] = f"<p>{history_text}</p>"
            else:
                course_data['difficulty_summary'] = f"<p>{clean_and_format_text(general_value)}</p>"

    # Awards and recognitions with better formatting
    if 'awards' in json_data:
        awards = json_data['awards']
        award_texts = []

        for award_type in ['recognitions', 'rankings', 'certifications']:
            award_value = get_nested_value(awards, award_type)
            if award_value:
                # Add section header for each type
                section_title = award_type.replace('_', ' ').title()
                award_texts.append(f"<strong>{section_title}:</strong>")
                if isinstance(award_value, list):
                    for award in award_value:
                        award_texts.append(f"• {award}")
                else:
                    award_texts.append(f"• {award_value}")

        if award_texts:
            course_data['events'] = '<br>'.join(award_texts)

    # Policies with better formatting
    if 'policies' in json_data:
        policies_value = get_nested_value(json_data['policies'], 'course_policies')
        if policies_value:
            course_data['policies'] = format_policies_text(policies_value)

    # Contact information for events
    if 'amateur_professional_events' in json_data:
        contact_value = get_nested_value(json_data['amateur_professional_events'], 'contact_for_events')
        if contact_value:
            # Add to events field if it exists, or create a new one
            if 'events' in course_data:
                course_data['events'] += f"<br><br><strong>Event Contact:</strong> {format_contact_info(contact_value)}"
            else:
                course_data['events'] = f"<strong>Event Contact:</strong> {format_contact_info(contact_value)}"

    # Social media URLs
    if 'social' in json_data:
        social = json_data['social']

        social_mappings = {
            'facebook_url': 'facebook_url',
            'instagram_url': 'instagram_url',
            'twitter_url': 'twitter_url',
            'youtube_url': 'youtube_url',
            'tiktok_url': 'tiktok_url'
        }

        for json_field, db_field in social_mappings.items():
            social_value = get_nested_value(social, json_field)
            if social_value and social_value.strip():
                course_data[db_field] = social_value.strip()

    return course_data

def create_auth_headers():
    """Create authentication headers"""
    credentials = f"{ADMIN_USERNAME}:{ADMIN_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('ascii')

    return {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/json',
        'User-Agent': 'Python-Course-Update-Script/1.0'
    }

def authenticate(host):
    """Test authentication with the API"""
    # Try using requests built-in auth first
    try:
        response = requests.get(
            f"{host}/api/admin/courses",
            auth=(ADMIN_USERNAME, ADMIN_PASSWORD),
            timeout=10
        )
        response.raise_for_status()

        # If successful, return auth tuple for future requests
        return {'auth': (ADMIN_USERNAME, ADMIN_PASSWORD)}

    except requests.exceptions.RequestException as e:
        # Fallback to manual headers
        headers = create_auth_headers()
        try:
            response = requests.get(f"{host}/api/admin/courses", headers=headers, timeout=10)
            response.raise_for_status()
            return {'headers': headers}
        except requests.exceptions.RequestException as e2:
            print(f"Debug: First auth attempt failed with: {e}")
            print(f"Debug: Second auth attempt failed with: {e2}")
            if 'response' in locals():
                print(f"Debug: Response status: {response.status_code}")
                if hasattr(response, 'text'):
                    print(f"Debug: Response text: {response.text[:200]}")
            else:
                print("Debug: No response received")
            raise Exception(f"Authentication failed. Both auth methods failed: {e2}")

def get_current_course(host, course_id, auth_config):
    """Get current course data"""
    try:
        if 'auth' in auth_config:
            response = requests.get(
                f"{host}/api/admin/courses",
                auth=auth_config['auth'],
                timeout=10
            )
        else:
            response = requests.get(
                f"{host}/api/admin/courses",
                headers=auth_config['headers'],
                timeout=10
            )

        response.raise_for_status()

        courses = response.json()
        course = next((c for c in courses if str(c['coursenumber']) == str(course_id)), None)

        if not course:
            raise Exception(f"Course with number {course_id} not found")

        return course
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to get current course: {e}")

def update_course(host, course_number, update_data, auth_config):
    """Update course data"""
    response = None
    try:
        if 'auth' in auth_config:
            response = requests.put(
                f"{host}/api/admin/courses/by-number/{course_number}",
                auth=auth_config['auth'],
                json=update_data,
                timeout=30
            )
        else:
            response = requests.put(
                f"{host}/api/admin/courses/by-number/{course_number}",
                headers=auth_config['headers'],
                json=update_data,
                timeout=30
            )

        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_text = ""
        try:
            if response and hasattr(response, 'text'):
                error_text = f" - {response.text}"
        except:
            pass
        raise Exception(f"Failed to update course: {e}{error_text}")

def main():
    """Main execution function"""
    try:
        args = parse_args()

        print("🚀 Golf Course Update Script (Python) - With Text Formatting")
        print(f"📁 JSON File: {args.json_file_path}")
        print(f"🎯 Course Number: {args.course_number}")
        print(f"🌐 API Host: {args.host}")
        print(f"🔍 Mode: {'DRY RUN' if args.dry_run else 'LIVE UPDATE'}")
        print()

        # Read and parse JSON file
        json_path = Path(args.json_file_path)
        if not json_path.exists():
            raise Exception(f"JSON file not found: {args.json_file_path}")

        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        print("✅ JSON file parsed successfully")

        # Map JSON to database structure with formatting
        update_data = map_json_to_database(json_data)
        print(f"📊 Mapped {len(update_data)} fields for update (with text formatting)")

        # Authenticate
        print("🔐 Authenticating...")
        auth_config = authenticate(args.host)
        print("✅ Authentication successful")

        # Get current course data
        print("📖 Fetching current course data...")
        current_course = get_current_course(args.host, args.course_number, auth_config)
        print(f"✅ Found course: {current_course['course_name']}")

        # Filter update data to only include fields that should be updated
        # Preserve existing data - only update empty/null fields or when new data is provided
        filtered_update_data = {}

        print("\n📝 Analyzing fields for update:")
        print("=" * 60)

        for field, new_value in update_data.items():
            current_value = current_course.get(field)

            # Skip if new value is empty/null
            if not new_value or str(new_value).strip() == '':
                print(f"⏭️  {field}: Skipping - no new data provided")
                continue

            # Skip if current field has data and new value is the same
            if current_value and str(current_value).strip() != '' and current_value == new_value:
                print(f"✅ {field}: Already up to date")
                continue

            # Only update if:
            # 1. Current field is empty/null, OR
            # 2. Current field has different data than new value
            should_update = (
                not current_value or
                str(current_value).strip() == '' or
                current_value != new_value
            )

            if should_update:
                filtered_update_data[field] = new_value
                print(f"📌 {field}:")
                print(f"   Current: {current_value or '(empty)'}")

                # Handle different value types for display
                if isinstance(new_value, bool):
                    print(f"   New: {new_value}")
                elif isinstance(new_value, (int, float)):
                    print(f"   New: {new_value}")
                else:
                    # For strings and other types, truncate if needed
                    new_value_str = str(new_value)
                    if len(new_value_str) > 100:
                        print(f"   New: {new_value_str[:100]}...")
                    else:
                        print(f"   New: {new_value_str}")
                print()
            else:
                print(f"🔒 {field}: Preserving existing data")

        changed_fields = list(filtered_update_data.keys())
        update_data = filtered_update_data

        if not changed_fields:
            print("ℹ️  No updates needed - all fields are either up to date or being preserved")
            return

        print(f"🔢 Total fields to update: {len(changed_fields)}")
        print(f"🔒 Preserving existing data in other fields")
        print(f"🎨 Text formatting applied for web display")

        if args.dry_run:
            print("\n🔍 DRY RUN - No changes made")
            print("Run without --dry-run to apply changes")
            print("📋 Summary:")
            print(f"   - Would update {len(changed_fields)} fields")
            print(f"   - Would preserve all existing data in other fields")
            print(f"   - Text formatting ready for web display")
            return

        # Confirm update
        if not args.auto_confirm:
            print(f"\n⚠️  Ready to update {len(changed_fields)} fields while preserving existing data. Continue? (y/N): ", end="")
            answer = input().strip().lower()

            if answer not in ['y', 'yes']:
                print("❌ Update cancelled")
                return

        # Perform update
        print("⏳ Updating course...")
        result = update_course(args.host, args.course_number, update_data, auth_config)

        print("✅ Course updated successfully!")
        print(f"📊 Updated fields: {', '.join(changed_fields)}")

        if 'course' in result:
            course = result['course']
            print(f"📝 Course: {course['course_name']}")
            print(f"🏌️ Type: {course.get('course_type', 'N/A')}")
            print(f"📍 Location: {course.get('city', 'N/A')}, {course.get('state', 'N/A')}")

    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
