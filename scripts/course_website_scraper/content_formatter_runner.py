#!/usr/bin/env python3
"""
Content Formatter Runner
Processes structured JSON files from website scraping and formats content using OpenAI,
then updates the initial_course_upload table via API with only the formatted fields.
"""

import json
import os
import sys
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional
import argparse
import openai
from dataclasses import dataclass

@dataclass
class ProcessingStats:
    total_files: int = 0
    processed_files: int = 0
    updated_courses: int = 0
    skipped_files: int = 0
    errors: int = 0
    total_cost: float = 0.0

class ContentFormatterRunner:
    def __init__(self, api_base_url: str = "http://localhost:3000", api_key: str = None):
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.stats = ProcessingStats()

        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable or pass as parameter.")

        openai.api_key = self.api_key

        # Rate limiting
        self.last_api_call = 0
        self.min_delay = 1.0  # Minimum 1 second between API calls

    def extract_course_number(self, filename: str) -> Optional[str]:
        """Extract course number from filename (e.g., MA-111 from MA-111_Crumpin-Fox_Club...)"""
        parts = filename.split('_')
        if parts and '-' in parts[0]:
            return parts[0]
        return None

    def get_pricing_prompt(self, messy_pricing: str, course_name: str = "this golf course") -> str:
        """Generate the prompt for OpenAI to format pricing data (HTML output)"""
        return f"""Clean up this pricing information for {course_name} and format as HTML for a golf course website.

Requirements:
- Organize into logical sections (Greens Fees, Cart Fees, Special Rates, Policies)
- Use simple HTML: <h3>, <h4>, <p>, <strong>, <br>, <ul>, <li>, <table>, <tr>, <td>, <th>
- Highlight all prices with <strong> tags
- Remove JavaScript/HTML artifacts and excessive whitespace
- Make scannable for golfers

Pricing data:
{messy_pricing}

Return clean HTML only:"""

    def get_description_prompt(self, raw_description: str, course_name: str = "this golf course") -> str:
        """Generate prompt for course description (text output)"""
        return f"""Rewrite this course description for {course_name} using different wording while keeping all facts.

Requirements:
- Keep all facts, features, and selling points
- Use different sentence structure and vocabulary
- Write 2-3 paragraphs (150-250 words)
- Make engaging for golfers
- Remove HTML/JavaScript artifacts
- Plain text only - no HTML tags

Original description:
{raw_description}

Rewrite in fresh, original language:"""

    def get_history_prompt(self, raw_history: str, course_name: str = "this golf course") -> str:
        """Generate prompt for course history (text output)"""
        return f"""Rewrite this course history for {course_name} using different wording while keeping all historical facts.

Requirements:
- Keep all dates, names, and events
- Use different sentence structure
- Write 2-3 paragraphs (150-200 words)
- Professional storytelling tone
- Plain text only

Original history:
{raw_history}

Rewrite with fresh language:"""

    def get_food_beverage_prompt(self, raw_food_info: str, course_name: str = "this golf course") -> str:
        """Generate prompt for food & beverage options (text output)"""
        return f"""Rewrite this food & beverage information for {course_name} using different wording while keeping all details.

Requirements:
- Keep all facts about dining options and amenities
- Use different sentence structure
- Write 1-2 paragraphs (100-150 words)
- Appealing marketing tone
- Plain text only

Original food & beverage info:
{raw_food_info}

Rewrite with fresh language:"""

    def get_seo_metadata_prompt(self, course_name: str, course_description: str, pricing_info: str = "") -> str:
        """Generate prompt for SEO metadata (structured output)"""
        return f"""Create SEO metadata for {course_name} as JSON with these exact fields:

- slug: lowercase course name with underscores, max 50 chars
- meta_title: title for search results with location, max 60 chars
- meta_description: search snippet description, max 155 chars
- open_graph_title: social media title, max 70 chars
- open_graph_description: social media description, max 200 chars

Course: {course_name}
Description: {course_description[:300]}...

Return JSON only:"""

    def format_content_with_openai(self, content: str, content_type: str, course_name: str = "Golf Course") -> Dict:
        """Use OpenAI to format different types of content"""
        try:
            # Rate limiting
            time_since_last = time.time() - self.last_api_call
            if time_since_last < self.min_delay:
                time.sleep(self.min_delay - time_since_last)

            # Select appropriate prompt
            if content_type == "pricing":
                prompt = self.get_pricing_prompt(content, course_name)
            elif content_type == "description":
                prompt = self.get_description_prompt(content, course_name)
            elif content_type == "history":
                prompt = self.get_history_prompt(content, course_name)
            elif content_type == "food_beverage":
                prompt = self.get_food_beverage_prompt(content, course_name)
            elif content_type == "seo_metadata":
                prompt = self.get_seo_metadata_prompt(course_name, content)
            else:
                raise ValueError(f"Unknown content type: {content_type}")

            # Call OpenAI API
            print(f"   🤖 Formatting {content_type}...", end=" ")
            self.last_api_call = time.time()

            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",  # Much cheaper than GPT-4 variants
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional golf course content writer specializing in clear, engaging copy."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=1000 if content_type == "pricing" else 500,  # Reduced token limits
                temperature=0.2  # Lower temperature for more consistent, focused output
            )

            # Extract the formatted content
            formatted_content = response.choices[0].message.content.strip()

            # Clean up any remaining markdown artifacts
            if content_type == "pricing":
                formatted_content = formatted_content.replace('```html\n', '').replace('```html', '')
                formatted_content = formatted_content.replace('```\n', '').replace('```', '')
                formatted_content = formatted_content.strip()
            elif content_type == "seo_metadata":
                formatted_content = formatted_content.replace('```json\n', '').replace('```json', '')
                formatted_content = formatted_content.replace('```\n', '').replace('```', '')
                formatted_content = formatted_content.strip()

            # Track token usage and cost
            usage = response.usage
            tokens_used = usage.total_tokens

            # GPT-3.5-turbo pricing: $0.50/1M input tokens, $1.50/1M output tokens
            input_cost = (usage.prompt_tokens / 1_000_000) * 0.50
            output_cost = (usage.completion_tokens / 1_000_000) * 1.50
            call_cost = input_cost + output_cost

            self.stats.total_cost += call_cost

            print(f"✅ ({tokens_used} tokens, ${call_cost:.4f})")

            return {
                "success": True,
                "formatted_content": formatted_content,
                "tokens_used": tokens_used,
                "cost": call_cost
            }

        except Exception as e:
            print(f"❌ OpenAI API error: {e}")
            return {
                "success": False,
                "error": str(e),
                "formatted_content": None
            }

    def extract_course_name(self, course_data: Dict) -> str:
        """Extract course name from JSON data"""
        name_paths = [
            ["general_info", "name", "value"],
            ["name"],
            ["course_name"]
        ]

        for path in name_paths:
            try:
                current = course_data
                for key in path:
                    current = current[key]
                if current:
                    return current
            except (KeyError, TypeError):
                continue

        return "Golf Course"

    def extract_content_for_formatting(self, data: Dict) -> Dict[str, str]:
        """Extract all content that needs formatting"""
        content = {}

        # Pricing information
        pricing_path = data.get('rates', {}).get('pricing_information', {})
        if pricing_path and pricing_path.get('value'):
            content['pricing'] = pricing_path['value']

        # Course description
        desc_path = data.get('general_info', {}).get('course_description', {})
        if desc_path and desc_path.get('value'):
            if isinstance(desc_path['value'], list):
                content['description'] = ' '.join(desc_path['value'])
            else:
                content['description'] = desc_path['value']

        # SEO metadata - use course description for generating metadata
        if desc_path and desc_path.get('value'):
            description_text = desc_path['value']
            if isinstance(description_text, list):
                description_text = ' '.join(description_text)
            content['seo_metadata'] = description_text

        # Course history
        history_section = data.get('course_history', {})
        if history_section:
            # Try to get general history field first
            general_history = history_section.get('general', {})
            if general_history and general_history.get('value'):
                if isinstance(general_history['value'], list):
                    content['history'] = ' '.join(general_history['value'])
                else:
                    content['history'] = general_history['value']
            else:
                # Combine all available history fields
                history_parts = []
                for field in ['architect', 'year_built', 'notable_events', 'design_features']:
                    field_data = history_section.get(field, {})
                    if field_data:
                        if isinstance(field_data, dict) and 'value' in field_data:
                            value = field_data['value']
                        else:
                            value = field_data

                        if isinstance(value, list):
                            history_parts.extend(value)
                        elif value:
                            history_parts.append(str(value))

                if history_parts:
                    content['history'] = ' '.join(history_parts)

        # Food & beverage options
        food_text = None
        amenities_path = data.get('general_info', {}).get('amenities', {})

        # Try multiple paths to find food & beverage text
        if amenities_path:
            food_desc = amenities_path.get('food_beverage_options_description', {})
            if food_desc and food_desc.get('value'):
                food_text = food_desc['value']

        if not food_text:
            amenities_section = data.get('amenities', {})
            if amenities_section:
                food_desc = amenities_section.get('food_beverage_options_description', {})
                if food_desc and food_desc.get('value'):
                    food_text = food_desc['value']

        if not food_text and amenities_path:
            food_options = amenities_path.get('food_beverage_options', {})
            if food_options and food_options.get('value'):
                food_text = food_options['value']

        if food_text:
            content['food_beverage'] = food_text

        return content

    def check_course_exists(self, course_number: str) -> bool:
        """Check if course exists in the course_scraping_data table"""
        try:
            response = requests.get(f"{self.api_base_url}/api/course-scraping/{course_number}")
            return response.status_code == 200
        except Exception as e:
            print(f"   ❌ Error checking if course exists: {e}")
            return False

    def update_course_in_database(self, course_number: str, formatted_data: Dict) -> bool:
        """Update only the formatted fields in the initial_course_upload table"""
        try:
            # Prepare the update payload with only the formatted fields
            update_payload = {
                "course_number": course_number,  # Required for upsert
                "updated_at": time.strftime('%Y-%m-%d %H:%M:%S')
            }

            # Map formatted content to existing database columns
            if 'pricing' in formatted_data:
                update_payload['rates_and_pricing'] = formatted_data['pricing']

            if 'description' in formatted_data:
                update_payload['course_description'] = formatted_data['description']

            if 'history' in formatted_data:
                update_payload['history'] = formatted_data['history']

            if 'food_beverage' in formatted_data:
                update_payload['food_and_drink'] = formatted_data['food_beverage']

            if 'seo' in formatted_data:
                seo_data = formatted_data['seo']
                update_payload.update({
                    'meta_title': seo_data.get('meta_title'),
                    'meta_data_description': seo_data.get('meta_description'),  # Note: schema has 'meta_data_description'
                    'open_graph_title': seo_data.get('open_graph_title'),
                    'open_graph_description': seo_data.get('open_graph_description')
                })

            # Make API call to update the course
            response = requests.post(
                f"{self.api_base_url}/api/initial-course-upload",
                json=update_payload,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code in [200, 201]:
                result = response.json()
                print(f"   ✅ Updated course {course_number} with {len([k for k in update_payload.keys() if k not in ['course_number', 'updated_at']])} formatted fields")
                return True
            else:
                print(f"   ❌ API error updating course {course_number}: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"   ❌ Error updating course {course_number}: {e}")
            return False

    def process_file(self, file_path: Path, dry_run: bool = False) -> bool:
        """Process a single structured JSON file"""
        try:
            course_number = self.extract_course_number(file_path.stem)
            if not course_number:
                print(f"   ⏭️  Could not extract course number from {file_path.name}")
                self.stats.skipped_files += 1
                return False

            print(f"\n📁 Processing: {file_path.name}")
            print(f"   🏌️  Course: {course_number}")

            # Check if course exists in database
            if not dry_run and not self.check_course_exists(course_number):
                print(f"   ❌ Course {course_number} not found in initial_course_upload table")
                self.stats.errors += 1
                return False

            # Read the structured JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract course name for better context
            course_name = self.extract_course_name(data)
            print(f"   📝 Course Name: {course_name}")

            # Extract all content that needs formatting
            content_to_format = self.extract_content_for_formatting(data)

            if not content_to_format:
                print("   ⏭️  No content found to format")
                self.stats.skipped_files += 1
                return False

            print(f"   📊 Found {len(content_to_format)} content types to format: {list(content_to_format.keys())}")

            if dry_run:
                for content_type in content_to_format:
                    print(f"      - {content_type}: {len(content_to_format[content_type])} chars")
                return True

            # Format each type of content
            formatted_data = {}
            file_cost = 0.0

            for content_type, content_text in content_to_format.items():
                # Skip if too short
                if len(content_text.strip()) < 20:
                    print(f"   ⏭️  {content_type} too short to format")
                    continue

                # Skip pricing if already looks like HTML
                if content_type == 'pricing' and ('<table>' in content_text.lower() or '<div>' in content_text.lower()):
                    print(f"   ⏭️  {content_type} already appears to be formatted HTML")
                    continue

                # Format with OpenAI
                result = self.format_content_with_openai(content_text, content_type, course_name)

                if not result["success"]:
                    print(f"   ❌ {content_type} formatting failed: {result.get('error', 'Unknown error')}")
                    continue

                formatted_content = result["formatted_content"]
                file_cost += result["cost"]

                # Validate the result
                if len(formatted_content) < 10:
                    print(f"   ❌ {content_type} result too short")
                    continue

                if content_type == 'seo_metadata':
                    # Parse SEO JSON
                    try:
                        seo_data = json.loads(formatted_content)
                        formatted_data['seo'] = seo_data
                    except json.JSONDecodeError:
                        print(f"   ❌ Failed to parse SEO metadata JSON")
                        continue
                else:
                    formatted_data[content_type] = formatted_content

            if formatted_data:
                # Update the database
                if self.update_course_in_database(course_number, formatted_data):
                    print(f"   💰 File cost: ${file_cost:.4f}")
                    self.stats.processed_files += 1
                    self.stats.updated_courses += 1
                    return True
                else:
                    self.stats.errors += 1
                    return False
            else:
                print("   ⏭️  No content was successfully formatted")
                self.stats.skipped_files += 1
                return False

        except Exception as e:
            print(f"   ❌ Error processing file: {e}")
            self.stats.errors += 1
            return False

    def find_structured_files(self, directory: Path) -> List[Path]:
        """Find all *structured.json files in the directory"""
        pattern = "*structured.json"
        files = list(directory.glob(pattern))
        return sorted(files)

    def run(self, directory: str, dry_run: bool = False, max_files: Optional[int] = None, skip_existing: bool = False):
        """Run the content formatter on all structured JSON files"""
        directory_path = Path(directory)

        if not directory_path.exists():
            print(f"❌ Directory not found: {directory}")
            return

        if not directory_path.is_dir():
            print(f"❌ Path is not a directory: {directory}")
            return

        # Find all structured JSON files
        files = self.find_structured_files(directory_path)

        if not files:
            print(f"❌ No *structured.json files found in {directory}")
            return

        if max_files:
            files = files[:max_files]

        self.stats.total_files = len(files)

        print("🤖 Content Formatter Runner")
        print("=" * 70)
        print(f"📁 Directory: {directory}")
        print(f"📊 Found {len(files)} structured JSON files")

        if dry_run:
            print("🔍 DRY RUN MODE - No changes will be made")
        else:
            estimated_cost = len(files) * 0.03  # Much lower estimate for GPT-3.5-turbo
            print(f"💰 Estimated cost: ~${estimated_cost:.2f}")

            response = input(f"Process {len(files)} files? This will cost real money! (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("❌ Cancelled by user")
                return

        print()

        # Process each file
        start_time = time.time()

        for i, file_path in enumerate(files, 1):
            print(f"\n[{i}/{len(files)}]", end=" ")
            self.process_file(file_path, dry_run)

        # Print final statistics
        elapsed_time = time.time() - start_time
        print("\n" + "=" * 70)
        print("📊 PROCESSING COMPLETE")
        print(f"⏱️  Total time: {elapsed_time:.1f} seconds")
        print(f"📁 Total files: {self.stats.total_files}")
        print(f"✅ Processed: {self.stats.processed_files}")
        print(f"🔄 Updated courses: {self.stats.updated_courses}")
        print(f"⏭️  Skipped: {self.stats.skipped_files}")
        print(f"❌ Errors: {self.stats.errors}")
        if not dry_run:
            print(f"💰 Total cost: ${self.stats.total_cost:.4f}")

def main():
    parser = argparse.ArgumentParser(description="Format golf course content from structured JSON files")
    parser.add_argument("directory", help="Directory containing *structured.json files")
    parser.add_argument("--dry-run", action="store_true", help="Preview processing without making changes")
    parser.add_argument("--api-key", help="OpenAI API key (defaults to OPENAI_API_KEY env var)")
    parser.add_argument("--api-url", default="http://localhost:3000", help="API base URL")
    parser.add_argument("--max-files", type=int, help="Maximum number of files to process")
    parser.add_argument("--skip-existing", action="store_true", help="Skip files that already have formatted content")

    args = parser.parse_args()

    try:
        runner = ContentFormatterRunner(api_base_url=args.api_url, api_key=args.api_key)
        runner.run(args.directory, args.dry_run, args.max_files, args.skip_existing)
    except ValueError as e:
        print(f"❌ {e}")
        print("Get your API key from: https://platform.openai.com/api-keys")
        return 1
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
