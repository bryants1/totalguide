#!/usr/bin/env python3
"""
Enhanced OpenAI-Powered Golf Course Content Formatter
Uses OpenAI to format pricing (HTML), course description, course history, and food & beverage options (text)
"""

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import openai

class EnhancedContentFormatter:
    def __init__(self, api_key: str = None):
        # Set up OpenAI client
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY environment variable or pass as parameter.")

        openai.api_key = self.api_key

        # Stats tracking
        self.processed_count = 0
        self.formatted_count = 0
        self.error_count = 0
        self.total_tokens_used = 0
        self.total_cost = 0.0

        # Rate limiting
        self.last_api_call = 0
        self.min_delay = 1.0  # Minimum 1 second between API calls

    def get_pricing_prompt(self, messy_pricing: str, course_name: str = "this golf course") -> str:
        """Generate the prompt for OpenAI to format pricing data (HTML output)"""
        return f"""You are a professional golf course web content editor. Please clean up this messy pricing information for {course_name} and create a concise, well-organized summary suitable for a golf course website's pricing section.

REQUIREMENTS:
1. Extract and organize ALL pricing information into logical sections
2. Remove JavaScript code, HTML artifacts, excessive whitespace, and garbled text
3. Create a concise but complete summary - don't lose important pricing details
4. Use simple HTML tags: <h3>, <h4>, <p>, <strong>, <br>, <ul>, <li>, <table>, <tr>, <td>, <th>
5. Organize into clear sections: Greens Fees, Cart Fees, Special Rates, Policies
6. Use tables for complex rate schedules, lists for simple pricing
7. Highlight all prices with <strong> tags
8. Keep important policies and restrictions but make them concise
9. Remove redundant information but preserve all unique pricing details
10. Make it scannable - golfers should quickly find the rates they need

STRUCTURE PREFERENCE:
- Lead with main greens fees (most important)
- Follow with cart fees and rental costs
- Include special rates (junior, senior, military, twilight)
- End with key policies/restrictions
- Keep everything concise but complete

MESSY PRICING DATA:
{messy_pricing}

Return ONLY the cleaned, summarized HTML content suitable for a golf course website pricing section."""

    def get_description_prompt(self, raw_description: str, course_name: str = "this golf course") -> str:
        """Generate prompt for course description (text output)"""
        return f"""You are a professional golf course marketing writer. Please completely rewrite this course description for {course_name} using entirely different wording while keeping the same factual information and key ideas.

REQUIREMENTS:
1. Keep all the same facts, features, and key selling points
2. Use completely different sentence structure and vocabulary
3. Write in your own words - do NOT copy any phrases from the original
4. Create 2-3 well-written paragraphs (150-250 words total)
5. Make it engaging and appealing to potential golfers
6. Remove any HTML, JavaScript, or formatting artifacts
7. Focus on what makes this course special
8. Write in an inviting, professional tone
9. Return ONLY plain text - no HTML tags, no formatting
10. IMPORTANT: Rewrite everything in your own words to avoid any plagiarism

ORIGINAL DESCRIPTION TO REWRITE:
{raw_description}

Return a completely rewritten, plagiarism-free course description that conveys the same information in fresh, original language."""

    def get_history_prompt(self, raw_history: str, course_name: str = "this golf course") -> str:
        """Generate prompt for course history (text output)"""
        return f"""You are a golf course historian and content writer. Please completely rewrite this course history information for {course_name} using entirely different wording while preserving all the same historical facts and details.

REQUIREMENTS:
1. Keep all the same historical facts, dates, names, and events
2. Use completely different sentence structure and vocabulary
3. Write in your own words - do NOT copy any phrases from the original
4. Create 2-3 well-written paragraphs about the course's history (150-200 words)
5. Make it engaging - tell the story of the course in fresh language
6. Remove any HTML, JavaScript, or formatting artifacts
7. Write in a professional, storytelling tone
8. Include all founding dates, architect names, notable events mentioned
9. Return ONLY plain text - no HTML tags, no formatting
10. IMPORTANT: Rewrite everything in your own words to avoid any plagiarism

ORIGINAL HISTORY TO REWRITE:
{raw_history}

Return a completely rewritten, plagiarism-free course history that tells the same story using fresh, original language."""

    def get_seo_metadata_prompt(self, course_name: str, course_description: str, pricing_info: str = "") -> str:
        """Generate prompt for SEO metadata (structured output)"""
        return f"""You are an SEO expert specializing in golf course websites. Create SEO metadata for {course_name} following these exact specifications:

REQUIREMENTS:
1. Generate 5 specific fields with exact character limits
2. Use the course name and description provided
3. Follow SEO best practices for golf courses
4. Make content engaging and search-friendly
5. Return ONLY a JSON object with these exact fields

FIELD SPECIFICATIONS:
- slug: Course name only (no state/number), lowercase, underscores for spaces, max 50 chars
- meta_title: Compelling title for search results, include location, max 60 chars
- meta_description: Descriptive summary for search snippets, max 155 chars
- open_graph_title: Social media title, can be longer than meta_title, max 70 chars
- open_graph_description: Social media description, more detailed, max 200 chars

COURSE INFORMATION:
Course Name: {course_name}
Course Description: {course_description}
Pricing Info: {pricing_info}

Return ONLY a JSON object in this exact format:
{{
  "slug": "course_name_example",
  "meta_title": "Course Name - Location | Golf Course",
  "meta_description": "Description that includes key features and location within 155 characters",
  "open_graph_title": "Course Name - Premium Golf Experience in Location",
  "open_graph_description": "Longer description for social media that can include more details about the course experience and amenities"
}}

Generate the JSON now:"""

    def get_food_beverage_prompt(self, raw_food_info: str, course_name: str = "this golf course") -> str:
        """Generate prompt for food & beverage options (text output)"""
        return f"""You are a golf course amenities writer. Please completely rewrite this food and beverage information for {course_name} using entirely different wording while keeping the same factual details about dining options.

REQUIREMENTS:
1. Keep all the same facts about restaurants, dining options, and amenities
2. Use completely different sentence structure and vocabulary
3. Write in your own words - do NOT copy any phrases from the original
4. Create 1-2 paragraphs about dining options (100-150 words)
5. Make it sound inviting and professional using fresh language
6. Remove any HTML, JavaScript, or formatting artifacts
7. Include all the same dining details, hours, special offerings mentioned
8. Write in an appealing, marketing tone
9. Return ONLY plain text - no HTML tags, no formatting
10. IMPORTANT: Rewrite everything in your own words to avoid any plagiarism

ORIGINAL FOOD & BEVERAGE INFO TO REWRITE:
{raw_food_info}

Return a completely rewritten, plagiarism-free food & beverage description that conveys the same information using fresh, original language."""

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
                # For SEO, content is actually course_description and course_name is already set
                prompt = self.get_seo_metadata_prompt(course_name, content)
            else:
                raise ValueError(f"Unknown content type: {content_type}")

            # Call OpenAI API
            print(f"   🤖 Formatting {content_type}...", end=" ")
            self.last_api_call = time.time()

            response = openai.chat.completions.create(
                model="gpt-4o-mini",  # Cost-effective model
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
                max_tokens=1500 if content_type == "pricing" else 800,
                temperature=0.3  # Low temperature for consistent formatting
            )

            # Extract the formatted content
            formatted_content = response.choices[0].message.content.strip()

            # Clean up any remaining markdown artifacts (for pricing HTML)
            if content_type == "pricing":
                formatted_content = re.sub(r'```html\n?', '', formatted_content)
                formatted_content = re.sub(r'```\n?', '', formatted_content)
                formatted_content = formatted_content.strip()
            elif content_type == "seo_metadata":
                # Clean up JSON response
                formatted_content = re.sub(r'```json\n?', '', formatted_content)
                formatted_content = re.sub(r'```\n?', '', formatted_content)
                formatted_content = formatted_content.strip()

            # Track token usage and cost
            usage = response.usage
            tokens_used = usage.total_tokens

            # GPT-4o-mini pricing (as of 2024): $0.15/1M input tokens, $0.60/1M output tokens
            input_cost = (usage.prompt_tokens / 1_000_000) * 0.15
            output_cost = (usage.completion_tokens / 1_000_000) * 0.60
            call_cost = input_cost + output_cost

            self.total_tokens_used += tokens_used
            self.total_cost += call_cost

            print(f"✅ ({tokens_used} tokens, ${call_cost:.4f})")

            return {
                "success": True,
                "formatted_content": formatted_content,
                "tokens_used": tokens_used,
                "cost": call_cost,
                "original_length": len(content),
                "formatted_length": len(formatted_content)
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

        # Course description - use existing description text
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

        # Course history - use existing history text (not the individual fields)
        history_section = data.get('course_history', {})
        if history_section:
            # First try to get an existing general history field
            general_history = history_section.get('general', {})
            if general_history and general_history.get('value'):
                if isinstance(general_history['value'], list):
                    content['history'] = ' '.join(general_history['value'])
                else:
                    content['history'] = general_history['value']
            else:
                # If no general history, combine all available history fields
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

        # Food & beverage options - use existing food/beverage description
        # Try multiple paths to find existing food & beverage text
        food_text = None

        # Path 1: general_info.amenities.food_beverage_options_description
        amenities_path = data.get('general_info', {}).get('amenities', {})
        if amenities_path:
            food_desc = amenities_path.get('food_beverage_options_description', {})
            if food_desc and food_desc.get('value'):
                food_text = food_desc['value']

        # Path 2: amenities.food_beverage_options_description
        if not food_text:
            amenities_section = data.get('amenities', {})
            if amenities_section:
                food_desc = amenities_section.get('food_beverage_options_description', {})
                if food_desc and food_desc.get('value'):
                    food_text = food_desc['value']

        # Path 3: general_info.amenities.food_beverage_options
        if not food_text:
            if amenities_path:
                food_options = amenities_path.get('food_beverage_options', {})
                if food_options and food_options.get('value'):
                    food_text = food_options['value']

        # Path 4: amenities.food_beverage_options
        if not food_text:
            amenities_section = data.get('amenities', {})
            if amenities_section:
                food_options = amenities_section.get('food_beverage_options', {})
                if food_options and food_options.get('value'):
                    food_text = food_options['value']

        # Path 5: Look in clubhouse details as fallback
        if not food_text:
            amenities_section = data.get('amenities', {})
            if amenities_section:
                clubhouse = amenities_section.get('clubhouse', {})
                if clubhouse and clubhouse.get('value'):
                    if isinstance(clubhouse['value'], list):
                        clubhouse_text = ' '.join(clubhouse['value'])
                    else:
                        clubhouse_text = clubhouse['value']

                    # Only use clubhouse text if it mentions food/restaurant/dining
                    if any(word in clubhouse_text.lower() for word in ['restaurant', 'food', 'dining', 'bar', 'grill', 'cafe']):
                        food_text = clubhouse_text

        if food_text:
            content['food_beverage'] = food_text

        return content

    def format_course_file(self, file_path: Path, output_suffix: str = "_formatted", dry_run: bool = False, content_types: List[str] = None) -> bool:
        """Format content in a single course file using OpenAI and create new output file"""
        if content_types is None:
            content_types = ['pricing', 'description', 'history', 'food_beverage', 'seo_metadata']

        try:
            # Read the file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Create a deep copy for the output file
            import copy
            output_data = copy.deepcopy(data)

            # Extract all content that needs formatting
            content_to_format = self.extract_content_for_formatting(data)

            # Filter by requested content types
            content_to_format = {k: v for k, v in content_to_format.items() if k in content_types}

            if not content_to_format:
                print("   ⏭️  No content found to format")
                return False

            # Extract course name for better context
            course_name = self.extract_course_name(data)

            # Generate output file path
            output_path = file_path.with_name(file_path.stem + output_suffix + file_path.suffix)

            if dry_run:
                print(f"   🔍 Would format {len(content_to_format)} content types for {course_name}")
                print(f"   📁 Output would be: {output_path.name}")
                for content_type in content_to_format:
                    print(f"      - {content_type}: {len(content_to_format[content_type])} chars")
                return True

            # Format each type of content
            formatted_any = False
            total_cost = 0.0

            for content_type, content_text in content_to_format.items():
                # Skip if already formatted or too short
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
                total_cost += result["cost"]

                # Validate the result
                if len(formatted_content) < 10:
                    print(f"   ❌ {content_type} result too short")
                    continue

                # Update the appropriate field in the output_data
                if content_type == 'pricing':
                    output_data['rates']['pricing_information']['value'] = formatted_content

                elif content_type == 'description':
                    if 'general_info' not in output_data:
                        output_data['general_info'] = {}
                    if 'course_description' not in output_data['general_info']:
                        output_data['general_info']['course_description'] = {}
                    output_data['general_info']['course_description']['value'] = formatted_content

                elif content_type == 'history':
                    if 'course_history' not in output_data:
                        output_data['course_history'] = {}
                    if 'general' not in output_data['course_history']:
                        output_data['course_history']['general'] = {}
                    output_data['course_history']['general']['value'] = formatted_content

                elif content_type == 'food_beverage':
                    if 'amenities' not in output_data:
                        output_data['amenities'] = {}
                    if 'food_beverage_options_description' not in output_data['amenities']:
                        output_data['amenities']['food_beverage_options_description'] = {}
                    output_data['amenities']['food_beverage_options_description']['value'] = formatted_content

                elif content_type == 'seo_metadata':
                    # Parse the JSON response and update SEO fields
                    try:
                        import json as json_lib
                        seo_data = json_lib.loads(formatted_content)

                        # Add SEO fields to the root level
                        output_data['slug'] = seo_data.get('slug', '')
                        output_data['meta_title'] = seo_data.get('meta_title', '')
                        output_data['meta_description'] = seo_data.get('meta_description', '')
                        output_data['open_graph_title'] = seo_data.get('open_graph_title', '')
                        output_data['open_graph_description'] = seo_data.get('open_graph_description', '')

                    except json_lib.JSONDecodeError:
                        print(f"   ❌ Failed to parse SEO metadata JSON")
                        continue

                formatted_any = True

            if formatted_any:
                # Write the new output file (not the original)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)

                print(f"   ✅ Created {output_path.name} with {len(content_to_format)} formatted content types (${total_cost:.4f})")
                return True
            else:
                print("   ⏭️  No content was formatted")
                return False

        except Exception as e:
            print(f"   ❌ Error processing file: {e}")
            self.error_count += 1
            return False

    def extract_course_number(self, filename: str) -> str:
        """Extract course number from filename"""
        match = re.match(r'(MA-\d+(?:-\d+)?)', filename)
        return match.group(1) if match else filename

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Format golf course content using OpenAI")
    parser.add_argument("path", nargs='?', help="Directory containing JSON files OR single JSON file")
    parser.add_argument("--preview", help="Preview formatting for a specific file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    parser.add_argument("--api-key", help="OpenAI API key (or set OPENAI_API_KEY env var)")
    parser.add_argument("--content-types", nargs='+', choices=['pricing', 'description', 'history', 'food_beverage', 'seo_metadata'], default=['pricing', 'description', 'history', 'food_beverage', 'seo_metadata'], help="Content types to format")
    parser.add_argument("--output-suffix", default="_formatted", help="Suffix to add to output filenames")

    args = parser.parse_args()

    try:
        formatter = EnhancedContentFormatter(api_key=args.api_key)
    except ValueError as e:
        print(f"❌ {e}")
        print("Get your API key from: https://platform.openai.com/api-keys")
        return

    if args.preview:
        preview_file = Path(args.preview)
        if preview_file.exists():
            print("Preview mode not implemented in this simplified version")
        else:
            print(f"❌ Preview file not found: {preview_file}")
        return

    if not args.path:
        print("❌ Path argument is required")
        print("Usage: python enhanced_formatter.py file.json")
        return

    path = Path(args.path)
    if not path.exists():
        print(f"❌ Path not found: {path}")
        return

    if path.is_file():
        if not args.dry_run:
            estimated_cost = len(args.content_types) * 0.02
            print(f"💰 Estimated cost: ${estimated_cost:.2f} for 1 file")
            print(f"📝 Content types: {', '.join(args.content_types)}")

            response = input(f"Format this file using OpenAI? This will cost real money! (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("❌ Cancelled by user")
                return

        print("🤖 Enhanced OpenAI Golf Course Content Formatter")
        print("=" * 70)

        course_number = formatter.extract_course_number(path.name)
        print(f"Processing: {course_number}")

        if formatter.format_course_file(path, output_suffix=args.output_suffix, dry_run=args.dry_run, content_types=args.content_types):
            if not args.dry_run:
                print(f"\n✅ Successfully created formatted file!")
                output_file = path.with_name(path.stem + args.output_suffix + path.suffix)
                print(f"📁 Output: {output_file}")
                print(f"💰 Total cost: ${formatter.total_cost:.4f}")
        else:
            print("❌ Processing failed or no content to format")
    else:
        print("❌ Directory processing not implemented in this simplified version")

if __name__ == "__main__":
    main()
