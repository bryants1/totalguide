#!/usr/bin/env python3
"""
FIXED Golf Course Rubric Script - With Correct File Paths and Structured JSON
Usage: python fixed_rubric_script.py single MA-111
"""

import json
import os
import sys
import time
import asyncio
import glob
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

RUBRIC_STRUCTURE = {
    "Course Conditions": {
        "Fairways": {"max_score": 10, "method": "hybrid"},
        "Greens": {"max_score": 10, "method": "hybrid"},
        "Bunkers": {"max_score": 5, "method": "hybrid"},
        "Tee Boxes": {"max_score": 5, "method": "hybrid"}
    },
    "Course Layout & Design": {
        "Shot Variety / Hole Uniqueness": {"max_score": 10, "method": "ai"},
        "Signature Holes / Quirky/Fun Design Features": {"max_score": 5, "method": "ai"},
        "Water & OB Placement": {"max_score": 4, "method": "ai"},
        "Overall feel / Scenery": {"max_score": 4, "method": "ai"},
        "Green Complexity": {"max_score": 2, "method": "hybrid"}
    },
    "Amenities": {
        "Driving Range": {"max_score": 3, "method": "rule"},
        "Putting & Short Game Areas": {"max_score": 3, "method": "rule"},
        "Availability": {"max_score": 3, "method": "rule"},
        "Snack Bar-1, Snack Bar w/ Alcohol-2, Grill w/ Alcohol-3, Full Bar & Lounge-4, Full Service Restaurant-5": {"max_score": 5, "method": "ai"},
        "Locker room & Showers": {"max_score": 3, "method": "rule"},
        "Pro-shop": {"max_score": 5, "method": "ai"}
    },
    "Player Experience": {
        "Staff Friendliness, After-Round Experience": {"max_score": 2, "method": "ai"},
        "Eco-friendless and sustainability": {"max_score": 3, "method": "ai"},
        "Course History": {"max_score": 2, "method": "ai"},
        "Architect": {"max_score": 2, "method": "ai"},
        "Green Fees vs. Quality": {"max_score": 5, "method": "hybrid"},
        "Replay Value": {"max_score": 3, "method": "ai"},
        "Ease of Walking": {"max_score": 3, "method": "rule"},
        "Pace of Play": {"max_score": 3, "method": "hybrid"}
    }
}

class FixedRubricPopulator:
    def __init__(self):
        self.states_dir = Path("../states")
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.openai_client = None

        if self.openai_api_key and OPENAI_AVAILABLE:
            try:
                self.openai_client = OpenAI(api_key=self.openai_api_key)
                logger.info("✅ AI scoring enabled")
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI: {e}")
        else:
            logger.info("⚠️ AI scoring disabled")

        self.course_data = {}
        self.reviews_data = {}
        self.analysis_data = {}
        self.vector_data = {}
        self.excel_data = {}
        self.ai_explanations = {}

    def find_files(self, course_id: str):
        """Find files in the correct locations - read from images_elevation, write to course_scores"""
        base_path = "../states/ma"
        logger.info(f"🔍 Searching for {course_id} files...")

        files = {}

        # 1. Images Elevation: ../states/ma/images_elevation/MA-111_**/ (PRIMARY source for analysis & vector)
        elevation_pattern = f"{base_path}/images_elevation/{course_id}_**/*"
        elevation_files = glob.glob(elevation_pattern, recursive=True)

        logger.info(f"  🏔️ Images Elevation: {base_path}/images_elevation/")

        for file_path in elevation_files:
            if file_path.endswith('.json'):
                filename = os.path.basename(file_path).lower()
                if filename == 'vector_attributes_only.json':
                    files['vector'] = file_path
                    logger.info(f"    ✅ Found vector: {filename}")
                elif filename == 'comprehensive_analysis.json':
                    files['analysis'] = file_path
                    logger.info(f"    ✅ Found analysis: {filename}")

        # 2. Reviews: ../states/ma/reviews/scores/
        reviews_pattern = f"{base_path}/reviews/scores/*"
        reviews_files = glob.glob(reviews_pattern)

        logger.info(f"  📋 Reviews: {base_path}/reviews/scores/")

        for file_path in reviews_files:
            if file_path.endswith('.json') and course_id in os.path.basename(file_path):
                filename = os.path.basename(file_path).lower()
                if 'reviews_summary' in filename or 'summary' in filename:
                    files['reviews'] = file_path
                    logger.info(f"    ✅ Found reviews: {os.path.basename(file_path)}")
                    break

        # 3. Website Data: ../states/ma/website_data/general/
        website_pattern = f"{base_path}/website_data/general/{course_id}*_structured.json"
        website_files = glob.glob(website_pattern)

        logger.info(f"  🏌️ Website Data: {base_path}/website_data/general/")

        if website_files:
            files['course'] = website_files[0]  # Take the first match
            logger.info(f"    ✅ Found course data: {os.path.basename(website_files[0])}")
        else:
            logger.warning(f"    ❌ No course data found")

        # 4. Excel: ../states/ma/course_list/ (keep original logic)
        excel_path = Path(f"{base_path}/course_list")
        logger.info(f"  📈 Excel: {excel_path}")

        if excel_path.exists():
            # Look for the specific file first
            target_excel = excel_path / "USGolfData-WithPlaceDetails_with_urls.xlsx"
            if target_excel.exists():
                files['excel'] = target_excel
                logger.info(f"    ✅ Found target Excel: {target_excel.name}")
            else:
                # Fallback to any Excel file, but prefer non-"old" versions
                excel_files = list(excel_path.glob("USGolfData*.xlsx"))
                non_old_files = [f for f in excel_files if "_old" not in f.name]

                if non_old_files:
                    files['excel'] = non_old_files[0]
                    logger.info(f"    ✅ Found Excel: {non_old_files[0].name}")
                elif excel_files:
                    files['excel'] = excel_files[0]
                    logger.info(f"    ⚠️ Using fallback Excel: {excel_files[0].name}")
                else:
                    logger.warning(f"    ❌ No Excel files found")

        logger.info(f"📋 File search summary: Found {len(files)} files")
        return files

    def load_data(self, course_id: str):
        """Load all the data files"""
        files = self.find_files(course_id)

        # Define required files (matching vector creator requirements)
        required_files = [
            'analysis',      # comprehensive_analysis
            'vector',        # vector_attributes
            'reviews',       # reviews_summary
            'course'         # course_data (structured.json)
        ]

        print(f"Found files for course {course_id}:")
        for file_type, file_path in files.items():
            print(f"  ✓ {file_type}: {file_path}")

        # Check if all required files are present
        missing_files = []
        for required_file in required_files:
            if required_file not in files:
                missing_files.append(required_file)

        if missing_files:
            print(f"\n❌ SKIPPING COURSE {course_id}")
            print(f"Missing required files: {', '.join(missing_files)}")
            print(f"Found {len(files)} of {len(required_files)} required files")
            print(f"Required files: {', '.join(required_files)}")

            print(f"🔧 DEBUG: Starting directory creation process...")

            # Create output directory for skipped course
            try:
                output_dir = Path(f"../states/ma/course_scores")
                print(f"🔧 DEBUG: Creating base directory: {output_dir}")
                output_dir.mkdir(parents=True, exist_ok=True)

                course_output_dir = output_dir / f"{course_id}_failed"
                print(f"🔧 DEBUG: Creating course directory: {course_output_dir}")
                course_output_dir.mkdir(parents=True, exist_ok=True)

                print(f"🔧 DEBUG: Directories created successfully")

                # Create detailed missing files report
                missing_files_text = f"RUBRIC CREATION SKIPPED - MISSING FILES\n"
                missing_files_text += f"Course: {course_id}\n"
                missing_files_text += f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                missing_files_text += f"="*60 + "\n\n"

                missing_files_text += f"MISSING REQUIRED FILES ({len(missing_files)} of {len(required_files)}):\n"
                for missing_file in missing_files:
                    missing_files_text += f"  ❌ {missing_file}\n"

                missing_files_text += f"\nFOUND FILES ({len(files)} of {len(required_files)}):\n"
                for file_type, file_path in files.items():
                    if file_type in required_files:
                        missing_files_text += f"  ✅ {file_type}: {file_path}\n"

                missing_files_text += f"\nREQUIRED FILE LOCATIONS:\n"
                missing_files_text += f"  - analysis: ../states/ma/images_elevation/{course_id}_**/comprehensive_analysis.json\n"
                missing_files_text += f"  - vector: ../states/ma/images_elevation/{course_id}_**/vector_attributes_only.json\n"
                missing_files_text += f"  - reviews: ../states/ma/reviews/scores/{course_id}_*_reviews_summary.json\n"
                missing_files_text += f"  - course: ../states/ma/website_data/general/{course_id}*_structured.json\n"

                # Save the missing files report
                missing_files_file = course_output_dir / f"{course_id}_missing_files.txt"
                print(f"🔧 DEBUG: Writing file to: {missing_files_file}")

                with open(missing_files_file, 'w') as f:
                    f.write(missing_files_text)

                print(f"📁 Missing files report saved to: {missing_files_file}")
                print(f"🔧 DEBUG: File creation completed successfully")

            except Exception as e:
                print(f"⚠️  Could not create missing files report: {e}")
                import traceback
                print(f"🔧 DEBUG: Full error details:")
                traceback.print_exc()

            print(f"🔧 DEBUG: About to exit with code 2")
            sys.exit(2)  # Exit immediately with code 2

        print(f"\n✓ All {len(required_files)} required files found. Proceeding with rubric creation...")

        # Reset all data
        self.course_data = {}
        self.reviews_data = {}
        self.analysis_data = {}
        self.vector_data = {}
        self.excel_data = {}

        loaded_count = 0

        try:
            # Load each file type
            for file_type, file_path in files.items():
                if file_type == 'excel':
                    if PANDAS_AVAILABLE:
                        df = pd.read_excel(file_path)
                        # Find the course row
                        course_row = df[df.iloc[:, 0] == course_id]
                        if not course_row.empty:
                            self.excel_data = course_row.iloc[0].to_dict()
                            logger.info(f"✅ Loaded Excel: {len(self.excel_data)} fields")
                            loaded_count += 1
                    continue

                # Load JSON files
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                data_size = len(json.dumps(data))

                if file_type == 'reviews':
                    self.reviews_data = data
                    logger.info(f"✅ Loaded reviews: {data_size:,} chars")
                elif file_type == 'course':
                    self.course_data = data
                    logger.info(f"✅ Loaded course: {data_size:,} chars")
                elif file_type == 'analysis':
                    self.analysis_data = data
                    logger.info(f"✅ Loaded analysis: {data_size:,} chars")
                elif file_type == 'vector':
                    self.vector_data = data
                    logger.info(f"✅ Loaded vector: {data_size:,} chars")

                loaded_count += 1

            total_data_size = (
                len(json.dumps(self.course_data)) +
                len(json.dumps(self.reviews_data)) +
                len(json.dumps(self.analysis_data)) +
                len(json.dumps(self.vector_data)) +
                len(json.dumps(self.excel_data))
            )

            logger.info(f"📊 Total loaded: {loaded_count} files, {total_data_size:,} total characters")
            return True

        except Exception as e:
            logger.error(f"❌ Error loading files: {e}")
            import traceback
            traceback.print_exc()
            return False

    # Rule-based scoring methods with better explanations
    def score_fairways(self):
        fairways = self.reviews_data.get('text_insight_averages', {}).get('Fairways')
        return max(0, min(10, round((fairways + 1) * 5))) if fairways is not None else 5

    def score_greens(self):
        greens = self.reviews_data.get('text_insight_averages', {}).get('Greens')
        return max(0, min(10, round((greens + 1) * 5))) if greens is not None else 5

    def score_bunkers(self):
        bunkers = self.reviews_data.get('text_insight_averages', {}).get('Bunkers')
        return max(0, min(5, round((bunkers + 1) * 2.5))) if bunkers is not None else 3

    def score_tee_boxes(self):
        tee_boxes = self.reviews_data.get('text_insight_averages', {}).get('Tee Boxes')
        return max(0, min(5, round((tee_boxes + 1) * 2.5))) if tee_boxes is not None else 3

    def score_driving_range(self):
        dr = self.course_data.get('amenities', {}).get('driving_range', {}).get('available')
        return 3 if dr else 0 if dr is False else 1

    def score_putting_short_game_areas(self):
        pg = self.course_data.get('amenities', {}).get('practice_green', {}).get('available')
        sg = self.course_data.get('amenities', {}).get('short_game_practice_area', {}).get('available')
        if pg and sg: return 3
        elif pg: return 2
        elif pg is False and sg is False: return 0
        return 1

    def score_availability(self):
        course_type = self.course_data.get('general_info', {}).get('course_type', {}).get('value')
        if course_type == "Public": return 3
        elif course_type == "Semi-Private": return 2
        elif course_type == "Private": return 1
        return 2

    def score_locker_room_showers(self):
        lr = self.course_data.get('amenities', {}).get('locker_rooms', {}).get('available')
        sh = self.course_data.get('amenities', {}).get('showers', {}).get('available')
        if lr and sh: return 3
        elif lr or sh: return 2
        elif lr is False and sh is False: return 0
        return 1

    def score_ease_of_walking(self):
        elevation = self.analysis_data.get('elevation_analysis', {}).get('course_elevation_summary', {}).get('total_elevation_change_m')
        if not elevation: return 2
        if elevation < 50: return 3
        elif elevation < 150: return 2
        return 1

    def score_green_fees_vs_quality(self):
        value = self.reviews_data.get('form_category_averages', {}).get('Value')
        if not value: return 3
        if value >= 4.5: return 5
        elif value >= 4.0: return 4
        elif value >= 3.5: return 3
        elif value >= 3.0: return 2
        return 1

    def score_green_complexity(self):
        gc = self.reviews_data.get('text_insight_averages', {}).get('Green Complexity')
        return max(0, min(2, round((gc + 1)))) if gc is not None else 1

    def score_pace_of_play(self):
        pace = self.reviews_data.get('form_category_averages', {}).get('Pace')
        if not pace: return 2
        if pace >= 4.5: return 3
        elif pace >= 4.0: return 2
        return 1

    async def score_with_ai(self, categories, max_retries=2):
        """Send ALL data to OpenAI with forced JSON response format"""
        if not self.openai_client:
            return self.get_fallback_scores(categories)

        course_name = self.course_data.get('general_info', {}).get('name', {}).get('value', 'Unknown')

        prompt = f"""Score this golf course: {course_name}

COMPLETE COURSE DATA:
{json.dumps(self.course_data, indent=2)}

COMPLETE REVIEWS DATA:
{json.dumps(self.reviews_data, indent=2)}

COMPLETE ANALYSIS DATA:
{json.dumps(self.analysis_data, indent=2)}

COURSE VECTOR ATTRIBUTES:
{json.dumps(self.vector_data, indent=2)}

EXCEL DATA:
{json.dumps(self.excel_data, indent=2)}

Analyze ALL this comprehensive data and provide scores WITH EXPLANATIONS for these categories:

"""
        for cat in categories:
            for section_data in RUBRIC_STRUCTURE.values():
                if cat in section_data:
                    prompt += f'"{cat}": (0-{section_data[cat]["max_score"]}) - {self.get_scoring_guide(cat)}\n'
                    break

        prompt += '''
CRITICAL: Return valid JSON in this exact format:
{
  "category_name": {
    "score": numeric_score,
    "explanation": "detailed explanation citing specific data points"
  }
}
'''

        # Retry logic for consistency with forced JSON output
        for attempt in range(max_retries):
            try:
                logger.info(f"🤖 AI Attempt {attempt + 1}/{max_retries}...")

                response = await asyncio.to_thread(
                    self.openai_client.chat.completions.create,
                    model="gpt-4-turbo-preview",
                    messages=[
                        {"role": "system", "content": "You are an expert golf course evaluator. Analyze the comprehensive course data thoroughly and provide detailed explanations for your scores. You must respond with valid JSON only."},
                        {"role": "user", "content": prompt}
                    ],
                    response_format={"type": "json_object"},  # Force JSON output
                    temperature=0.0,  # Zero temperature for maximum consistency
                    max_tokens=2500,  # Increased for explanations
                    seed=42  # Fixed seed for reproducible results
                )

                ai_response = response.choices[0].message.content.strip()
                logger.info(f"🤖 AI Response: {len(ai_response)} chars")

                # Parse JSON with explanations (should always be valid now)
                result_with_explanations = json.loads(ai_response)

                # Validate the response structure
                valid_response = True
                for category, data in result_with_explanations.items():
                    if not isinstance(data, dict) or 'score' not in data:
                        valid_response = False
                        break

                if not valid_response:
                    raise ValueError("Invalid response structure")

                # Extract scores and explanations
                scores = {}
                explanations = {}

                for category, data in result_with_explanations.items():
                    scores[category] = data['score']
                    explanations[category] = data.get('explanation', 'No explanation provided')

                # Store explanations for later use
                self.ai_explanations = explanations

                logger.info(f"✅ AI scored {len(scores)} categories successfully on attempt {attempt + 1}")
                return scores

            except json.JSONDecodeError as e:
                logger.warning(f"❌ Attempt {attempt + 1}: JSON decode error: {e}")
                if attempt < max_retries - 1:
                    logger.info("🔄 Retrying...")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"💥 All {max_retries} attempts failed")

            except Exception as e:
                logger.warning(f"❌ Attempt {attempt + 1}: API error: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"💥 All {max_retries} attempts failed due to API errors")

        # If all retries failed, use fallback
        logger.warning("🔄 Using fallback scores due to AI failures")
        return self.get_fallback_scores(categories)

    def get_scoring_guide(self, category):
        """Get scoring guide for a category"""
        guides = {
            "Shot Variety / Hole Uniqueness": "0-3: Repetitive shots. 4-6: Decent mix. 7-8: Excellent variety of clubs and strategy.",
            "Signature Holes / Quirky/Fun Design Features": "0-2: Lacks character. 3-4: One or two fun holes. 5: Memorable signature hole(s), playful design.",
            "Water & OB Placement": "0-1: Poorly placed or irrelevant. 2-3: Adds strategy. 4: Enhances both challenge and visual appeal.",
            "Overall feel / Scenery": "0-1: Dull surroundings. 2-3: Decent visual appeal. 4: Stunning setting, immersive experience.",
            "Snack Bar-1, Snack Bar w/ Alcohol-2, Grill w/ Alcohol-3, Full Bar & Lounge-4, Full Service Restaurant-5": "1: Basic snack bar. 2: Alcohol available. 3: Grilled food + drinks. 4: Full bar/lounge. 5: Full-service restaurant.",
            "Pro-shop": "0–2: Sparse or outdated. 3–4: Good selection of essentials and apparel. 5: Well-stocked, modern, helpful staff.",
            "Staff Friendliness, After-Round Experience": "0-1: Cold, unwelcoming. 2: Friendly staff, good vibe.",
            "Eco-friendless and sustainability": "0-1: No evident effort. 2: Some sustainable practices. 3: Actively eco-conscious, certified programs.",
            "Course History": "0-1: No notable history. 2: Rich backstory or long-standing tradition.",
            "Architect": "0-1: Unknown or basic design. 2: Renowned or respected designer.",
            "Replay Value": "0-1: Would not return. 2: Worth a revisit. 3: Must-play again.",
            "Fairways": "0-3: Patchy or inconsistent. 4-7: Solid but variable. 8-10: Lush, well-maintained, consistent.",
            "Greens": "0-3: Bumpy or diseased. 4-7: Good speed and surface. 8-10: Smooth, fast, and true.",
            "Bunkers": "0-2: Poor sand or maintenance. 3-4: Functional but inconsistent. 5: Well-raked, good sand, well placed.",
            "Tee Boxes": "0–2: Uneven, poor shape, lack of variety. 3–4: Generally level and playable. 5: Multiple sets, well-maintained, clearly marked.",
            "Green Complexity": "0: Flat, basic greens. 1: Some slope or tiers. 2: Strategic, varied contours",
            "Green Fees vs. Quality": "0-1: Overpriced. 2-4: Reasonable value. 5: Excellent value for quality.",
            "Pace of Play": "0-1: Slow rounds. 2: Generally good. 3: Consistently 4-4.5 hours."
        }
        return guides.get(category, "")

    def get_fallback_scores(self, categories):
        """Rule-based fallback scores with explanations"""
        scores = {}
        explanations = {}

        method_map = {
            'Fairways': (self.score_fairways, "Review text insights analysis"),
            'Greens': (self.score_greens, "Review text insights analysis"),
            'Bunkers': (self.score_bunkers, "Review text insights analysis"),
            'Tee Boxes': (self.score_tee_boxes, "Review text insights analysis"),
            'Driving Range': (self.score_driving_range, "Amenities availability data"),
            'Putting & Short Game Areas': (self.score_putting_short_game_areas, "Practice facilities availability"),
            'Availability': (self.score_availability, "Course type (Public/Private) assessment"),
            'Locker room & Showers': (self.score_locker_room_showers, "Facility amenities data"),
            'Ease of Walking': (self.score_ease_of_walking, "Elevation analysis - total change in meters"),
            'Green Fees vs. Quality': (self.score_green_fees_vs_quality, "Review value ratings analysis"),
            'Green Complexity': (self.score_green_complexity, "Review text insights on green design"),
            'Pace of Play': (self.score_pace_of_play, "Review pace ratings analysis")
        }

        for cat in categories:
            if cat in method_map:
                method_func, data_source = method_map[cat]
                score = method_func()
                scores[cat] = score

                # Get max score for context
                max_score = None
                for section_data in RUBRIC_STRUCTURE.values():
                    if cat in section_data:
                        max_score = section_data[cat]['max_score']
                        break

                explanations[cat] = f"Rule-based scoring ({data_source}): {score}/{max_score}. Calculated using structured data from course amenities and review analytics."
            else:
                # Default to middle score for categories without specific methods
                for section_data in RUBRIC_STRUCTURE.values():
                    if cat in section_data:
                        score = section_data[cat]['max_score'] // 2
                        scores[cat] = score
                        explanations[cat] = f"Default rule-based scoring: {score}/{section_data[cat]['max_score']} (middle score - insufficient data for detailed analysis)"
                        break

        # Add rule-based explanations to the main explanations storage
        if not hasattr(self, 'ai_explanations'):
            self.ai_explanations = {}
        self.ai_explanations.update(explanations)

        return scores

    async def process_course(self, course_id: str):
        """Process a single course"""
        logger.info(f"🏌️ Processing {course_id}...")

        if not self.load_data(course_id):
            logger.error(f"❌ Skipped {course_id} due to missing required files")
            # Exit with code 2 to indicate skipped due to missing files
            sys.exit(2)

        # Initialize explanations storage
        self.ai_explanations = {}

        # Separate AI and rule-based categories
        ai_categories = []
        rule_categories = []

        for section, subcats in RUBRIC_STRUCTURE.items():
            for subcat, info in subcats.items():
                if info['method'] in ['ai', 'hybrid'] and self.openai_client:
                    ai_categories.append(subcat)
                else:
                    rule_categories.append(subcat)

        # Score with AI
        ai_scores = {}
        if ai_categories:
            logger.info(f"🤖 Using AI for {len(ai_categories)} categories...")
            ai_scores = await self.score_with_ai(ai_categories)

        # Get rule scores
        rule_scores = self.get_fallback_scores(rule_categories)

        # Combine scores
        all_scores = {**ai_scores, **rule_scores}

        # Calculate totals and organize results
        total_score = 0
        total_max = 0
        results = {}
        detailed_results = {}

        for section, subcats in RUBRIC_STRUCTURE.items():
            results[section] = {}
            detailed_results[section] = {}

            for subcat in subcats:
                score = all_scores.get(subcat, None)
                max_score = RUBRIC_STRUCTURE[section][subcat]['max_score']
                method = RUBRIC_STRUCTURE[section][subcat]['method']
                explanation = self.ai_explanations.get(subcat, 'Rule-based scoring used')

                results[section][subcat] = score
                detailed_results[section][subcat] = {
                    'score': score,
                    'max_score': max_score,
                    'method': method,
                    'explanation': explanation
                }

                total_max += max_score
                if score is not None:
                    total_score += score

        percentage = round(total_score / total_max * 100) if total_max > 0 else 0
        course_name = self.course_data.get('general_info', {}).get('name', {}).get('value', 'Unknown')

        # Save results to course_scores directory (matching the vector creator pattern)
        output_dir = Path(f"../states/ma/course_scores")
        course_output_dir = None

        # Find the existing course directory
        course_dirs = list(output_dir.glob(f"{course_id}_*"))
        if course_dirs:
            course_output_dir = course_dirs[0]
        else:
            course_output_dir = output_dir / f"{course_id}_rubric_output"
            course_output_dir.mkdir(exist_ok=True)

        result = {
            'course_id': course_id,
            'course_name': course_name,
            'total_score': total_score,
            'max_score': total_max,
            'percentage': percentage,
            'scores': results,
            'detailed_scores_with_explanations': detailed_results,
            'ai_enhanced': bool(self.openai_client),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }

        # Save the rubric file in the expected location
        rubric_file = course_output_dir / f"{course_id}_rubric.json"
        with open(rubric_file, 'w') as f:
            json.dump(result, f, indent=2)

        # Also create a human-readable explanations file
        explanations_text = f"GOLF COURSE RUBRIC EXPLANATIONS\n"
        explanations_text += f"Course: {course_name} ({course_id})\n"
        explanations_text += f"Total Score: {total_score}/{total_max} ({percentage}%)\n"
        explanations_text += f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        explanations_text += "="*80 + "\n\n"

        for section, subcats in detailed_results.items():
            explanations_text += f"{section.upper()}\n" + "-"*50 + "\n"

            for subcat, details in subcats.items():
                score = details['score']
                max_score = details['max_score']
                method = details['method']
                explanation = details['explanation']

                explanations_text += f"\n{subcat}: {score}/{max_score} ({method.upper()})\n"
                explanations_text += f"Explanation: {explanation}\n"

            explanations_text += "\n"

        with open(course_output_dir / f"{course_id}_explanations.txt", 'w') as f:
            f.write(explanations_text)

        logger.info(f"✅ {course_name}: {total_score}/{total_max} ({percentage}%)")
        logger.info(f"📁 Results saved to: {rubric_file}")

        # Print some key explanations to console
        logger.info("\n📋 KEY AI SCORING EXPLANATIONS:")
        for category, explanation in self.ai_explanations.items():
            if len(explanation) > 20:  # Only show substantial explanations
                logger.info(f"  • {category}: {explanation[:150]}...")


def main():
    if len(sys.argv) < 3:
        print("Usage: python fixed_rubric_script.py single MA-111")
        sys.exit(1)

    command = sys.argv[1].lower()
    course_id = sys.argv[2].upper()

    if command == "single":
        populator = FixedRubricPopulator()
        asyncio.run(populator.process_course(course_id))
    else:
        print("Only 'single' command supported")
        sys.exit(1)

if __name__ == "__main__":
    main()
