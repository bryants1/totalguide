
import base64
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
import asyncio
import aiohttp
from datetime import datetime
from collections import Counter
import time
import re

class GolfCourseVectorGenerator:
    """Generate standardized vector attributes for golfer-course matching."""

    def __init__(self, openai_api_key: str):
        self.api_key = openai_api_key
        self.vision_endpoint = "https://api.openai.com/v1/chat/completions"

        # Define standardized attribute schemas - ALL 0-100 scale
        self.attribute_schemas = {
            "ball_findability": {
                "type": "numeric",
                "scale": "0-100",
                "description": "How easy it is to find wayward shots (0=lost forever, 100=always findable)"
            },
            "tree_coverage": {
                "type": "numeric",
                "scale": "0-100",
                "description": "Density of tree coverage affecting play (0=open, 100=heavily forested)"
            },
            "visual_tightness": {
                "type": "numeric",
                "scale": "0-100",
                "description": "How narrow/tight the course appears (0=wide open, 100=very tight)"
            },
            "natural_integration": {
                "type": "numeric",
                "scale": "0-100",
                "description": "How well the design works with natural landscape (0=artificial, 100=natural)"
            },
            "water_prominence": {
                "type": "numeric",
                "scale": "0-100",
                "description": "How prominently water features affect play (0=no water, 100=water dominant)"
            },
            "course_openness": {
                "type": "numeric",
                "scale": "0-100",
                "description": "Overall sense of space (0=claustrophobic, 100=wide open)"
            },
            "walkability": {
                "type": "numeric",
                "scale": "0-100",
                "description": "How easy to walk the course (0=very difficult, 100=easy pleasant walk)"
            },
            "shot_shaping_required": {
                "type": "numeric",
                "scale": "0-100",
                "description": "Demand for curved shots (0=straight shots work, 100=constant shaping)"
            },
            "overall_difficulty": {
                "type": "numeric",
                "scale": "0-100",
                "description": "Overall challenge level (0=very easy, 100=expert only)"
            },
            "beginner_friendly": {
                "type": "numeric",
                "scale": "0-100",
                "description": "Suitability for new golfers (0=not suitable, 100=very welcoming)"
            },
            "design_style": {
                "type": "categorical",
                "options": ["links", "parkland", "desert", "mountain", "forest", "resort", "heathland", "coastal"],
                "description": "Primary architectural design style"
            },
            "routing_style": {
                "type": "categorical",
                "options": ["traditional_out_back", "modern_loop", "natural_flow", "forced_routing"],
                "description": "How holes are connected and flow"
            }
        }

    def encode_image(self, image_path: str) -> str:
        """Encode image to base64 for API calls."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def normalize_to_100_scale(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Convert any scale to 0-100 for consistency."""

        normalized = {}

        # Handle different attribute names and scales
        conversions = {
            # 1-10 scale to 0-100
            'ball_findability_score': ('ball_findability', 10),
            'ball_findability': ('ball_findability', 10),
            'visual_tightness': ('visual_tightness', 10),
            'natural_integration': ('natural_integration', 10),
            'water_prominence': ('water_prominence', 10),
            'course_openness': ('course_openness', 10),
            'walkability': ('walkability', 10),
            'shot_shaping_required': ('shot_shaping_required', 10),
            'overall_difficulty': ('overall_difficulty', 10),
            'beginner_friendly': ('beginner_friendly', 10),
            'terrain_visual_complexity': ('terrain_visual_complexity', 10),
            'elevation_feature_prominence': ('elevation_feature_prominence', 10),

            # 1-5 scale to 0-100
            'tree_coverage_density': ('tree_coverage', 5),
            'tree_coverage': ('tree_coverage', 5),
        }

        for old_name, (new_name, max_val) in conversions.items():
            if old_name in attributes:
                value = attributes[old_name]
                if isinstance(value, (int, float)):
                    # If value seems to already be 0-100, keep it
                    if value > max_val:
                        normalized[new_name] = int(value)
                    else:
                        # Convert to 0-100 scale
                        normalized[new_name] = int((value / max_val) * 100)

        # Copy over categorical attributes as-is
        categorical = ['design_style', 'design_style_category', 'routing_style']
        for attr in categorical:
            if attr in attributes:
                clean_name = attr.replace('_category', '')
                normalized[clean_name] = attributes[attr]

        # Copy any other attributes that might be in correct scale
        for key, value in attributes.items():
            if key not in normalized and not key.endswith('_category') and not key.endswith('_score'):
                if isinstance(value, (int, float)) and value <= 100:
                    normalized[key] = int(value)
                elif isinstance(value, str):
                    normalized[key] = value

        return normalized

    def update_text_report_difficulty(self, analysis_dir: Path, new_difficulty: float) -> None:
        """Update the difficulty rating in the text report"""

        # Look in analysis_output directory
        report_files = list(analysis_dir.glob("comprehensive_analysis.txt"))
        if not report_files:
            # Try older naming pattern
            report_files = list(analysis_dir.glob("*_report.txt"))

        if not report_files:
            print(f"   ⚠️ No text report found in {analysis_dir}")
            return

        report_file = report_files[0]

        # Read the report
        with open(report_file, 'r') as f:
            content = f.read()

        # Determine category
        if new_difficulty <= 40:
            category = "EASY"
            skill = "ALL SKILL LEVELS"
        elif new_difficulty <= 70:
            category = "MODERATE"
            skill = "INTERMEDIATE TO ADVANCED PLAYERS"
        else:
            category = "CHALLENGING"
            skill = "ADVANCED PLAYERS ONLY"

        # Update difficulty rating line
        old_pattern = r'OVERALL DIFFICULTY RATING: \d+/100 \([A-Z]+\)'
        new_line = f'OVERALL DIFFICULTY RATING: {int(new_difficulty)}/100 ({category})'
        content = re.sub(old_pattern, new_line, content)

        # Update skill recommendation
        old_skill_pattern = r'Recommended for: [A-Z\s]+(?=\n)'
        new_skill = f'Recommended for: {skill}'
        content = re.sub(old_skill_pattern, new_skill, content)

        # Write updated report
        with open(report_file, 'w') as f:
            f.write(content)

        print(f"   ✅ Updated report difficulty: {new_difficulty}/100 ({category})")

    def save_vector_attributes(self, course_path: Path, vector_data: Dict[str, Any]) -> None:
        """Update existing comprehensive analysis with visual attributes"""

        # Look for analysis_output directory
        analysis_dir = course_path / "analysis_output"
        if not analysis_dir.exists():
            # Fallback to course directory itself for older structure
            analysis_dir = course_path

        # Find the main analysis file
        analysis_files = list(analysis_dir.glob("comprehensive_analysis.json"))
        if not analysis_files:
            # Try older naming pattern
            analysis_files = list(analysis_dir.glob("*_analysis.json"))

        if analysis_files:
            analysis_file = analysis_files[0]

            # Load existing analysis
            with open(analysis_file, 'r') as f:
                comprehensive_data = json.load(f)

            # Add/update course_vectors section
            comprehensive_data['course_vectors'] = vector_data.get('vector_attributes', {})
            comprehensive_data['vector_generation_timestamp'] = vector_data.get('generation_timestamp')

            # Initialize new_difficulty variable
            new_difficulty = None

            # RECALCULATE DIFFICULTY WITH VISION DATA
            if 'vector_attributes' in vector_data:
                new_difficulty = self.recalculate_difficulty_with_vision(
                    analysis_dir,
                    vector_data['vector_attributes'],
                    comprehensive_data
                )

                if new_difficulty is not None:
                    old_difficulty = comprehensive_data.get('difficulty_score', 'N/A')
                    comprehensive_data['difficulty_score'] = new_difficulty
                    comprehensive_data['difficulty_calculation'] = 'integrated_with_vision'
                    print(f"   📊 Updated difficulty: {old_difficulty} → {new_difficulty}/100")

                    # UPDATE THE TEXT REPORT TOO
                    self.update_text_report_difficulty(analysis_dir, new_difficulty)

            comprehensive_data['last_updated'] = datetime.now().isoformat()

            # Save updated analysis
            with open(analysis_file, 'w') as f:
                json.dump(comprehensive_data, f, indent=2)
            print(f"   ✅ Updated comprehensive analysis: {analysis_file.name}")

            # Also update the summary file - PRESERVE ALL EXISTING DATA
            summary_files = list(analysis_dir.glob("analysis_summary.json"))
            if not summary_files:
                # Try older naming pattern
                summary_files = list(analysis_dir.glob("*_summary.json"))

            if summary_files and new_difficulty is not None:
                summary_file = summary_files[0]
                with open(summary_file, 'r') as f:
                    summary_data = json.load(f)

                # ONLY UPDATE SPECIFIC FIELDS, DON'T OVERWRITE EVERYTHING
                # Add vision vectors
                summary_data['course_vectors'] = vector_data.get('vector_attributes', {})

                # Update difficulty score in key_metrics
                if 'key_metrics' not in summary_data:
                    summary_data['key_metrics'] = {}
                summary_data['key_metrics']['difficulty_score'] = new_difficulty

                # Update skill recommendation
                if 'recommendations' not in summary_data:
                    summary_data['recommendations'] = {}

                if new_difficulty <= 40:
                    skill_level = "All Skill Levels"
                elif new_difficulty <= 70:
                    skill_level = "Intermediate to Advanced"
                else:
                    skill_level = "Advanced Players Only"

                summary_data['recommendations']['skill_level'] = skill_level

                # PRESERVE ALL OTHER EXISTING DATA - Don't delete anything!
                # The landing_zone_safety, handedness_advantage, etc. should remain

                with open(summary_file, 'w') as f:
                    json.dump(summary_data, f, indent=2)
                print(f"   ✅ Updated summary: {summary_file.name}")

            # Update the text report with vector attributes (Section 6)
            self.update_text_report(analysis_dir, vector_data)
        else:
            print(f"   ⚠️ No analysis file found in {analysis_dir}")

    async def generate_vector_attributes(self, satellite_image: str,
                                       course_data: Dict = None) -> Dict[str, Any]:
        """
        Generate standardized vector attributes for golfer-course matching.
        ALL numeric attributes on 0-100 scale for consistency.
        """

        base64_image = self.encode_image(satellite_image)

        # Include relevant numerical context for calibration
        context = ""
        if course_data:
            strategic_data = course_data.get('strategic_analysis', {})
            avg_safety = None
            total_hazards = 0

            if strategic_data.get('course_strategy_summary'):
                landing_analysis = strategic_data['course_strategy_summary'].get('landing_zone_analysis', {})
                avg_safety = landing_analysis.get('average_hitter', {}).get('safety_percentage')

            if strategic_data.get('hole_analysis'):
                for hole_data in strategic_data['hole_analysis'].values():
                    bunkers = hole_data.get('bunker_strategy', {}).get('total_bunkers', 0)
                    total_hazards += bunkers

            context = f"""
NUMERICAL CONTEXT for calibration (don't repeat, use for scaling):
- Average landing zone safety: {avg_safety}%
- Total course hazards: {total_hazards}
- Course complexity level: {"high" if avg_safety and avg_safety < 60 else "moderate" if avg_safety and avg_safety < 75 else "low"}
"""

        vector_prompt = f"""
Generate STANDARDIZED VECTOR ATTRIBUTES for golfer-course matching from this aerial view.
ALL numeric scores must be on a 0-100 scale for consistency.
{context}

Return attributes in this EXACT JSON format with 0-100 scores:

{{
  "ball_findability": <0-100 integer>,
  "tree_coverage": <0-100 integer>,
  "visual_tightness": <0-100 integer>,
  "natural_integration": <0-100 integer>,
  "water_prominence": <0-100 integer>,
  "course_openness": <0-100 integer>,
  "walkability": <0-100 integer>,
  "shot_shaping_required": <0-100 integer>,
  "design_style": "<links|parkland|desert|mountain|forest|resort|heathland|coastal>",
  "routing_style": "<traditional_out_back|modern_loop|natural_flow|forced_routing>",
  "overall_difficulty": <0-100 integer>,
  "beginner_friendly": <0-100 integer>
}}

Analyze the aerial image and provide ONLY the JSON response with 0-100 scores.
"""

        result = await self._make_vector_call(base64_image, vector_prompt)

        # Normalize the results to 0-100 if they come back in wrong scale
        if result.get("status") == "success" and result.get("vector_attributes"):
            result["vector_attributes"] = self.normalize_to_100_scale(result["vector_attributes"])

        return result

    async def generate_elevation_attributes(self, elevation_image: str,
                                          elevation_data: Dict = None) -> Dict[str, Any]:
        """
        Generate visual attributes from elevation imagery that complement numerical analysis.
        """

        base64_image = self.encode_image(elevation_image)

        context = ""
        if elevation_data:
            net_change = elevation_data.get('net_elevation_change_m', 0)
            difficulty = elevation_data.get('difficulty_rating', 'unknown')
            context = f"""
ELEVATION CONTEXT for visual assessment:
- Measured elevation change: {net_change}m
- Statistical difficulty: {difficulty}
"""

        elevation_prompt = f"""
Generate VISUAL ELEVATION ATTRIBUTES that complement numerical terrain analysis.
ALL numeric scores must be on a 0-100 scale for consistency.
{context}

Return attributes in this EXACT JSON format with 0-100 scores:

{{
  "terrain_visual_complexity": <0-100 integer>,
  "elevation_feature_prominence": <0-100 integer>
}}

Analyze the elevation visualization and provide ONLY the JSON response with 0-100 scores.
"""

        result = await self._make_vector_call(base64_image, elevation_prompt)

        # Normalize the results to 0-100 if they come back in wrong scale
        if result.get("status") == "success" and result.get("vector_attributes"):
            result["vector_attributes"] = self.normalize_to_100_scale(result["vector_attributes"])

        return result

    async def _make_vector_call(self, base64_image: str, prompt: str) -> Dict[str, Any]:
        """Make API call optimized for structured vector attribute generation."""

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1000,
            "temperature": 0.1
        }

        try:
            # Create SSL context that bypasses certificate verification for macOS
            import ssl
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            # Create connector with the SSL context
            connector = aiohttp.TCPConnector(ssl=ssl_context)

            print(f"         🔄 Making API call to OpenAI...")
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(self.vision_endpoint, headers=headers, json=payload) as response:
                    print(f"         📡 Response status: {response.status}")

                    if response.status != 200:
                        error_text = await response.text()
                        print(f"         ❌ API Error {response.status}: {error_text[:500]}")
                        return {
                            "status": "failed",
                            "error": f"API returned {response.status}: {error_text[:500]}"
                        }

                    result = await response.json()
                    print(f"         ✅ Got response from API")

            if 'error' in result:
                print(f"         ❌ API returned error: {result['error']}")
                return {
                    "status": "failed",
                    "error": f"API error: {result['error']}"
                }

            response_content = result['choices'][0]['message']['content']

            # Extract JSON from response
            json_start = response_content.find('{')
            json_end = response_content.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                attributes_json = json.loads(response_content[json_start:json_end])

                return {
                    "vector_attributes": attributes_json,
                    "generation_timestamp": datetime.now().isoformat(),
                    "api_usage": result.get('usage', {}),
                    "status": "success"
                }
            else:
                return {
                    "status": "failed",
                    "error": "Could not extract JSON from response",
                    "raw_response": response_content[:500]
                }

        except Exception as e:
            print(f"         ❌ Error: {str(e)}")
            return {
                "status": "failed",
                "error": f"Vector generation failed: {str(e)}"
            }

    async def generate_course_vectors(self, course_path: Path,
                                    comprehensive_analysis: Dict = None,
                                    max_satellite_images: int = 3,
                                    max_elevation_images: int = 2,
                                    analyze_individual_holes: bool = False) -> Dict[str, Any]:
        """
        Generate complete set of vector attributes for a golf course.
        """

        print(f"🎯 Generating Vector Attributes: {course_path.name}")
        print(f"   📊 Strategic 3-image approach: overlay + aerial + elevation")

        results = {
            "course_path": str(course_path),
            "generation_timestamp": datetime.now().isoformat(),
            "analysis_config": {
                "max_satellite_images": max_satellite_images,
                "max_elevation_images": max_elevation_images,
                "analyze_individual_holes": analyze_individual_holes
            },
            "vector_attributes": {},
            "attribute_schemas": self.attribute_schemas
        }

        # Find all available images
        satellite_images = self._find_all_satellite_images(course_path)
        elevation_images = self._find_all_elevation_images(course_path)

        print(f"   📸 Found {len(satellite_images)} satellite images, {len(elevation_images)} elevation images")
        print(f"   🎯 Using best 2 satellite + 1 elevation (3 total images)")

        # Limit to requested number of images
        selected_satellite = satellite_images[:max_satellite_images]
        selected_elevation = elevation_images[:max_elevation_images]

        # Generate attributes from multiple satellite images
        if selected_satellite:
            print("   📊 Analyzing satellite images for course character")

            satellite_results = []
            for i, img_path in enumerate(selected_satellite):
                print(f"      • {img_path.name} ({i+1}/{len(selected_satellite)})")
                result = await self.generate_vector_attributes(str(img_path), comprehensive_analysis)
                if result.get("status") == "success":
                    satellite_results.append(result["vector_attributes"])

            # Combine results from multiple satellite images
            if satellite_results:
                combined_satellite = self._combine_satellite_attributes(satellite_results)
                results["vector_attributes"].update(combined_satellite)
                results["satellite_analysis_count"] = len(satellite_results)

        # Generate attributes from elevation images
        if selected_elevation:
            print("   🏔️ Analyzing elevation image for terrain characteristics")

            elevation_results = []
            elevation_analysis = self._get_elevation_context(comprehensive_analysis)

            for i, img_path in enumerate(selected_elevation):
                print(f"      • {img_path.name} ({i+1}/{len(selected_elevation)})")
                result = await self.generate_elevation_attributes(str(img_path), elevation_analysis)
                if result.get("status") == "success":
                    elevation_results.append(result["vector_attributes"])

            # Combine results from multiple elevation images
            if elevation_results:
                combined_elevation = self._combine_elevation_attributes(elevation_results)
                results["vector_attributes"].update(combined_elevation)
                results["elevation_analysis_count"] = len(elevation_results)

        # Validate attribute consistency
        if results["vector_attributes"]:
            validation_issues = self._validate_attribute_consistency(results["vector_attributes"])
            if validation_issues:
                results["validation_warnings"] = validation_issues
                print(f"   ⚠️ Detected {len(validation_issues)} consistency issues")
                for issue in validation_issues:
                    print(f"      • {issue}")

        return results

    def recalculate_difficulty_with_vision(self, analysis_dir: Path, vector_attrs: Dict, comprehensive_data: Dict) -> float:
        """Recalculate difficulty score incorporating vision data"""

        # Use the passed comprehensive_data instead of loading from file
        analysis = comprehensive_data

        # Calculate integrated difficulty
        score = 0

        # Data-based factors (60% weight)
        # Elevation (25 points max)
        elev_avg = analysis['elevation_analysis']['course_elevation_summary'].get('average_elevation_change_m', 0)
        score += min(25, (elev_avg / 3) * 10)

        # Wind (20 points max)
        wind_avg = analysis['weather_analysis'].get('avg_wind_kph', 0)
        score += min(20, (wind_avg / 50) * 20)

        # Hazards/Safety (15 points max)
        strategy = analysis.get('strategic_analysis', {}).get('course_strategy_summary', {})
        landing = strategy.get('landing_zone_analysis', {}).get('average_hitter', {})
        safety_pct = landing.get('safety_percentage', 50)
        score += (100 - safety_pct) * 0.15

        # Vision-based factors (40% weight)
        # Visual difficulty (25 points)
        visual_diff = vector_attrs.get('overall_difficulty', 50)
        score += visual_diff * 0.25

        # Visual tightness (8 points)
        tightness = vector_attrs.get('visual_tightness', 50)
        score += tightness * 0.08

        # Shot shaping required (7 points)
        shot_shaping = vector_attrs.get('shot_shaping_required', 50)
        score += shot_shaping * 0.07

        return round(min(100, score), 1)

    def _validate_attribute_consistency(self, attributes: Dict[str, Any]) -> List[str]:
        """Validate logical consistency between related attributes."""

        issues = []

        # Update for 0-100 scale
        tree_coverage = attributes.get("tree_coverage")
        course_openness = attributes.get("course_openness")
        visual_tightness = attributes.get("visual_tightness")
        ball_findability = attributes.get("ball_findability")
        water_prominence = attributes.get("water_prominence")
        design_style = attributes.get("design_style")

        if tree_coverage and course_openness:
            # Heavy tree coverage (>60) should mean less openness (<40)
            if tree_coverage >= 60 and course_openness >= 60:
                issues.append(f"High tree coverage ({tree_coverage}) but high openness ({course_openness}) seems inconsistent")

        if tree_coverage and ball_findability:
            # Heavy tree coverage (>60) should mean lower findability (<40)
            if tree_coverage >= 60 and ball_findability >= 60:
                issues.append(f"Heavy tree coverage ({tree_coverage}) but high ball findability ({ball_findability}) seems inconsistent")

        if water_prominence and design_style:
            # Desert courses shouldn't have high water prominence
            if design_style == "desert" and water_prominence >= 50:
                issues.append(f"Desert course style but high water prominence ({water_prominence}) seems inconsistent")

        return issues

    def update_text_report(self, analysis_dir: Path, vector_data: Dict[str, Any]) -> None:
        """Update the text report with vector attributes."""

        # Find the report file in analysis_output directory
        report_files = list(analysis_dir.glob("comprehensive_analysis.txt"))
        if not report_files:
            # Try older naming pattern
            report_files = list(analysis_dir.glob("*_report.txt"))

        if not report_files:
            print(f"   ⚠️ No text report found in {analysis_dir}")
            return

        report_file = report_files[0]

        # Read existing report
        with open(report_file, 'r') as f:
            lines = f.readlines()

        # Check if vector section already exists
        content = ''.join(lines)
        if "COURSE VECTOR ATTRIBUTES" in content:
            print(f"   ℹ️ Vector attributes already in report, skipping update")
            return

        # Find where to insert (before END OF ANALYSIS)
        insert_index = -1
        for i, line in enumerate(lines):
            if "END OF ANALYSIS" in line:
                insert_index = i
                break

        if insert_index == -1:
            print(f"   ⚠️ Could not find insertion point in report")
            return

        # Create vector attributes section
        attrs = vector_data.get('vector_attributes', {})
        vector_section = []

        vector_section.append("\n" + "="*100 + "\n")
        vector_section.append("\nSECTION 6: COURSE VECTOR ATTRIBUTES (AI Vision Analysis)\n")
        vector_section.append("="*100 + "\n\n")

        # Course Character (0-100 scale)
        vector_section.append("COURSE CHARACTER SCORES (0-100 scale):\n")
        vector_section.append("-" * 50 + "\n")

        ball_find = attrs.get('ball_findability', 'N/A')
        if ball_find != 'N/A':
            desc = '(Easy to find balls)' if ball_find > 66 else '(Moderate ball search)' if ball_find > 33 else '(Frequent lost balls)'
            vector_section.append(f"• Ball Findability:        {ball_find:>3}/100 {desc}\n")

        tree_cov = attrs.get('tree_coverage', 'N/A')
        if tree_cov != 'N/A':
            desc = '(Heavily forested)' if tree_cov > 66 else '(Moderate trees)' if tree_cov > 33 else '(Open course)'
            vector_section.append(f"• Tree Coverage:           {tree_cov:>3}/100 {desc}\n")

        vis_tight = attrs.get('visual_tightness', 'N/A')
        if vis_tight != 'N/A':
            desc = '(Very tight corridors)' if vis_tight > 66 else '(Moderate width)' if vis_tight > 33 else '(Wide open)'
            vector_section.append(f"• Visual Tightness:        {vis_tight:>3}/100 {desc}\n")

        course_open = attrs.get('course_openness', 'N/A')
        if course_open != 'N/A':
            desc = '(Spacious feeling)' if course_open > 66 else '(Mixed open/enclosed)' if course_open > 33 else '(Claustrophobic)'
            vector_section.append(f"• Course Openness:         {course_open:>3}/100 {desc}\n")

        nat_int = attrs.get('natural_integration', 'N/A')
        if nat_int != 'N/A':
            desc = '(Follows natural contours)' if nat_int > 66 else '(Mix natural/artificial)' if nat_int > 33 else '(Heavily artificial)'
            vector_section.append(f"• Natural Integration:     {nat_int:>3}/100 {desc}\n")

        water_prom = attrs.get('water_prominence', 'N/A')
        if water_prom != 'N/A':
            desc = '(Water dominates play)' if water_prom > 50 else '(Some water hazards)' if water_prom > 20 else '(Minimal water)'
            vector_section.append(f"• Water Prominence:        {water_prom:>3}/100 {desc}\n")

        vector_section.append("\nPLAYABILITY SCORES (0-100 scale):\n")
        vector_section.append("-" * 50 + "\n")

        overall_diff = attrs.get('overall_difficulty', 'N/A')
        if overall_diff != 'N/A':
            desc = '(Expert level)' if overall_diff > 66 else '(Moderate challenge)' if overall_diff > 33 else '(Beginner friendly)'
            vector_section.append(f"• Overall Difficulty:      {overall_diff:>3}/100 {desc}\n")

        beginner = attrs.get('beginner_friendly', 'N/A')
        if beginner != 'N/A':
            desc = '(Very welcoming)' if beginner > 66 else '(Playable)' if beginner > 33 else '(Challenging for beginners)'
            vector_section.append(f"• Beginner Friendly:       {beginner:>3}/100 {desc}\n")

        walk = attrs.get('walkability', 'N/A')
        if walk != 'N/A':
            desc = '(Easy pleasant walk)' if walk > 66 else '(Moderate walking)' if walk > 33 else '(Difficult to walk)'
            vector_section.append(f"• Walkability:             {walk:>3}/100 {desc}\n")

        shot_shape = attrs.get('shot_shaping_required', 'N/A')
        if shot_shape != 'N/A':
            desc = '(Constant shaping needed)' if shot_shape > 66 else '(Some shaping needed)' if shot_shape > 33 else '(Mostly straight shots)'
            vector_section.append(f"• Shot Shaping Required:   {shot_shape:>3}/100 {desc}\n")

        # Terrain complexity if available
        if 'terrain_visual_complexity' in attrs:
            vector_section.append("\nTERRAIN ANALYSIS (0-100 scale):\n")
            vector_section.append("-" * 50 + "\n")

            terrain_comp = attrs.get('terrain_visual_complexity', 'N/A')
            if terrain_comp != 'N/A':
                desc = '(Complex varied terrain)' if terrain_comp > 66 else '(Moderate variation)' if terrain_comp > 33 else '(Simple uniform terrain)'
                vector_section.append(f"• Terrain Complexity:      {terrain_comp:>3}/100 {desc}\n")

            elev_prom = attrs.get('elevation_feature_prominence', 'N/A')
            if elev_prom != 'N/A':
                desc = '(Dramatic elevation)' if elev_prom > 66 else '(Noticeable features)' if elev_prom > 33 else '(Subtle changes)'
                vector_section.append(f"• Elevation Prominence:    {elev_prom:>3}/100 {desc}\n")

        vector_section.append("\nCOURSE STYLE:\n")
        vector_section.append("-" * 50 + "\n")
        design = attrs.get('design_style', 'N/A')
        if design != 'N/A':
            vector_section.append(f"• Design Style:            {design.title()}\n")
        routing = attrs.get('routing_style', 'N/A')
        if routing != 'N/A':
            vector_section.append(f"• Routing Style:           {routing.replace('_', ' ').title()}\n")

        # Add key insights based on scores
        vector_section.append("\nKEY INSIGHTS FROM VISUAL ANALYSIS:\n")
        vector_section.append("-" * 50 + "\n")

        insights = []

        # Ball loss risk
        if isinstance(ball_find, (int, float)):
            if ball_find < 40:
                insights.append("⚠️ High ball loss risk - bring extra balls")
            elif ball_find > 70:
                insights.append("✓ Low ball loss risk - forgiving for wayward shots")

        # Tree impact
        if isinstance(tree_cov, (int, float)):
            if tree_cov > 60:
                insights.append("🌳 Heavily wooded - accuracy crucial off the tee")
            elif tree_cov < 30:
                insights.append("☀️ Open layout - driver-friendly course")

        # Difficulty assessment
        if isinstance(overall_diff, (int, float)):
            if overall_diff > 70:
                insights.append("🎯 Expert-level challenge - not for beginners")
            elif overall_diff < 40:
                insights.append("👍 Beginner-friendly - great for learning")

        # Shot shaping
        if isinstance(shot_shape, (int, float)):
            if shot_shape > 60:
                insights.append("🔄 Requires shot shaping - work the ball both ways")

        # Water hazards
        if isinstance(water_prom, (int, float)):
            if water_prom > 50:
                insights.append("💧 Water comes into play frequently")

        for insight in insights:
            vector_section.append(f"{insight}\n")

        vector_section.append("\n" + "="*100 + "\n")

        # Insert the new section before END OF ANALYSIS
        lines[insert_index:insert_index] = vector_section

        # Write updated report
        with open(report_file, 'w') as f:
            f.writelines(lines)

        print(f"   ✅ Updated text report: {report_file.name}")

    def _find_all_satellite_images(self, course_path: Path) -> List[Path]:
        """Find satellite images - always naip_overlay.png and naip_image.jpg in main directory."""

        satellite_images = []

        # Look for the two specific satellite image files in main directory
        naip_overlay = course_path / "naip_overlay.png"
        if naip_overlay.exists():
            satellite_images.append(naip_overlay)

        naip_image = course_path / "naip_image.jpg"
        if naip_image.exists():
            satellite_images.append(naip_image)

        return satellite_images[:2]

    def _find_all_elevation_images(self, course_path: Path) -> List[Path]:
        """Find elevation image - always elevation_overlay.png in main directory."""

        elevation_images = []

        # Look for the specific elevation image file in main directory
        elevation_overlay = course_path / "elevation_overlay.png"
        if elevation_overlay.exists():
            elevation_images.append(elevation_overlay)

        return elevation_images[:1]

    def _combine_satellite_attributes(self, satellite_results: List[Dict]) -> Dict[str, Any]:
        """Combine attributes from multiple satellite image analyses."""

        if not satellite_results:
            return {}

        if len(satellite_results) == 1:
            return satellite_results[0]

        print(f"      🔄 Combining insights from {len(satellite_results)} satellite analyses")

        combined = {}

        # Updated numeric attributes for 0-100 scale
        numeric_attrs = [
            "ball_findability", "tree_coverage", "visual_tightness",
            "natural_integration", "water_prominence", "course_openness",
            "walkability", "shot_shaping_required", "overall_difficulty", "beginner_friendly"
        ]

        for attr in numeric_attrs:
            values = [result.get(attr) for result in satellite_results if result.get(attr) is not None]
            if values:
                combined[attr] = int(round(sum(values) / len(values)))

        # Categorical attributes
        categorical_attrs = ["design_style", "routing_style"]

        for attr in categorical_attrs:
            values = [result.get(attr) for result in satellite_results if result.get(attr) is not None]
            if values:
                most_common = Counter(values).most_common(1)
                combined[attr] = most_common[0][0]

        combined["analysis_method"] = f"combined_from_{len(satellite_results)}_satellite_images"

        return combined

    def _combine_elevation_attributes(self, elevation_results: List[Dict]) -> Dict[str, Any]:
        """Combine attributes from multiple elevation image analyses."""

        if not elevation_results:
            return {}

        if len(elevation_results) == 1:
            return elevation_results[0]

        print(f"      🔄 Combining insights from {len(elevation_results)} elevation analyses")

        combined = {}

        elevation_attrs = [
            "terrain_visual_complexity", "elevation_feature_prominence"
        ]

        for attr in elevation_attrs:
            values = [result.get(attr) for result in elevation_results if result.get(attr) is not None]
            if values:
                combined[attr] = int(round(sum(values) / len(values)))

        combined["analysis_method"] = f"combined_from_{len(elevation_results)}_elevation_images"

        return combined

    def _get_elevation_context(self, comprehensive_analysis: Dict) -> Optional[Dict]:
        """Extract elevation context from comprehensive analysis."""

        if not comprehensive_analysis:
            return None

        elev_data = comprehensive_analysis.get('elevation_analysis', {})
        hole_analysis = elev_data.get('hole_elevation_analysis', {})

        if hole_analysis:
            return next(iter(hole_analysis.values()))

        return None

# BATCH PROCESSING FUNCTIONS

async def process_all_ma_courses():
    """Process all Massachusetts golf courses for vector attribute generation."""

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    if not OPENAI_API_KEY:
        print("❌ OPENAI_API_KEY environment variable required")
        return

    generator = GolfCourseVectorGenerator(OPENAI_API_KEY)

    golf_results_dir = Path("./golf_results")
    if not golf_results_dir.exists():
        print(f"❌ Golf results directory not found: {golf_results_dir}")
        return

    ma_courses = []
    for course_dir in golf_results_dir.iterdir():
        if course_dir.is_dir() and course_dir.name.startswith("MA-"):
            ma_courses.append(course_dir)

    if not ma_courses:
        print("❌ No MA course directories found")
        return

    print(f"🏌️ Found {len(ma_courses)} Massachusetts courses to process")
    print("=" * 60)

    successful_courses = []
    failed_courses = []
    skipped_courses = []

    for i, course_path in enumerate(ma_courses, 1):
        print(f"\n[{i}/{len(ma_courses)}] Processing: {course_path.name}")
        print("-" * 40)

        try:
            # Check for analysis_output directory
            analysis_dir = course_path / "analysis_output"
            if not analysis_dir.exists():
                analysis_dir = course_path  # Fallback to course directory

            # Load existing comprehensive analysis first
            analysis_files = list(analysis_dir.glob("comprehensive_analysis.json"))
            if not analysis_files:
                analysis_files = list(analysis_dir.glob("*_analysis.json"))

            comprehensive_data = None

            if analysis_files:
                with open(analysis_files[0], 'r') as f:
                    comprehensive_data = json.load(f)
                print(f"   📊 Loaded existing analysis: {analysis_files[0].name}")

                # Check if vectors already exist AFTER loading
                if 'course_vectors' in comprehensive_data and comprehensive_data['course_vectors']:
                    print(f"   ⚠️ Vector attributes already exist in analysis, skipping...")
                    skipped_courses.append(course_path.name)
                    continue  # Skip to next course
            else:
                print(f"   ⚠️ No comprehensive analysis found, proceeding without context")

            # Only generate if we didn't skip above
            print(f"   🚀 Starting vector attribute generation...")
            start_time = time.time()

            vector_results = await generator.generate_course_vectors(
                course_path,
                comprehensive_data,
                max_satellite_images=2,
                max_elevation_images=1
            )

            elapsed_time = time.time() - start_time

            if vector_results.get("vector_attributes"):
                generator.save_vector_attributes(course_path, vector_results)
                print(f"   ✅ Success! Generated vector attributes in {elapsed_time:.1f}s")

                attrs = vector_results["vector_attributes"]
                print(f"      🎯 Ball findability: {attrs.get('ball_findability', 'N/A')}/100")
                print(f"      🌳 Tree coverage: {attrs.get('tree_coverage', 'N/A')}/100")
                print(f"      🏞️ Course style: {attrs.get('design_style', 'N/A')}")
                print(f"      📈 Overall difficulty: {attrs.get('overall_difficulty', 'N/A')}/100")

                successful_courses.append(course_path.name)
            else:
                print(f"   ❌ Failed: No vector attributes generated")
                failed_courses.append(course_path.name)

        except Exception as e:
            print(f"   ❌ Error processing {course_path.name}: {str(e)}")
            failed_courses.append(course_path.name)

        if i < len(ma_courses):
            print(f"   ⏱️ Waiting 2 seconds before next course...")
            await asyncio.sleep(2)


    # Final summary
    print("\n" + "=" * 60)
    print("🏁 BATCH PROCESSING COMPLETE")
    print("=" * 60)
    print(f"✅ Successful: {len(successful_courses)} courses")
    print(f"⚠️ Skipped: {len(skipped_courses)} courses")
    print(f"❌ Failed: {len(failed_courses)} courses")

    summary_report = {
        "processing_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_courses": len(ma_courses),
        "successful": len(successful_courses),
        "skipped": len(skipped_courses),
        "failed": len(failed_courses),
        "successful_courses": successful_courses,
        "skipped_courses": skipped_courses,
        "failed_courses": failed_courses
    }

    summary_file = golf_results_dir / "ma_vector_processing_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary_report, f, indent=2)

    print(f"\n📄 Summary report saved: {summary_file}")

async def main():
    """Main function with options for different processing modes."""
    import sys

    if len(sys.argv) > 1:
        course_names = sys.argv[1:]
        print(f"🎯 Processing specific courses: {', '.join(course_names)}")
        # You can implement specific course processing here if needed
    else:
        print(f"🎯 Processing ALL Massachusetts courses")
        await process_all_ma_courses()

if __name__ == "__main__":
    asyncio.run(main())
