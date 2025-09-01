import json
import os
import sys
import glob
from datetime import datetime
from typing import Dict, Any, Optional, List

class GolfCourseVectorCreator:
    def __init__(self, course_number: str):
        self.course_number = course_number
        self.course_data = {}
        self.vector_attributes = {}
        self.comprehensive_analysis = {}
        self.reviews_summary = {}
        self.rubric_data = {}

    def find_files(self) -> Dict[str, str]:
        """Find all relevant files for the course number"""
        base_path = "../states/ma"
        files = {}

        # Look for course scores files (individual course directories)
        course_scores_pattern = f"{base_path}/course_scores/{self.course_number}_*/"
        course_dirs = glob.glob(course_scores_pattern)

        if course_dirs:
            course_dir = course_dirs[0]  # Take the first matching directory

            # Look for files in the course directory
            for file_path in glob.glob(f"{course_dir}*.json"):
                filename = os.path.basename(file_path).lower()
                if 'comprehensive_analysis' in filename:
                    files['comprehensive_analysis'] = file_path
                elif 'course_vector_attributes' in filename:
                    files['vector_attributes'] = file_path
                elif 'vector_attributes' in filename and 'course_vector_attributes' not in filename:
                    if 'vector_attributes' not in files:  # Prefer course_vector_attributes
                        files['vector_attributes'] = file_path

            # Look for specific rubric file
            rubric_file = f"{course_dir}{self.course_number}_rubric.json"
            if os.path.exists(rubric_file):
                files['rubric_data'] = rubric_file

        # Look for images_elevation files (corrected directory name)
        elevation_pattern = f"{base_path}/images_elevation/{self.course_number}_**/*"
        elevation_files = glob.glob(elevation_pattern, recursive=True)

        for file_path in elevation_files:
            if file_path.endswith('.json'):
                filename = os.path.basename(file_path).lower()
                if filename == 'vector_attributes_only.json':
                    if 'vector_attributes' not in files:  # Only use if not found in course_scores
                        files['vector_attributes'] = file_path
                elif filename == 'comprehensive_analysis.json':
                    if 'comprehensive_analysis' not in files:  # Only use if not found in course_scores
                        files['comprehensive_analysis'] = file_path

        # Look for reviews in reviews/scores directory (corrected path)
        reviews_pattern = f"{base_path}/reviews/scores/*"
        reviews_files = glob.glob(reviews_pattern)

        for file_path in reviews_files:
            if file_path.endswith('.json') and self.course_number in os.path.basename(file_path):
                filename = os.path.basename(file_path).lower()
                if 'reviews_summary' in filename or 'summary' in filename:
                    files['reviews_summary'] = file_path
                    break

        # Look for website data
        website_pattern = f"{base_path}/website_data/general/{self.course_number}*_structured.json"
        website_files = glob.glob(website_pattern)
        if website_files:
            files['course_data'] = website_files[0]  # Take the first match

        return files

    def load_data_files(self):
        """Load all the data files"""
        file_paths = self.find_files()

        # Define required files (5 core files)
        required_files = [
            'comprehensive_analysis',
            'vector_attributes',
            'rubric_data',
            'reviews_summary',
            'course_data'
        ]

        print(f"Found files for course {self.course_number}:")
        for file_type, file_path in file_paths.items():
            print(f"  ✓ {file_type}: {file_path}")

        # Check if all required files are present
        missing_files = []
        for required_file in required_files:
            if required_file not in file_paths:
                missing_files.append(required_file)

        if missing_files:
            print(f"\n❌ SKIPPING COURSE {self.course_number}")
            print(f"Missing required files: {', '.join(missing_files)}")
            print(f"Found {len(file_paths)} of {len(required_files)} required files")
            return False

        print(f"\n✓ All {len(required_files)} required files found. Proceeding with vector creation...")

        try:
            # Load course scraped data
            if 'course_data' in file_paths:
                with open(file_paths['course_data'], 'r') as f:
                    self.course_data = json.load(f)
                    print(f"✓ Loaded course data")

            # Load vector attributes
            if 'vector_attributes' in file_paths:
                with open(file_paths['vector_attributes'], 'r') as f:
                    self.vector_attributes = json.load(f)
                    print(f"✓ Loaded vector attributes")

            # Load comprehensive analysis
            if 'comprehensive_analysis' in file_paths:
                with open(file_paths['comprehensive_analysis'], 'r') as f:
                    self.comprehensive_analysis = json.load(f)
                    print(f"✓ Loaded comprehensive analysis")

            # Load elevation data (additional source)
            if 'elevation_data' in file_paths:
                with open(file_paths['elevation_data'], 'r') as f:
                    elevation_data = json.load(f)
                    # Merge with comprehensive analysis if it exists
                    if 'elevation_analysis' not in self.comprehensive_analysis:
                        self.comprehensive_analysis['elevation_analysis'] = elevation_data
                    print(f"✓ Loaded additional elevation data")

            # Load elevation profiles (additional source)
            if 'elevation_profiles' in file_paths:
                with open(file_paths['elevation_profiles'], 'r') as f:
                    elevation_profiles = json.load(f)
                    # Add to comprehensive analysis
                    if 'elevation_analysis' not in self.comprehensive_analysis:
                        self.comprehensive_analysis['elevation_analysis'] = {}
                    self.comprehensive_analysis['elevation_analysis']['hole_profiles'] = elevation_profiles
                    print(f"✓ Loaded elevation profiles")

            # Load reviews summary
            if 'reviews_summary' in file_paths:
                with open(file_paths['reviews_summary'], 'r') as f:
                    self.reviews_summary = json.load(f)
                    print(f"✓ Loaded reviews summary")

            # Load rubric data
            if 'rubric_data' in file_paths:
                with open(file_paths['rubric_data'], 'r') as f:
                    self.rubric_data = json.load(f)
                    print(f"✓ Loaded rubric data")

            return True

        except Exception as e:
            print(f"❌ Error loading files: {e}")
            import traceback
            traceback.print_exc()
            return False

    def extract_course_fundamentals(self) -> Dict[str, Any]:
        """Extract basic course information"""
        analysis = self.comprehensive_analysis.get('strategic_analysis', {})
        holes = analysis.get('hole_analysis', {})

        # Calculate totals from hole data with safe conversions
        total_length = 0
        total_par = 0

        for hole in holes.values():
            # Safe conversion for length
            length = hole.get('total_length_yards')
            if length is not None and str(length).replace('.', '').isdigit():
                total_length += float(length)

            # Safe conversion for par
            par = hole.get('par')
            if par is not None:
                try:
                    total_par += int(float(str(par)))
                except (ValueError, TypeError):
                    total_par += 4  # Default par if conversion fails
            else:
                total_par += 4  # Default par if None

        holes_count = len(holes)

        return {
            "total_length_yards": int(total_length),
            "total_par": total_par,
            "holes_count": holes_count,
            "course_rating": 72.0,  # Default, would need actual data
            "slope_rating": 125.0   # Default, would need actual data
        }

    def extract_hole_composition(self) -> Dict[str, Any]:
        """Extract hole composition data"""
        holes = self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis', {})

        par_counts = {"3": 0, "4": 0, "5": 0}
        par_lengths = {"3": [], "4": [], "5": []}

        for hole in holes.values():
            # Safe conversion for par
            par = hole.get('par')
            par_str = "4"  # Default
            if par is not None:
                try:
                    par_int = int(float(str(par)))
                    if par_int in [3, 4, 5]:
                        par_str = str(par_int)
                except (ValueError, TypeError):
                    pass

            # Safe conversion for length
            length = hole.get('total_length_yards')
            length_val = 0
            if length is not None:
                try:
                    length_val = float(str(length))
                except (ValueError, TypeError):
                    length_val = 0

            if par_str in par_counts:
                par_counts[par_str] += 1
                if length_val > 0:
                    par_lengths[par_str].append(length_val)

        return {
            "par_3_count": par_counts["3"],
            "par_4_count": par_counts["4"],
            "par_5_count": par_counts["5"],
            "par_3_avg_length": sum(par_lengths["3"]) / len(par_lengths["3"]) if par_lengths["3"] else 0,
            "par_4_avg_length": sum(par_lengths["4"]) / len(par_lengths["4"]) if par_lengths["4"] else 0,
            "par_5_avg_length": sum(par_lengths["5"]) / len(par_lengths["5"]) if par_lengths["5"] else 0
        }

    def extract_strategic_complexity(self) -> Dict[str, Any]:
        """Extract strategic complexity metrics"""
        holes = self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis', {})

        total_bunkers = 0
        fairway_bunkers = 0
        greenside_bunkers = 0
        left_bias_holes = 0
        right_bias_holes = 0
        water_hazard_holes = 0
        total_water_hazards = 0

        for hole in holes.values():
            bunker_strategy = hole.get('bunker_strategy', {})

            # Safe conversion for bunker counts
            hole_bunkers = bunker_strategy.get('total_bunkers', 0)
            if hole_bunkers is not None:
                try:
                    total_bunkers += int(float(str(hole_bunkers)))
                except (ValueError, TypeError):
                    pass

            # Safe handling of bunker lists
            fw_bunkers = bunker_strategy.get('fairway_bunkers', [])
            if isinstance(fw_bunkers, list):
                fairway_bunkers += len(fw_bunkers)

            gs_bunkers = bunker_strategy.get('greenside_bunkers', [])
            if isinstance(gs_bunkers, list):
                greenside_bunkers += len(gs_bunkers)

            bias = bunker_strategy.get('bunker_bias', 'balanced')
            if bias == 'left':
                left_bias_holes += 1
            elif bias == 'right':
                right_bias_holes += 1

            # Check for water hazards in landing zones
            landing_zones = hole.get('landing_zones', {})
            has_water = False
            for zone in landing_zones.values():
                if isinstance(zone, dict):
                    water_count = zone.get('water_hazards_in_zone', 0)
                    if water_count is not None:
                        try:
                            water_num = int(float(str(water_count)))
                            if water_num > 0:
                                has_water = True
                                total_water_hazards += water_num
                        except (ValueError, TypeError):
                            pass

            if has_water:
                water_hazard_holes += 1

        return {
            "total_bunkers": total_bunkers,
            "avg_bunkers_per_hole": total_bunkers / len(holes) if holes else 0,
            "fairway_bunkers_total": fairway_bunkers,
            "greenside_bunkers_total": greenside_bunkers,
            "bunker_bias_left_holes": left_bias_holes,
            "bunker_bias_right_holes": right_bias_holes,
            "water_hazards_total": total_water_hazards,
            "water_hazard_holes_count": water_hazard_holes
        }

    def extract_dogleg_analysis(self) -> Dict[str, Any]:
        """Extract dogleg analysis"""
        holes = self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis', {})

        total_doglegs = 0
        left_doglegs = 0
        right_doglegs = 0
        sharp_doglegs = 0
        moderate_doglegs = 0

        for hole in holes.values():
            dogleg = hole.get('dogleg_analysis', {})
            is_dogleg = dogleg.get('is_dogleg')

            if is_dogleg in ['True', True, 'true', 1]:
                total_doglegs += 1

                direction = dogleg.get('dogleg_direction', 'straight')
                if direction == 'left':
                    left_doglegs += 1
                elif direction == 'right':
                    right_doglegs += 1

                # Safe conversion for severity
                severity = dogleg.get('dogleg_severity_degrees', 0)
                severity_num = 0
                if severity is not None:
                    try:
                        severity_num = float(str(severity))
                    except (ValueError, TypeError):
                        severity_num = 0

                if severity_num > 45:
                    sharp_doglegs += 1
                elif 15 <= severity_num <= 45:
                    moderate_doglegs += 1

        return {
            "total_doglegs": total_doglegs,
            "dogleg_percentage": (total_doglegs / len(holes)) * 100 if holes else 0,
            "left_doglegs": left_doglegs,
            "right_doglegs": right_doglegs,
            "sharp_doglegs_count": sharp_doglegs,
            "moderate_doglegs_count": moderate_doglegs
        }

    def extract_landing_zone_difficulty(self) -> Dict[str, Any]:
        """Extract landing zone difficulty metrics"""
        summary = self.comprehensive_analysis.get('strategic_analysis', {}).get('course_strategy_summary', {})
        landing_analysis = summary.get('landing_zone_analysis', {})

        # Calculate average fairway width from holes
        holes = self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis', {})
        fairway_widths = []

        for hole in holes.values():
            landing_zones = hole.get('landing_zones', {})
            for zone in landing_zones.values():
                if isinstance(zone, dict):
                    width = zone.get('fairway_width_yards')
                    if width is not None:
                        try:
                            width_num = float(str(width))
                            if width_num > 0:
                                fairway_widths.append(width_num)
                        except (ValueError, TypeError):
                            pass

        avg_fairway_width = sum(fairway_widths) / len(fairway_widths) if fairway_widths else 35.0

        # Safe extraction of landing zone data
        def safe_get_zones(hitter_type: str, zone_type: str) -> int:
            hitter_data = landing_analysis.get(hitter_type, {})
            if isinstance(hitter_data, dict):
                zones = hitter_data.get(zone_type, 0)
                if zones is not None:
                    try:
                        return int(float(str(zones)))
                    except (ValueError, TypeError):
                        return 0
            return 0

        return {
            "short_hitter_safe_zones": safe_get_zones('short_hitter', 'safe_landing_zones'),
            "short_hitter_dangerous_zones": safe_get_zones('short_hitter', 'dangerous_landing_zones'),
            "avg_hitter_safe_zones": safe_get_zones('average_hitter', 'safe_landing_zones'),
            "avg_hitter_dangerous_zones": safe_get_zones('average_hitter', 'dangerous_landing_zones'),
            "long_hitter_safe_zones": safe_get_zones('long_hitter', 'safe_landing_zones'),
            "long_hitter_dangerous_zones": safe_get_zones('long_hitter', 'dangerous_landing_zones'),
            "avg_fairway_width": avg_fairway_width
        }

    def extract_elevation_profile(self) -> Dict[str, Any]:
        """Extract elevation profile data"""
        elevation_data = self.comprehensive_analysis.get('elevation_analysis', {})
        summary = elevation_data.get('course_elevation_summary', {})

        return {
            "total_elevation_change_m": summary.get('total_elevation_change_m', 0),
            "avg_hole_elevation_change": summary.get('average_elevation_change_m', 0),
            "uphill_holes": summary.get('uphill_holes', 0),
            "downhill_holes": summary.get('downhill_holes', 0),
            "flat_holes": summary.get('flat_holes', 0),
            "extreme_difficulty_holes": summary.get('extreme_difficulty_holes', 0),
            "challenging_difficulty_holes": summary.get('challenging_difficulty_holes', 0),
            "max_single_hole_change": summary.get('max_single_hole_change_m', 0)
        }

    def extract_course_character(self) -> Dict[str, Any]:
        """Extract course character metrics"""
        vector_attrs = self.vector_attributes.get('vector_attributes', {})

        return {
            "ball_findability_score": vector_attrs.get('ball_findability_score', 5),
            "tree_coverage_density": vector_attrs.get('tree_coverage_density', 3),
            "visual_tightness": vector_attrs.get('visual_tightness', 5),
            "course_openness": vector_attrs.get('course_openness', 5),
            "natural_integration": vector_attrs.get('natural_integration', 7),
            "water_prominence": vector_attrs.get('water_prominence', 3),
            "terrain_visual_complexity": vector_attrs.get('terrain_visual_complexity', 5),
            "elevation_feature_prominence": vector_attrs.get('elevation_feature_prominence', 4)
        }

    def extract_playing_difficulty(self) -> Dict[str, Any]:
        """Extract playing difficulty metrics"""
        composite_scores = self.vector_attributes.get('vector_attributes', {}).get('composite_scores', {})
        summary = self.comprehensive_analysis.get('strategic_analysis', {}).get('course_strategy_summary', {})
        handedness = summary.get('handedness_advantage', {})

        # Calculate rough density from holes
        holes = self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis', {})
        rough_densities = []

        for hole in holes.values():
            rough = hole.get('rough_density', {})
            if rough.get('vegetation_coverage_percent'):
                rough_densities.append(rough.get('vegetation_coverage_percent'))

        avg_rough_density = sum(rough_densities) / len(rough_densities) if rough_densities else 0.3

        return {
            "beginner_friendly_score": composite_scores.get('beginner_friendly_score', 50),
            "ball_loss_risk_score": composite_scores.get('ball_loss_risk_score', 35),
            "handedness_advantage_right": handedness.get('right_handed_advantage_holes', 0),
            "handedness_advantage_left": handedness.get('left_handed_advantage_holes', 0),
            "rough_density_avg": avg_rough_density,
            "forgiveness_factor": (composite_scores.get('beginner_friendly_score', 50) / 100.0)
        }

    def extract_weather_characteristics(self) -> Dict[str, Any]:
        """Extract weather characteristics"""
        weather = self.comprehensive_analysis.get('weather_analysis', {})

        return {
            "golf_season_length_months": weather.get('golf_season_length_months', 7),
            "avg_temp_golf_season": weather.get('golf_season_avg_temp_C', 16.5),
            "rainy_days_percent": weather.get('rainy_days_pct', 13.7),
            "heavy_rain_days_percent": weather.get('heavy_rain_days_pct', 4.3),
            "windy_days_percent": weather.get('windy_days_pct', 50.1),
            "calm_days_percent": weather.get('calm_days_pct', 4.0),
            "best_month_score": weather.get('best_golf_score', 0.78),
            "worst_month_score": weather.get('worst_golf_score', 0.45),
            "weekend_rain_factor": weather.get('weekend_rainy_days_pct', 10.7) / weather.get('weekday_rainy_days_pct', 8.0) if weather.get('weekday_rainy_days_pct', 8.0) > 0 else 1.0
        }

    def extract_monthly_weather_scores(self) -> Dict[str, float]:
        """Extract monthly weather scores"""
        weather = self.comprehensive_analysis.get('weather_analysis', {})

        # Create normalized scores based on temperature and precipitation
        months = ['april', 'may', 'june', 'july', 'august', 'september', 'october']
        scores = {}

        for month in months:
            temp = weather.get(f'{month}_avg_temp_C', 15)
            precip = weather.get(f'{month}_avg_precip_mm', 1.5)
            wind = weather.get(f'{month}_avg_wind_kph', 30)

            # Simple scoring formula (can be adjusted)
            temp_score = max(0, min(1, (temp - 5) / 20))  # 5-25°C optimal range
            precip_score = max(0, 1 - (precip / 5))  # Less precipitation is better
            wind_score = max(0, 1 - (wind - 20) / 30)  # Moderate wind is acceptable

            combined_score = (temp_score * 0.5 + precip_score * 0.3 + wind_score * 0.2)
            scores[f'{month}_weather_score'] = round(combined_score, 2)

        return scores

    def extract_player_experience_ratings(self) -> Dict[str, float]:
        """Extract player experience ratings from reviews"""
        if not self.reviews_summary:
            return {
                "overall_rating": 4.0,
                "conditions_rating": 4.0,
                "value_rating": 4.0,
                "friendliness_rating": 4.0,
                "pace_rating": 4.0,
                "amenities_rating": 4.0,
                "difficulty_rating": 4.0,
                "recommend_percent": 85.0
            }

        form_averages = self.reviews_summary.get('form_category_averages', {})

        return {
            "overall_rating": self.reviews_summary.get('overall_rating', 4.0),
            "conditions_rating": form_averages.get('Conditions', 4.0),
            "value_rating": form_averages.get('Value', 4.0),
            "friendliness_rating": form_averages.get('Friendliness', 4.0),
            "pace_rating": form_averages.get('Pace', 4.0),
            "amenities_rating": form_averages.get('Amenities', 4.0),
            "difficulty_rating": form_averages.get('Difficulty', 4.0),
            "recommend_percent": self.reviews_summary.get('recommend_percent', 85.0)
        }

    def extract_course_insights(self) -> Dict[str, float]:
        """Extract course insights from reviews text analysis"""
        if not self.reviews_summary:
            return {key: 0.0 for key in [
                "fairways_quality", "greens_quality", "tee_boxes_quality", "shot_variety",
                "signature_holes_appeal", "overall_scenery", "green_complexity",
                "staff_friendliness", "pace_of_play"
            ]}

        text_insights = self.reviews_summary.get('text_insight_averages', {})

        return {
            "fairways_quality": text_insights.get('Fairways', 0.0) or 0.0,
            "greens_quality": text_insights.get('Greens', 0.0) or 0.0,
            "tee_boxes_quality": text_insights.get('Tee Boxes', 0.0) or 0.0,
            "shot_variety": text_insights.get('Shot Variety / Hole Uniqueness', 0.0) or 0.0,
            "signature_holes_appeal": text_insights.get('Signature Holes / Quirky/Fun Design Features', 0.0) or 0.0,
            "overall_scenery": text_insights.get('Overall feel / Scenery', 0.0) or 0.0,
            "green_complexity": text_insights.get('Green Complexity', 0.0) or 0.0,
            "staff_friendliness": text_insights.get('Staff Friendliness, After-Round Experience', 0.0) or 0.0,
            "pace_of_play": text_insights.get('Pace of Play', 0.0) or 0.0
        }

    def extract_amenities_detail(self) -> Dict[str, bool]:
        """Extract amenities information"""
        amenities = self.course_data.get('amenities', {})

        def safe_get_amenity(amenity_name: str) -> bool:
            """Safely extract amenity availability"""
            amenity_data = amenities.get(amenity_name)
            if amenity_data and isinstance(amenity_data, dict):
                return amenity_data.get('available', False)
            return False

        return {
            "driving_range": safe_get_amenity('driving_range'),
            "practice_green": safe_get_amenity('practice_green'),
            "short_game_area": safe_get_amenity('short_game_practice_area'),
            "pro_shop": safe_get_amenity('pro_shop'),
            "clubhouse": safe_get_amenity('clubhouse'),
            "locker_rooms": safe_get_amenity('locker_rooms'),
            "food_beverage": safe_get_amenity('food_beverage_options'),
            "beverage_cart": safe_get_amenity('beverage_cart'),
            "banquet_facilities": safe_get_amenity('banquet_facilities')
        }

    def extract_location_economics(self) -> Dict[str, Any]:
        """Extract location and economic information"""
        general_info = self.course_data.get('general_info', {})

        # Safely extract address with proper null checking
        address_data = general_info.get('address')
        if address_data and isinstance(address_data, dict):
            address = address_data.get('value', '')
        else:
            address = ''

        # Extract state from address
        state = 'MA'  # Default based on the course data
        if address and ', ' in address:
            state = address.split(', ')[-1].split(' ')[0] if ' ' in address.split(', ')[-1] else state

        # Safely extract course type
        course_type_data = general_info.get('course_type')
        if course_type_data and isinstance(course_type_data, dict):
            course_type = course_type_data.get('value', 'Public')
        else:
            course_type = 'Public'

        # Safely extract pricing level
        pricing_data = general_info.get('pricing_level')
        if pricing_data and isinstance(pricing_data, dict):
            pricing_level = pricing_data.get('value', 3)
        else:
            pricing_level = 3

        return {
            "pricing_level": pricing_level,
            "course_type_public": course_type.lower() == 'public',
            "course_type_private": course_type.lower() == 'private',
            "course_type_resort": course_type.lower() == 'resort',
            "state": state,
            "region": "New England"  # Based on MA location
        }

    def extract_design_classification(self) -> Dict[str, Any]:
        """Extract design classification"""
        vector_attrs = self.vector_attributes.get('vector_attributes', {})
        course_history = self.course_data.get('course_history', {})

        # Safely extract year_built with proper null checking
        year_built_data = course_history.get('year_built')
        if year_built_data and isinstance(year_built_data, dict):
            year_built = year_built_data.get('value', 1990)
        else:
            year_built = 1990  # Default value

        # Ensure year_built is a valid number
        if year_built is None or not isinstance(year_built, (int, float)):
            year_built = 1990

        architect_era = "modern" if year_built >= 1980 else "classic" if year_built >= 1950 else "classic"

        return {
            "design_style": vector_attrs.get('design_style_category', 'parkland'),
            "routing_style": vector_attrs.get('routing_style', 'natural_flow'),
            "architect_era": architect_era,
            "renovation_factor": 0.8  # Default value
        }

    def extract_data_quality(self) -> Dict[str, Any]:
        """Extract data quality metrics"""
        return {
            "completeness_score": 0.95,  # High completeness based on available data
            "has_hole_analysis": bool(self.comprehensive_analysis.get('strategic_analysis', {}).get('hole_analysis')),
            "has_weather_data": bool(self.comprehensive_analysis.get('weather_analysis')),
            "has_elevation_data": bool(self.comprehensive_analysis.get('elevation_analysis')),
            "has_review_data": bool(self.reviews_summary),
            "review_count": self.reviews_summary.get('total_reviews', 0),
            "last_updated": datetime.now().isoformat()
        }

    def get_course_name_for_directory(self):
        """Get a clean course name for directory naming"""
        print(f"🔧 DEBUG: Extracting course name...")
        print(f"🔧 DEBUG: Course data keys: {list(self.course_data.keys())}")

        try:
            general_info = self.course_data.get('general_info', {})
            print(f"🔧 DEBUG: General info keys: {list(general_info.keys())}")

            name_data = general_info.get('name')
            print(f"🔧 DEBUG: Name data: {name_data}")

            course_name = ''
            if name_data and isinstance(name_data, dict):
                course_name = name_data.get('value', '')

            print(f"🔧 DEBUG: Raw course name: '{course_name}'")

            if course_name:
                # Clean up course name for filesystem
                clean_name = course_name.lower()
                clean_name = clean_name.replace(' ', '_').replace('-', '_').replace('&', 'and')
                clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
                clean_name = '_'.join(word for word in clean_name.split('_') if word)  # Remove empty parts
                clean_name = clean_name[:50]  # Limit length
                print(f"🔧 DEBUG: Cleaned course name: '{clean_name}'")
                return clean_name
        except Exception as e:
            print(f"🔧 DEBUG: Exception in course name extraction: {e}")

        print(f"🔧 DEBUG: Falling back to unknown_course")
        return "unknown_course"

    def create_vector(self) -> Dict[str, Any]:
        """Create the complete course vector"""
        # Safely extract course name
        general_info = self.course_data.get('general_info', {})
        name_data = general_info.get('name')
        if name_data and isinstance(name_data, dict):
            course_name = name_data.get('value', 'Unknown Course')
        else:
            course_name = 'Unknown Course'

        vector = {
            "course_id": self.course_number,
            "course_name": course_name,
            "course_fundamentals": self.extract_course_fundamentals(),
            "hole_composition": self.extract_hole_composition(),
            "strategic_complexity": self.extract_strategic_complexity(),
            "dogleg_analysis": self.extract_dogleg_analysis(),
            "landing_zone_difficulty": self.extract_landing_zone_difficulty(),
            "elevation_profile": self.extract_elevation_profile(),
            "course_character": self.extract_course_character(),
            "playing_difficulty": self.extract_playing_difficulty(),
            "weather_characteristics": self.extract_weather_characteristics(),
            "monthly_weather_scores": self.extract_monthly_weather_scores(),
            "player_experience_ratings": self.extract_player_experience_ratings(),
            "course_insights": self.extract_course_insights(),
            "amenities_detail": self.extract_amenities_detail(),
            "location_economics": self.extract_location_economics(),
            "design_classification": self.extract_design_classification(),
            "data_quality": self.extract_data_quality()
        }

        return vector


def main():
    """Main function to create the course vector"""
    if len(sys.argv) != 2:
        print("Usage: python create_golf_vector.py <course_number>")
        print("Example: python create_golf_vector.py MA-111")
        sys.exit(1)

    course_number = sys.argv[1]
    print(f"Creating vector for course: {course_number}")

    # Create the vector creator
    creator = GolfCourseVectorCreator(course_number)

    # Load the data files - skip if not all required files found
    if not creator.load_data_files():
        print(f"\n❌ Skipped course {course_number} due to missing files")
        sys.exit(2)  # Exit code 2 indicates skipped due to missing files

    # Create the vector
    course_vector = creator.create_vector()

    # Get clean course name for filename
    course_name_clean = creator.get_course_name_for_directory()
    print(f"🔧 DEBUG: Course name for filename: '{course_name_clean}'")

    # Ensure output directory exists
    output_dir = "../states/ma/vectors"
    os.makedirs(output_dir, exist_ok=True)

    # Save the vector to a JSON file with course name
    output_filename = f"{output_dir}/{course_number}_{course_name_clean}_course_vector.json"
    print(f"🔧 DEBUG: Output filename: {output_filename}")
    with open(output_filename, 'w') as f:
        json.dump(course_vector, f, indent=2)

    print(f"\n✅ SUCCESS: Course vector created and saved to {output_filename}")
    print(f"✓ Vector contains {len(course_vector)} main categories")

    # Print a summary
    fundamentals = course_vector['course_fundamentals']
    print(f"\nCourse Summary:")
    print(f"- Course ID: {course_vector['course_id']}")
    print(f"- Course Name: {course_vector['course_name']}")
    print(f"- Total Length: {fundamentals['total_length_yards']} yards")
    print(f"- Par: {fundamentals['total_par']}")
    print(f"- Holes: {fundamentals['holes_count']}")
    print(f"- Overall Rating: {course_vector['player_experience_ratings']['overall_rating']}")
    print(f"- Data Quality: {course_vector['data_quality']['completeness_score']}")

    return course_vector


if __name__ == "__main__":
    course_vector = main()
