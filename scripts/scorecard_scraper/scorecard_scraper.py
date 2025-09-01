#!/usr/bin/env python3
"""
Golf Scorecard OCR and Data Extractor - Local Directory Version
Processes scorecard files from ../golf_scraper/output/scorecards/sc directory
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import base64
import tempfile
import argparse
import re

import requests
from playwright.async_api import async_playwright
import pytesseract
from PIL import Image
import openai
from openai import OpenAI
import fitz  # PyMuPDF for PDF handling

class ScorecardExtractor:
    def __init__(self, openai_api_key: str, use_vision: bool = True, output_dir: str = "../new_course_scraper/output/teesandpars/sc"):
        """Initialize the scorecard extractor with OpenAI API key and configurable output directory."""
        self.client = OpenAI(api_key=openai_api_key)
        self.temp_dir = Path(tempfile.mkdtemp())
        self.use_vision = use_vision
        # Set the output directory (now configurable)
        self.output_dir = Path(output_dir)

    def extract_course_id_from_filename(self, filename: str) -> Optional[int]:
        """Extract course ID from filename using various patterns."""
        # First try numeric patterns
        numeric_patterns = [
            r'course_(\d+)',
            r'(\d+)_',
            r'^(\d+)',
            r'id_(\d+)',
            r'golf_(\d+)',
            r'(\d+)\.pdf$',
            r'(\d+)\.png$',
            r'(\d+)\.jpg$'
        ]

        for pattern in numeric_patterns:
            match = re.search(pattern, filename.lower())
            if match:
                return int(match.group(1))

        # If no numeric ID found, extract course name and create hash-based ID
        course_name = self.extract_course_name_from_filename(filename)
        if course_name:
            # Create a consistent numeric ID from course name hash
            course_id = abs(hash(course_name)) % 100000
            print(f"Generated ID {course_id} from course name: {course_name}")
            return course_id

        return None

    def extract_course_name_from_filename(self, filename: str) -> Optional[str]:
        """Extract course name from filename patterns."""
        # Remove extension
        name_without_ext = Path(filename).stem

        # Common patterns for course names
        patterns = [
            r'^(.+?)_scorecard$',
            r'^(.+?)_golf$',
            r'^(.+?)_course$',
            r'^(.+?)_cc$',
            r'^(.+?)_gc$'
        ]

        for pattern in patterns:
            match = re.search(pattern, name_without_ext, re.IGNORECASE)
            if match:
                course_name = match.group(1).replace('_', ' ').title()
                return course_name

        # If no pattern matches, use the whole filename (cleaned up)
        if name_without_ext and not name_without_ext.isdigit():
            course_name = name_without_ext.replace('_', ' ').title()
            return course_name

        return None

    def find_scorecard_files(self, directory: Path) -> List[Tuple[Path, int]]:
        """Find all scorecard files in directory and extract course IDs."""
        if not directory.exists():
            print(f"Directory {directory} does not exist!")
            return []

        # Supported file extensions
        extensions = ['*.pdf', '*.png', '*.jpg', '*.jpeg', '*.tiff', '*.bmp']
        files_with_ids = []

        for ext in extensions:
            for file_path in directory.glob(ext):
                course_id = self.extract_course_id_from_filename(file_path.name)
                if course_id:
                    # Also extract course name for display
                    course_name = self.extract_course_name_from_filename(file_path.name)
                    display_name = f" ({course_name})" if course_name else ""
                    files_with_ids.append((file_path, course_id))
                    print(f"Found: {file_path.name} -> Course ID: {course_id}{display_name}")
                else:
                    print(f"Warning: Could not extract course ID from {file_path.name}, skipping...")

        return files_with_ids

    async def process_directory(self, directory_path: str, specific_course_id: Optional[int] = None) -> Dict[int, List[str]]:
        """Process all scorecard files in the specified directory."""
        directory = Path(directory_path).resolve()
        print(f"Processing scorecards from: {directory}")

        files_with_ids = self.find_scorecard_files(directory)

        if not files_with_ids:
            print("No valid scorecard files found!")
            return {}

        # Filter by specific course ID if provided
        if specific_course_id:
            files_with_ids = [(f, cid) for f, cid in files_with_ids if cid == specific_course_id]
            if not files_with_ids:
                print(f"No files found for course ID {specific_course_id}")
                return {}

        print(f"Found {len(files_with_ids)} files to process")

        all_results = {}

        for file_path, course_id in files_with_ids:
            print(f"\n{'='*60}")
            print(f"Processing: {file_path.name} (Course ID: {course_id})")
            print(f"{'='*60}")

            try:
                output_files = await self.process_local_file(course_id, file_path)
                all_results[course_id] = output_files
                print(f"✅ Successfully processed {file_path.name}")

            except Exception as e:
                print(f"❌ Error processing {file_path.name}: {e}")
                all_results[course_id] = []

        return all_results

    async def process_local_file(self, course_id: int, file_path: Path) -> List[str]:
        """Process a local scorecard file."""
        print(f"Processing course ID {course_id} from file: {file_path}")

        # Determine file type and get image paths
        if file_path.suffix.lower() == '.pdf':
            image_paths = await self.convert_pdf_to_images(file_path)
        else:
            # It's already an image file
            image_paths = [file_path]

        # Process images with OpenAI Vision or OCR
        if self.use_vision:
            print("Using OpenAI Vision to analyze images...")
            tee_data, par_data = self.parse_images_with_vision(course_id, image_paths)
        else:
            # Perform OCR on all images (old method)
            ocr_text = ""
            for image_path in image_paths:
                page_text = self.perform_ocr(image_path)
                ocr_text += f"\n--- Page {image_paths.index(image_path) + 1} ---\n{page_text}"

            print(f"OCR completed. Extracted {len(ocr_text)} characters.")

            # Save OCR text for debugging
            ocr_debug_file = f"debug_ocr_text_course_{course_id}.txt"
            with open(ocr_debug_file, 'w', encoding='utf-8') as f:
                f.write(ocr_text)
            print(f"OCR text saved to {ocr_debug_file}")

            # Use OpenAI to parse the OCR text
            tee_data, par_data = self.parse_with_openai_including_pars(course_id, ocr_text)

        # Save individual files for each tee and par data
        output_files = self.save_tee_and_par_data(course_id, tee_data, par_data, file_path.name)

        return output_files

    async def convert_pdf_to_images(self, pdf_path: Path) -> List[Path]:
        """Convert PDF pages to images."""
        print(f"Converting PDF to images: {pdf_path}")

        # Convert PDF pages to images
        pdf_doc = fitz.open(str(pdf_path))
        image_paths = []

        for page_num in range(pdf_doc.page_count):
            page = pdf_doc[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0))  # High resolution
            img_path = self.temp_dir / f"{pdf_path.stem}_page_{page_num + 1}.png"
            pix.save(str(img_path))
            image_paths.append(img_path)
            print(f"Converted PDF page {page_num + 1} to image")

        pdf_doc.close()
        return image_paths

    def perform_ocr(self, image_path: Path) -> str:
        """Perform OCR on an image file."""
        print(f"Performing OCR on {image_path}")

        # Save a debug copy
        debug_copy = Path(f"debug_ocr_image_{image_path.name}")
        import shutil
        shutil.copy2(image_path, debug_copy)
        print(f"Debug copy of OCR image saved to {debug_copy}")

        try:
            # Open and preprocess image for better OCR
            image = Image.open(image_path)

            # Convert to grayscale if needed
            if image.mode != 'L':
                image = image.convert('L')

            # Perform OCR with specific configuration for better results
            custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,-: '
            text = pytesseract.image_to_string(image, config=custom_config)

            return text.strip()

        except Exception as e:
            print(f"OCR error: {e}")
            return ""

    def encode_image_to_base64(self, image_path: Path) -> str:
        """Encode image to base64 for OpenAI Vision API."""
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    def parse_images_with_vision(self, course_id: int, image_paths: List[Path]) -> Tuple[List[Dict], Optional[Dict]]:
        """Use OpenAI Vision to directly analyze scorecard images."""
        print(f"Analyzing {len(image_paths)} images with OpenAI Vision...")

        all_tee_data = []
        par_data = None

        for i, image_path in enumerate(image_paths):
            print(f"Processing image {i+1}/{len(image_paths)}: {image_path.name}")

            # Encode image to base64
            base64_image = self.encode_image_to_base64(image_path)

            prompt = f"""
            Analyze this golf scorecard image and extract ALL tee information AND par information. Look for different tee colors/names like:
            - Championship/Black/Tips
            - Blue/Men's
            - White/Regular
            - Red/Women's/Forward
            - Gold/Senior
            - Any other tee designations

            For EACH tee you find, create a JSON object with this EXACT structure:
            {{
                "course_id": {course_id},
                "tee_name": "TEE_NAME_IN_CAPS",
                "total_yardage": total_yards_number,
                "rating": course_rating_decimal,
                "slope": slope_rating_integer,
                "hole_1": hole_1_yardage,
                "hole_2": hole_2_yardage,
                "hole_3": hole_3_yardage,
                "hole_4": hole_4_yardage,
                "hole_5": hole_5_yardage,
                "hole_6": hole_6_yardage,
                "hole_7": hole_7_yardage,
                "hole_8": hole_8_yardage,
                "hole_9": hole_9_yardage,
                "out_9": front_nine_total,
                "hole_10": hole_10_yardage,
                "hole_11": hole_11_yardage,
                "hole_12": hole_12_yardage,
                "hole_13": hole_13_yardage,
                "hole_14": hole_14_yardage,
                "hole_15": hole_15_yardage,
                "hole_16": hole_16_yardage,
                "hole_17": hole_17_yardage,
                "hole_18": hole_18_yardage,
                "in_9": back_nine_total
            }}

            ALSO create a separate JSON object for the PAR information (this should be the same for all tees):
            {{
                "course_id": {course_id},
                "par_1": hole_1_par,
                "par_2": hole_2_par,
                "par_3": hole_3_par,
                "par_4": hole_4_par,
                "par_5": hole_5_par,
                "par_6": hole_6_par,
                "par_7": hole_7_par,
                "par_8": hole_8_par,
                "par_9": hole_9_par,
                "out_par": front_nine_par_total,
                "par_10": hole_10_par,
                "par_11": hole_11_par,
                "par_12": hole_12_par,
                "par_13": hole_13_par,
                "par_14": hole_14_par,
                "par_15": hole_15_par,
                "par_16": hole_16_par,
                "par_17": hole_17_par,
                "par_18": hole_18_par,
                "in_par": back_nine_par_total,
                "total_par": total_course_par
            }}

            Return ONLY valid JSON objects. Start each tee object on a new line, then include ONE par object at the end.
            If you can't find a value, use null. Look carefully at the scorecard and extract data for ALL visible tees.
            """

            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an expert at analyzing golf scorecards. Extract structured data from scorecard images and return only valid JSON objects. Be thorough and find all tee information AND par information for each hole."
                        },
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": prompt},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{base64_image}",
                                        "detail": "high"
                                    }
                                }
                            ]
                        }
                    ],
                    temperature=0.1,
                    max_tokens=3000
                )

                response_text = response.choices[0].message.content
                print(f"Vision API response for image {i+1}:")
                print("=== DEBUG: Vision Response ===")
                print(response_text)
                print("=== End Vision Response ===")

                # Parse JSON objects from response
                image_tee_data, image_par_data = self.parse_json_response_with_pars(response_text)
                all_tee_data.extend(image_tee_data)

                # Store par data (should be the same across all images, but we'll take the last one)
                if image_par_data:
                    par_data = image_par_data

            except Exception as e:
                print(f"Vision API error for image {i+1}: {e}")
                continue

        print(f"Total tees found across all images: {len(all_tee_data)}")
        return all_tee_data, par_data

    def parse_json_response_with_pars(self, response_text: str) -> Tuple[List[Dict], Optional[Dict]]:
        """Parse JSON objects from OpenAI response text, separating tee data and par data."""
        tee_data = []
        par_data = None

        # Try parsing line by line first
        for line in response_text.split('\n'):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                try:
                    data = json.loads(line)
                    # Check if this is par data (has par_1, par_2, etc.)
                    if 'par_1' in data:
                        par_data = data
                        print(f"Successfully parsed par data")
                    # Check if this is tee data (has tee_name)
                    elif 'tee_name' in data:
                        tee_data.append(data)
                        print(f"Successfully parsed tee: {data.get('tee_name', 'UNKNOWN')}")
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON line: {line[:100]}...")
                    continue

        # If no line-by-line parsing worked, try other approaches
        if not tee_data and not par_data:
            print("No line-by-line JSON found, trying alternative parsing...")
            try:
                # Try to extract JSON blocks with regex
                json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                json_matches = re.findall(json_pattern, response_text, re.DOTALL)

                for match in json_matches:
                    try:
                        data = json.loads(match)
                        if 'par_1' in data:
                            par_data = data
                            print(f"Regex-parsed par data")
                        elif 'tee_name' in data:
                            tee_data.append(data)
                            print(f"Regex-parsed tee: {data.get('tee_name', 'UNKNOWN')}")
                    except json.JSONDecodeError:
                        continue

            except Exception as e:
                print(f"Regex parsing error: {e}")

            # Final attempt: try as JSON array
            if not tee_data and not par_data:
                try:
                    if response_text.strip().startswith('['):
                        all_data = json.loads(response_text.strip())
                        for data in all_data:
                            if 'par_1' in data:
                                par_data = data
                            elif 'tee_name' in data:
                                tee_data.append(data)
                        print("Parsed as JSON array")
                except json.JSONDecodeError:
                    print("Could not parse as JSON array either")

        return tee_data, par_data

    def parse_json_response(self, response_text: str) -> List[Dict]:
        """Parse JSON objects from OpenAI response text."""
        tee_data = []

        # Try parsing line by line first
        for line in response_text.split('\n'):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                try:
                    data = json.loads(line)
                    tee_data.append(data)
                    print(f"Successfully parsed tee: {data.get('tee_name', 'UNKNOWN')}")
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON line: {line[:100]}...")
                    continue

        # If no line-by-line parsing worked, try other approaches
        if not tee_data:
            print("No line-by-line JSON found, trying alternative parsing...")
            try:
                # Try to extract JSON blocks with regex
                json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                json_matches = re.findall(json_pattern, response_text, re.DOTALL)

                for match in json_matches:
                    try:
                        data = json.loads(match)
                        tee_data.append(data)
                        print(f"Regex-parsed tee: {data.get('tee_name', 'UNKNOWN')}")
                    except json.JSONDecodeError:
                        continue

            except Exception as e:
                print(f"Regex parsing error: {e}")

            # Final attempt: try as JSON array
            if not tee_data:
                try:
                    if response_text.strip().startswith('['):
                        tee_data = json.loads(response_text.strip())
                        print("Parsed as JSON array")
                except json.JSONDecodeError:
                    print("Could not parse as JSON array either")

        return tee_data

    def parse_with_openai_including_pars(self, course_id: int, ocr_text: str) -> Tuple[List[Dict], Optional[Dict]]:
        """Use OpenAI to parse OCR text and extract structured golf data including pars."""
        print("Parsing OCR text with OpenAI...")

        prompt = f"""
        I have OCR text from a golf course scorecard. Please extract the information for each tee AND the par information, and format them as JSON objects.

        For each tee (like Championship, Blue, White, Red, etc.), create a JSON object with this exact structure:
        {{
            "course_id": {course_id},
            "tee_name": "TEE_NAME_IN_CAPS",
            "total_yardage": total_yards_number,
            "rating": course_rating_decimal,
            "slope": slope_rating_integer,
            "hole_1": hole_1_yardage,
            "hole_2": hole_2_yardage,
            "hole_3": hole_3_yardage,
            "hole_4": hole_4_yardage,
            "hole_5": hole_5_yardage,
            "hole_6": hole_6_yardage,
            "hole_7": hole_7_yardage,
            "hole_8": hole_8_yardage,
            "hole_9": hole_9_yardage,
            "out_9": front_nine_total,
            "hole_10": hole_10_yardage,
            "hole_11": hole_11_yardage,
            "hole_12": hole_12_yardage,
            "hole_13": hole_13_yardage,
            "hole_14": hole_14_yardage,
            "hole_15": hole_15_yardage,
            "hole_16": hole_16_yardage,
            "hole_17": hole_17_yardage,
            "hole_18": hole_18_yardage,
            "in_9": back_nine_total
        }}

        ALSO create a separate JSON object for the PAR information (this should be the same for all tees):
        {{
            "course_id": {course_id},
            "par_1": hole_1_par,
            "par_2": hole_2_par,
            "par_3": hole_3_par,
            "par_4": hole_4_par,
            "par_5": hole_5_par,
            "par_6": hole_6_par,
            "par_7": hole_7_par,
            "par_8": hole_8_par,
            "par_9": hole_9_par,
            "out_par": front_nine_par_total,
            "par_10": hole_10_par,
            "par_11": hole_11_par,
            "par_12": hole_12_par,
            "par_13": hole_13_par,
            "par_14": hole_14_par,
            "par_15": hole_15_par,
            "par_16": hole_16_par,
            "par_17": hole_17_par,
            "par_18": hole_18_par,
            "in_par": back_nine_par_total,
            "total_par": total_course_par
        }}

        Return only valid JSON objects. Start each tee object on a new line, then include ONE par object at the end.
        If you can't find a value, use null.

        OCR Text:
        {ocr_text}
        """

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert at parsing golf scorecard data. Extract structured data from OCR text and return only valid JSON objects for both tee yardages and par values."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=3000
            )

            response_text = response.choices[0].message.content
            print("OpenAI response received")
            print("=== DEBUG: OpenAI Response ===")
            print(response_text)
            print("=== End OpenAI Response ===")

            # Parse JSON objects from response
            return self.parse_json_response_with_pars(response_text)

        except Exception as e:
            print(f"OpenAI parsing error: {e}")
            return [], None

    def parse_with_openai(self, course_id: int, ocr_text: str) -> List[Dict]:
        """Use OpenAI to parse OCR text and extract structured golf data."""
        print("Parsing OCR text with OpenAI...")

        prompt = f"""
        I have OCR text from a golf course scorecard. Please extract the information for each tee and format it as JSON objects.

        For each tee (like Championship, Blue, White, Red, etc.), create a JSON object with this exact structure:
        {{
            "course_id": {course_id},
            "tee_name": "TEE_NAME_IN_CAPS",
            "total_yardage": total_yards_number,
            "rating": course_rating_decimal,
            "slope": slope_rating_integer,
            "hole_1": hole_1_yardage,
            "hole_2": hole_2_yardage,
            "hole_3": hole_3_yardage,
            "hole_4": hole_4_yardage,
            "hole_5": hole_5_yardage,
            "hole_6": hole_6_yardage,
            "hole_7": hole_7_yardage,
            "hole_8": hole_8_yardage,
            "hole_9": hole_9_yardage,
            "out_9": front_nine_total,
            "hole_10": hole_10_yardage,
            "hole_11": hole_11_yardage,
            "hole_12": hole_12_yardage,
            "hole_13": hole_13_yardage,
            "hole_14": hole_14_yardage,
            "hole_15": hole_15_yardage,
            "hole_16": hole_16_yardage,
            "hole_17": hole_17_yardage,
            "hole_18": hole_18_yardage,
            "in_9": back_nine_total
        }}

        Return only valid JSON objects, one per tee. If you can't find a value, use null.

        OCR Text:
        {ocr_text}
        """

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert at parsing golf scorecard data. Extract structured data from OCR text and return only valid JSON objects."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=2000
            )

            response_text = response.choices[0].message.content
            print("OpenAI response received")
            print("=== DEBUG: OpenAI Response ===")
            print(response_text)
            print("=== End OpenAI Response ===")

            # Parse JSON objects from response
            tee_data = []
            for line in response_text.split('\n'):
                line = line.strip()
                if line.startswith('{') and line.endswith('}'):
                    try:
                        data = json.loads(line)
                        tee_data.append(data)
                        print(f"Successfully parsed tee: {data.get('tee_name', 'UNKNOWN')}")
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse JSON line: {line}")
                        print(f"JSON Error: {e}")
                        continue

            if not tee_data:
                print("WARNING: No valid JSON objects found in OpenAI response")
                # Try to parse the entire response as JSON
                try:
                    if response_text.strip().startswith('['):
                        # Try as JSON array
                        tee_data = json.loads(response_text.strip())
                    elif response_text.strip().startswith('{'):
                        # Try as single JSON object
                        tee_data = [json.loads(response_text.strip())]
                except json.JSONDecodeError:
                    print("Could not parse response as JSON array or object either")

            return tee_data

        except Exception as e:
            print(f"OpenAI parsing error: {e}")
            return []

    def save_tee_and_par_data(self, course_id: int, tee_data: List[Dict], par_data: Optional[Dict], original_filename: str = "") -> List[str]:
        """Save each tee's data and par data to individual files in the output directory."""
        output_files = []

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Saving files to: {self.output_dir}")

        # Use original filename stem as base (without extension)
        if original_filename:
            base_filename = Path(original_filename).stem
        else:
            base_filename = f"course_{course_id}"

        # Save tee data files
        for i, data in enumerate(tee_data):
            tee_name = data.get('tee_name', f'TEE_{i+1}').replace(' ', '_')
            filename = f"{base_filename}_{tee_name.lower()}.json"
            file_path = self.output_dir / filename

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)

            output_files.append(str(file_path))
            print(f"Saved tee data to {file_path}")

        # Save par data file
        if par_data:
            par_filename = f"{base_filename}_pars.json"
            par_file_path = self.output_dir / par_filename

            with open(par_file_path, 'w') as f:
                json.dump(par_data, f, indent=2)

            output_files.append(str(par_file_path))
            print(f"Saved par data to {par_file_path}")
        else:
            print("Warning: No par data found to save")

        return output_files

    def save_tee_data(self, course_id: int, tee_data: List[Dict], original_filename: str = "") -> List[str]:
        """Save each tee's data to individual files in the output directory."""
        output_files = []

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Use original filename stem as base (without extension)
        if original_filename:
            base_filename = Path(original_filename).stem
        else:
            base_filename = f"course_{course_id}"

        for i, data in enumerate(tee_data):
            tee_name = data.get('tee_name', f'TEE_{i+1}').replace(' ', '_')
            filename = f"{base_filename}_{tee_name.lower()}.json"
            file_path = self.output_dir / filename

            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)

            output_files.append(str(file_path))
            print(f"Saved tee data to {file_path}")

        return output_files

    def cleanup(self):
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

async def main():
    parser = argparse.ArgumentParser(description='Extract golf scorecard data from local files using OCR and OpenAI')
    parser.add_argument('--directory', '-d',
                       default='../new_course_scraper/output/scorecards/sc',
                       help='Directory containing scorecard files (default: ../new_course_scraper/output/scorecards/sc)')
    parser.add_argument('--output', '-o',
                       default='../new_course_scraper/output/teesandpars/sc',
                       help='Output directory for extracted data (default: ../new_course_scraper/output/teesandpars/sc)')
    parser.add_argument('--course-id', '-c', type=int,
                       help='Process only files for specific course ID (default: process all)')
    parser.add_argument('--openai-key', help='OpenAI API key (or set OPENAI_API_KEY env var)')
    parser.add_argument('--use-ocr', action='store_true',
                       help='Use OCR instead of OpenAI Vision (default: use Vision)')
    parser.add_argument('--list-files', action='store_true',
                       help='List all found files and exit')

    args = parser.parse_args()

    # Get OpenAI API key
    openai_key = args.openai_key or os.getenv('OPENAI_API_KEY')
    if not openai_key and not args.list_files:
        print("Error: OpenAI API key required. Use --openai-key or set OPENAI_API_KEY environment variable.")
        sys.exit(1)

    # Use Vision by default, OCR only if requested
    use_vision = not args.use_ocr
    extractor = ScorecardExtractor(openai_key, use_vision=use_vision, output_dir=args.output) if openai_key else None

    # List files mode
    if args.list_files:
        print(f"Scanning directory: {args.directory}")
        temp_extractor = ScorecardExtractor("dummy", use_vision=True, output_dir=args.output)
        files_with_ids = temp_extractor.find_scorecard_files(Path(args.directory))

        if files_with_ids:
            print(f"\nFound {len(files_with_ids)} scorecard files:")
            for file_path, course_id in sorted(files_with_ids, key=lambda x: x[1]):
                course_name = temp_extractor.extract_course_name_from_filename(file_path.name)
                display_name = f" ({course_name})" if course_name else ""
                print(f"  Course {course_id:5d}: {file_path.name}{display_name}")
        else:
            print("No valid scorecard files found!")
        print(f"\nOutput would be saved to: {args.output}")
        return

    print(f"Using {'OpenAI Vision' if use_vision else 'OCR + Text Analysis'} for data extraction")
    print(f"Output files will be saved to: {extractor.output_dir}")

    try:
        all_results = await extractor.process_directory(args.directory, args.course_id)

        print(f"\n{'='*60}")
        print("PROCESSING SUMMARY")
        print(f"{'='*60}")

        total_files = 0
        total_courses = len(all_results)

        for course_id, output_files in all_results.items():
            total_files += len(output_files)
            status = "✅ Success" if output_files else "❌ Failed"
            print(f"Course {course_id:5d}: {status} - {len(output_files)} tee files created")
            for file in output_files:
                print(f"              {file}")

        print(f"\nProcessed {total_courses} courses, created {total_files} total tee files")
        print(f"All files saved to: {extractor.output_dir}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if extractor:
            extractor.cleanup()

if __name__ == "__main__":
    print("Golf Scorecard Local File Processor")
    print("=" * 40)
    print("Required packages:")
    print("  pip install playwright pytesseract Pillow openai PyMuPDF requests")
    print("\nAlso install Playwright browsers: playwright install")
    print("Note: Tesseract OCR only needed if using --use-ocr flag")
    print("OpenAI Vision is used by default (better than OCR)")
    print("\nUsage Examples:")
    print("  python script.py                                    # Process all files in default directories")
    print("  python script.py --course-id 230                    # Process only course 230")
    print("  python script.py --directory /path/to/scorecards    # Use custom input directory")
    print("  python script.py --output /path/to/output           # Use custom output directory")
    print("  python script.py -d /input -o /output               # Use custom input and output directories")
    print("  python script.py --list-files                       # List all found files")
    print("  python script.py --use-ocr                          # Use OCR instead of Vision")
    print("\nOutput files created:")
    print("  - ORIGINAL_FILENAME_TEE_NAME.json (yardage data for each tee)")
    print("  - ORIGINAL_FILENAME_pars.json (par data for all holes)")
    print("  Example: ma-73-1_the_captains_blue.json, ma-73-1_the_captains_pars.json")
    print("\nDefault directories:")
    print("  Input:  ../new_course_scraper/output/scorecards/sc")
    print("  Output: ../new_course_scraper/output/teesandpars/sc")
    print("\nSupported filename patterns:")
    print("  - Numeric IDs: 123.pdf, course_456.png, golf_789.jpg")
    print("  - Course names: Woburn_scorecard.png, Pine_Hills_golf.pdf")
    print("  - Mixed: Any filename will work - numeric IDs preferred, names auto-converted")
    print("")

    asyncio.run(main())
