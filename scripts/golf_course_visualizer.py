#!/usr/bin/env python3
"""
USGS NAIP Aerial Imagery and OpenStreetMap Golf Course Data Collector
WITH INTEGRATED ELEVATION ANALYSIS AND SKIP EXISTING FUNCTIONALITY

This script downloads aerial imagery from USGS NAIP WMS service and overlays
OpenStreetMap golf course polygons for geospatial visualization.

Updated for batch processing with Excel file input and DEM elevation data.
Now includes folder existence checking to skip already processed courses.
No OpenAI required - this is pure data collection and visualization.

FIXED: Replaced broken USGS WCS with reliable py3dep package
NEW: Added individual layer photo generation for all golf course features
NEW: Enhanced elevation data processing with py3dep
NEW: Comprehensive terrain analysis and mapping
NEW: Skip existing folders functionality

DEPENDENCIES:
pip install matplotlib geopandas pandas requests shapely pillow numpy contextily osmnx openpyxl scipy rasterio py3dep

Author: Updated Script with Enhanced Elevation Analysis and Skip Functionality
Date: July 25, 2025
"""

import os
import sys
import json
import math
import requests
import geopandas as gpd
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import LinearSegmentedColormap
from shapely.geometry import Polygon, Point, LineString
from PIL import Image, ImageDraw
import io
import numpy as np
import contextily as ctx
from typing import Tuple, Optional, Dict, Any
import re
from scipy.interpolate import griddata
from scipy.spatial.distance import cdist
import time

# DEM processing imports
try:
    import rasterio
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    from rasterio.mask import mask
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    RASTERIO_AVAILABLE = True
    print("✅ Rasterio available - will use DEM files for elevation data")
except ImportError:
    RASTERIO_AVAILABLE = False
    print("⚠️ Rasterio not available - will use elevation APIs as fallback")

# py3dep import (the fix!)
try:
    import py3dep
    PY3DEP_AVAILABLE = True
    print("✅ py3dep available - will use reliable USGS 3DEP API")
except ImportError:
    PY3DEP_AVAILABLE = False
    print("⚠️ py3dep not available - install with: pip install py3dep")
try:
    from meteostat import Point as MeteoPoint, Daily
    METEOSTAT_AVAILABLE = True
    print("✅ Meteostat available - will collect weather data")
except ImportError:
    METEOSTAT_AVAILABLE = False
    print("⚠️ Meteostat not available - weather data collection disabled")


# ============================================================================
# CONFIGURATION - Modify these settings
# ============================================================================

# Excel file configuration
EXCEL_FILE_PATH = "../states/ma/course_list/USGolfData-WithPlaceDetails_with_urls.xlsx"

# Processing configuration
MAX_COURSES_TO_PROCESS = 220  # Change this to process more/fewer courses
START_INDEX = 0             # Starting row in Excel (0 = first row)
OUTPUT_DIR = "golf_results" # Output directory

# Column mapping for your Excel file
COLUMN_MAPPING = {
    'name': 'DisplayName',           # Course name column
    'latitude': 'Latitude',          # Latitude column
    'longitude': 'Longitude',        # Longitude column
    'ecoursenumber': 'cCourseNumber' # Course number for folder prefix (e.g., "MA-10")
}


class GeospatialVisualizer:
    """Main class for handling aerial imagery download and golf course overlay."""

    def __init__(self, center_lat: float, center_lon: float, extent_km: float = 2.0,
                 basemap_type: str = "satellite", course_name: str = "unknown_course",
                 output_dir: str = "golf_results", ecoursenumber: str = None):
        """
        Initialize the visualizer with center coordinates and extent.

        Args:
            center_lat: Center latitude in decimal degrees
            center_lon: Center longitude in decimal degrees
            extent_km: Extent in kilometers (default 2km for 2km x 2km area)
            basemap_type: Type of basemap for overlay ("satellite", "osm", "terrain", "naip")
            course_name: Name of the course for folder creation
            output_dir: Base output directory (default: golf_results)
            ecoursenumber: Course number for folder prefix (e.g., "MA-11")
        """
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.extent_km = extent_km
        self.basemap_type = basemap_type
        self.course_name = self._sanitize_course_name(course_name)
        self.ecoursenumber = ecoursenumber

        # Create course-specific folder structure with ecoursenumber prefix
        self.base_output_dir = output_dir
        if ecoursenumber:
            folder_name = f"{ecoursenumber}_{self.course_name}"
        else:
            folder_name = f"course_{self.course_name}"
        self.course_folder = os.path.join(output_dir, folder_name)
        os.makedirs(self.course_folder, exist_ok=True)

        print(f"📁 Created course folder: {self.course_folder}")

        # Calculate bounding box
        self.bbox = self._calculate_bbox()

        # WMS and API configuration - try multiple endpoints with correct layer names
        self.wms_configs = [
            ("https://imagery.nationalmap.gov/arcgis/services/USGSNAIPPlus/ImageServer/WMSServer", "USGSNAIPPlus"),
            ("https://imagery.nationalmap.gov/arcgis/services/USGSNAIPImagery/ImageServer/WMSServer", "USGSNAIPImagery"),
            ("https://basemap.nationalmap.gov/arcgis/services/USGSImageryOnly/MapServer/WMSServer", "USGSImageryOnly"),
            ("https://services.nationalmap.gov/arcgis/services/USGSNAIPImagery/ImageServer/WMSServer", "0")
        ]
        self.overpass_url = "https://overpass-api.de/api/interpreter"

        # DEM file configuration
        self.dem_file = os.path.join(self.course_folder, "dem_data.tif")

        # Elevation API configuration (fallback if DEM not available)
        self.elevation_apis = [
            {
                'name': 'Open-Elevation',
                'url': 'https://api.open-elevation.com/api/v1/lookup',
                'type': 'open-elevation'
            },
            {
                'name': 'USGS Elevation (Backup)',
                'url': 'https://nationalmap.gov/epqs/pqs.php',
                'type': 'usgs'
            },
            {
                'name': 'OpenTopoData',
                'url': 'https://api.opentopodata.org/v1/ned10m',
                'type': 'opentopodata'
            }
        ]

        print(f"Initialized GeospatialVisualizer:")
        print(f"  Course: {course_name}")
        print(f"  Center: {center_lat:.5f}, {center_lon:.5f}")
        print(f"  Extent: {extent_km}km x {extent_km}km")
        print(f"  Bounding box: {self.bbox}")
        print(f"  Output folder: {self.course_folder}")

    @staticmethod
    def _sanitize_course_name(course_name: str) -> str:
        """
        Create a filesystem-safe course name.

        Args:
            course_name: Original course name

        Returns:
            Sanitized course name safe for filesystem
        """
        # Remove special characters and spaces, replace with underscores
        sanitized = re.sub(r'[^\w\s-]', '', course_name.lower())
        sanitized = re.sub(r'[-\s]+', '_', sanitized)
        sanitized = sanitized.strip('_')

        # Limit length and handle empty names
        if len(sanitized) > 50:
            sanitized = sanitized[:50].rstrip('_')
        if not sanitized:
            sanitized = "unknown_course"

        return sanitized

    def _calculate_bbox(self) -> Tuple[float, float, float, float]:
        """
        Calculate bounding box from center point and extent.

        Returns:
            Tuple of (min_lon, min_lat, max_lon, max_lat)
        """
        # Approximate conversion: 1 degree ≈ 111 km
        # Calculate latitude and longitude offsets
        lat_degrees_per_km = 1.0 / 111.0
        lon_degrees_per_km = 1.0 / (111.0 * math.cos(math.radians(self.center_lat)))

        half_extent = self.extent_km / 2.0
        lat_delta = half_extent * lat_degrees_per_km
        lon_delta = half_extent * lon_degrees_per_km

        # Calculate bounds
        south = self.center_lat - lat_delta
        north = self.center_lat + lat_delta
        west = self.center_lon - lon_delta
        east = self.center_lon + lon_delta

        return (west, south, east, north)  # (min_lon, min_lat, max_lon, max_lat)

    def collect_weather_data(self, weather_years: int = 5) -> bool:
        """
        Collect raw weather data and save to course folder.
        NO ANALYSIS - just data collection.

        Args:
            weather_years: Number of years of historical data to collect

        Returns:
            True if successful, False otherwise
        """
        if not METEOSTAT_AVAILABLE:
            print("⚠️ Meteostat not available - skipping weather data collection")
            return False

        print(f"🌦️ Collecting {weather_years} years of weather data...")

        try:
            from datetime import datetime

            # FIX SSL CERTIFICATE ISSUE FOR MACOS
            import ssl
            import urllib.request

            # Create unverified SSL context (for data collection only)
            ssl._create_default_https_context = ssl._create_unverified_context

            # Alternative: If you want to use certifi
            # import certifi
            # ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())

            # Calculate date range
            current_year = datetime.now().year
            start_year = current_year - (weather_years - 1)
            start_date = datetime(start_year, 1, 1)
            end_date = datetime(current_year, 12, 31)

            # Create Meteostat point
            point = MeteoPoint(self.center_lat, self.center_lon)


            # Fetch daily weather data
            print(f"   📍 Fetching data for coordinates: {self.center_lat:.4f}, {self.center_lon:.4f}")
            print(f"   📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

            weather_data = Daily(point, start_date, end_date).fetch()

            if weather_data.empty:
                print("❌ No weather data available for this location and date range")
                return False

            # Convert to JSON-serializable format
            weather_json = {
                'metadata': {
                    'location': {
                        'latitude': self.center_lat,
                        'longitude': self.center_lon,
                        'course_name': self.course_name
                    },
                    'date_range': {
                        'start_date': start_date.isoformat(),
                        'end_date': end_date.isoformat(),
                        'years_collected': weather_years
                    },
                    'collection_timestamp': datetime.now().isoformat(),
                    'total_days': len(weather_data),
                    'data_source': 'meteostat'
                },
                'daily_data': []
            }

            # Convert daily data to JSON format
            for date, row in weather_data.iterrows():
                daily_record = {
                    'date': date.isoformat(),
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'day_of_year': date.timetuple().tm_yday,
                    'weekday': date.weekday(),  # 0=Monday, 6=Sunday
                    'temperature_avg_c': float(row['tavg']) if pd.notna(row['tavg']) else None,
                    'temperature_min_c': float(row['tmin']) if pd.notna(row['tmin']) else None,
                    'temperature_max_c': float(row['tmax']) if pd.notna(row['tmax']) else None,
                    'precipitation_mm': float(row['prcp']) if pd.notna(row['prcp']) else None,
                    'snowfall_mm': float(row['snow']) if pd.notna(row['snow']) else None,
                    'wind_direction_deg': float(row['wdir']) if pd.notna(row['wdir']) else None,
                    'wind_speed_kmh': float(row['wspd'] * 3.6) if pd.notna(row['wspd']) else None,  # Convert m/s to km/h
                    'pressure_hpa': float(row['pres']) if pd.notna(row['pres']) else None,
                    'sunshine_minutes': float(row['tsun']) if pd.notna(row['tsun']) else None
                }
                weather_json['daily_data'].append(daily_record)

            # Save raw weather data
            weather_file = os.path.join(self.course_folder, "weather_raw_data.json")
            with open(weather_file, 'w') as f:
                json.dump(weather_json, f, indent=2)

            print(f"✅ Collected weather data: {len(weather_data)} days")
            print(f"   📄 Saved to: {weather_file}")
            print(f"   📊 Data columns: {list(weather_data.columns)}")

            # Quick data quality check
            valid_temp_days = weather_data['tavg'].notna().sum()
            valid_precip_days = weather_data['prcp'].notna().sum()
            valid_wind_days = weather_data['wspd'].notna().sum()

            print(f"   📈 Data quality:")
            print(f"      Temperature: {valid_temp_days}/{len(weather_data)} days ({100*valid_temp_days/len(weather_data):.1f}%)")
            print(f"      Precipitation: {valid_precip_days}/{len(weather_data)} days ({100*valid_precip_days/len(weather_data):.1f}%)")
            print(f"      Wind: {valid_wind_days}/{len(weather_data)} days ({100*valid_wind_days/len(weather_data):.1f}%)")

            return True

        except Exception as e:
            print(f"❌ Error collecting weather data: {e}")
            return False

    def download_naip_image(self, image_size: int = 1024) -> Optional[Image.Image]:
        """
        Download aerial imagery from USGS NAIP WMS service.

        Args:
            image_size: Size of the image in pixels (default 1024x1024)

        Returns:
            PIL Image object or None if download fails
        """
        print(f"Downloading NAIP aerial image ({image_size}x{image_size})...")

        # Try multiple WMS endpoints with correct layer names
        for i, (wms_url, layer_name) in enumerate(self.wms_configs):
            try:
                print(f"Attempting WMS endpoint {i+1}/{len(self.wms_configs)}: {wms_url}")
                print(f"  Using layer: {layer_name}")

                # WMS parameters as specified
                params = {
                    'SERVICE': 'WMS',
                    'VERSION': '1.3.0',
                    'REQUEST': 'GetMap',
                    'FORMAT': 'image/jpeg',
                    'LAYERS': layer_name,
                    'CRS': 'EPSG:4326',
                    'BBOX': f"{self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]}",  # min_lat,min_lon,max_lat,max_lon for EPSG:4326
                    'WIDTH': str(image_size),
                    'HEIGHT': str(image_size)
                }

                response = requests.get(wms_url, params=params, timeout=30)
                response.raise_for_status()

                # Check if response is actually an image
                content_type = response.headers.get('content-type', '')
                if 'image' not in content_type:
                    print(f"  Non-image response: {content_type}")
                    continue

                image = Image.open(io.BytesIO(response.content))
                print(f"✅ Successfully downloaded NAIP image from endpoint {i+1}: {image.size}")
                return image

            except requests.exceptions.RequestException as e:
                print(f"  ❌ Endpoint {i+1} failed: {str(e)[:100]}...")
                continue
            except Exception as e:
                print(f"  ❌ Error processing response from endpoint {i+1}: {e}")
                continue

        print("❌ All NAIP WMS endpoints failed")
        print("Attempting fallback satellite imagery...")

        # Try OpenStreetMap satellite imagery as fallback
        return self._download_fallback_imagery(image_size)

    def _download_fallback_imagery(self, image_size: int = 1024) -> Optional[Image.Image]:
        """
        Download satellite imagery from alternative sources as fallback.

        Args:
            image_size: Size of the image in pixels

        Returns:
            PIL Image object or None if download fails
        """
        try:
            # Use Esri World Imagery service as fallback
            esri_url = "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export"

            params = {
                'bbox': f"{self.bbox[0]},{self.bbox[1]},{self.bbox[2]},{self.bbox[3]}",
                'bboxSR': '4326',
                'imageSR': '4326',
                'size': f"{image_size},{image_size}",
                'format': 'jpeg',
                'f': 'image'
            }

            print(f"Trying Esri World Imagery service...")
            response = requests.get(esri_url, params=params, timeout=30)
            response.raise_for_status()

            if 'image' in response.headers.get('content-type', ''):
                image = Image.open(io.BytesIO(response.content))
                print(f"✅ Downloaded fallback satellite imagery: {image.size}")
                return image

        except Exception as e:
            print(f"Fallback imagery also failed: {e}")

        return None

    def download_golf_courses(self) -> Optional[gpd.GeoDataFrame]:
        """
        Download golf course polygons and features from OpenStreetMap using Overpass API.
        Includes golf holes, tees, greens, fairways, bunkers, water hazards, and other golf features.

        Returns:
            GeoDataFrame with golf course features or None if download fails
        """
        print("Downloading golf course data from OpenStreetMap...")
        return self._download_golf_courses_overpass()

    def _download_golf_courses_overpass(self) -> Optional[gpd.GeoDataFrame]:
        """
        Fallback method using direct Overpass API calls.
        """
        overpass_query = f"""[out:json][timeout:30];
(
  way["leisure"="golf_course"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  way["golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  node["golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  relation["leisure"="golf_course"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  relation["golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  way["sport"="golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  node["sport"="golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
  relation["sport"="golf"]({self.bbox[1]},{self.bbox[0]},{self.bbox[3]},{self.bbox[2]});
);
out body;
>;
out skel qt;"""

        try:
            response = requests.post(self.overpass_url, data=overpass_query, timeout=30)
            response.raise_for_status()
            osm_data = response.json()

            if not osm_data.get('elements'):
                return gpd.GeoDataFrame()

            return self._osm_to_geodataframe(osm_data)

        except Exception as e:
            print(f"Error with Overpass API fallback: {e}")
            return None

    def _osm_to_geodataframe(self, osm_data: Dict[str, Any]) -> gpd.GeoDataFrame:
        """
        Convert OSM JSON data to GeoDataFrame.

        Args:
            osm_data: OSM data from Overpass API

        Returns:
            GeoDataFrame with golf course polygons
        """
        features = []
        nodes = {}

        # First pass: collect all nodes for reference
        for element in osm_data['elements']:
            if element['type'] == 'node':
                nodes[element['id']] = (element['lon'], element['lat'])

        # Second pass: process ways and relations
        for element in osm_data['elements']:
            if element['type'] == 'way':
                # Handle ways with node references or geometry
                coords = []

                if 'geometry' in element:
                    # Direct geometry data (from out geom)
                    coords = [(node['lon'], node['lat']) for node in element['geometry']]
                elif 'nodes' in element:
                    # Node references (from out body)
                    coords = []
                    for node_id in element['nodes']:
                        if node_id in nodes:
                            coords.append(nodes[node_id])

                if len(coords) >= 2:
                    tags = element.get('tags', {})

                    # Determine if this should be a LineString or Polygon
                    # Check if the way is naturally closed (first and last points are the same)
                    is_naturally_closed = len(coords) >= 3 and coords[0] == coords[-1]

                    # Area features that should be polygons (when closed)
                    area_golf_types = ['fairway', 'green', 'tee', 'bunker', 'water_hazard', 'lateral_water_hazard', 'rough', 'driving_range']

                    is_area_feature = (
                        tags.get('golf') in area_golf_types or
                        tags.get('leisure') == 'golf_course' or
                        tags.get('area') == 'yes'
                    )

                    # Special handling for golf=hole: keep as line unless explicitly closed
                    if tags.get('golf') == 'hole':
                        if is_naturally_closed and len(coords) >= 4:
                            # It's a closed area representing a hole boundary
                            geometry = Polygon(coords)
                        else:
                            # It's a centerline representing the path from tee to green
                            geometry = LineString(coords)
                    elif is_area_feature and is_naturally_closed:
                        # Create polygon for area features that are naturally closed
                        geometry = Polygon(coords)
                    elif is_area_feature and len(coords) >= 3:
                        # Force close polygon for area features that should be areas
                        if coords[0] != coords[-1]:
                            coords.append(coords[0])
                        geometry = Polygon(coords)
                    else:
                        # Create LineString for linear features
                        geometry = LineString(coords)

                    features.append({
                        'geometry': geometry,
                        'osm_id': element['id'],
                        'osm_type': element['type'],
                        'tags': tags,
                        'leisure': tags.get('leisure'),
                        'golf': tags.get('golf'),
                        'name': tags.get('name'),
                        'ref': tags.get('ref'),
                        'par': tags.get('par'),
                        'handicap': tags.get('handicap')
                    })

        if not features:
            return gpd.GeoDataFrame()

        gdf = gpd.GeoDataFrame(features, crs='EPSG:4326')
        return gdf

    def get_elevation_data(self, grid_resolution: int = 50) -> Optional[Dict[str, Any]]:
        """
        Collect elevation data for the golf course area.
        Uses py3dep package (preferred) and falls back to elevation APIs.

        Args:
            grid_resolution: Number of points per side for elevation grid

        Returns:
            Dictionary with elevation data or None if collection fails
        """
        print(f"Collecting elevation data with {grid_resolution}x{grid_resolution} grid...")

        # Method 1: Try py3dep package (much more reliable than WCS)
        if PY3DEP_AVAILABLE and RASTERIO_AVAILABLE:
            print("🏔️ Attempting to use py3dep package...")

            if self.download_dem_data():
                elevation_data = self.get_elevation_from_dem(grid_resolution)
                if elevation_data:
                    print("✅ Successfully collected elevation data using py3dep")
                    return elevation_data
                else:
                    print("❌ Failed to extract elevation from py3dep DEM, trying APIs...")
            else:
                print("❌ Failed to download DEM with py3dep, trying APIs...")

        # Method 2: Fall back to elevation APIs
        print("🌐 Using elevation APIs as fallback...")
        return self.get_elevation_data_from_apis(grid_resolution)

    def get_elevation_data_from_apis(self, grid_resolution: int = 50) -> Optional[Dict[str, Any]]:
        """
        Collect elevation data for the golf course area using elevation APIs.

        Args:
            grid_resolution: Number of points per side for elevation grid

        Returns:
            Dictionary with elevation data or None if collection fails
        """
        # Create a grid of points across the bounding box
        west, south, east, north = self.bbox

        # Create coordinate grids
        lons = np.linspace(west, east, grid_resolution)
        lats = np.linspace(south, north, grid_resolution)
        lon_grid, lat_grid = np.meshgrid(lons, lats)

        # Flatten for API queries
        points = [(lon, lat) for lon, lat in zip(lon_grid.flatten(), lat_grid.flatten())]

        print(f"Querying elevation APIs for {len(points)} points...")

        # Try each elevation API
        elevation_data = None
        for api_config in self.elevation_apis:
            try:
                print(f"Trying {api_config['name']} API...")
                elevation_data = self._query_elevation_api(points, api_config)
                if elevation_data:
                    print(f"✅ Successfully got elevation data from {api_config['name']}")
                    break
                else:
                    print(f"❌ {api_config['name']} failed or returned no data")
            except Exception as e:
                print(f"❌ Error with {api_config['name']}: {e}")
                continue

        if not elevation_data:
            print("❌ All elevation APIs failed")
            return None

        # Reshape elevation data back to grid
        try:
            elevations = np.array([point['elevation'] for point in elevation_data])
            elevation_grid = elevations.reshape(grid_resolution, grid_resolution)

            # Calculate elevation statistics
            valid_elevations = elevations[~np.isnan(elevations)]
            elevation_stats = {
                'min_elevation': float(np.min(valid_elevations)),
                'max_elevation': float(np.max(valid_elevations)),
                'mean_elevation': float(np.mean(valid_elevations)),
                'elevation_range': float(np.max(valid_elevations) - np.min(valid_elevations))
            }

            print(f"📊 API Elevation Statistics:")
            print(f"   Min: {elevation_stats['min_elevation']:.1f}m")
            print(f"   Max: {elevation_stats['max_elevation']:.1f}m")
            print(f"   Mean: {elevation_stats['mean_elevation']:.1f}m")
            print(f"   Range: {elevation_stats['elevation_range']:.1f}m")

            return {
                'lon_grid': lon_grid,
                'lat_grid': lat_grid,
                'elevation_grid': elevation_grid,
                'points': elevation_data,
                'stats': elevation_stats,
                'grid_resolution': grid_resolution,
                'source': 'API'
            }

        except Exception as e:
            print(f"❌ Error processing elevation data: {e}")
            return None

    def _query_elevation_api(self, points: list, api_config: dict) -> Optional[list]:
        """
        Query elevation API for a list of points.

        Args:
            points: List of (lon, lat) tuples
            api_config: API configuration dictionary

        Returns:
            List of elevation data points or None if failed
        """
        if api_config['type'] == 'open-elevation':
            return self._query_open_elevation(points)
        elif api_config['type'] == 'usgs':
            return self._query_usgs_elevation(points)
        elif api_config['type'] == 'opentopodata':
            return self._query_opentopodata_elevation(points)
        else:
            return None

    def _query_open_elevation(self, points: list) -> Optional[list]:
        """Query Open-Elevation API (can handle batch requests)."""
        try:
            # Open-Elevation can handle batch requests, but let's limit batch size
            batch_size = 100  # Increased batch size
            elevation_data = []

            for i in range(0, len(points), batch_size):
                batch_points = points[i:i+batch_size]
                print(f"   Querying batch {i//batch_size + 1}/{(len(points) + batch_size - 1)//batch_size} ({len(batch_points)} points)")

                locations = [{'latitude': lat, 'longitude': lon} for lon, lat in batch_points]

                try:
                    response = requests.post(
                        'https://api.open-elevation.com/api/v1/lookup',
                        json={'locations': locations},
                        timeout=60,
                        headers={'Content-Type': 'application/json'}
                    )
                    response.raise_for_status()

                    data = response.json()
                    results = data.get('results', [])

                    for result in results:
                        elevation_data.append({
                            'longitude': result['longitude'],
                            'latitude': result['latitude'],
                            'elevation': result.get('elevation', np.nan)
                        })

                    print(f"      ✅ Got {len(results)} elevation points")

                except Exception as e:
                    print(f"      ❌ Batch failed: {e}")
                    # Add NaN values for failed batch
                    for lon, lat in batch_points:
                        elevation_data.append({
                            'longitude': lon,
                            'latitude': lat,
                            'elevation': np.nan
                        })

                # Delay between batches to be respectful
                time.sleep(1)

            valid_count = sum(1 for p in elevation_data if not np.isnan(p['elevation']))
            print(f"   Open-Elevation: {valid_count}/{len(elevation_data)} valid elevations")

            if valid_count > len(elevation_data) * 0.5:  # If we got >50% valid data
                return elevation_data
            else:
                return None

        except Exception as e:
            print(f"Error with Open-Elevation API: {e}")
            return None

    def _query_usgs_elevation(self, points: list) -> Optional[list]:
        """Query USGS Elevation Point Query Service (as backup)."""
        elevation_data = []

        print("   USGS API querying individual points (this may take a while)...")

        # USGS API handles individual points only
        for i, (lon, lat) in enumerate(points):
            if i % 100 == 0:  # Progress update every 100 points
                print(f"   Progress: {i+1}/{len(points)} points")

            try:
                params = {
                    'x': lon,
                    'y': lat,
                    'units': 'Meters',
                    'output': 'json'
                }

                response = requests.get(
                    'https://nationalmap.gov/epqs/pqs.php',
                    params=params,
                    timeout=10,
                    headers={'User-Agent': 'Golf Course Analyzer'}
                )

                if response.status_code == 200 and response.text.strip():
                    try:
                        data = response.json()
                        elevation_query = data.get('USGS_Elevation_Point_Query_Service', {})
                        elevation_result = elevation_query.get('Elevation_Query', {})
                        elevation = elevation_result.get('Elevation')

                        if elevation is not None and elevation != -1000000:  # USGS returns -1000000 for no data
                            elevation_data.append({
                                'longitude': lon,
                                'latitude': lat,
                                'elevation': float(elevation)
                            })
                        else:
                            elevation_data.append({
                                'longitude': lon,
                                'latitude': lat,
                                'elevation': np.nan
                            })
                    except json.JSONDecodeError:
                        elevation_data.append({
                            'longitude': lon,
                            'latitude': lat,
                            'elevation': np.nan
                        })
                else:
                    elevation_data.append({
                        'longitude': lon,
                        'latitude': lat,
                        'elevation': np.nan
                    })

                # Small delay to avoid overwhelming the API
                time.sleep(0.1)

            except Exception as e:
                if i < 5:  # Only print first few errors
                    print(f"   Error querying USGS point {i}: {e}")
                elevation_data.append({
                    'longitude': lon,
                    'latitude': lat,
                    'elevation': np.nan
                })

        valid_count = sum(1 for p in elevation_data if not np.isnan(p['elevation']))
        print(f"   USGS: {valid_count}/{len(elevation_data)} valid elevations")

        if valid_count > len(elevation_data) * 0.3:  # Lower threshold for USGS
            return elevation_data
        else:
            return None

    def _query_opentopodata_elevation(self, points: list) -> Optional[list]:
        """Query OpenTopoData API."""
        try:
            # OpenTopoData can handle batch requests
            batch_size = 100
            elevation_data = []

            for i in range(0, len(points), batch_size):
                batch_points = points[i:i+batch_size]
                print(f"   Querying OpenTopoData batch {i//batch_size + 1}/{(len(points) + batch_size - 1)//batch_size}")

                # Format locations as lat,lon pairs
                locations = '|'.join([f"{lat},{lon}" for lon, lat in batch_points])

                try:
                    response = requests.get(
                        f'https://api.opentopodata.org/v1/ned10m?locations={locations}',
                        timeout=60
                    )
                    response.raise_for_status()

                    data = response.json()
                    results = data.get('results', [])

                    for i, result in enumerate(results):
                        lon, lat = batch_points[i]
                        elevation_data.append({
                            'longitude': lon,
                            'latitude': lat,
                            'elevation': result.get('elevation', np.nan)
                        })

                    print(f"      ✅ Got {len(results)} elevation points")

                except Exception as e:
                    print(f"      ❌ OpenTopoData batch failed: {e}")
                    # Add NaN values for failed batch
                    for lon, lat in batch_points:
                        elevation_data.append({
                            'longitude': lon,
                            'latitude': lat,
                            'elevation': np.nan
                        })

                # Delay between batches
                time.sleep(1)

            valid_count = sum(1 for p in elevation_data if not np.isnan(p['elevation']))
            print(f"   OpenTopoData: {valid_count}/{len(elevation_data)} valid elevations")

            if valid_count > len(elevation_data) * 0.5:
                return elevation_data
            else:
                return None

        except Exception as e:
            print(f"Error with OpenTopoData API: {e}")
            return None

    def download_dem_data(self) -> bool:
        """
        Download USGS DEM data using py3dep package (much more reliable than WCS).

        Returns:
            True if DEM data is available, False otherwise
        """
        if not PY3DEP_AVAILABLE:
            print("⚠️ py3dep not available - cannot download DEM data")
            return False

        if not RASTERIO_AVAILABLE:
            print("⚠️ Rasterio not available - cannot save DEM data")
            return False

        try:
            print("🌍 Using py3dep package to download USGS 3DEP elevation data...")

            # Calculate bounds with small buffer
            west, south, east, north = self.bbox
            buffer = 0.002  # ~200m buffer
            west -= buffer
            south -= buffer
            east += buffer
            north += buffer

            print(f"📍 Requesting DEM for bounds: {west:.6f}, {south:.6f}, {east:.6f}, {north:.6f}")

            # Use py3dep to get elevation data with correct API syntax
            # The geometry parameter expects (xmin, ymin, xmax, ymax)
            bbox_tuple = (west, south, east, north)

            # Method 1: Try static_3dep_dem first (faster for 30m resolution)
            try:
                print("🚀 Trying static_3dep_dem (faster method)...")
                dem_data = py3dep.static_3dep_dem(
                    geometry=bbox_tuple,
                    crs="EPSG:4326",  # Input CRS
                    resolution=30     # 30-meter resolution
                )
                print("✅ Successfully retrieved DEM data using static_3dep_dem")
            except Exception as e:
                print(f"⚠️ static_3dep_dem failed: {e}")
                print("🚀 Trying get_dem (alternative method)...")

                # Method 2: Try get_dem as fallback
                dem_data = py3dep.get_dem(
                    geometry=bbox_tuple,
                    resolution=30,
                    geo_crs="EPSG:4326"  # Input geometry CRS
                )
                print("✅ Successfully retrieved DEM data using get_dem")

            print(f"📊 Retrieved DEM data: {dem_data.shape}")
            print(f"   Data type: {dem_data.dtype}")
            print(f"   CRS: {dem_data.rio.crs}")
            print(f"   Bounds: {dem_data.rio.bounds()}")
            print(f"   Resolution: {dem_data.rio.resolution()}")

            # Save as GeoTIFF using rioxarray (built into py3dep data)
            print(f"💾 Saving DEM data to: {self.dem_file}")

            # Use rioxarray to save directly (much simpler)
            dem_data.rio.to_raster(self.dem_file, compress='lzw')

            print(f"✅ Saved DEM data successfully")

            # Validate the saved file
            with rasterio.open(self.dem_file) as src:
                print(f"📊 DEM Validation:")
                print(f"   File dimensions: {src.shape}")
                print(f"   Data type: {src.dtypes[0]}")
                print(f"   CRS: {src.crs}")
                print(f"   Bounds: {src.bounds}")

                # Sample some elevation values
                sample_data = src.read(1)
                # Handle nodata values
                if src.nodata is not None:
                    valid_data = sample_data[sample_data != src.nodata]
                else:
                    valid_data = sample_data[~np.isnan(sample_data)]

                if len(valid_data) > 0:
                    print(f"   Elevation range: {valid_data.min():.1f}m to {valid_data.max():.1f}m")
                    print(f"   Valid pixels: {len(valid_data)}/{sample_data.size} ({100*len(valid_data)/sample_data.size:.1f}%)")
                else:
                    print("   ⚠️ No valid elevation data found")
                    return False

            return True

        except ImportError as e:
            print(f"❌ py3dep package not available: {e}")
            print("💡 Install with: pip install py3dep")
            return False
        except Exception as e:
            print(f"❌ Error downloading DEM data with py3dep: {e}")
            print(f"   Error type: {type(e).__name__}")
            print("💡 Falling back to elevation APIs...")
            return False

    def get_elevation_from_dem(self, grid_resolution: int = 50) -> Optional[Dict[str, Any]]:
        """
        Extract elevation data from DEM file downloaded with py3dep.

        Args:
            grid_resolution: Number of points per side for elevation grid

        Returns:
            Dictionary with elevation data or None if extraction fails
        """
        if not RASTERIO_AVAILABLE:
            return None

        if not os.path.exists(self.dem_file):
            print("❌ DEM file not found")
            return None

        try:
            print(f"📖 Reading elevation data from DEM file...")

            with rasterio.open(self.dem_file) as dem:
                print(f"   DEM bounds: {dem.bounds}")
                print(f"   DEM CRS: {dem.crs}")
                print(f"   DEM shape: {dem.shape}")
                print(f"   DEM resolution: {dem.res}")
                print(f"   DEM nodata value: {dem.nodata}")

                # Create grid of points covering our area of interest
                west, south, east, north = self.bbox
                lons = np.linspace(west, east, grid_resolution)
                lats = np.linspace(south, north, grid_resolution)
                lon_grid, lat_grid = np.meshgrid(lons, lats)

                # Read the entire DEM array
                dem_array = dem.read(1)

                # Sample elevations at grid points
                elevation_grid = np.zeros((grid_resolution, grid_resolution))

                for i in range(grid_resolution):
                    for j in range(grid_resolution):
                        lon, lat = lon_grid[i, j], lat_grid[i, j]

                        try:
                            # Convert geographic coordinates to pixel coordinates
                            row, col = dem.index(lon, lat)

                            # Check if coordinates are within DEM bounds
                            if (0 <= row < dem.height and 0 <= col < dem.width):
                                elevation = dem_array[row, col]

                                # Handle nodata values more robustly
                                if dem.nodata is not None and elevation == dem.nodata:
                                    elevation_grid[i, j] = np.nan
                                elif np.isnan(elevation):
                                    elevation_grid[i, j] = np.nan
                                elif elevation < -1000 or elevation > 10000:  # Sanity check
                                    elevation_grid[i, j] = np.nan
                                else:
                                    elevation_grid[i, j] = elevation
                            else:
                                elevation_grid[i, j] = np.nan

                        except Exception as e:
                            # If any conversion fails, set to NaN
                            elevation_grid[i, j] = np.nan

                # Calculate statistics on valid elevations
                valid_elevations = elevation_grid[~np.isnan(elevation_grid)]

                if len(valid_elevations) == 0:
                    print("❌ No valid elevation data found in DEM")
                    return None

                elevation_stats = {
                    'min_elevation': float(np.min(valid_elevations)),
                    'max_elevation': float(np.max(valid_elevations)),
                    'mean_elevation': float(np.mean(valid_elevations)),
                    'elevation_range': float(np.max(valid_elevations) - np.min(valid_elevations))
                }

                print(f"📊 DEM Elevation Statistics:")
                print(f"   Min: {elevation_stats['min_elevation']:.1f}m")
                print(f"   Max: {elevation_stats['max_elevation']:.1f}m")
                print(f"   Mean: {elevation_stats['mean_elevation']:.1f}m")
                print(f"   Range: {elevation_stats['elevation_range']:.1f}m")
                print(f"   Valid points: {len(valid_elevations)}/{grid_resolution*grid_resolution} ({100*len(valid_elevations)/(grid_resolution*grid_resolution):.1f}%)")

                # Create elevation data points for compatibility with API methods
                elevation_points = []
                for i in range(grid_resolution):
                    for j in range(grid_resolution):
                        elevation_points.append({
                            'longitude': lon_grid[i, j],
                            'latitude': lat_grid[i, j],
                            'elevation': elevation_grid[i, j]
                        })

                return {
                    'lon_grid': lon_grid,
                    'lat_grid': lat_grid,
                    'elevation_grid': elevation_grid,
                    'points': elevation_points,
                    'stats': elevation_stats,
                    'grid_resolution': grid_resolution,
                    'source': 'py3dep'
                }

        except Exception as e:
            print(f"❌ Error reading DEM file: {e}")
            print(f"   Error type: {type(e).__name__}")
            return None

    def save_elevation_data(self, elevation_data: dict, filename: str = "elevation_data.json") -> bool:
        """
        Save elevation data to course folder.

        Args:
            elevation_data: Elevation data dictionary
            filename: Output filename

        Returns:
            True if successful, False otherwise
        """
        try:
            filepath = os.path.join(self.course_folder, filename)

            # Convert numpy arrays to lists for JSON serialization
            save_data = {
                'lon_grid': elevation_data['lon_grid'].tolist(),
                'lat_grid': elevation_data['lat_grid'].tolist(),
                'elevation_grid': elevation_data['elevation_grid'].tolist(),
                'stats': elevation_data['stats'],
                'grid_resolution': elevation_data['grid_resolution'],
                'bbox': list(self.bbox),
                'source': elevation_data.get('source', 'Unknown')
            }

            with open(filepath, 'w') as f:
                json.dump(save_data, f, indent=2)

            print(f"✅ Saved elevation data: {filepath}")
            return True

        except Exception as e:
            print(f"❌ Error saving elevation data: {e}")
            return False

    def create_elevation_overlay(self, elevation_data: dict, gdf: gpd.GeoDataFrame,
                               output_filename: str = "elevation_overlay.png") -> bool:
        """
        Create elevation contour overlay with golf course features.

        Args:
            elevation_data: Elevation data dictionary
            gdf: GeoDataFrame with golf course features
            output_filename: Output filename

        Returns:
            True if successful, False otherwise
        """
        print("Creating elevation overlay...")

        try:
            # Create figure
            fig, ax = plt.subplots(1, 1, figsize=(16, 16))

            # Create elevation contour plot
            lon_grid = elevation_data['lon_grid']
            lat_grid = elevation_data['lat_grid']
            elevation_grid = elevation_data['elevation_grid']

            # Create custom colormap for elevation (terrain-like)
            colors = ['#1f4e79', '#2e8b57', '#228b22', '#9acd32', '#ffd700', '#daa520', '#cd853f', '#a0522d', '#8b4513', '#ffffff']
            n_bins = 20
            elevation_cmap = LinearSegmentedColormap.from_list('elevation', colors, N=n_bins)

            # Create contour plot
            elevation_min = elevation_data['stats']['min_elevation']
            elevation_max = elevation_data['stats']['max_elevation']
            levels = np.linspace(elevation_min, elevation_max, 15)

            # Filled contours
            contour_filled = ax.contourf(lon_grid, lat_grid, elevation_grid,
                                       levels=levels, cmap=elevation_cmap, alpha=0.7)

            # Contour lines
            contour_lines = ax.contour(lon_grid, lat_grid, elevation_grid,
                                     levels=levels, colors='black', alpha=0.3, linewidths=0.5)

            # Add contour labels
            ax.clabel(contour_lines, inline=True, fontsize=8, fmt='%0.0f m')

            # Add colorbar
            cbar = plt.colorbar(contour_filled, ax=ax, shrink=0.8, aspect=30)
            cbar.set_label('Elevation (meters)', rotation=270, labelpad=20)

            # Overlay golf course features
            if not gdf.empty:
                # Golf course colors for overlay on elevation
                color_map = {
                    'fairway': {'color': '#90EE90', 'alpha': 0.6, 'linewidth': 1},
                    'green': {'color': '#32CD32', 'alpha': 0.8, 'linewidth': 1},
                    'tee': {'color': '#32CD32', 'alpha': 0.8, 'linewidth': 1},
                    'bunker': {'color': '#F4A460', 'alpha': 0.8, 'linewidth': 1},
                    'water_hazard': {'color': '#4169E1', 'alpha': 0.8, 'linewidth': 1},
                    'hole': {'color': 'white', 'alpha': 0.9, 'linewidth': 2}
                }

                for golf_type, style in color_map.items():
                    if golf_type == 'hole':
                        mask = (gdf['golf'] == golf_type)
                    else:
                        mask = (gdf['golf'] == golf_type)

                    features_subset = gdf[mask]
                    if not features_subset.empty:
                        if golf_type == 'hole':
                            # Render holes as lines
                            for idx, row in features_subset.iterrows():
                                if row.geometry.geom_type == 'LineString':
                                    gpd.GeoSeries([row.geometry]).plot(ax=ax, color=style['color'],
                                                                     linewidth=style['linewidth'], alpha=style['alpha'])
                        else:
                            # Render other features as filled areas with borders
                            features_subset.plot(ax=ax, facecolor=style['color'],
                                               edgecolor='black', linewidth=0.5, alpha=style['alpha'])

                # Add hole number labels
                holes = gdf[gdf['golf'] == 'hole']
                for idx, row in holes.iterrows():
                    hole_number = row.get('ref', '')
                    if hole_number:
                        if row.geometry.geom_type == 'LineString':
                            midpoint = row.geometry.interpolate(0.5, normalized=True)
                        else:
                            midpoint = row.geometry.centroid

                        ax.text(midpoint.x, midpoint.y, str(hole_number),
                               fontsize=10, fontweight='bold',
                               ha='center', va='center',
                               color='black',
                               bbox=dict(boxstyle='circle,pad=0.2',
                                       facecolor='white',
                                       alpha=0.8,
                                       edgecolor='black'))

            # Set title and labels
            ax.set_title(f'Elevation Map: {self.course_name.replace("_", " ").title()}\n'
                        f'Range: {elevation_data["stats"]["min_elevation"]:.0f}m - '
                        f'{elevation_data["stats"]["max_elevation"]:.0f}m '
                        f'(Total: {elevation_data["stats"]["elevation_range"]:.0f}m)',
                        fontsize=14, fontweight='bold')

            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

            # Set aspect ratio and limits
            ax.set_aspect('equal')
            ax.set_xlim(self.bbox[0], self.bbox[2])
            ax.set_ylim(self.bbox[1], self.bbox[3])

            # Add grid
            ax.grid(True, alpha=0.3)

            # Save the overlay
            filepath = os.path.join(self.course_folder, output_filename)
            plt.tight_layout()
            plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Saved elevation overlay: {filepath}")
            return True

        except Exception as e:
            print(f"❌ Error creating elevation overlay: {e}")
            return False

    def get_hole_elevation_profile_from_dem(self, hole_geometry, dem_file: str = None) -> Optional[Dict]:
        """
        Get elevation profile for a specific hole using DEM data.

        Args:
            hole_geometry: Shapely geometry of the hole centerline
            dem_file: Path to DEM file (uses self.dem_file if None)

        Returns:
            Dictionary with elevation profile or None if failed
        """
        if not RASTERIO_AVAILABLE:
            return None

        dem_path = dem_file or self.dem_file
        if not os.path.exists(dem_path):
            return None

        try:
            with rasterio.open(dem_path) as dem:
                if hole_geometry.geom_type == 'LineString':
                    # Sample points along the hole centerline
                    num_points = 20
                    sample_points = [hole_geometry.interpolate(dist, normalized=True)
                                   for dist in np.linspace(0, 1, num_points)]

                    elevations = []
                    distances = []
                    coordinates = []

                    for i, point in enumerate(sample_points):
                        lon, lat = point.x, point.y

                        try:
                            row, col = dem.index(lon, lat)

                            if (0 <= row < dem.height and 0 <= col < dem.width):
                                elevation = dem.read(1)[row, col]

                                if elevation != dem.nodata and elevation > -1000:
                                    elevations.append(float(elevation))
                                    distances.append(i / (num_points - 1))  # Normalized distance
                                    coordinates.append({'longitude': lon, 'latitude': lat})
                                else:
                                    elevations.append(np.nan)
                                    distances.append(i / (num_points - 1))
                                    coordinates.append({'longitude': lon, 'latitude': lat})
                            else:
                                elevations.append(np.nan)
                                distances.append(i / (num_points - 1))
                                coordinates.append({'longitude': lon, 'latitude': lat})

                        except Exception:
                            elevations.append(np.nan)
                            distances.append(i / (num_points - 1))
                            coordinates.append({'longitude': lon, 'latitude': lat})

                    # Filter out NaN values for statistics
                    valid_elevations = [e for e in elevations if not np.isnan(e)]

                    if len(valid_elevations) > 0:
                        return {
                            'elevations': elevations,
                            'distances': distances,
                            'coordinates': coordinates,
                            'hole_length_degrees': hole_geometry.length,
                            'elevation_change': max(valid_elevations) - min(valid_elevations),
                            'max_elevation': max(valid_elevations),
                            'min_elevation': min(valid_elevations),
                            'valid_points': len(valid_elevations),
                            'total_points': len(elevations)
                        }

        except Exception as e:
            print(f"Error getting hole elevation profile: {e}")
            return None

    def create_elevation_profile_data(self, elevation_data: dict, gdf: gpd.GeoDataFrame) -> Optional[Dict]:
        """
        Create elevation profile data for each golf hole.
        Uses DEM data if available, otherwise falls back to grid interpolation.

        Args:
            elevation_data: Elevation data dictionary
            gdf: GeoDataFrame with golf course features

        Returns:
            Dictionary with hole elevation profiles
        """
        print("Creating elevation profile data for holes...")

        try:
            holes = gdf[gdf['golf'] == 'hole']
            if holes.empty:
                print("No holes found for elevation profiling")
                return None

            hole_profiles = {}

            # Check if we have DEM data available
            use_dem = (RASTERIO_AVAILABLE and
                      os.path.exists(self.dem_file) and
                      elevation_data.get('source') == 'py3dep')

            if use_dem:
                print("Using DEM data for precise hole elevation profiles...")

                for idx, hole in holes.iterrows():
                    hole_number = hole.get('ref', f'hole_{idx}')

                    profile = self.get_hole_elevation_profile_from_dem(hole.geometry)
                    if profile:
                        hole_profiles[hole_number] = profile
                        print(f"   Hole {hole_number}: {profile['valid_points']}/{profile['total_points']} valid points, "
                              f"{profile['elevation_change']:.1f}m elevation change")
            else:
                print("Using grid interpolation for hole elevation profiles...")

                # Fallback to grid interpolation method
                elevation_points = elevation_data['points']
                coords = np.array([(p['longitude'], p['latitude']) for p in elevation_points])
                elevations = np.array([p['elevation'] for p in elevation_points])

                # Remove NaN elevations
                valid_mask = ~np.isnan(elevations)
                coords = coords[valid_mask]
                elevations = elevations[valid_mask]

                for idx, hole in holes.iterrows():
                    hole_number = hole.get('ref', f'hole_{idx}')

                    if hole.geometry.geom_type == 'LineString':
                        # Sample points along the hole centerline
                        line = hole.geometry
                        distances = np.linspace(0, line.length, 20)  # 20 points along the hole

                        profile_points = []
                        profile_elevations = []

                        for distance in distances:
                            point = line.interpolate(distance)

                            # Find nearest elevation data point
                            point_coords = np.array([[point.x, point.y]])
                            distances_to_elevation_points = cdist(point_coords, coords)[0]
                            nearest_idx = np.argmin(distances_to_elevation_points)

                            if distances_to_elevation_points[nearest_idx] < 0.001:  # Within reasonable distance
                                profile_points.append({
                                    'longitude': point.x,
                                    'latitude': point.y,
                                    'distance_along_hole': distance
                                })
                                profile_elevations.append(elevations[nearest_idx])

                        if profile_elevations:
                            hole_profiles[hole_number] = {
                                'elevations': profile_elevations,
                                'coordinates': profile_points,
                                'hole_length_degrees': line.length,
                                'elevation_change': max(profile_elevations) - min(profile_elevations),
                                'max_elevation': max(profile_elevations),
                                'min_elevation': min(profile_elevations),
                                'valid_points': len(profile_elevations),
                                'total_points': len(profile_elevations)
                            }

                            print(f"   Hole {hole_number}: {len(profile_elevations)} elevation points, "
                                  f"{hole_profiles[hole_number]['elevation_change']:.1f}m elevation change")

            return hole_profiles

        except Exception as e:
            print(f"❌ Error creating elevation profiles: {e}")
            return None

    def save_elevation_profiles(self, hole_profiles: dict, filename: str = "hole_elevation_profiles.json") -> bool:
        """
        Save hole elevation profile data.

        Args:
            hole_profiles: Dictionary with hole elevation profiles
            filename: Output filename

        Returns:
            True if successful, False otherwise
        """
        try:
            filepath = os.path.join(self.course_folder, filename)

            with open(filepath, 'w') as f:
                json.dump(hole_profiles, f, indent=2)

            print(f"✅ Saved hole elevation profiles: {filepath}")
            return True

        except Exception as e:
            print(f"❌ Error saving elevation profiles: {e}")
            return False

    def create_individual_hole_elevation_maps(self, elevation_data: dict, gdf: gpd.GeoDataFrame, hole_profiles: dict) -> bool:
        """
        Create individual elevation maps for each hole.

        Args:
            elevation_data: Elevation data dictionary
            gdf: GeoDataFrame with golf course features
            hole_profiles: Dictionary with hole elevation profiles

        Returns:
            True if successful, False otherwise
        """
        print("Creating individual hole elevation maps...")

        try:
            # Create hole maps directory
            hole_maps_dir = os.path.join(self.course_folder, "hole_elevation_maps")
            os.makedirs(hole_maps_dir, exist_ok=True)

            holes = gdf[gdf['golf'] == 'hole']
            if holes.empty:
                print("No holes found for individual maps")
                return False

            success_count = 0
            for idx, hole in holes.iterrows():
                hole_number = hole.get('ref', f'hole_{idx}')

                try:
                    # Create figure for this hole
                    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

                    # Plot elevation background
                    lon_grid = elevation_data['lon_grid']
                    lat_grid = elevation_data['lat_grid']
                    elevation_grid = elevation_data['elevation_grid']

                    # Create terrain colormap
                    colors = ['#2e8b57', '#228b22', '#9acd32', '#ffd700', '#daa520', '#cd853f']
                    elevation_cmap = LinearSegmentedColormap.from_list('terrain', colors, N=15)

                    # Filled contours
                    contour_filled = ax.contourf(lon_grid, lat_grid, elevation_grid,
                                               levels=15, cmap=elevation_cmap, alpha=0.8)

                    # Highlight this specific hole
                    if hole.geometry.geom_type == 'LineString':
                        gpd.GeoSeries([hole.geometry]).plot(ax=ax, color='red', linewidth=4, alpha=0.9)
                    else:
                        gpd.GeoSeries([hole.geometry]).plot(ax=ax, facecolor='red', edgecolor='darkred',
                                                           linewidth=2, alpha=0.7)

                    # Add hole number label
                    if hole.geometry.geom_type == 'LineString':
                        midpoint = hole.geometry.interpolate(0.5, normalized=True)
                    else:
                        midpoint = hole.geometry.centroid

                    ax.text(midpoint.x, midpoint.y, str(hole_number),
                           fontsize=16, fontweight='bold',
                           ha='center', va='center',
                           color='white',
                           bbox=dict(boxstyle='circle,pad=0.3',
                                   facecolor='red',
                                   alpha=0.9,
                                   edgecolor='darkred',
                                   linewidth=2))

                    # Add other golf features in muted colors
                    other_features = gdf[gdf.index != idx]
                    if not other_features.empty:
                        other_features.plot(ax=ax, facecolor='lightgreen', edgecolor='darkgreen',
                                          alpha=0.3, linewidth=0.5)

                    # Set title with elevation info
                    title = f'Hole {hole_number} Elevation Profile'
                    if hole_number in hole_profiles:
                        profile = hole_profiles[hole_number]
                        title += f'\nElevation Change: {profile["elevation_change"]:.1f}m'
                        title += f' (Min: {profile["min_elevation"]:.1f}m, Max: {profile["max_elevation"]:.1f}m)'

                    ax.set_title(title, fontsize=12, fontweight='bold')

                    # Focus on hole area with buffer
                    bounds = hole.geometry.bounds
                    buffer = 0.0005  # Small buffer around hole
                    ax.set_xlim(bounds[0] - buffer, bounds[2] + buffer)
                    ax.set_ylim(bounds[1] - buffer, bounds[3] + buffer)

                    ax.set_aspect('equal')
                    ax.axis('off')

                    # Add colorbar
                    cbar = plt.colorbar(contour_filled, ax=ax, shrink=0.6, aspect=20)
                    cbar.set_label('Elevation (m)', rotation=270, labelpad=15)

                    # Save individual hole map
                    hole_filename = f"hole_{hole_number}_elevation.png"
                    hole_filepath = os.path.join(hole_maps_dir, hole_filename)
                    plt.tight_layout()
                    plt.savefig(hole_filepath, dpi=200, bbox_inches='tight', facecolor='white')
                    plt.close()

                    print(f"   ✅ Created map for hole {hole_number}")
                    success_count += 1

                except Exception as e:
                    print(f"   ❌ Failed to create map for hole {hole_number}: {e}")
                    plt.close()
                    continue

            print(f"✅ Successfully created {success_count}/{len(holes)} individual hole elevation maps")
            return success_count > 0

        except Exception as e:
            print(f"❌ Error creating individual hole maps: {e}")
            return False

    def _is_centerline_feature(self, row):
        """
        VERY conservative centerline detection - only for obvious narrow corridors.
        """
        geom = row.geometry

        if geom.geom_type == 'Polygon':
            # Much more conservative criteria
            bounds = geom.bounds
            width = bounds[2] - bounds[0]  # max_x - min_x
            height = bounds[3] - bounds[1]  # max_y - min_y

            if min(width, height) > 0:
                aspect_ratio = max(width, height) / min(width, height)

                # MUCH more conservative - only very narrow corridors (aspect ratio > 20)
                if aspect_ratio > 20:
                    # Additional check: must be very small area (narrow corridor)
                    area = geom.area
                    if area < 0.000005:  # Very restrictive area threshold
                        return True

                # Even more restrictive compactness check
                area = geom.area
                perimeter = geom.length

                if perimeter > 0 and area > 0:
                    compactness = (perimeter * perimeter) / area
                    # Only extremely narrow shapes (much higher threshold)
                    if compactness > 2000 and area < 0.000005:
                        return True

        return False

    def _render_golf_features(self, ax, features_subset, style_copy, golf_type):
        """
        Render golf features with centerline detection for ANY golf feature type.
        """
        centerline_count = 0
        polygon_count = 0

        for idx, row in features_subset.iterrows():
            if self._is_centerline_feature(row):
                centerline_count += 1
                # Render narrow polygons as centerlines
                geom = row.geometry
                if geom.geom_type == 'Polygon':
                    # Get the skeleton/centerline of the polygon
                    coords = list(geom.exterior.coords)
                    if len(coords) >= 2:
                        # Method: Simple line from centroid through longest axis
                        bounds = geom.bounds
                        width = bounds[2] - bounds[0]
                        height = bounds[3] - bounds[1]

                        if width > height:
                            # Horizontal orientation - create line along width
                            start_point = (bounds[0], geom.centroid.y)
                            end_point = (bounds[2], geom.centroid.y)
                        else:
                            # Vertical orientation - create line along height
                            start_point = (geom.centroid.x, bounds[1])
                            end_point = (geom.centroid.x, bounds[3])

                        centerline = LineString([start_point, end_point])

                        # Clip centerline to polygon boundary
                        try:
                            clipped_line = centerline.intersection(geom)
                            if hasattr(clipped_line, 'coords') and len(list(clipped_line.coords)) >= 2:
                                gpd.GeoSeries([clipped_line]).plot(ax=ax, color='black', linewidth=1, alpha=0.8)
                            else:
                                # Fallback: just use the simple centerline
                                gpd.GeoSeries([centerline]).plot(ax=ax, color='black', linewidth=1, alpha=0.8)
                        except:
                            # Fallback: use simple centerline
                            gpd.GeoSeries([centerline]).plot(ax=ax, color='black', linewidth=1, alpha=0.8)

                        aspect_ratio = max(width, height)/min(width, height) if min(width, height) > 0 else float('inf')
                        area = geom.area
                        perimeter = geom.length
                        compactness = (perimeter * perimeter) / area if area > 0 else 0
                        print(f"  ✅ Rendered {golf_type} {idx} as CENTERLINE (aspect: {aspect_ratio:.1f}, area: {area:.6f}, compactness: {compactness:.1f})")
                else:
                    # Already a line
                    gpd.GeoSeries([row.geometry]).plot(ax=ax, color='black', linewidth=1, alpha=0.8)
            else:
                polygon_count += 1
                # Render as normal filled area
                gpd.GeoSeries([row.geometry]).plot(ax=ax, **style_copy)
                geom = row.geometry
                if geom.geom_type == 'Polygon':
                    bounds = geom.bounds
                    width = bounds[2] - bounds[0]
                    height = bounds[3] - bounds[1]
                    aspect_ratio = max(width, height)/min(width, height) if min(width, height) > 0 else float('inf')
                    area = geom.area
                    perimeter = geom.length
                    compactness = (perimeter * perimeter) / area if area > 0 else 0
                    print(f"  ⬜ Rendered {golf_type} {idx} as POLYGON (aspect: {aspect_ratio:.1f}, area: {area:.6f}, compactness: {compactness:.1f})")

        print(f"{golf_type} rendering summary: {centerline_count} centerlines, {polygon_count} polygons")

    def save_geojson(self, gdf: gpd.GeoDataFrame, filename: str = "golf_course_mask.geojson") -> bool:
        """
        Save GeoDataFrame as GeoJSON file in the course folder.

        Args:
            gdf: GeoDataFrame to save
            filename: Output filename (saved in course folder)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Save to course folder
            filepath = os.path.join(self.course_folder, filename)

            if gdf.empty:
                # Create empty GeoJSON
                empty_geojson = {
                    "type": "FeatureCollection",
                    "features": []
                }
                with open(filepath, 'w') as f:
                    json.dump(empty_geojson, f, indent=2)
                print(f"Saved empty GeoJSON file: {filepath}")
            else:
                gdf.to_file(filepath, driver='GeoJSON')
                print(f"Saved GeoJSON file: {filepath} ({len(gdf)} features)")
            return True
        except Exception as e:
            print(f"Error saving GeoJSON file: {e}")
            return False

    def create_individual_hole_clean_maps(self, gdf: gpd.GeoDataFrame) -> bool:
        """
        Create individual clean golf course maps for each hole (like Image 1).

        Args:
            gdf: GeoDataFrame with golf course features

        Returns:
            True if successful, False otherwise
        """
        print("Creating individual hole clean maps...")

        try:
            # Create hole maps directory
            hole_maps_dir = os.path.join(self.course_folder, "hole_clean_maps")
            os.makedirs(hole_maps_dir, exist_ok=True)

            holes = gdf[gdf['golf'] == 'hole']
            if holes.empty:
                print("No holes found for individual clean maps")
                return False

            # Define clean map colors (like Image 1)
            color_map = {
                'golf_course': {'color': '#c5d96f', 'alpha': 0.7, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'hole': {'color': '#c5d96f', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'fairway': {'color': '#c5d96f', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'green': {'color': '#7ED321', 'alpha': 0.95, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'tee': {'color': '#7ED321', 'alpha': 0.95, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'bunker': {'color': '#F5A623', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'water_hazard': {'color': '#1E3A8A', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'lateral_water_hazard': {'color': '#1E3A8A', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'rough': {'color': '#a8cc5c', 'alpha': 0.6, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'driving_range': {'color': '#c5d96f', 'alpha': 0.7, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'clubhouse': {'color': '#d0743c', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'pin': {'color': 'red', 'alpha': 1.0, 'edgecolor': 'none', 'linewidth': 0, 'fill': True}
            }

            success_count = 0
            for idx, hole in holes.iterrows():
                hole_number = hole.get('ref', f'hole_{idx}')

                try:
                    # Create figure for this hole
                    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
                    ax.set_facecolor('#f6f7f6')  # Light grey background like OSM

                    # Focus on hole area with buffer
                    bounds = hole.geometry.bounds
                    buffer = 0.001  # Buffer around hole
                    hole_bbox = (bounds[0] - buffer, bounds[1] - buffer,
                                bounds[2] + buffer, bounds[3] + buffer)

                    # Filter features within hole area
                    hole_area_features = gdf.cx[hole_bbox[0]:hole_bbox[2], hole_bbox[1]:hole_bbox[3]]

                    # Plot features in order
                    plot_order = ['golf_course', 'fairway', 'rough', 'water_hazard', 'lateral_water_hazard',
                                 'bunker', 'tee', 'green', 'hole', 'pin', 'driving_range', 'clubhouse']

                    for golf_type in plot_order:
                        if golf_type not in color_map:
                            continue

                        style = color_map[golf_type]

                        # Filter features by type
                        if golf_type == 'golf_course':
                            mask = (hole_area_features['leisure'] == 'golf_course')
                        else:
                            mask = (hole_area_features['golf'] == golf_type)

                        features_subset = hole_area_features[mask]
                        if not features_subset.empty:
                            style_copy = style.copy()
                            linewidth = style_copy.pop('linewidth', 0)
                            should_fill = style_copy.pop('fill', True)

                            if should_fill:
                                features_subset.plot(ax=ax, linewidth=linewidth, **style_copy)

                    # Highlight this specific hole
                    if hole.geometry.geom_type == 'LineString':
                        gpd.GeoSeries([hole.geometry]).plot(ax=ax, color='red', linewidth=4, alpha=0.9)
                        midpoint = hole.geometry.interpolate(0.5, normalized=True)
                    else:
                        gpd.GeoSeries([hole.geometry]).plot(ax=ax, facecolor='red', edgecolor='darkred',
                                                           linewidth=2, alpha=0.7)
                        midpoint = hole.geometry.centroid

                    # Add hole number label
                    ax.text(midpoint.x, midpoint.y, str(hole_number),
                           fontsize=16, fontweight='bold',
                           ha='center', va='center',
                           color='white',
                           bbox=dict(boxstyle='circle,pad=0.3',
                                   facecolor='red',
                                   alpha=0.9,
                                   edgecolor='darkred',
                                   linewidth=2))

                    # Set title
                    ax.set_title(f'Hole {hole_number} - Clean Map', fontsize=14, fontweight='bold')

                    # Set bounds
                    ax.set_xlim(hole_bbox[0], hole_bbox[2])
                    ax.set_ylim(hole_bbox[1], hole_bbox[3])
                    ax.set_aspect('equal')
                    ax.axis('off')

                    # Save individual hole map
                    hole_filename = f"hole_{hole_number}_clean.png"
                    hole_filepath = os.path.join(hole_maps_dir, hole_filename)
                    plt.tight_layout()
                    plt.savefig(hole_filepath, dpi=200, bbox_inches='tight',
                               facecolor='#f6f7f6', edgecolor='none', pad_inches=0)
                    plt.close()

                    print(f"   ✅ Created clean map for hole {hole_number}")
                    success_count += 1

                except Exception as e:
                    print(f"   ❌ Failed to create clean map for hole {hole_number}: {e}")
                    plt.close()
                    continue

            print(f"✅ Successfully created {success_count}/{len(holes)} individual hole clean maps")
            return success_count > 0

        except Exception as e:
            print(f"❌ Error creating individual hole clean maps: {e}")
            return False

    def create_individual_hole_satellite_overlay_maps(self, gdf: gpd.GeoDataFrame) -> bool:
        """
        Create individual satellite overlay maps for each hole (like Image 2).

        Args:
            gdf: GeoDataFrame with golf course features

        Returns:
            True if successful, False otherwise
        """
        print("Creating individual hole satellite overlay maps...")

        try:
            # Create hole maps directory
            hole_maps_dir = os.path.join(self.course_folder, "hole_satellite_overlay_maps")
            os.makedirs(hole_maps_dir, exist_ok=True)

            holes = gdf[gdf['golf'] == 'hole']
            if holes.empty:
                print("No holes found for individual satellite overlay maps")
                return False

            # Define overlay colors (like Image 2)
            color_map = {
                'golf_course': {'color': '#c5d96f', 'alpha': 0.4, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'hole': {'color': '#c5d96f', 'alpha': 0.6, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'fairway': {'color': '#c5d96f', 'alpha': 0.6, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'green': {'color': '#7ED321', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'tee': {'color': '#7ED321', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'bunker': {'color': '#F5A623', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'water_hazard': {'color': '#1E3A8A', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'lateral_water_hazard': {'color': '#1E3A8A', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'rough': {'color': '#a8cc5c', 'alpha': 0.5, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'driving_range': {'color': '#c5d96f', 'alpha': 0.4, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'clubhouse': {'color': '#d0743c', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                'pin': {'color': 'red', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True}
            }

            success_count = 0
            for idx, hole in holes.iterrows():
                hole_number = hole.get('ref', f'hole_{idx}')

                try:
                    # Create figure for this hole
                    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

                    # Focus on hole area with buffer
                    bounds = hole.geometry.bounds
                    buffer = 0.001  # Buffer around hole
                    hole_bbox = (bounds[0] - buffer, bounds[1] - buffer,
                                bounds[2] + buffer, bounds[3] + buffer)

                    # Filter features within hole area and convert to Web Mercator
                    hole_area_features = gdf.cx[hole_bbox[0]:hole_bbox[2], hole_bbox[1]:hole_bbox[3]]
                    if not hole_area_features.empty:
                        hole_area_features_mercator = hole_area_features.to_crs(epsg=3857)

                        # Plot features in order
                        plot_order = ['fairway', 'rough', 'water_hazard', 'lateral_water_hazard',
                                     'bunker', 'tee', 'green', 'hole', 'pin', 'driving_range', 'clubhouse']

                        for golf_type in plot_order:
                            if golf_type not in color_map:
                                continue

                            style = color_map[golf_type]

                            # Filter features by type
                            if golf_type == 'golf_course':
                                mask = (hole_area_features_mercator['leisure'] == 'golf_course')
                            else:
                                mask = (hole_area_features_mercator['golf'] == golf_type)

                            features_subset = hole_area_features_mercator[mask]
                            if not features_subset.empty:
                                style_copy = style.copy()
                                linewidth = style_copy.pop('linewidth', 0)
                                should_fill = style_copy.pop('fill', True)

                                if golf_type == 'hole':
                                    # Handle holes specially
                                    for idx_feat, row in features_subset.iterrows():
                                        if row.geometry.geom_type == 'LineString':
                                            gpd.GeoSeries([row.geometry], crs=features_subset.crs).plot(
                                                ax=ax, color='black', linewidth=1, alpha=0.8)
                                        else:
                                            gpd.GeoSeries([row.geometry], crs=features_subset.crs).plot(
                                                ax=ax, **style_copy)
                                elif should_fill:
                                    features_subset.plot(ax=ax, linewidth=linewidth, **style_copy)

                        # Add satellite basemap
                        try:
                            ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, alpha=0.8)
                        except Exception as e:
                            print(f"      ⚠️ Failed to add satellite basemap: {e}")

                        # Set bounds based on hole area
                        ax.set_xlim(hole_area_features_mercator.total_bounds[0],
                                   hole_area_features_mercator.total_bounds[2])
                        ax.set_ylim(hole_area_features_mercator.total_bounds[1],
                                   hole_area_features_mercator.total_bounds[3])

                    # Highlight this specific hole in Web Mercator
                    hole_mercator = gpd.GeoDataFrame([hole], crs='EPSG:4326').to_crs(epsg=3857)
                    if hole.geometry.geom_type == 'LineString':
                        hole_mercator.plot(ax=ax, color='red', linewidth=4, alpha=0.9)
                        midpoint_mercator = hole_mercator.geometry.iloc[0].interpolate(0.5, normalized=True)
                    else:
                        hole_mercator.plot(ax=ax, facecolor='red', edgecolor='darkred',
                                          linewidth=2, alpha=0.7)
                        midpoint_mercator = hole_mercator.geometry.iloc[0].centroid

                    # Add hole number label
                    ax.text(midpoint_mercator.x, midpoint_mercator.y, str(hole_number),
                           fontsize=16, fontweight='bold',
                           ha='center', va='center',
                           color='white',
                           bbox=dict(boxstyle='circle,pad=0.3',
                                   facecolor='red',
                                   alpha=0.9,
                                   edgecolor='darkred',
                                   linewidth=2))

                    # Set title
                    ax.set_title(f'Hole {hole_number} - Satellite Overlay', fontsize=14, fontweight='bold')

                    ax.set_aspect('equal')
                    ax.axis('off')

                    # Save individual hole map
                    hole_filename = f"hole_{hole_number}_satellite_overlay.png"
                    hole_filepath = os.path.join(hole_maps_dir, hole_filename)
                    plt.tight_layout()
                    plt.savefig(hole_filepath, dpi=200, bbox_inches='tight',
                               facecolor='white', edgecolor='none', pad_inches=0)
                    plt.close()

                    print(f"   ✅ Created satellite overlay map for hole {hole_number}")
                    success_count += 1

                except Exception as e:
                    print(f"   ❌ Failed to create satellite overlay map for hole {hole_number}: {e}")
                    plt.close()
                    continue

            print(f"✅ Successfully created {success_count}/{len(holes)} individual hole satellite overlay maps")
            return success_count > 0

        except Exception as e:
            print(f"❌ Error creating individual hole satellite overlay maps: {e}")
            return False

    def create_individual_hole_satellite_maps(self, gdf: gpd.GeoDataFrame) -> bool:
        """
        Create individual plain satellite maps for each hole (like Image 3).

        Args:
            gdf: GeoDataFrame with golf course features

        Returns:
            True if successful, False otherwise
        """
        print("Creating individual hole plain satellite maps...")

        try:
            # Create hole maps directory
            hole_maps_dir = os.path.join(self.course_folder, "hole_satellite_maps")
            os.makedirs(hole_maps_dir, exist_ok=True)

            holes = gdf[gdf['golf'] == 'hole']
            if holes.empty:
                print("No holes found for individual satellite maps")
                return False

            success_count = 0
            for idx, hole in holes.iterrows():
                hole_number = hole.get('ref', f'hole_{idx}')

                try:
                    # Create figure for this hole
                    fig, ax = plt.subplots(1, 1, figsize=(12, 8))

                    # Focus on hole area with buffer
                    bounds = hole.geometry.bounds
                    buffer = 0.001  # Buffer around hole
                    hole_bbox = (bounds[0] - buffer, bounds[1] - buffer,
                                bounds[2] + buffer, bounds[3] + buffer)

                    # Convert hole to Web Mercator for contextily
                    hole_mercator = gpd.GeoDataFrame([hole], crs='EPSG:4326').to_crs(epsg=3857)

                    # Calculate bounds in Web Mercator
                    mercator_bounds = hole_mercator.total_bounds
                    mercator_buffer = 100  # 100 meters buffer in Web Mercator
                    mercator_bbox = (mercator_bounds[0] - mercator_buffer,
                                    mercator_bounds[1] - mercator_buffer,
                                    mercator_bounds[2] + mercator_buffer,
                                    mercator_bounds[3] + mercator_buffer)

                    # Set bounds for satellite image
                    ax.set_xlim(mercator_bbox[0], mercator_bbox[2])
                    ax.set_ylim(mercator_bbox[1], mercator_bbox[3])

                    # Add satellite basemap
                    try:
                        ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, alpha=1.0)
                    except Exception as e:
                        print(f"      ⚠️ Failed to add satellite basemap: {e}")

                    # Add minimal hole indicator (just a small red dot and number)
                    if hole.geometry.geom_type == 'LineString':
                        midpoint_mercator = hole_mercator.geometry.iloc[0].interpolate(0.5, normalized=True)
                    else:
                        midpoint_mercator = hole_mercator.geometry.iloc[0].centroid

                    # Add small hole number label
                    ax.text(midpoint_mercator.x, midpoint_mercator.y, str(hole_number),
                           fontsize=14, fontweight='bold',
                           ha='center', va='center',
                           color='white',
                           bbox=dict(boxstyle='circle,pad=0.2',
                                   facecolor='red',
                                   alpha=0.8,
                                   edgecolor='darkred',
                                   linewidth=1))

                    # Set title
                    ax.set_title(f'Hole {hole_number} - Satellite View', fontsize=14, fontweight='bold')

                    ax.set_aspect('equal')
                    ax.axis('off')

                    # Save individual hole map
                    hole_filename = f"hole_{hole_number}_satellite.png"
                    hole_filepath = os.path.join(hole_maps_dir, hole_filename)
                    plt.tight_layout()
                    plt.savefig(hole_filepath, dpi=200, bbox_inches='tight',
                               facecolor='white', edgecolor='none', pad_inches=0)
                    plt.close()

                    print(f"   ✅ Created satellite map for hole {hole_number}")
                    success_count += 1

                except Exception as e:
                    print(f"   ❌ Failed to create satellite map for hole {hole_number}: {e}")
                    plt.close()
                    continue

            print(f"✅ Successfully created {success_count}/{len(holes)} individual hole satellite maps")
            return success_count > 0

        except Exception as e:
            print(f"❌ Error creating individual hole satellite maps: {e}")
            return False

    def create_contextily_overlay(self, gdf: gpd.GeoDataFrame,
                                     output_filename: str = "naip_overlay.png",
                                     basemap_source: str = "satellite") -> bool:
        """
        Create overlay visualization using contextily basemaps.

        Args:
            gdf: GeoDataFrame with golf course polygons
            output_filename: Output filename for overlay image (saved in course folder)
            basemap_source: Basemap type ('satellite', 'osm', 'terrain', etc.)

        Returns:
            True if successful, False otherwise
        """
        print(f"Creating contextily overlay with {basemap_source} basemap...")

        try:
            # Create matplotlib figure
            fig, ax = plt.subplots(1, 1, figsize=(12, 12))

            # Overlay golf course polygons with different colors based on golf feature type
            if not gdf.empty:
                # Convert to Web Mercator for contextily
                gdf_mercator = gdf.to_crs(epsg=3857)
                if gdf_mercator.empty:
                    print("⚠️ Cannot overlay: projected GeoDataFrame is empty.")
                    return False

                # Use enhanced colors for overlay visualization
                color_map = {
                    'golf_course': {'color': '#c5d96f', 'alpha': 0.4, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'hole': {'color': '#c5d96f', 'alpha': 0.6, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'fairway': {'color': '#c5d96f', 'alpha': 0.6, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'green': {'color': '#7ED321', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'tee': {'color': '#7ED321', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'bunker': {'color': '#F5A623', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'water_hazard': {'color': '#1E3A8A', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'lateral_water_hazard': {'color': '#1E3A8A', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'rough': {'color': '#a8cc5c', 'alpha': 0.5, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'driving_range': {'color': '#c5d96f', 'alpha': 0.4, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'clubhouse': {'color': '#d0743c', 'alpha': 0.8, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'pin': {'color': 'red', 'alpha': 0.9, 'edgecolor': 'none', 'linewidth': 0, 'fill': True},
                    'default': {'color': '#c5d96f', 'alpha': 0.4, 'edgecolor': 'none', 'linewidth': 0, 'fill': True}
                }

                # Sort features by size
                gdf_sorted = gdf_mercator.copy()
                gdf_sorted['area'] = gdf_sorted.geometry.area
                gdf_sorted = gdf_sorted.sort_values('area', ascending=False)

                # Plot golf features
                feature_counts = {}
                plot_order = ['fairway', 'rough', 'water_hazard', 'lateral_water_hazard',
                             'bunker', 'tee', 'green', 'hole', 'pin', 'driving_range', 'clubhouse']

                for golf_type in plot_order:
                    if golf_type not in color_map:
                        continue

                    style = color_map[golf_type]

                    # Filter features by type
                    if golf_type == 'golf_course':
                        mask = (gdf_sorted['leisure'] == 'golf_course')
                    else:
                        mask = (gdf_sorted['golf'] == golf_type)

                    features_subset = gdf_sorted[mask]
                    if not features_subset.empty:
                        style_copy = style.copy()
                        linewidth = style_copy.pop('linewidth', 0)
                        should_fill = style_copy.pop('fill', True)

                        if golf_type == 'fairway':
                            # Only apply centerline detection to fairways, not holes
                            self._render_golf_features(ax, features_subset, style_copy, golf_type)
                        elif golf_type == 'hole':
                            # Handle holes: LineStrings as centerlines, Polygons as areas
                            from geopandas import GeoSeries
                            for idx, row in features_subset.iterrows():
                                if row.geometry.geom_type == 'LineString':
                                    # Render hole centerlines as thin black lines
                                    GeoSeries([row.geometry], crs=gdf_mercator.crs).plot(ax=ax, color='black', linewidth=1, alpha=0.8)
                                    print(f"  ✅ Rendered hole {idx} as CENTERLINE (LineString)")
                                else:
                                    # Render hole areas as filled polygons
                                    GeoSeries([row.geometry], crs=gdf_mercator.crs).plot(ax=ax, **style_copy)
                                    print(f"  ⬜ Rendered hole {idx} as POLYGON")
                        elif should_fill:
                            features_subset.plot(ax=ax, linewidth=linewidth, **style_copy)

                        feature_counts[golf_type] = len(features_subset)

                        # Add text labels for golf holes (both LineString and Polygon holes)
                        if golf_type == 'hole':
                            for idx, row in features_subset.iterrows():
                                hole_number = row.get('ref', '')

                                if hole_number:
                                    if row.geometry.geom_type == 'Polygon':
                                        # For polygon holes, place label at centroid
                                        centroid = row.geometry.centroid
                                        ax.text(centroid.x, centroid.y, str(hole_number),
                                               fontsize=8, fontweight='bold',
                                               ha='center', va='center',
                                               color='#333333',
                                               bbox=dict(boxstyle='circle,pad=0.15',
                                                       facecolor='white',
                                                       alpha=0.9,
                                                       edgecolor='#999999',
                                                       linewidth=1))
                                    elif row.geometry.geom_type == 'LineString':
                                        # For line holes, place label at midpoint of the line
                                        midpoint = row.geometry.interpolate(0.5, normalized=True)
                                        ax.text(midpoint.x, midpoint.y, str(hole_number),
                                               fontsize=8, fontweight='bold',
                                               ha='center', va='center',
                                               color='#333333',
                                               bbox=dict(boxstyle='circle,pad=0.15',
                                                       facecolor='white',
                                                       alpha=0.9,
                                                       edgecolor='#999999',
                                                       linewidth=1))

                # Add contextily basemap with WORKING providers (fixed!)
                basemap_sources = {
                    'satellite': ctx.providers.Esri.WorldImagery,
                    'osm': ctx.providers.OpenStreetMap.Mapnik,
                    'terrain': ctx.providers.CartoDB.Voyager,  # ✅ FIXED: Using working provider instead of broken Stamen
                    'topographic': ctx.providers.Esri.WorldTopoMap,
                    'streets': ctx.providers.CartoDB.Positron
                }

                # Use satellite as fallback if requested provider doesn't exist
                source = basemap_sources.get(basemap_source, ctx.providers.Esri.WorldImagery)

                print(f"🗺️ Using basemap provider: {basemap_source}")
                try:
                    ctx.add_basemap(ax, source=source, alpha=0.8)
                    print(f"✅ Successfully added {basemap_source} basemap")
                except Exception as e:
                    print(f"⚠️ Failed to add {basemap_source} basemap: {e}")
                    print("🔄 Falling back to satellite imagery...")
                    # Fallback to reliable Esri World Imagery
                    ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, alpha=0.8)
                    print("✅ Successfully added fallback satellite basemap")

                # Set bounds based on data
                ax.set_xlim(gdf_mercator.total_bounds[0], gdf_mercator.total_bounds[2])
                ax.set_ylim(gdf_mercator.total_bounds[1], gdf_mercator.total_bounds[3])

                print(f"Overlaid {len(gdf_mercator)} golf course features on {basemap_source} basemap")

            # Remove axis elements for clean appearance
            ax.set_aspect('equal')
            ax.axis('off')

            # Save the overlay to course folder
            filepath = os.path.join(self.course_folder, output_filename)
            plt.tight_layout()
            plt.savefig(filepath, dpi=300, bbox_inches='tight',
                       facecolor='white', edgecolor='none', pad_inches=0)
            plt.close()

            print(f"Saved contextily overlay: {filepath}")
            return True

        except Exception as e:
            print(f"Error creating contextily overlay: {e}")
            print(f"Error type: {type(e).__name__}")
            return False

    def process_elevation_data(self, grid_resolution: int = 50) -> Optional[Dict[str, Any]]:
        """
        Complete elevation data processing workflow.

        Args:
            grid_resolution: Number of points per side for elevation grid

        Returns:
            Dictionary with elevation statistics or None if failed
        """
        try:
            # Get elevation data
            elevation_data = self.get_elevation_data(grid_resolution)
            if not elevation_data:
                return None

            # Save elevation data
            if not self.save_elevation_data(elevation_data):
                print("❌ Failed to save elevation data")

            # Create elevation overlay if we have golf course data
            try:
                golf_courses = gpd.read_file(os.path.join(self.course_folder, "golf_course_mask.geojson"))
            except:
                golf_courses = gpd.GeoDataFrame()

            if not golf_courses.empty:
                if self.create_elevation_overlay(elevation_data, golf_courses):
                    print("✅ Created elevation overlay")
                else:
                    print("❌ Failed to create elevation overlay")

                # Create hole elevation profiles
                hole_profiles = self.create_elevation_profile_data(elevation_data, golf_courses)
                if hole_profiles:
                    if self.save_elevation_profiles(hole_profiles):
                        print("✅ Created and saved hole elevation profiles")

                        # Create individual hole elevation maps
                        if self.create_individual_hole_elevation_maps(elevation_data, golf_courses, hole_profiles):
                            print("✅ Created individual hole elevation maps")
                        else:
                            print("⚠️  Failed to create some individual hole maps")
                    else:
                        print("❌ Failed to save hole elevation profiles")
                else:
                    print("⚠️  No hole elevation profiles created")

            # Return elevation statistics for reporting
            stats = elevation_data.get('stats', {})
            return {
                'elevation_source': elevation_data.get('source', 'Unknown'),
                'elevation_range_m': stats.get('elevation_range', 0),
                'min_elevation_m': stats.get('min_elevation', 0),
                'max_elevation_m': stats.get('max_elevation', 0),
                'mean_elevation_m': stats.get('mean_elevation', 0),
                'terrain_classification': self._classify_terrain(stats.get('elevation_range', 0))
            }

        except Exception as e:
            print(f"❌ Error in elevation data processing: {e}")
            return None

    def _classify_terrain(self, elevation_range: float) -> str:
        """Classify terrain based on elevation range."""
        if elevation_range < 10:
            return "flat"
        elif elevation_range < 30:
            return "gently_rolling"
        elif elevation_range < 60:
            return "moderately_hilly"
        elif elevation_range < 100:
            return "hilly"
        else:
            return "mountainous"

    def run(self) -> bool:
        """
        Execute the complete workflow INCLUDING weather data collection.

        Returns:
            True if all steps completed successfully, False otherwise
        """
        print("=" * 60)
        print(f"GOLF COURSE DATA COLLECTION: {self.course_name.upper()}")
        print("=" * 60)

        success = True

        # Step 1: Download aerial imagery
        image = self.download_naip_image()
        if image is None:
            print("❌ Failed to download aerial imagery")
            success = False
        else:
            # Save the aerial image to course folder
            try:
                image_path = os.path.join(self.course_folder, "naip_image.jpg")
                image.save(image_path, "JPEG", quality=95)
                print(f"✅ Saved aerial image: {image_path}")
            except Exception as e:
                print(f"❌ Error saving aerial image: {e}")
                success = False

        # Step 2: Download golf course data
        golf_courses = self.download_golf_courses()
        if golf_courses is None:
            print("❌ Failed to download golf course data")
            success = False
            golf_courses = gpd.GeoDataFrame()  # Use empty GDF for remaining steps

        # Step 3: Save GeoJSON
        if not self.save_geojson(golf_courses):
            print("❌ Failed to save GeoJSON file")
            success = False
        else:
            print("✅ Saved GeoJSON file")

        # Step 4: Collect weather data (NEW!)
        print("\nStep 4: Collecting weather data...")
        if self.collect_weather_data():
            print("✅ Weather data collection completed")
        else:
            print("⚠️ Weather data collection failed or skipped")
            # Don't fail the whole process for weather data

        # Step 5: Create individual hole maps for all types
        if not golf_courses.empty:
            print("\nStep 5: Creating individual hole maps for all layer types...")

            # Clean maps (like Image 1)
            if self.create_individual_hole_clean_maps(golf_courses):
                print("✅ Created individual hole clean maps")
            else:
                print("⚠️  Failed to create individual hole clean maps")

            # Satellite overlay maps (like Image 2)
            if self.create_individual_hole_satellite_overlay_maps(golf_courses):
                print("✅ Created individual hole satellite overlay maps")
            else:
                print("⚠️  Failed to create individual hole satellite overlay maps")

            # Plain satellite maps (like Image 3)
            if self.create_individual_hole_satellite_maps(golf_courses):
                print("✅ Created individual hole plain satellite maps")
            else:
                print("⚠️  Failed to create individual hole plain satellite maps")

        # Step 6: Download and process elevation data
        print("\nStep 6: Processing elevation data...")
        try:
            elevation_data = self.process_elevation_data()
            if elevation_data:
                print("✅ Completed elevation data processing")
            else:
                print("⚠️ Elevation data processing failed or not available")
        except Exception as e:
            print(f"⚠️ Error in elevation processing: {e}")

        # Step 7: Create contextily overlay
        if self.create_contextily_overlay(golf_courses):
            print("✅ Created contextily overlay")
        else:
            print("❌ Failed to create contextily overlay")
            success = False

        print("=" * 60)
        if success:
            print("✅ All operations completed successfully!")
            print(f"\nCourse folder: {self.course_folder}")
            print("Output files:")
            print("  - naip_image.jpg (aerial imagery)")
            print("  - golf_course_mask.geojson (golf course polygons)")
            print("  - weather_raw_data.json (5 years of daily weather data)")  # NEW!
            print("  - elevation_data.json (elevation grid data)")
            print("  - elevation_overlay.png (elevation contour map)")
            print("  - naip_overlay.png (contextily satellite overlay)")

            # Individual hole maps
            if os.path.exists(os.path.join(self.course_folder, "hole_clean_maps")):
                print("  - hole_clean_maps/ (individual hole clean maps)")
            if os.path.exists(os.path.join(self.course_folder, "hole_satellite_overlay_maps")):
                print("  - hole_satellite_overlay_maps/ (individual hole satellite overlay maps)")
            if os.path.exists(os.path.join(self.course_folder, "hole_satellite_maps")):
                print("  - hole_satellite_maps/ (individual hole plain satellite maps)")
            if os.path.exists(os.path.join(self.course_folder, "dem_data.tif")):
                print("  - dem_data.tif (py3dep elevation data)")
            if os.path.exists(os.path.join(self.course_folder, "hole_elevation_profiles.json")):
                print("  - hole_elevation_profiles.json (hole elevation profiles)")
            if os.path.exists(os.path.join(self.course_folder, "hole_elevation_maps")):
                print("  - hole_elevation_maps/ (individual hole elevation maps)")

            print("\n💡 Run weather analysis on collected data:")
            print("   python analyze_weather.py")
        else:
            print("❌ Some operations failed. Check error messages above.")

        return success


def load_courses_from_excel(excel_path: str = EXCEL_FILE_PATH,
                          max_courses: int = MAX_COURSES_TO_PROCESS,
                          start_index: int = START_INDEX) -> list:
    """
    Load golf course data from Excel file using the configured column names.

    Args:
        excel_path: Path to Excel file
        max_courses: Maximum number of courses to load
        start_index: Starting index

    Returns:
        List of course dictionaries
    """
    try:
        print(f"📁 Loading Excel file: {excel_path}")
        print(f"🎯 Loading {max_courses} courses starting from index {start_index}")

        # Load Excel file
        df = pd.read_excel(excel_path)

        print(f"✅ Loaded {len(df)} total courses from Excel")

        # Check if required columns exist
        required_columns = [COLUMN_MAPPING['name'], COLUMN_MAPPING['latitude'], COLUMN_MAPPING['longitude']]
        optional_columns = [COLUMN_MAPPING['ecoursenumber']]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            available_cols = [col for col in df.columns if any(keyword in col.lower()
                            for keyword in ['name', 'display', 'lat', 'lon', 'lng', 'ecourse', 'course'])]
            print(f"🔍 Available relevant columns: {available_cols}")
            return []

        print(f"✅ Found all required columns:")
        print(f"   Name: {COLUMN_MAPPING['name']}")
        print(f"   Latitude: {COLUMN_MAPPING['latitude']}")
        print(f"   Longitude: {COLUMN_MAPPING['longitude']}")

        # Check for optional ecoursenumber column
        has_ecoursenumber = COLUMN_MAPPING['ecoursenumber'] in df.columns
        if has_ecoursenumber:
            print(f"   ✅ Found ecoursenumber: {COLUMN_MAPPING['ecoursenumber']}")
        else:
            print(f"   ⚠️  No ecoursenumber column found - will use sequential numbering")

        # Slice the data for processing
        end_index = start_index + max_courses
        test_df = df.iloc[start_index:end_index].copy()

        print(f"\n📊 Processing rows {start_index} to {end_index-1} ({len(test_df)} courses)")

        # Convert to course list
        courses = []
        for idx, row in test_df.iterrows():
            course_name = row[COLUMN_MAPPING['name']]
            latitude = row[COLUMN_MAPPING['latitude']]
            longitude = row[COLUMN_MAPPING['longitude']]

            # Get ecoursenumber if available
            if has_ecoursenumber:
                ecoursenumber = row[COLUMN_MAPPING['ecoursenumber']]
                if pd.isna(ecoursenumber) or str(ecoursenumber).strip() == '':
                    ecoursenumber = f"COURSE-{idx}"  # Fallback if ecoursenumber is missing
                else:
                    ecoursenumber = str(ecoursenumber).strip()
            else:
                ecoursenumber = f"COURSE-{idx}"  # Sequential numbering if column doesn't exist

            # Skip if missing coordinates or name
            if pd.isna(latitude) or pd.isna(longitude) or pd.isna(course_name):
                print(f"⏭️  Skipping row {idx} - missing data")
                continue

            # Clean up course name
            course_name = str(course_name).strip()
            if not course_name or course_name.lower() in ['nan', 'none', '']:
                print(f"⏭️  Skipping row {idx} - invalid course name")
                continue

            courses.append({
                'name': course_name,
                'lat': float(latitude),
                'lon': float(longitude),
                'ecoursenumber': ecoursenumber,
                'extent_km': 2.0,
                'basemap_type': 'satellite'
            })

        print(f"\n🎯 Selected {len(courses)} valid courses:")
        for i, course in enumerate(courses):
            print(f"  {i+1}. {course['ecoursenumber']} - {course['name']}")
            print(f"      📍 ({course['lat']:.4f}, {course['lon']:.4f})")

        return courses

    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        return []


def process_multiple_courses(courses_data: list, output_dir: str = OUTPUT_DIR) -> list:
    """
    Process multiple golf courses in batch WITH elevation processing (weather analysis removed).

    Args:
        courses_data: List of dicts with 'name', 'lat', 'lon' keys
        output_dir: Base output directory

    Returns:
        List of results for each course
    """
    results = []

    for i, course in enumerate(courses_data):
        print(f"\n{'='*70}")
        print(f"PROCESSING COURSE {i+1}/{len(courses_data)}: {course['name']}")
        print(f"{'='*70}")

        try:
            # Check if course folder already exists
            ecoursenumber = course.get('ecoursenumber')
            course_name_sanitized = GeospatialVisualizer._sanitize_course_name(course['name'])

            if ecoursenumber:
                folder_name = f"{ecoursenumber}_{course_name_sanitized}"
            else:
                folder_name = f"course_{course_name_sanitized}"

            course_folder = os.path.join(output_dir, folder_name)

            # Check if folder exists and contains expected files
            if os.path.exists(course_folder):
                # Check for key files to ensure processing was completed
                key_files = [
                    "golf_course_mask.geojson",
                    "naip_overlay.png"
                ]

                files_exist = all(os.path.exists(os.path.join(course_folder, f)) for f in key_files)

                if files_exist:
                    print(f"⏭️  SKIPPING: Course folder already exists with completed processing")
                    print(f"📁 Folder: {course_folder}")
                    print(f"💡 To reprocess this course, delete the folder first")

                    results.append({
                        'course_name': course['name'],
                        'success': True,
                        'skipped': True,
                        'folder': course_folder,
                        'lat': course['lat'],
                        'lon': course['lon']
                    })
                    continue
                else:
                    print(f"⚠️  Course folder exists but appears incomplete - will reprocess")
                    print(f"📁 Folder: {course_folder}")

            visualizer = GeospatialVisualizer(
                center_lat=course['lat'],
                center_lon=course['lon'],
                extent_km=course.get('extent_km', 2.0),
                basemap_type=course.get('basemap_type', 'satellite'),
                course_name=course['name'],
                output_dir=output_dir,
                ecoursenumber=course.get('ecoursenumber')
            )

            success = visualizer.run()

            results.append({
                'course_name': course['name'],
                'success': success,
                'skipped': False,
                'folder': visualizer.course_folder,
                'lat': course['lat'],
                'lon': course['lon']
            })

        except Exception as e:
            print(f"❌ Error processing {course['name']}: {e}")
            results.append({
                'course_name': course['name'],
                'success': False,
                'skipped': False,
                'error': str(e),
                'lat': course['lat'],
                'lon': course['lon']
            })

    return results


def main():
    """Main function to run the complete batch processing workflow with enhanced elevation analysis."""
    print("=" * 70)
    print("GOLF COURSE BATCH PROCESSOR WITH ELEVATION ANALYSIS AND SKIP FUNCTIONALITY")
    print("=" * 70)
    print(f"📂 Excel file: {EXCEL_FILE_PATH}")
    print(f"🎯 Courses to process: {MAX_COURSES_TO_PROCESS}")
    print(f"📊 Starting from row: {START_INDEX}")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print(f"📋 Using columns:")
    print(f"   Name: {COLUMN_MAPPING['name']}")
    print(f"   Latitude: {COLUMN_MAPPING['latitude']}")
    print(f"   Longitude: {COLUMN_MAPPING['longitude']}")
    print(f"   ECourseNumber: {COLUMN_MAPPING['ecoursenumber']}")

    # Check package availability
    print(f"\n🔍 Package availability:")
    print(f"   py3dep (elevation): {'✅' if PY3DEP_AVAILABLE else '❌'}")
    print(f"   Rasterio (DEM): {'✅' if RASTERIO_AVAILABLE else '❌'}")

    if not PY3DEP_AVAILABLE:
        print("   ⚠️  Install py3dep for elevation data: pip install py3dep")
    if not RASTERIO_AVAILABLE:
        print("   ⚠️  Install rasterio for DEM processing: pip install rasterio")

    print(f"\n📈 Analysis Features:")
    print(f"   • High-resolution elevation data (USGS 3DEP)")
    print(f"   • Terrain classification and golf playability")
    print(f"   • Slope analysis and topographic mapping")
    print(f"   • Individual hole maps in multiple styles")
    print(f"   • Comprehensive geospatial data collection")
    print(f"   • Skip existing folders functionality")

    print(f"\n💡 Note: Weather analysis is now standalone!")
    print(f"   Run 'python analyze_weather.py' after this completes")

    # Check if Excel file exists
    if not os.path.exists(EXCEL_FILE_PATH):
        print(f"\n❌ Excel file not found: {EXCEL_FILE_PATH}")
        print("Please check the file path and try again.")
        return False

    try:
        # Load courses from Excel
        courses = load_courses_from_excel()

        if not courses:
            print("❌ No courses loaded from Excel file")
            return False

        # Process the courses
        print(f"\n🚀 Starting batch processing of {len(courses)} courses...")
        print("Creating comprehensive analysis for each course:")
        print("  • Clean maps (like OpenStreetMap style)")
        print("  • Satellite overlay maps")
        print("  • Plain satellite maps")
        print("  • High-resolution elevation data and terrain analysis")
        print("  • Geospatial data and aerial imagery")
        print("  • Skip already processed courses")

        results = process_multiple_courses(courses)

        # Report results
        successful = [r for r in results if r.get('success', False) and not r.get('skipped', False)]
        skipped = [r for r in results if r.get('skipped', False)]
        failed = [r for r in results if not r.get('success', False)]

        print(f"\n📊 BATCH PROCESSING COMPLETE!")
        print(f"✅ Processed: {len(successful)}")
        print(f"⏭️  Skipped (already exists): {len(skipped)}")
        print(f"❌ Failed: {len(failed)}")

        if skipped:
            print(f"\n⏭️  Skipped courses (already processed):")
            for result in skipped:
                print(f"  • {result['course_name']}")
                print(f"    📁 {result['folder']}")

        if successful:
            print(f"\n✅ Successfully processed courses:")
            for result in successful:
                print(f"  • {result['course_name']}")
                print(f"    📁 {result['folder']}")
                print(f"    📸 Individual hole maps created in subfolders")
                print(f"    🏔️  Elevation data and terrain analysis")

        if failed:
            print(f"\n❌ Failed courses:")
            for result in failed:
                error = result.get('error', 'Unknown error')
                print(f"  • {result['course_name']}: {error}")

        print(f"\n🎉 Processing complete! Check the '{OUTPUT_DIR}' folder for results.")
        print(f"Each course now has:")
        print(f"  • Individual hole maps in 3 different styles")
        print(f"  • High-resolution elevation data and terrain analysis")
        print(f"  • Geospatial data and aerial imagery")

        if skipped:
            print(f"\n💡 To reprocess skipped courses, delete their folders first:")
            for result in skipped:
                print(f"  rm -rf '{result['folder']}'")

        print(f"\n🌤️  Next step: Run weather analysis!")
        print(f"   python analyze_weather.py")
        print(f"   This will add comprehensive weather and sun analysis to each course.")

        return len(successful) + len(skipped) > 0

    except KeyboardInterrupt:
        print("\n❌ Processing cancelled by user")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    main()
