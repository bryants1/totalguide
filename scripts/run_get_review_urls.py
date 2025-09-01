#!/usr/bin/env python3
"""
API-Driven Golf Course URL Finder
Pulls course data from primary_data via API, checks review_urls table via API,
and writes results back to review_urls table via API.
No Excel files involved - pure API integration.
"""
import asyncio
import urllib.parse
from urllib.parse import quote
from typing import Optional, Tuple, List, Dict
import logging
from pathlib import Path
import random
import argparse
import sys
import os
import requests
import json
from datetime import datetime
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load environment variables - automatically searches up directory tree
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_api_url():
    """Get API URL from environment variable - REQUIRED"""
    api_url = os.getenv('API_URL')
    if not api_url:
        raise ValueError("API_URL environment variable is required but not set")
    return api_url

def get_setting(key: str, default=None):
    """Get setting from environment variables with GOLF_PIPELINE_ prefix"""
    env_key = f"GOLF_PIPELINE_{key.upper()}"
    env_value = os.getenv(env_key)

    if env_value is not None:
        # Handle boolean values
        if env_value.lower() in ('true', 'false'):
            return env_value.lower() == 'true'

        # Handle numeric values
        try:
            if '.' in env_value:
                return float(env_value)
            else:
                return int(env_value)
        except ValueError:
            pass

        # Return as string
        return env_value

    return default

class APIGolfFinder:
    def __init__(self, headless: bool = True, force_reprocess: bool = False, state: str = None, max_courses: int = None, course: str = None):
        self.headless = headless
        self.force_reprocess = force_reprocess
        self.state = state
        self.max_courses = max_courses
        self.course = course
        self.browser = None
        self.context = None

        # API configuration
        self.api_url = get_api_url()
        self.session = requests.Session()

        # Get settings from environment variables
        self.db_timeout = get_setting('db_timeout_seconds', 30)

        logger.info(f"🔧 Using API URL: {self.api_url}")

        if course:
            logger.info(f"🎯 Single course mode: {course}")
        elif state:
            logger.info(f"🗺️ State mode: {state}")
        else:
            logger.info(f"🌍 All courses mode")

        if force_reprocess:
            logger.info("🔥 FORCE REPROCESS MODE ENABLED - will reprocess courses with existing URLs")
        if max_courses:
            logger.info(f"📊 Max courses: {max_courses}")

    def get_single_course(self, course_number: str) -> List[Dict]:
        """Get a single course by course number via API."""
        try:
            # Try primary_data table first
            logger.info(f"📡 Fetching course {course_number} from primary_data via API...")
            response = self.session.get(
                f"{self.api_url}/api/courses/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found course {course_number} in primary_data")
                return [course_data]

            # If not found in primary_data, try initial_course_upload
            logger.info(f"📡 Trying initial_course_upload for {course_number}...")
            response = self.session.get(
                f"{self.api_url}/api/initial-courses/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found course {course_number} in initial_course_upload")
                return [course_data]

            logger.error(f"❌ Course {course_number} not found in either table")
            return []

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching course {course_number}: {e}")
            return []

    def get_courses_needing_urls(self) -> List[Dict]:
        """Get courses that need URL processing via API."""
        try:
            # Handle single course mode
            if self.course:
                logger.info(f"🎯 Getting single course: {self.course}")
                return self.get_single_course(self.course)

            # Handle multiple courses mode
            params = {}
            if self.state:
                params['state'] = self.state.upper()
            if self.max_courses:
                params['limit'] = self.max_courses

            # Get all courses from primary_data
            logger.info("📡 Fetching courses from primary_data via API...")
            response = self.session.get(
                f"{self.api_url}/api/courses",
                params=params,
                timeout=self.db_timeout
            )
            response.raise_for_status()

            all_courses = response.json()
            logger.info(f"📋 Found {len(all_courses)} total courses")

            if not self.force_reprocess:
                # Filter out courses that already have URLs
                courses_needing_urls = []
                skipped_count = 0

                for course in all_courses:
                    course_number = course.get('course_number')
                    if course_number and self.needs_url_processing(course_number):
                        courses_needing_urls.append(course)
                    else:
                        skipped_count += 1

                if skipped_count > 0:
                    logger.info(f"⏭️ Skipped {skipped_count} courses (already have URLs)")

                logger.info(f"🔄 {len(courses_needing_urls)} courses need URL processing")
                return courses_needing_urls
            else:
                logger.info(f"🔥 Force mode: processing all {len(all_courses)} courses")
                return all_courses

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get courses from API: {e}")
            return []

    def needs_url_processing(self, course_number: str) -> bool:
        """Check if a course needs URL processing by checking review_urls table via API."""
        try:
            response = self.session.get(
                f"{self.api_url}/api/review-urls/{course_number}",
                timeout=self.db_timeout
            )

            if response.status_code == 404:
                # No entry in review_urls table, needs processing
                logger.info(f"📄 Course {course_number} not in review_urls table, needs processing")
                return True

            if response.status_code == 200:
                result = response.json()

                # Properly access the data structure
                if result.get('success') and result.get('data'):
                    url_data = result.get('data', {})
                    golf_now_url = url_data.get('golf_now_url', '')
                    golf_pass_url = url_data.get('golf_pass_url', '')

                    # Needs processing if either URL is missing
                    needs_processing = (
                        not golf_now_url or
                        not golf_pass_url or
                        golf_now_url.strip() == '' or
                        golf_pass_url.strip() == ''
                    )

                    if needs_processing:
                        logger.info(f"📄 Course {course_number} has incomplete URLs, needs processing")
                    else:
                        logger.info(f"📄 Course {course_number} has complete URLs, skipping")

                    return needs_processing
                else:
                    # No valid data, needs processing
                    logger.info(f"📄 Course {course_number} has no valid URL data, needs processing")
                    return True

            # On other errors, assume processing is needed
            logger.info(f"📄 Course {course_number} API error ({response.status_code}), assuming needs processing")
            return True

        except requests.exceptions.RequestException as e:
            logger.info(f"Error checking URLs for {course_number}: {e}")
            # On error, assume processing is needed
            return True

    def update_course_urls_in_db(self, course_number: str, golfnow_url: str = None, golfpass_url: str = None) -> bool:
        """Update course URLs in the review_urls table via API."""
        try:
            payload = {
                'course_number': course_number,
                'golfnow_url': golfnow_url,
                'golfpass_url': golfpass_url,
                'force': self.force_reprocess
            }

            # Use upsert endpoint for review_urls table
            response = self.session.post(
                f"{self.api_url}/api/review-urls/upsert",
                json=payload,
                timeout=self.db_timeout
            )

            if response.status_code in [200, 201]:
                result = response.json()
                if result.get('success'):
                    action = result.get('action', 'updated')
                    logger.info(f"💾 {action.title()} URLs for {course_number}")
                    return True
                elif result.get('action') == 'skipped':
                    logger.info(f"⏭️ Skipped {course_number} (URLs already exist, use --force to override)")
                    return True

            logger.error(f"Failed to update URLs for {course_number}: {response.status_code}")
            return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update URLs for {course_number}: {e}")
            return False

    async def init_browser(self):
        """Initialize browser with enhanced anti-detection settings."""
        self.playwright = await async_playwright().start()

        # Enhanced anti-detection launch args
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--exclude-switches=enable-automation',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-renderer-backgrounding',
                '--disable-backgrounding-occluded-windows',
                '--disable-background-tasks',
                '--disable-default-apps',
                '--disable-extensions',
                '--disable-hang-monitor',
                '--disable-popup-blocking',
                '--disable-prompt-on-repost',
                '--disable-sync',
                '--disable-translate',
                '--no-first-run',
                '--metrics-recording-only',
                '--safebrowsing-disable-auto-update',
                '--enable-automation=false',
                '--password-store=basic',
                '--use-mock-keychain',
                '--hide-scrollbars',
                '--mute-audio'
            ]
        )

        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'max-age=0',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1'
            }
        )

        # Enhanced anti-detection script
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    }
                ],
            });

            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });

            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };

            Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
            Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
        """)

    async def close_browser(self):
        """Clean browser shutdown."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if hasattr(self, 'playwright'):
            await self.playwright.stop()

    async def safe_popup_dismissal(self, page, site_name=""):
        """Enhanced popup dismissal that won't get stuck."""
        logger.debug(f"🚧 Starting safe popup dismissal for {site_name}...")

        await asyncio.sleep(random.uniform(1, 2))
        dismissed_count = 0

        # JavaScript dismissal first - much safer
        try:
            removed = await page.evaluate("""
                () => {
                    let removed = 0;
                    const patterns = [
                        '.ju_Con',
                        '[id*="ju_Con"]',
                        'iframe[title*="GN_Acquisition_Email_PopUp"]',
                        '.modal-backdrop',
                        '.popup-overlay',
                        '[style*="z-index: 999"]:not(input):not(select)',
                        '[style*="z-index: 9999"]:not(input):not(select)',
                        '.reveal-modal-bg'
                    ];

                    patterns.forEach(pattern => {
                        document.querySelectorAll(pattern).forEach(el => {
                            const style = window.getComputedStyle(el);
                            if (style.position === 'fixed' || style.position === 'absolute') {
                                el.style.display = 'none';
                                removed++;
                            }
                        });
                    });

                    document.body.classList.remove('modal-open', 'has-modal');
                    return removed;
                }
            """)

            if removed > 0:
                dismissed_count += removed
                logger.debug(f"   🛠️ JavaScript removed {removed} popup elements")

        except Exception as e:
            logger.debug(f"   ⚠️ JavaScript removal error: {e}")

        # Safe manual clicking as fallback
        if dismissed_count == 0:
            safe_selectors = [
                'button:has-text("No Thanks"):not([disabled]):visible',
                'button:has-text("Maybe Later"):not([disabled]):visible',
                'button:has-text("Close"):not([disabled]):visible'
            ]

            for selector in safe_selectors:
                try:
                    element = await page.wait_for_selector(selector, timeout=1000)
                    if element and await element.is_enabled() and await element.is_visible():
                        await element.click(timeout=2000)
                        dismissed_count += 1
                        logger.debug(f"   ✅ Safely clicked: {selector}")
                        break
                except:
                    continue

        logger.debug(f"🚧 Safe popup dismissal complete! Dismissed: {dismissed_count} elements")
        return dismissed_count > 0

    def is_course_match(self, course_name: str, city: str, href: str, text: str) -> bool:
        """Check if a link matches the course we're looking for."""
        course_lower = course_name.lower()
        href_lower = href.lower()
        text_lower = text.lower()

        # Check for course name words in URL or text
        course_words = [word for word in course_lower.split() if len(word) > 3]
        course_match = any(word in href_lower or word in text_lower for word in course_words)

        return course_match

    async def search_golfpass_simple(self, page, course_name: str, city: str, state: str) -> Optional[str]:
        """Enhanced GolfPass search with better error handling."""
        try:
            # Build search URL
            query = course_name.replace(' ', '%20')
            search_url = f"https://www.golfpass.com/search?q={query}&global=enabled"

            logger.debug(f"🔍 GolfPass: {search_url}")

            # Navigate with timeout
            await page.goto(search_url, wait_until='domcontentloaded', timeout=15000)
            await page.wait_for_timeout(2000)

            # Enhanced popup dismissal
            await self.safe_popup_dismissal(page, "golfpass")

            # Check for paywall indicators
            paywall_indicators = [
                'text="Add to Cart"',
                'text="Maybe Later"',
                'text="LIMITED-TIME OFFER"',
                'text="$9.99/year"',
                'text="Unlock expert tips"'
            ]

            for indicator in paywall_indicators:
                if await page.locator(indicator).count() > 0:
                    logger.debug(f"   🚫 PAYWALL DETECTED! Found: {indicator}")

                    # Try to dismiss safely
                    try:
                        dismiss_button = await page.wait_for_selector('button:has-text("Maybe Later"):not([disabled])', timeout=2000)
                        if dismiss_button:
                            await dismiss_button.click(timeout=3000)
                            await page.wait_for_timeout(1000)
                    except:
                        pass
                    break

            # Check page content for course name
            page_content = await page.content()
            course_words = [word for word in course_name.lower().split() if len(word) > 3]
            course_found = any(word in page_content.lower() for word in course_words)

            if not course_found:
                logger.debug("   ❌ Course name not found on page")
                return None

            # Extract all course links
            try:
                links = await page.locator('a[href*="/courses/"]').all()
                logger.debug(f"   📋 Found {len(links)} course links")
            except Exception as e:
                logger.debug(f"   ⚠️ Error getting links: {e}")
                return None

            # Score links based on course name match
            best_match = None
            best_score = 0

            for link in links[:10]:  # Limit for performance
                try:
                    href = await link.get_attribute('href')
                    parent = link.locator('..')
                    parent_text = await parent.inner_text()

                    # Score based on course name match
                    score = 0
                    text_lower = parent_text.lower()

                    # Course name word matches
                    course_matches = sum(1 for word in course_words if word in text_lower)
                    score += course_matches * 30

                    # Exact course name match
                    if course_name.lower() in text_lower:
                        score += 100

                    # Bonus for location match
                    if city.lower() in text_lower:
                        score += 10
                    if state.lower() in text_lower:
                        score += 5

                    if score > best_score:
                        best_score = score
                        best_match = {'href': href, 'text': parent_text, 'score': score}

                except Exception as e:
                    continue

            # Use best match if score is good enough
            if best_match and best_match['score'] >= 30:
                href = best_match['href']
                logger.debug(f"   🎯 BEST MATCH: {href} (Score: {best_match['score']})")
                if href.startswith('/'):
                    return f"https://www.golfpass.com{href}"
                return href

            return None

        except Exception as e:
            logger.debug(f"GolfPass search failed: {e}")
            return None

    async def search_golfnow_simple(self, page, course_name: str, city: str, state: str) -> Optional[str]:
        """Simplified GolfNow search focusing on directory approach."""
        try:
            # Convert state to lowercase for URL
            state_code = state.lower()
            directory_url = f"https://www.golfnow.com/course-directory/us/{state_code}"

            logger.debug(f"🔍 GolfNow directory: {directory_url}")

            # Navigate to state directory
            await page.goto(directory_url, wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(random.uniform(1, 2))

            await self.safe_popup_dismissal(page, "golfnow")

            # Look for city-related links
            city_variations = [city.lower(), city.lower().replace(' ', '')]
            all_links = await page.query_selector_all('a[href]')

            city_links = []
            for link in all_links[:50]:  # Limit for performance
                try:
                    href = await link.get_attribute('href')
                    text = await link.inner_text()

                    if not href or not text:
                        continue

                    text_lower = text.strip().lower()

                    # Check for city match
                    for city_var in city_variations:
                        if city_var in text_lower:
                            city_links.append(href)
                            break

                except Exception:
                    continue

            if not city_links:
                return None

            # Try the first few city links
            for city_link in city_links[:2]:  # Limit for performance
                try:
                    if city_link.startswith('/'):
                        target_url = f"https://www.golfnow.com{city_link}"
                    else:
                        target_url = city_link

                    await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
                    await asyncio.sleep(1)

                    # Look for course links
                    course_links = await page.query_selector_all('a[href*="/tee-times/facility/"], a[href*="/courses/"]')

                    course_words = [word.lower() for word in course_name.split() if len(word) > 3]

                    for course_link in course_links[:10]:  # Limit for performance
                        try:
                            text = await course_link.inner_text()
                            href = await course_link.get_attribute('href')

                            if not text or not href:
                                continue

                            text_lower = text.strip().lower()

                            # Check for course name match
                            word_matches = sum(1 for word in course_words if word in text_lower)
                            if word_matches >= 2 or course_name.lower() in text_lower:
                                if href.startswith('/'):
                                    return f"https://www.golfnow.com{href}"
                                return href

                        except Exception:
                            continue

                except Exception:
                    continue

            return None

        except Exception as e:
            logger.debug(f"GolfNow search failed: {e}")
            return None

    async def find_course_urls(self, course_name: str, city: str, state: str) -> Tuple[Optional[str], Optional[str]]:
        """Find URLs using API-driven approach."""
        logger.info(f"🔍 Searching: {course_name} in {city}, {state}")

        page = None
        try:
            page = await self.context.new_page()

            # Search GolfPass
            try:
                golfpass_url = await asyncio.wait_for(
                    self.search_golfpass_simple(page, course_name, city, state),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                golfpass_url = None
            except Exception:
                golfpass_url = None

            await page.wait_for_timeout(500)

            # Search GolfNow
            try:
                golfnow_url = await asyncio.wait_for(
                    self.search_golfnow_simple(page, course_name, city, state),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                golfnow_url = None
            except Exception:
                golfnow_url = None

            return golfnow_url, golfpass_url

        except Exception as e:
            logger.info(f"Error searching {course_name}: {e}")
            return None, None
        finally:
            if page:
                try:
                    await page.close()
                except:
                    pass

    async def process_courses(self) -> Dict:
        """Process courses with full API integration - FIXED."""
        # Get courses that need URL processing via API
        courses = self.get_courses_needing_urls()

        if not courses:
            logger.info("✅ No courses need URL processing")
            return {
                'total_courses': 0,
                'processed': 0,
                'successful_updates': 0,
                'failed_updates': 0,
                'skipped': 0
            }

        logger.info(f"🔄 Processing {len(courses)} courses via API")

        # Track results
        successful_updates = 0
        failed_updates = 0
        processed = 0

        # Process each course
        for i, course in enumerate(courses):
            course_number = course.get('course_number', '')

            # Only course_number is required for review_urls table
            if not course_number:
                logger.warning(f"⏭️ Skipping course {i+1}: missing course_number")
                failed_updates += 1
                continue

            # Get other fields for searching (use defaults if missing)
            course_name = (course.get('course_name') or
                          course.get('name') or
                          course.get('display_name') or
                          f"Course {course_number}")

            city = course.get('city', '')
            state = course.get('state', 'Unknown')

            # Use state as city fallback for searching
            search_city = city if city else state

            logger.info(f"🏌️ Processing {i+1}/{len(courses)}: {course_name} ({course_number})")

            # Find URLs using available data
            golfnow_url, golfpass_url = await self.find_course_urls(course_name, search_city, state)

            # Update database via API
            update_success = self.update_course_urls_in_db(course_number, golfnow_url, golfpass_url)

            if update_success:
                successful_updates += 1
                url_status = f"GolfNow={'✅' if golfnow_url else '❌'}, GolfPass={'✅' if golfpass_url else '❌'}"
                logger.info(f"✅ Updated {course_number}: {url_status}")
            else:
                failed_updates += 1
                logger.error(f"❌ Failed to update {course_number}")

            processed += 1

            # Respectful delay
            if i < len(courses) - 1:
                delay = random.randint(2, 5)
                await asyncio.sleep(delay)

        return {
            'total_courses': len(courses),
            'processed': processed,
            'successful_updates': successful_updates,
            'failed_updates': failed_updates,
            'skipped': len(courses) - processed
        }

    async def run(self) -> bool:
        """Main execution method with full API integration."""
        try:
            logger.info("🚀 Starting API-Driven Golf Course URL Finder")

            await self.init_browser()
            results = await self.process_courses()

            # Summary
            logger.info("📊 FINAL SUMMARY:")
            logger.info(f"   Total courses found: {results['total_courses']}")
            logger.info(f"   Courses processed: {results['processed']}")
            logger.info(f"   Successful database updates: {results['successful_updates']}")
            logger.info(f"   Failed database updates: {results['failed_updates']}")

            if results['processed'] > 0:
                success_rate = (results['successful_updates'] / results['processed']) * 100
                logger.info(f"   Success rate: {success_rate:.1f}%")

            # Return success if any updates were successful
            return results['successful_updates'] > 0

        except Exception as e:
            logger.error(f"💥 Critical error: {e}")
            raise
        finally:
            await self.close_browser()

def parse_args():
    """Parse command line arguments for API-driven script."""
    parser = argparse.ArgumentParser(description="API-driven golf course URL finder")

    parser.add_argument('--state', help='Filter by state code (e.g., MA, CA, TX)')
    parser.add_argument('--max-courses', type=int, help='Maximum number of courses to process')
    parser.add_argument('--headless', action='store_true', default=True, help='Run in headless mode')
    parser.add_argument('--no-headless', action='store_false', dest='headless', help='Run with visible browser')
    parser.add_argument('--force', '--force-reprocess', action='store_true',
                       help='Force reprocess courses even if URLs already exist')
    parser.add_argument('--course', help='Process a specific course number')

    return parser.parse_args()

async def main():
    """Main function with API integration and single course support."""
    args = parse_args()

    finder = APIGolfFinder(
        headless=args.headless,
        force_reprocess=args.force,
        state=args.state,
        max_courses=args.max_courses,
        course=args.course
    )

    try:
        success = await finder.run()
        if success:
            logger.info("✅ Script completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Script completed but no URLs were successfully updated")
            sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
