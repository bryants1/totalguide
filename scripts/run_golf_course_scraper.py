#!/usr/bin/env python3
"""
Smart Golf Course Website Scraper Script
Handles targeting (single course, state, or all) AND processing.
API-driven architecture with consistent argument handling.
"""

import asyncio
import urllib.parse
import requests
import subprocess
import json
import os
import sys
import argparse
import re
import gzip
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from difflib import SequenceMatcher

import aiofiles

from playwright.async_api import async_playwright, Page, Browser
from openai import OpenAI

# Content extraction for clean text
try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
    print("✅ trafilatura available for content extraction")
except ImportError:
    TRAFILATURA_AVAILABLE = False
    print("⚠️ trafilatura not available - install with: pip install trafilatura")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SmartGolfCourseScraper:
    def __init__(self, api_base_url="http://localhost:3000", course=None, state=None, force=False, limit=None, dry_run=False):
        self.api_base_url = api_base_url
        self.session = requests.Session()

        # Standard argument handling
        self.course = course
        self.state = state
        self.force = force
        self.limit = limit
        self.dry_run = dry_run

        # Scraping components
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None

        # Results tracking
        self.results = {
            'total_courses': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'files_created': []
        }

        logger.info(f"🏌️ Smart Golf Course Scraper initialized")
        if self.course:
            logger.info(f"🎯 Single course mode: {self.course}")
        if self.state:
            logger.info(f"🗺️ State filter: {self.state}")
        if self.force:
            logger.info(f"🔥 Force mode: will reprocess existing")
        if self.dry_run:
            logger.info(f"🔍 Dry run mode: will show what would be processed")

    def get_target_courses(self):
        """Get list of courses to process based on arguments"""
        try:
            if self.course:
                # Single course mode - get from primary_data or initial_course_upload
                course_info = self.get_course_info(self.course)
                if course_info:
                    return [course_info]
                else:
                    logger.error(f"❌ Course {self.course} not found")
                    return []
            else:
                # Multiple courses mode
                params = {}
                if self.state:
                    params['state'] = self.state.upper()
                if self.limit:
                    params['limit'] = self.limit
                if self.force:
                    params['force'] = 'true'

                response = self.session.get(
                    f"{self.api_base_url}/api/courses-to-scrape",
                    params=params
                )
                response.raise_for_status()
                courses = response.json()

                logger.info(f"📋 Found {len(courses)} courses to process")
                return courses

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get target courses: {e}")
            return []

    def get_course_info(self, course_number):
        """Get course information from primary_data or initial_course_upload"""
        try:
            logger.info(f"🏌️ Looking up course info for: {course_number}")

            # Try primary_data first
            response = self.session.get(f"{self.api_base_url}/api/courses/{course_number}")
            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found {course_number} in primary_data")
                return course_data

            # Fall back to initial_course_upload
            response = self.session.get(f"{self.api_base_url}/api/initial-courses/{course_number}")
            if response.status_code == 200:
                course_data = response.json()
                logger.info(f"✅ Found {course_number} in initial_course_upload")
                return course_data

            logger.warning(f"❌ Course {course_number} not found in either table")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get course info for {course_number}: {e}")
            return None

    def extract_website_url(self, course_data):
        """Extract website URL from course data"""
        if not course_data:
            return None

        url_fields = [
            'website', 'website_url', 'url', 'course_website', 'web_site',
            'site_url', 'homepage', 'web_url', 'course_url', 'official_website',
            'Website', 'Website_URL', 'URL'
        ]

        for field in url_fields:
            if field in course_data and course_data[field]:
                url = str(course_data[field]).strip()
                if url and not url.lower() in ['n/a', 'none', 'null', '', 'nan']:
                    if not url.startswith(('http://', 'https://')):
                        url = 'https://' + url
                    logger.info(f"🌐 Found website URL in field '{field}': {url}")
                    return url

        logger.warning(f"❌ No website URL found for course {course_data.get('course_number', 'unknown')}")
        return None

    def check_existing_data(self, course_number):
        """Check if scraping data already exists for this course"""
        try:
            response = self.session.get(f"{self.api_base_url}/api/course-scraping-data/{course_number}")
            if response.status_code == 200:
                data = response.json()
                if data and data.get('success') and data.get('data'):  # ← Fixed this line
                    logger.debug(f"📄 Existing scraping data found for {course_number}")
                    return True
            return False
        except Exception as e:
            logger.debug(f"Error checking existing data for {course_number}: {e}")
            return False
            
    def update_pipeline_status(self, course_number, step, status, details=None, error=None):
        """Update pipeline status for a course"""
        try:
            step_number = 9 if step == 'website_scraping' else step
            progress_percent = {
                'started': 10,
                'in_progress': 50,
                'completed': 100,
                'failed': 0
            }.get(status, 0)

            step_details = {
                'step_name': 'website_scraping',
                'timestamp': datetime.now().isoformat(),
                'status': status,
                'tool': 'smart_scraper'
            }

            if details:
                step_details.update(details)

            payload = {
                'course_number': course_number,
                'current_step': step_number,
                'progress_percent': progress_percent,
                'status': status,
                'step_details': json.dumps(step_details),
                'error_message': error
            }

            response = self.session.post(
                f"{self.api_base_url}/api/pipeline/update",
                json=payload
            )
            response.raise_for_status()
            logger.debug(f"📊 Updated pipeline status for {course_number}: {status}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update pipeline status for {course_number}: {e}")

    def upload_scraping_data(self, course_number, structured_data):
        """Upload structured scraping data to the database"""
        try:
            payload = {
                'course_number': course_number,
                'scraping_data': structured_data,
                'scraped_at': datetime.now().isoformat(),
                'source': 'smart_scraper'
            }

            response = self.session.post(
                f"{self.api_base_url}/api/course-scraping-data",
                json=payload
            )
            response.raise_for_status()
            logger.info(f"💾 Uploaded scraping data for {course_number} to database")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to upload scraping data for {course_number}: {e}")
            return False

    async def initialize_browser(self):
        """Initialize the browser and page"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=VizDisplayCompositor',
                '--disable-notifications',
                '--disable-popup-blocking=false',
                '--disable-default-apps',
                '--disable-extensions-file-access-check',
                '--disable-web-security',
                '--allow-running-insecure-content'
            ]
        )

        context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={"width": 1280, "height": 720},
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            },
            geolocation={'latitude': 42.3601, 'longitude': -71.0589},
            permissions=['geolocation']
        )

        async def handle_request(route, request):
            try:
                blocked_patterns = [
                    'googletagmanager', 'doubleclick', 'facebook.com/tr', 'google-analytics',
                    'hotjar', 'mixpanel', 'segment.com', 'intercom', 'zendesk', 'drift.com',
                    'hubspot', 'popup', 'modal', 'overlay'
                ]

                request_url = getattr(request, 'url', '')
                if request_url and isinstance(request_url, str):
                    if any(pattern in request_url.lower() for pattern in blocked_patterns):
                        await route.abort()
                        return

                await route.continue_()
            except Exception as e:
                try:
                    await route.continue_()
                except:
                    pass

        try:
            await context.route('**/*', handle_request)
            logger.debug("🛡️ Request blocking enabled for pop-up prevention")
        except Exception as e:
            logger.warning(f"⚠️ Could not enable request blocking: {str(e)}")

        self.page = await context.new_page()

        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });

            window.open = function() { return null; };
            window.$ = window.jQuery = undefined;

            window.addEventListener('load', () => {
                document.addEventListener('mouseleave', (e) => e.stopPropagation(), true);

                const originalSetTimeout = window.setTimeout;
                window.setTimeout = function(callback, delay) {
                    if (delay > 5000) return;
                    return originalSetTimeout(callback, delay);
                };
            });
        """)

        logger.debug("🌐 Browser initialized with enhanced pop-up blocking")

    async def close_browser(self):
        """Close the browser and playwright with proper error handling"""
        try:
            if self.browser and self.browser.is_connected():
                await self.browser.close()
                logger.debug("🔧 Browser closed successfully")
        except Exception as e:
            logger.debug(f"⚠️ Browser already closed or connection lost: {str(e)}")

        try:
            if hasattr(self, 'playwright') and self.playwright:
                await self.playwright.stop()
                logger.debug("🔧 Playwright stopped successfully")
        except Exception as e:
            logger.debug(f"⚠️ Playwright cleanup warning: {str(e)}")

    async def dismiss_popups(self, url: str):
        """Enhanced pop-up detection and dismissal"""
        try:
            logger.debug(f"🔍 Checking for pop-ups...")
            await self.page.wait_for_timeout(2000)

            close_selectors = [
                'button[aria-label*="close" i]',
                '[data-dismiss="modal"]',
                '.modal-close', '.popup-close', '.close-button', '.close-btn',
                'button:has-text("×")', 'button:has-text("✕")', 'button:has-text("X")',
                '[aria-label="Close"]',
                'button:has-text("Accept All")', 'button:has-text("Accept")',
                'button:has-text("OK")', 'button:has-text("Got it")',
                'button:has-text("I Agree")',
                'button:has-text("No Thanks")', 'button:has-text("Skip")',
                'button:has-text("Maybe Later")', 'button:has-text("Not Now")',
                'button:has-text("Close")',
                '.close', '.btn-close'
            ]

            dismissed_count = 0

            for selector in close_selectors:
                try:
                    element = await self.page.wait_for_selector(selector, timeout=1000)
                    if element and await element.is_visible():
                        await element.click(timeout=2000)
                        dismissed_count += 1
                        logger.debug(f"✅ Dismissed pop-up: {selector}")
                        await self.page.wait_for_timeout(1000)
                        break
                except Exception:
                    continue

            if dismissed_count == 0:
                try:
                    await self.page.keyboard.press('Escape')
                    await self.page.wait_for_timeout(500)
                    logger.debug(f"⌨️ Tried Escape key")
                except Exception:
                    pass

            if dismissed_count > 0:
                logger.debug(f"✅ Successfully dismissed {dismissed_count} pop-up(s)")
                await self.page.wait_for_timeout(1000)

            return dismissed_count

        except Exception as error:
            logger.debug(f"⚠️ Error handling pop-ups: {str(error)}")
            return 0

    async def scrape_golf_course_page(self, url: str, is_first_page: bool = False) -> Optional[Dict]:
        """Enhanced golf course page scraping"""
        try:
            logger.debug(f"Scraping: {url}")

            # Load page
            try:
                await self.page.goto(url, wait_until='networkidle', timeout=30000)
            except Exception:
                if url.startswith("http://"):
                    url = url.replace("http://", "https://", 1)
                    logger.debug(f"🔄 retrying with HTTPS → {url}")
                else:
                    logger.debug("⚠️ networkidle timed out; retrying domcontentloaded")
                await self.page.goto(url, wait_until='domcontentloaded', timeout=60000)

            # Handle popups on first page
            if is_first_page:
                await self.dismiss_popups(url)
                await self.page.wait_for_timeout(3000)
            else:
                await self.page.wait_for_timeout(1000)

            # Get raw HTML for content extraction
            page_html = await self.page.content()

            # Use trafilatura to extract clean main content if available
            clean_main_content = ""
            if TRAFILATURA_AVAILABLE:
                try:
                    clean_main_content = trafilatura.extract(page_html) or ""
                    if clean_main_content:
                        logger.debug(f"✅ Extracted {len(clean_main_content)} chars of clean content")
                except Exception as e:
                    logger.debug(f"⚠️ trafilatura extraction failed: {str(e)}")

            # Enhanced data extraction with comprehensive selectors
            golf_data = await self.page.evaluate(r"""
                (cleanContent) => {
                    // Helper functions
                    const getText = (selector) => {
                        const element = document.querySelector(selector);
                        return element ? element.textContent.trim() : '';
                    };

                    const getTexts = (selector) => {
                        const elements = document.querySelectorAll(selector);
                        return Array.from(elements)
                            .map(el => el.textContent.trim())
                            .filter(text => text && text.length > 0);
                    };

                    const getAllText = (selector) => {
                        const elements = document.querySelectorAll(selector);
                        return Array.from(elements).map(el => ({
                            text: el.textContent.trim(),
                            html: el.innerHTML,
                            tag: el.tagName.toLowerCase(),
                            classes: el.className || '',
                            id: el.id || ''
                        })).filter(item => item.text && item.text.length > 0);
                    };

                    const findPhoneNumber = () => {
                        const phoneRegex = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
                        const bodyText = document.body.innerText;
                        const matches = bodyText.match(phoneRegex);
                        return matches ? matches[0] : '';
                    };

                    const findEmail = () => {
                        const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
                        const bodyText = document.body.innerText;
                        const matches = bodyText.match(emailRegex);
                        return matches ? matches[0] : '';
                    };

                    const extractTableData = () => {
                        const tables = document.querySelectorAll('table');
                        return Array.from(tables).map(table => {
                            const rows = Array.from(table.querySelectorAll('tr'));
                            return rows.map(row => {
                                const cells = Array.from(row.querySelectorAll('td, th'));
                                return cells.map(cell => cell.textContent.trim());
                            }).filter(row => row.some(cell => cell.length > 0));
                        }).filter(table => table.length > 0);
                    };

                    const extractListData = () => {
                        const lists = document.querySelectorAll('ul, ol');
                        return Array.from(lists).map(list => {
                            const items = Array.from(list.querySelectorAll('li'));
                            return {
                                type: list.tagName.toLowerCase(),
                                items: items.map(item => item.textContent.trim()).filter(text => text.length > 0),
                                context: list.closest('[class*="amenity"], [class*="feature"], [class*="rate"], [class*="price"]')?.className || ''
                            };
                        }).filter(list => list.items.length > 0);
                    };

                    const extractCourseName = () => {
                        const selectors = [
                            'h1', '.course-name', '.site-title', '.header-title',
                            '[class*="course"][class*="name"]', '[class*="title"]',
                            '.hero h1', '.banner h1', 'header h1'
                        ];

                        for (const selector of selectors) {
                            const element = document.querySelector(selector);
                            if (element && element.textContent.trim().length > 3) {
                                return element.textContent.trim();
                            }
                        }

                        return document.title.split('-')[0].split('|')[0].trim();
                    };

                    const extractAddress = () => {
                        const addressSelectors = [
                            'address', '.address', '[class*="address"]', '[class*="location"]',
                            '[class*="contact"] .address', '.contact-info address',
                            '[itemtype*="PostalAddress"]', '[class*="postal"]'
                        ];

                        for (const selector of addressSelectors) {
                            const element = document.querySelector(selector);
                            if (element && element.textContent.trim().length > 10) {
                                return element.textContent.trim();
                            }
                        }

                        return '';
                    };

                    // Main data extraction
                    const data = {
                        url: window.location.href,
                        title: document.title,
                        timestamp: new Date().toISOString(),

                        // Basic info
                        courseName: extractCourseName(),

                        // Contact info
                        phone: getText('[href^="tel:"]') ||
                               getText('.phone') ||
                               getText('[class*="phone"]') ||
                               findPhoneNumber(),

                        email: getText('[href^="mailto:"]') ||
                               getText('.email') ||
                               getText('[class*="email"]') ||
                               findEmail(),

                        address: extractAddress(),

                        // Enhanced text content
                        allText: (cleanContent || document.body.innerText)
                            .replace(/\s+/g, ' ')
                            .trim()
                            .substring(0, 75000),

                        // Structured data extraction
                        headings: getAllText('h1, h2, h3, h4, h5, h6'),

                        // Pricing detection
                        priceElements: getAllText('[class*="price"], [class*="rate"], [class*="cost"], [class*="fee"], [class*="dollar"], .pricing, .rates'),

                        // Amenities detection
                        amenityElements: getAllText('[class*="amenity"], [class*="feature"], [class*="facility"], [class*="service"]'),

                        // Course-specific elements
                        courseElements: getAllText('[class*="course"], [class*="hole"], [class*="par"], [class*="yard"], [class*="tee"], [class*="green"]'),

                        // Hours and schedule
                        hoursElements: getAllText('[class*="hour"], [class*="time"], [class*="schedule"], [class*="open"]'),

                        // Tables and lists
                        tables: extractTableData(),
                        lists: extractListData(),

                        // Meta information
                        metaDescription: document.querySelector('meta[name="description"]')?.getAttribute('content') || '',
                        metaKeywords: document.querySelector('meta[name="keywords"]')?.getAttribute('content') || '',

                        // Enhanced social media and contact detection
                        socialLinks: Array.from(document.querySelectorAll('a[href]'))
                            .map(a => ({
                                text: a.textContent.trim(),
                                href: a.href,
                                title: a.getAttribute('title') || '',
                                ariaLabel: a.getAttribute('aria-label') || '',
                                className: a.className || '',
                                inFooter: a.closest('footer, .footer, #footer, [class*="footer"]') !== null
                            }))
                            .filter(link => {
                                const url = link.href.toLowerCase();
                                return url.includes('facebook.com') ||
                                       url.includes('instagram.com') ||
                                       url.includes('twitter.com') ||
                                       url.includes('x.com') ||
                                       url.includes('youtube.com') ||
                                       url.includes('tiktok.com') ||
                                       url.includes('linkedin.com') ||
                                       url.includes('pinterest.com') ||
                                       url.includes('snapchat.com') ||
                                       /facebook|instagram|twitter|youtube|tiktok|linkedin/i.test(
                                           link.text + ' ' + link.title + ' ' + link.ariaLabel + ' ' + link.className
                                       );
                            }),

                        // Enhanced contact information
                        contactInfo: {
                            phones: Array.from(document.querySelectorAll('a[href^="tel:"], [href*="tel:"]'))
                                .map(a => a.href.replace('tel:', ''))
                                .concat(
                                    Array.from(document.querySelectorAll('footer, .footer, .contact, [class*="contact"]'))
                                    .map(section => {
                                        const phoneRegex = /\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
                                        const matches = section.textContent.match(phoneRegex);
                                        return matches || [];
                                    })
                                    .flat()
                                ),
                            emails: Array.from(document.querySelectorAll('a[href^="mailto:"]'))
                                .map(a => a.href.replace('mailto:', '')),
                            addresses: Array.from(document.querySelectorAll('address, .address, [class*="address"], [itemtype*="PostalAddress"]'))
                                .map(el => el.textContent.trim())
                                .filter(addr => addr.length > 10)
                        },

                        // Enhanced internal links for page discovery
                        internalLinks: Array.from(document.querySelectorAll('a[href]'))
                            .map(a => ({
                                text: a.textContent.trim(),
                                href: a.href,
                                title: a.getAttribute('title') || '',
                                className: a.className || '',
                                isScorecard: /scorecard|score.card|course.info|yardage|course.layout|hole.info|hole.by.hole|course.guide|course.map|course.details|course.overview|golf.course.info|tee.distances|distances|layout|course.description|hole.descriptions/i.test(a.textContent + ' ' + a.href),
                                isRates: /rate|price|pricing|fee|cost|green.fee|membership/i.test(a.textContent + ' ' + a.href),
                                isReservation: /reservation|booking|tee.time|book/i.test(a.textContent + ' ' + a.href),
                                isAbout: /about|about.us|about.course|course.info|history|overview|our.course|the.course/i.test(a.textContent + ' ' + a.href),
                                isMembership: /membership|member|join|become.member|membership.info|dues/i.test(a.textContent + ' ' + a.href),
                                isTeeTime: /tee.time|book.now|reserve|reservation|online.booking|book.tee.time|tee.times|schedule/i.test(a.textContent + ' ' + a.href)
                            }))
                            .filter(link =>
                                link.text &&
                                link.text.length < 150 &&
                                link.text.length > 2 &&
                                (link.href.includes(window.location.hostname) || link.href.startsWith('/'))
                            )
                            .slice(0, 30),

                        // Course features detection
                        courseFeatures: {
                            hasProShop: /pro.shop|proshop|golf.shop/i.test(document.body.innerText),
                            hasDrivingRange: /driving.range|practice.range|range/i.test(document.body.innerText),
                            hasPuttingGreen: /putting.green|practice.green/i.test(document.body.innerText),
                            hasRestaurant: /restaurant|dining|grill|bar|clubhouse.dining/i.test(document.body.innerText),
                            hasWeddings: /wedding|event|banquet|reception/i.test(document.body.innerText),
                            hasLessons: /lesson|instruction|teaching|golf.pro/i.test(document.body.innerText),
                            hasCartRental: /cart.rental|golf.cart|cart/i.test(document.body.innerText)
                        },

                        // Course type detection
                        courseType: {
                            isPublic: /public.course|open.to.public|daily.fee/i.test(document.body.innerText),
                            isPrivate: /private.course|members.only|membership.required/i.test(document.body.innerText),
                            isSemiPrivate: /semi.private|semi-private|limited.public/i.test(document.body.innerText),
                            isMunicipal: /municipal|city.course|county.course/i.test(document.body.innerText)
                        }
                    };

                    return data;
                }
            """, clean_main_content)

            return golf_data

        except Exception as error:
            logger.error(f"Error scraping {url}: {str(error)}")
            return None

    async def scrape_golf_course_complete(self, base_url: str, max_pages: int = 10) -> List[Dict]:
        """Enhanced intelligent discovery and scraping of relevant golf course pages"""
        all_data = []
        visited_urls = set()

        # Start with the main page
        main_data = await self.scrape_golf_course_page(base_url, is_first_page=True)
        if main_data:
            all_data.append(main_data)
            visited_urls.add(base_url)

            # Analyze links and prioritize
            all_links = main_data.get('internalLinks', [])

            # Categorize links
            scorecard_links = []
            rates_links = []
            about_links = []
            membership_links = []
            tee_time_links = []
            amenity_links = []
            general_info_links = []

            for link in all_links:
                link_text = link['text'].lower()
                link_href = link['href'].lower()

                # Priority 1: Scorecard links
                if (link.get('isScorecard') or
                    any(pattern in link_href for pattern in [
                        'scorecard', 'score-card', 'score_card', 'course-info', 'course_info',
                        'yardage', 'course-layout', 'course_layout', 'hole-info', 'hole_info'
                    ]) or
                    any(keyword in link_text for keyword in [
                        'scorecard', 'score card', 'course info', 'yardage', 'course layout'
                    ])):
                    if link['href'] not in visited_urls:
                        scorecard_links.append(link['href'])

                # Priority 2: Rates/pricing links
                elif (link.get('isRates') or
                      any(pattern in link_href for pattern in [
                          'rate', 'price', 'pricing', 'green-fee', 'greenfee', 'cost', 'fees'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'rate', 'price', 'fee', 'cost', 'pricing', 'green fee'
                      ])):
                    if link['href'] not in visited_urls:
                        rates_links.append(link['href'])

                # Priority 3: About/Course Info links
                elif (link.get('isAbout') or
                      any(pattern in link_href for pattern in [
                          'about', 'about-us', 'about-course', 'history', 'overview'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'about us', 'about course', 'history', 'overview'
                      ])):
                    if link['href'] not in visited_urls:
                        about_links.append(link['href'])

                # Priority 4: Membership links
                elif (link.get('isMembership') or
                      any(pattern in link_href for pattern in [
                          'membership', 'member', 'join', 'become-member'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'membership', 'become a member', 'join'
                      ])):
                    if link['href'] not in visited_urls:
                        membership_links.append(link['href'])

                # Priority 5: Tee Time/Booking links
                elif (link.get('isTeeTime') or
                      any(pattern in link_href for pattern in [
                          'tee-time', 'tee_time', 'book-now', 'reserve', 'booking'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'book now', 'tee time', 'reserve', 'booking'
                      ])):
                    if link['href'] not in visited_urls:
                        tee_time_links.append(link['href'])

                # Priority 6: Amenities
                elif any(keyword in link_text for keyword in [
                    'amenity', 'facility', 'restaurant', 'pro shop', 'practice'
                ]):
                    if link['href'] not in visited_urls:
                        amenity_links.append(link['href'])

                # Priority 7: General info
                elif any(keyword in link_text for keyword in ['contact', 'clubhouse']):
                    if link['href'] not in visited_urls:
                        general_info_links.append(link['href'])

            # Prioritize links
            priority_links = (scorecard_links + rates_links + about_links +
                            membership_links + tee_time_links + amenity_links + general_info_links)

            # Remove duplicates while preserving order
            seen = set()
            unique_links = []
            for link in priority_links:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)

            logger.info(f"🔗 Found {len(scorecard_links)} scorecard, {len(rates_links)} rates, {len(about_links)} about, {len(membership_links)} membership, {len(tee_time_links)} tee-time links")
            logger.info(f"📄 Will scrape top {min(max_pages-1, len(unique_links))} priority pages")

            # Scrape priority pages
            for i, url in enumerate(unique_links[:max_pages - 1]):
                try:
                    logger.info(f"📄 Scraping page {i+2}/{min(max_pages, len(unique_links)+1)}: {url}")
                    page_data = await self.scrape_golf_course_page(url, is_first_page=False)
                    if page_data:
                        all_data.append(page_data)
                        visited_urls.add(url)

                    await asyncio.sleep(2)

                except Exception as e:
                    logger.warning(f"⚠️ Error scraping {url}: {str(e)}")
                    continue

        return all_data

    def create_analysis_ready_json(self, scraped_data: List[Dict]) -> Dict:
        """Create a clean, analysis-ready JSON structure for OpenAI"""
        analysis_data = {
            "metadata": {
                "total_pages": len(scraped_data),
                "main_url": scraped_data[0]['url'] if scraped_data else "",
                "analysis_timestamp": datetime.now().isoformat(),
                "has_scorecard_page": any(
                    any(keyword in page.get('url', '').lower() for keyword in [
                        'scorecard', 'score-card', 'course-info', 'yardage', 'layout'
                    ]) for page in scraped_data
                ),
                "has_rates_page": any('rate' in page.get('url', '').lower() or
                                    'price' in page.get('url', '').lower() or
                                    'fee' in page.get('url', '').lower()
                                    for page in scraped_data),
                "has_about_page": any('about' in page.get('url', '').lower() or
                                    'history' in page.get('url', '').lower()
                                    for page in scraped_data),
                "has_membership_page": any('membership' in page.get('url', '').lower() or
                                         'member' in page.get('url', '').lower()
                                         for page in scraped_data),
                "has_tee_time_page": any('tee-time' in page.get('url', '').lower() or
                                       'book' in page.get('url', '').lower()
                                       for page in scraped_data)
            },
            "pages": []
        }

        for i, page in enumerate(scraped_data):
            # Determine page type
            page_url = page.get('url', '').lower()
            page_type = "main"
            if any(keyword in page_url for keyword in [
                'scorecard', 'score-card', 'course-info', 'yardage', 'layout'
            ]):
                page_type = "scorecard"
            elif any(keyword in page_url for keyword in ['rate', 'price', 'fee']):
                page_type = "rates"
            elif any(keyword in page_url for keyword in ['about', 'history']):
                page_type = "about"
            elif any(keyword in page_url for keyword in ['membership', 'member']):
                page_type = "membership"
            elif any(keyword in page_url for keyword in ['tee-time', 'book', 'reserve']):
                page_type = "tee_time"

            page_data = {
                "page_number": i + 1,
                "page_type": page_type,
                "url": page.get('url', ''),
                "title": page.get('title', ''),
                "course_name": page.get('courseName', ''),
                "contact_info": {
                    "phone": page.get('phone', ''),
                    "email": page.get('email', ''),
                    "address": page.get('address', ''),
                    "all_phones": page.get('contactInfo', {}).get('phones', []),
                    "all_emails": page.get('contactInfo', {}).get('emails', []),
                    "all_addresses": page.get('contactInfo', {}).get('addresses', [])
                },
                "content": {
                    "full_text": page.get('allText', ''),
                    "headings": [h.get('text', '') for h in page.get('headings', [])],
                    "pricing_elements": [p.get('text', '') for p in page.get('priceElements', [])],
                    "amenity_elements": [a.get('text', '') for a in page.get('amenityElements', [])],
                    "course_elements": [c.get('text', '') for c in page.get('courseElements', [])],
                    "hours_elements": [h.get('text', '') for h in page.get('hoursElements', [])],
                    "meta_description": page.get('metaDescription', ''),
                    "meta_keywords": page.get('metaKeywords', '')
                },
                "structured_data": {
                    "tables": page.get('tables', []),
                    "lists": page.get('lists', [])
                },
                "course_features": page.get('courseFeatures', {}),
                "course_type": page.get('courseType', {}),
                "social_links": page.get('socialLinks', []),
                "internal_links": {
                    "scorecard_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isScorecard')],
                    "rates_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isRates')],
                    "about_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isAbout')],
                    "membership_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isMembership')],
                    "tee_time_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isTeeTime')],
                    "reservation_links": [link['href'] for link in page.get('internalLinks', []) if link.get('isReservation')]
                }
            }

            analysis_data["pages"].append(page_data)

        return analysis_data

    def extract_urls_for_text_file(self, scraped_data: List[Dict]) -> Dict[str, str]:
        """Extract important URLs for prominent display"""
        urls = {
            "scorecard_url": "",
            "rates_url": "",
            "about_url": "",
            "membership_url": "",
            "tee_time_url": "",
            "reservation_url": "",
            "main_website": ""
        }

        # Get main website
        if scraped_data:
            urls["main_website"] = scraped_data[0].get('url', '')

        # Find all URL types
        for page in scraped_data:
            page_url = page.get('url', '').lower()

            # Check for scorecard URL
            if not urls["scorecard_url"] and any(keyword in page_url for keyword in [
                'scorecard', 'score-card', 'course-info', 'yardage', 'layout'
            ]):
                urls["scorecard_url"] = page.get('url', '')

            # Check for rates URL
            if not urls["rates_url"] and any(keyword in page_url for keyword in [
                'rate', 'price', 'pricing', 'fee', 'cost'
            ]):
                urls["rates_url"] = page.get('url', '')

            # Check for about URL
            if not urls["about_url"] and any(keyword in page_url for keyword in [
                'about', 'about-us', 'history', 'overview'
            ]):
                urls["about_url"] = page.get('url', '')

            # Check for membership URL
            if not urls["membership_url"] and any(keyword in page_url for keyword in [
                'membership', 'member', 'join'
            ]):
                urls["membership_url"] = page.get('url', '')

            # Check for tee time URL
            if not urls["tee_time_url"] and any(keyword in page_url for keyword in [
                'tee-time', 'book-now', 'reserve', 'booking'
            ]):
                urls["tee_time_url"] = page.get('url', '')

            # Check internal links for missing URLs
            for link in page.get('internalLinks', []):
                if not urls["scorecard_url"] and link.get('isScorecard'):
                    urls["scorecard_url"] = link['href']
                if not urls["rates_url"] and link.get('isRates'):
                    urls["rates_url"] = link['href']
                if not urls["about_url"] and link.get('isAbout'):
                    urls["about_url"] = link['href']
                if not urls["membership_url"] and link.get('isMembership'):
                    urls["membership_url"] = link['href']
                if not urls["tee_time_url"] and link.get('isTeeTime'):
                    urls["tee_time_url"] = link['href']
                if not urls["reservation_url"] and link.get('isReservation'):
                    urls["reservation_url"] = link['href']

        return urls

    async def process_single_course(self, course_info, output_dir="scraped_courses"):
        """Process a single course with full scraping and AI analysis"""
        course_number = course_info.get('course_number')
        if not course_number:
            logger.error("❌ Course missing course_number")
            self.results['failed'] += 1
            return False

        logger.info(f"🏌️ Processing course: {course_number}")

        # Check existing data
        if not self.force and self.check_existing_data(course_number):
            logger.info(f"⏭️ Skipping {course_number} - data already exists (use --force to reprocess)")
            self.results['skipped'] += 1
            return True

        # Extract website URL
        website_url = self.extract_website_url(course_info)
        if not website_url:
            logger.error(f"❌ No website URL found for {course_number}")
            self.update_pipeline_status(
                course_number,
                'website_scraping',
                'failed',
                error="No website URL found"
            )
            self.results['failed'] += 1
            return False

        if self.dry_run:
            logger.info(f"🔍 DRY RUN: Would scrape {course_number} -> {website_url}")
            return True

        try:
            # Update pipeline to started
            self.update_pipeline_status(
                course_number,
                'website_scraping',
                'started',
                {
                    'website_url': website_url,
                    'force_reprocess': self.force,
                    'runner': 'smart_scraper'
                }
            )

            # Initialize browser if not already done
            if not self.browser:
                await self.initialize_browser()

            # Update to in progress
            self.update_pipeline_status(
                course_number,
                'website_scraping',
                'in_progress',
                {'scraping_pages': True}
            )

            # Scrape the course
            logger.info(f"🚀 Starting scrape for {course_number}: {website_url}")
            scraped_data = await self.scrape_golf_course_complete(website_url, max_pages=10)

            if not scraped_data:
                logger.error(f"❌ No data scraped for {course_number}")
                self.update_pipeline_status(
                    course_number,
                    'website_scraping',
                    'failed',
                    error="No data could be scraped from website"
                )
                self.results['failed'] += 1
                return False

            logger.info(f"✅ Scraped {len(scraped_data)} pages for {course_number}")

            # Create analysis-ready data
            analysis_ready_data = self.create_analysis_ready_json(scraped_data)
            important_urls = self.extract_urls_for_text_file(scraped_data)

            # Initialize OpenAI analyzer if API key is available
            if os.getenv('OPENAI_API_KEY'):
                try:
                    logger.info(f"🤖 Analyzing data with OpenAI for {course_number}")

                    # Import OpenAI analyzer components from the original script
                    analyzer = OpenAIAnalyzer()
                    structured_data = await analyzer.analyze_golf_course_data(analysis_ready_data)

                    if structured_data:
                        logger.info(f"✅ OpenAI analysis completed for {course_number}")

                        # Upload structured data to database
                        if self.upload_scraping_data(course_number, structured_data):
                            self.update_pipeline_status(
                                course_number,
                                'website_scraping',
                                'completed',
                                {
                                    'pages_scraped': len(scraped_data),
                                    'ai_analysis': True,
                                    'data_uploaded': True
                                }
                            )
                        else:
                            self.update_pipeline_status(
                                course_number,
                                'website_scraping',
                                'completed',
                                {
                                    'pages_scraped': len(scraped_data),
                                    'ai_analysis': True,
                                    'data_uploaded': False,
                                    'warning': 'Scraping completed but database upload failed'
                                }
                            )
                    else:
                        logger.warning(f"⚠️ OpenAI analysis failed for {course_number}")
                        # Still upload raw scraped data
                        raw_data = {
                            'scraped_data': scraped_data,
                            'analysis_ready_data': analysis_ready_data,
                            'important_urls': important_urls,
                            'ai_analysis_failed': True
                        }
                        if self.upload_scraping_data(course_number, raw_data):
                            self.update_pipeline_status(
                                course_number,
                                'website_scraping',
                                'completed',
                                {
                                    'pages_scraped': len(scraped_data),
                                    'ai_analysis': False,
                                    'data_uploaded': True,
                                    'warning': 'AI analysis failed but raw data saved'
                                }
                            )

                except Exception as e:
                    logger.error(f"❌ OpenAI analysis error for {course_number}: {str(e)}")
                    # Fall back to raw data upload
                    raw_data = {
                        'scraped_data': scraped_data,
                        'analysis_ready_data': analysis_ready_data,
                        'important_urls': important_urls,
                        'ai_analysis_error': str(e)
                    }
                    if self.upload_scraping_data(course_number, raw_data):
                        self.update_pipeline_status(
                            course_number,
                            'website_scraping',
                            'completed',
                            {
                                'pages_scraped': len(scraped_data),
                                'ai_analysis': False,
                                'data_uploaded': True,
                                'warning': f'AI analysis failed: {str(e)}'
                            }
                        )
            else:
                logger.warning(f"⚠️ No OpenAI API key - uploading raw scraped data for {course_number}")
                raw_data = {
                    'scraped_data': scraped_data,
                    'analysis_ready_data': analysis_ready_data,
                    'important_urls': important_urls,
                    'ai_analysis_skipped': 'No API key'
                }
                if self.upload_scraping_data(course_number, raw_data):
                    self.update_pipeline_status(
                        course_number,
                        'website_scraping',
                        'completed',
                        {
                            'pages_scraped': len(scraped_data),
                            'ai_analysis': False,
                            'data_uploaded': True,
                            'note': 'Raw data uploaded - no OpenAI API key'
                        }
                    )

            self.results['successful'] += 1
            return True

        except Exception as e:
            logger.error(f"❌ Unexpected error processing {course_number}: {e}")
            self.update_pipeline_status(
                course_number,
                'website_scraping',
                'failed',
                error=str(e)
            )
            self.results['failed'] += 1
            return False

    async def run(self):
        """Main execution method"""
        try:
            logger.info("🚀 Starting Smart Golf Course Scraper")

            # Get target courses
            target_courses = self.get_target_courses()
            if not target_courses:
                logger.error("❌ No courses found to process")
                return

            self.results['total_courses'] = len(target_courses)

            if self.dry_run:
                logger.info(f"🔍 DRY RUN: Would process {len(target_courses)} courses")
                for course in target_courses:
                    course_number = course.get('course_number', 'Unknown')
                    website_url = self.extract_website_url(course)
                    logger.info(f"  - {course_number}: {website_url or 'No URL'}")
                return

            logger.info(f"📋 Processing {len(target_courses)} courses")

            # Process each course
            for i, course in enumerate(target_courses, 1):
                course_number = course.get('course_number', 'Unknown')
                logger.info(f"📝 Processing course {i}/{len(target_courses)}: {course_number}")

                self.results['processed'] += 1
                await self.process_single_course(course)

                # Add delay between courses to be respectful
                if i < len(target_courses):
                    await asyncio.sleep(3)

            # Final results
            logger.info(f"🎯 Smart Golf Course Scraper completed:")
            logger.info(f"   📊 Total courses: {self.results['total_courses']}")
            logger.info(f"   ✅ Successful: {self.results['successful']}")
            logger.info(f"   ❌ Failed: {self.results['failed']}")
            logger.info(f"   ⏭️ Skipped: {self.results['skipped']}")
            if self.force:
                logger.info(f"   🔥 Force mode: {'Yes'}")

        finally:
            # Always close browser
            await self.close_browser()

# Add the OpenAI analyzer components from the original script
class OpenAIAnalyzer:
    def __init__(self, api_key: str = None, preferred_model: str = None):
        self.client = OpenAI(
            api_key=api_key or os.getenv('OPENAI_API_KEY'),
            default_headers={"Accept-Encoding": "gzip, deflate"}
        )
        self.primary_model = preferred_model or "gpt-4o-mini"

    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation: ~4 chars per token"""
        return len(text) // 4

    def remove_empty_fields(self, data):
        """Recursively remove empty fields to reduce payload size"""
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                cleaned_value = self.remove_empty_fields(value)
                if cleaned_value is not None and cleaned_value != [] and cleaned_value != {} and cleaned_value != "":
                    cleaned[key] = cleaned_value
            return cleaned if cleaned else None
        elif isinstance(data, list):
            cleaned = [self.remove_empty_fields(item) for item in data]
            return [item for item in cleaned if item is not None and item != [] and item != {} and item != ""]
        else:
            return data

    async def analyze_golf_course_data(self, analysis_ready_data: Dict) -> Dict:
        """Analyze golf course data with OpenAI and return structured JSON"""
        try:
            # Clean and optimize payload
            clean_data = self.remove_empty_fields(analysis_ready_data)
            if clean_data is None:
                clean_data = analysis_ready_data

            json_payload = json.dumps(clean_data, separators=(',', ':'), ensure_ascii=False)

            # Check payload size
            estimated_tokens = self.estimate_tokens(json_payload)
            if estimated_tokens > 120000:
                logger.warning(f"⚠️ Large payload ({estimated_tokens:,} tokens) - may hit limits")

            # OpenAI function schema (simplified for space)
            functions = [
                {
                    "name": "extract_golf_course_data",
                    "description": "Extract golf course details in structured format.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "general_info": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "address": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "phone": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "email": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "website": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "course_description": {"type": "object", "properties": {"value": {"type": "array", "items": {"type": "string"}}}, "required": ["value"]},
                                    "course_type": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                                    "18_hole_course": {"type": "object", "properties": {"value": {"type": "boolean"}}, "required": ["value"]},
                                    "9_hole_course": {"type": "object", "properties": {"value": {"type": "boolean"}}, "required": ["value"]},
                                    "pricing_level": {
                                        "type": "object",
                                        "properties": {
                                            "value": {"type": "integer", "minimum": 1, "maximum": 5},
                                            "description": {"type": "string"},
                                            "typical_18_hole_rate": {"type": "string"}
                                        },
                                        "required": ["value", "description", "typical_18_hole_rate"]
                                    }
                                },
                                "required": ["name", "address", "phone", "email", "website", "course_description", "course_type", "18_hole_course", "9_hole_course", "pricing_level"]
                            },
                            "rates": {
                                "type": "object",
                                "properties": {
                                    "pricing_information": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}
                                },
                                "required": ["pricing_information"]
                            },
                            "amenities": {
                                "type": "object",
                                "properties": {
                                    "pro_shop": {"type": "object", "properties": {"value": {"type": "array", "items": {"type": "string"}}, "available": {"type": "boolean"}}, "required": ["value", "available"]},
                                    "driving_range": {"type": "object", "properties": {"value": {"type": "array", "items": {"type": "string"}}, "available": {"type": "boolean"}}, "required": ["value", "available"]},
                                    "practice_green": {"type": "object", "properties": {"value": {"type": "array", "items": {"type": "string"}}, "available": {"type": "boolean"}}, "required": ["value", "available"]},
                                    "clubhouse": {"type": "object", "properties": {"value": {"type": "array", "items": {"type": "string"}}, "available": {"type": "boolean"}}, "required": ["value", "available"]},
                                    "food_beverage_options": {"type": "object", "properties": {"value": {"type": "string"}, "available": {"type": "boolean"}}, "required": ["value", "available"]}
                                },
                                "required": ["pro_shop", "driving_range", "practice_green", "clubhouse", "food_beverage_options"]
                            },
                            "metadata": {
                                "type": "object",
                                "properties": {
                                    "pages_crawled": {"type": "object", "properties": {"value": {"type": "integer"}}, "required": ["value"]},
                                    "last_updated": {"type": "object", "properties": {"value": {"type": "string", "format": "date-time"}}, "required": ["value"]},
                                    "spider_version": {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}
                                },
                                "required": ["pages_crawled", "last_updated", "spider_version"]
                            }
                        },
                        "required": ["general_info", "rates", "amenities", "metadata"]
                    }
                }
            ]

            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are an expert at extracting golf course data. Extract comprehensive information "
                        "including pricing, amenities, and course details. Format pricing information clearly "
                        "and determine appropriate pricing levels based on typical 18-hole rates. "
                        "Respond only by calling the function with the exact JSON schema."
                    )
                },
                {
                    "role": "user",
                    "content": json_payload
                }
            ]

            response = self.client.chat.completions.create(
                model=self.primary_model,
                messages=messages,
                functions=functions,
                function_call={"name": "extract_golf_course_data"},
                temperature=0.0,
                max_tokens=12000
            )

            func_call = response.choices[0].message.function_call
            structured_json = json.loads(func_call.arguments)

            logger.info(f"✅ OpenAI analysis completed successfully")
            return structured_json

        except Exception as e:
            logger.error(f"❌ OpenAI analysis failed: {str(e)}")
            return None

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Smart Golf Course Website Scraper with API integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all courses that need scraping
  python %(prog)s

  # Process only California courses
  python %(prog)s --state CA

  # Process specific course
  python %(prog)s --course COURSE123

  # Force reprocess all courses (including already processed)
  python %(prog)s --force

  # Dry run to see what would be processed
  python %(prog)s --dry-run

  # Process max 5 courses in Massachusetts, force reprocess
  python %(prog)s --state MA --limit 5 --force
        """
    )

    # Standard arguments (consistent with other pipeline scripts)
    parser.add_argument('--course', help='Process a specific course number', metavar='COURSE_NUM')
    parser.add_argument('--state', help='Process all courses in this state (e.g., MA, CA, TX)', metavar='STATE')
    parser.add_argument('--force', action='store_true', help='Force reprocess courses even if data already exists')
    parser.add_argument('--limit', type=int, help='Maximum number of courses to process', metavar='N')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed without actually doing it')

    # API configuration
    parser.add_argument('--api-url', default='http://localhost:3000',
                       help='Base URL for API (default: http://localhost:3000)', metavar='URL')

    # Logging level
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate state format
    if args.state and len(args.state) != 2:
        logger.error("❌ State must be a 2-letter code (e.g., CA, NY, TX)")
        sys.exit(1)

    async def run_scraper():
        scraper = SmartGolfCourseScraper(
            api_base_url=args.api_url,
            course=args.course,
            state=args.state,
            force=args.force,
            limit=args.limit,
            dry_run=args.dry_run
        )

        try:
            await scraper.run()

            # Print final results
            results = scraper.results
            total = results['total_courses']
            successful = results['successful']
            failed = results['failed']
            skipped = results['skipped']

            if total > 0:
                success_rate = (successful / total) * 100 if total > 0 else 0
                logger.info(f"📊 Final Results:")
                logger.info(f"   Success rate: {success_rate:.1f}% ({successful}/{total})")

                if failed > 0:
                    logger.warning(f"   ⚠️ {failed} courses failed")
                if skipped > 0:
                    logger.info(f"   ⏭️ {skipped} courses skipped (already processed)")

                # Exit with appropriate code
                if failed > 0 and successful == 0:
                    sys.exit(1)  # All failed
                elif failed > 0:
                    sys.exit(2)  # Some failed
                else:
                    sys.exit(0)  # All successful
            else:
                logger.warning("❌ No courses were processed")
                sys.exit(1)

        except KeyboardInterrupt:
            logger.info("🛑 Scraping interrupted by user")
            sys.exit(130)
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    # Run the async scraper
    try:
        asyncio.run(run_scraper())
    except Exception as e:
        logger.error(f"❌ Failed to start scraper: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
