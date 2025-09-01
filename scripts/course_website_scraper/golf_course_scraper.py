import asyncio
import json
import os
import re
import argparse
import sys
import gzip
from datetime import datetime
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse
from difflib import SequenceMatcher
from pathlib import Path

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

class golf_course_scraper:
    def __init__(self, force_update=False):
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.force_update = force_update
        if force_update:
            print("🔥 FORCE UPDATE MODE ENABLED - will overwrite existing files")

    async def check_and_handle_existing_files(self, output_file_base: str) -> bool:
        """
        Check if output files exist and handle based on force_update setting
        Returns True if scraping should proceed, False if should skip
        """
        output_path = Path(output_file_base)
        structured_file = output_path.with_name(f"{output_path.name}_structured.json")
        txt_file = output_path.with_suffix('.txt')

        existing_files = []
        if structured_file.exists():
            existing_files.append(str(structured_file))
        if txt_file.exists():
            existing_files.append(str(txt_file))

        if not existing_files:
            print("📄 No existing files found - proceeding with scraping")
            return True

        if self.force_update:
            print(f"🔥 FORCE UPDATE: Deleting {len(existing_files)} existing files...")
            for file_path in existing_files:
                try:
                    Path(file_path).unlink()
                    print(f"    ✅ Deleted: {file_path}")
                except Exception as e:
                    print(f"    ⚠️ Could not delete {file_path}: {e}")
            return True
        else:
            print(f"⏭️ SKIPPING: Found {len(existing_files)} existing files")
            for file_path in existing_files:
                print(f"    📄 Exists: {file_path}")
            print("    💡 Use --force to overwrite existing files")
            return False

    async def initialize(self):
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
            print("🛡️ Request blocking enabled for pop-up prevention")
        except Exception as e:
            print(f"⚠️ Could not enable request blocking: {str(e)}")

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

        print("🌐 Browser initialized with enhanced pop-up blocking")

    async def scrape_golf_course(self, url: str, is_first_page: bool = False) -> Optional[Dict]:
        """Enhanced golf course page scraping with original working pricing logic"""
        try:
            print(f"Scraping: {url}")

            # first try networkidle (all requests quiet)
            try:
                await self.page.goto(url, wait_until='networkidle', timeout=30000)
            except Exception:
                # if it hangs, retry with HTTPS or at least domcontentloaded
                if url.startswith("http://"):
                    url = url.replace("http://", "https://", 1)
                    print(f"🔄 retrying with HTTPS → {url}")
                else:
                    print("⚠️ networkidle timed out; retrying domcontentloaded")
                await self.page.goto(url, wait_until='domcontentloaded', timeout=60000)

            # on the very first page we might get popups
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
                        print(f"  ✅ Extracted {len(clean_main_content)} chars of clean content")
                except Exception as e:
                    print(f"  ⚠️ trafilatura extraction failed: {str(e)}")

            # Enhanced data extraction with original working selectors
            golf_data = await self.page.evaluate(r"""
                (cleanContent) => {
                    // Enhanced helper functions
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

                    // Enhanced course name detection
                    const extractCourseName = () => {
                        // Try multiple selectors for course name
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

                        // Fallback to page title
                        return document.title.split('-')[0].split('|')[0].trim();
                    };

                    // Enhanced address extraction
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

                        // Enhanced basic info
                        courseName: extractCourseName(),

                        // Enhanced contact info
                        phone: getText('[href^="tel:"]') ||
                               getText('.phone') ||
                               getText('[class*="phone"]') ||
                               findPhoneNumber(),

                        email: getText('[href^="mailto:"]') ||
                               getText('.email') ||
                               getText('[class*="email"]') ||
                               findEmail(),

                        address: extractAddress(),

                        // Enhanced text content with better limits (use trafilatura if available)
                        allText: (cleanContent || document.body.innerText)
                            .replace(/\s+/g, ' ')
                            .trim()
                            .substring(0, 75000), // Increased limit for better analysis

                        // Enhanced structured data extraction using original working approach
                        headings: getAllText('h1, h2, h3, h4, h5, h6'),

                        // Simple, effective pricing detection (original working logic)
                        priceElements: getAllText('[class*="price"], [class*="rate"], [class*="cost"], [class*="fee"], [class*="dollar"], .pricing, .rates'),

                        // Enhanced amenities detection
                        amenityElements: getAllText('[class*="amenity"], [class*="feature"], [class*="facility"], [class*="service"]'),

                        // Course-specific elements
                        courseElements: getAllText('[class*="course"], [class*="hole"], [class*="par"], [class*="yard"], [class*="tee"], [class*="green"]'),

                        // Hours and schedule
                        hoursElements: getAllText('[class*="hour"], [class*="time"], [class*="schedule"], [class*="open"]'),

                        // Tables (for scorecards, rates, etc.)
                        tables: extractTableData(),

                        // Lists (for amenities, features, etc.)
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

                        // Enhanced internal links for better page discovery with NEW URL types
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
                            .slice(0, 30), // Increased limit for better discovery

                        // Enhanced course features detection
                        courseFeatures: {
                            hasProShop: /pro.shop|proshop|golf.shop/i.test(document.body.innerText),
                            hasDrivingRange: /driving.range|practice.range|range/i.test(document.body.innerText),
                            hasPuttingGreen: /putting.green|practice.green/i.test(document.body.innerText),
                            hasRestaurant: /restaurant|dining|grill|bar|clubhouse.dining/i.test(document.body.innerText),
                            hasWeddings: /wedding|event|banquet|reception/i.test(document.body.innerText),
                            hasLessons: /lesson|instruction|teaching|golf.pro/i.test(document.body.innerText),
                            hasCartRental: /cart.rental|golf.cart|cart/i.test(document.body.innerText)
                        },

                        // Enhanced course type detection
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
                print(f"Error scraping {url}: {str(error)}")
                return None

    async def dismiss_popups(self, url: str):
        """Enhanced pop-up detection and dismissal"""
        try:
            print(f"  🔍 Checking for pop-ups on main page...")
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
                        print(f"    ✅ Dismissed pop-up: {selector}")
                        await self.page.wait_for_timeout(1000)
                        break
                except Exception:
                    continue

            if dismissed_count == 0:
                try:
                    await self.page.keyboard.press('Escape')
                    await self.page.wait_for_timeout(500)
                    print(f"    ⌨️ Tried Escape key")
                except Exception:
                    pass

            if dismissed_count > 0:
                print(f"    ✅ Successfully dismissed {dismissed_count} pop-up(s)")
                await self.page.wait_for_timeout(1000)
            else:
                print(f"    ✅ No pop-ups detected")

            return dismissed_count

        except Exception as error:
            print(f"    ⚠️ Error handling pop-ups: {str(error)}")
            return 0

    async def scrape_golf_course_complete(self, base_url: str, max_pages: int = 10) -> List[Dict]:
        """Enhanced intelligent discovery and scraping of relevant golf course pages"""
        all_data = []
        visited_urls = set()

        # Start with the main page
        main_data = await self.scrape_golf_course(base_url, is_first_page=True)
        if main_data:
            all_data.append(main_data)
            visited_urls.add(base_url)

            # Original simple link analysis and prioritization
            all_links = []
            if main_data.get('internalLinks'):
                all_links = main_data['internalLinks']

            # Categorize links with simple detection (back to original logic)
            scorecard_links = []
            rates_links = []
            about_links = []        # NEW
            membership_links = []   # NEW
            tee_time_links = []     # NEW
            amenity_links = []
            general_info_links = []

            for link in all_links:
                link_text = link['text'].lower()
                link_href = link['href'].lower()

                # Priority 1: Scorecard links (enhanced detection)
                if (link.get('isScorecard') or
                    # High priority URL patterns (most likely to be scorecards)
                    any(pattern in link_href for pattern in [
                        'scorecard', 'score-card', 'score_card'
                    ]) or
                    # High priority link text (most likely to be scorecards)
                    any(keyword in link_text for keyword in [
                        'scorecard', 'score card'
                    ]) or
                    # Medium priority URL patterns
                    any(pattern in link_href for pattern in [
                        'course-info', 'course_info', 'yardage', 'course-layout',
                        'course_layout', 'hole-info', 'hole_info', 'course-map',
                        'course_map', 'hole-by-hole', 'hole_by_hole', 'course-guide',
                        'course_guide', 'course-details', 'course_details'
                    ]) or
                    # Medium priority link text
                    any(keyword in link_text for keyword in [
                        'course info', 'yardage', 'course layout', 'hole info',
                        'course map', 'hole by hole', 'course guide', 'course details',
                        'tee distances', 'distances', 'layout', 'yardages'
                    ]) or
                    # Lower priority but still relevant patterns
                    any(pattern in link_href for pattern in [
                        'holes', 'course-overview', 'course_overview', 'golf-course',
                        'golf_course', 'course-description', 'course_description',
                        'hole-descriptions', 'hole_descriptions'
                    ]) or
                    any(keyword in link_text for keyword in [
                        'holes', 'course overview', 'golf course info', 'course description',
                        'hole descriptions', 'about the course', 'our course'
                    ])):
                    if link['href'] not in visited_urls:
                        # Prioritize based on quality - add to front for high priority patterns
                        if any(pattern in link_href for pattern in ['scorecard', 'score-card', 'score_card']) or \
                           any(keyword in link_text for keyword in ['scorecard', 'score card']):
                            scorecard_links.insert(0, link['href'])  # High priority - add to front
                            print(f"  🎯 Found HIGH PRIORITY scorecard link: {link_text} -> {link['href']}")
                        else:
                            scorecard_links.append(link['href'])  # Normal priority - add to end
                            print(f"  🎯 Found scorecard link: {link_text} -> {link['href']}")

                # Priority 2: Rates/pricing links
                elif (link.get('isRates') or
                      any(pattern in link_href for pattern in [
                          'rate', 'price', 'pricing', 'green-fee', 'greenfee',
                          'cost', 'fees', 'membership', 'dues'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'rate', 'price', 'fee', 'cost', 'pricing', 'green fee',
                          'membership', 'golf rate', 'play rate', 'dues'
                      ])):
                    if link['href'] not in visited_urls:
                        rates_links.append(link['href'])
                        print(f"  💰 Found rates link: {link_text} -> {link['href']}")

                # Priority 3: About/Course Info links (NEW)
                elif (link.get('isAbout') or
                      any(pattern in link_href for pattern in [
                          'about', 'about-us', 'about-course', 'course-info', 'course_info',
                          'history', 'overview', 'our-course', 'the-course'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'about us', 'about course', 'course info', 'our course',
                          'history', 'overview', 'the course', 'course details'
                      ])):
                    if link['href'] not in visited_urls:
                        about_links.append(link['href'])
                        print(f"  ℹ️ Found about link: {link_text} -> {link['href']}")

                # Priority 4: Membership links (NEW)
                elif (link.get('isMembership') or
                      any(pattern in link_href for pattern in [
                          'membership', 'member', 'join', 'become-member', 'membership-info',
                          'dues', 'member-info', 'memberships'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'membership', 'become a member', 'join', 'member info',
                          'membership info', 'dues', 'memberships'
                      ])):
                    if link['href'] not in visited_urls:
                        membership_links.append(link['href'])
                        print(f"  👥 Found membership link: {link_text} -> {link['href']}")

                # Priority 5: Tee Time/Booking links (NEW)
                elif (link.get('isTeeTime') or
                      any(pattern in link_href for pattern in [
                          'tee-time', 'tee_time', 'book-now', 'book_now', 'reserve',
                          'reservation', 'online-booking', 'book-tee-time', 'tee-times',
                          'booking', 'schedule'
                      ]) or
                      any(keyword in link_text for keyword in [
                          'book now', 'tee time', 'tee times', 'reserve', 'book tee time',
                          'online booking', 'make reservation', 'book online', 'schedule'
                      ])):
                    if link['href'] not in visited_urls:
                        tee_time_links.append(link['href'])
                        print(f"  📅 Found tee time link: {link_text} -> {link['href']}")

                # Priority 6: Amenities and facilities
                elif any(keyword in link_text for keyword in [
                    'amenity', 'facility', 'restaurant', 'dining', 'pro shop', 'proshop',
                    'lesson', 'instruction', 'tournament', 'event', 'practice', 'range'
                ]):
                    if link['href'] not in visited_urls:
                        amenity_links.append(link['href'])

                # Priority 7: General information
                elif any(keyword in link_text for keyword in [
                    'contact', 'clubhouse'
                ]):
                    if link['href'] not in visited_urls:
                        general_info_links.append(link['href'])

            # Prioritize links: scorecards first, then rates, then about, membership, tee times, then amenities, then general
            priority_links = (scorecard_links + rates_links + about_links +
                            membership_links + tee_time_links + amenity_links + general_info_links)

            # Remove duplicates while preserving order
            seen = set()
            unique_links = []
            for link in priority_links:
                if link not in seen:
                    seen.add(link)
                    unique_links.append(link)

            print(f"  🔗 Found {len(scorecard_links)} scorecard, {len(rates_links)} rates, {len(about_links)} about, {len(membership_links)} membership, {len(tee_time_links)} tee-time, {len(amenity_links)} amenity, {len(general_info_links)} general links")
            print(f"  📄 Will scrape top {min(max_pages-1, len(unique_links))} priority pages")

            # Scrape priority pages
            for i, url in enumerate(unique_links[:max_pages - 1]):
                try:
                    print(f"  📄 Scraping page {i+2}/{min(max_pages, len(unique_links)+1)}: {url}")
                    page_data = await self.scrape_golf_course(url, is_first_page=False)
                    if page_data:
                        all_data.append(page_data)
                        visited_urls.add(url)

                    await asyncio.sleep(2)

                except Exception as e:
                    print(f"  ⚠️ Error scraping {url}: {str(e)}")
                    continue

            # Enhanced fallback scorecard detection
            if not scorecard_links and len(all_data) < max_pages:
                print(f"  🔍 No scorecard links found, trying enhanced common paths...")

                common_scorecard_paths = [
                    # High priority paths (most likely to be scorecards)
                    '/scorecard', '/scorecard/', '/score-card', '/score-card/',
                    '/course/scorecard', '/course/scorecard/', '/golf/scorecard', '/golf/scorecard/',

                    # Medium priority paths
                    '/course-info', '/course-info/', '/course_info', '/course_info/',
                    '/yardage', '/yardage/', '/course-layout', '/course-layout/',
                    '/course_layout', '/course_layout/', '/course-map', '/course-map/',
                    '/course_map', '/course_map/', '/hole-info', '/hole-info/',
                    '/hole_info', '/hole_info/', '/holes', '/holes/',

                    # Additional comprehensive paths
                    '/hole-by-hole', '/hole-by-hole/', '/hole_by_hole', '/hole_by_hole/',
                    '/course-guide', '/course-guide/', '/course_guide', '/course_guide/',
                    '/course-details', '/course-details/', '/course_details', '/course_details/',
                    '/course-overview', '/course-overview/', '/course_overview', '/course_overview/',
                    '/golf-course', '/golf-course/', '/golf_course', '/golf_course/',
                    '/distances', '/distances/', '/layout', '/layout/',
                    '/course-description', '/course-description/', '/course_description', '/course_description/',
                    '/hole-descriptions', '/hole-descriptions/', '/hole_descriptions', '/hole_descriptions/',
                    '/tee-distances', '/tee-distances/', '/tee_distances', '/tee_distances/',

                    # Common CMS patterns
                    '/aboutus/scorecard', '/aboutus/scorecard/', '/about/scorecard', '/about/scorecard/',
                    '/aboutus/course-info', '/aboutus/course-info/', '/about/course-info', '/about/course-info/',
                    '/golf/course-info', '/golf/course-info/', '/golf/yardage', '/golf/yardage/',
                    '/golf/layout', '/golf/layout/', '/course/layout', '/course/layout/',
                    '/course/yardage', '/course/yardage/', '/course/holes', '/course/holes/',

                    # Less common but possible patterns
                    '/about-course', '/about-course/', '/about_course', '/about_course/',
                    '/our-course', '/our-course/', '/our_course', '/our_course/',
                    '/the-course', '/the-course/', '/the_course', '/the_course/'
                ]

                for path in common_scorecard_paths:
                    try:
                        from urllib.parse import urljoin
                        scorecard_url = urljoin(base_url, path)
                        if scorecard_url not in visited_urls:
                            print(f"    🎯 Trying scorecard path: {scorecard_url}")

                            response = await self.page.goto(scorecard_url, wait_until='domcontentloaded', timeout=10000)
                            if response and response.status == 200:
                                print(f"    ✅ Found scorecard at: {scorecard_url}")
                                page_data = await self.scrape_golf_course(scorecard_url, is_first_page=False)
                                if page_data:
                                    all_data.append(page_data)
                                    visited_urls.add(scorecard_url)
                                break

                    except Exception as e:
                        continue

        return all_data

    async def close(self):
        """Close the browser and playwright with proper error handling"""
        try:
            if self.browser and self.browser.is_connected():
                await self.browser.close()
                print("🔧 Browser closed successfully")
        except Exception as e:
            print(f"⚠️ Browser already closed or connection lost: {str(e)}")

        try:
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
                print("🔧 Playwright stopped successfully")
        except Exception as e:
            print(f"⚠️ Playwright cleanup warning: {str(e)}")

    def create_analysis_ready_json(self, scraped_data: List[Dict]) -> Dict:
        """Create a clean, analysis-ready JSON structure for OpenAI"""

        analysis_data = {
            "metadata": {
                "total_pages": len(scraped_data),
                "main_url": scraped_data[0]['url'] if scraped_data else "",
                "analysis_timestamp": datetime.now().isoformat(),
                "has_scorecard_page": any(
                    any(keyword in page.get('url', '').lower() for keyword in [
                        'scorecard', 'score-card', 'score_card', 'course-info', 'course_info',
                        'yardage', 'course-layout', 'course_layout', 'hole-info', 'hole_info',
                        'course-map', 'course_map', 'hole-by-hole', 'hole_by_hole', 'course-guide',
                        'course_guide', 'course-details', 'course_details', 'distances', 'layout'
                    ]) for page in scraped_data
                ),
                "has_rates_page": any('rate' in page.get('url', '').lower() or
                                    'price' in page.get('url', '').lower() or
                                    'fee' in page.get('url', '').lower()
                                    for page in scraped_data),
                "has_about_page": any('about' in page.get('url', '').lower() or
                                    'history' in page.get('url', '').lower() or
                                    'overview' in page.get('url', '').lower()
                                    for page in scraped_data),
                "has_membership_page": any('membership' in page.get('url', '').lower() or
                                         'member' in page.get('url', '').lower() or
                                         'join' in page.get('url', '').lower()
                                         for page in scraped_data),
                "has_tee_time_page": any('tee-time' in page.get('url', '').lower() or
                                       'book' in page.get('url', '').lower() or
                                       'reserve' in page.get('url', '').lower()
                                       for page in scraped_data)
            },
            "pages": []
        }

        for i, page in enumerate(scraped_data):
            # Determine page type (enhanced scorecard detection)
            page_url = page.get('url', '').lower()
            page_type = "main"
            if any(keyword in page_url for keyword in [
                'scorecard', 'score-card', 'score_card', 'course-info', 'course_info',
                'yardage', 'course-layout', 'course_layout', 'hole-info', 'hole_info',
                'course-map', 'course_map', 'hole-by-hole', 'hole_by_hole', 'course-guide',
                'course_guide', 'course-details', 'course_details', 'distances', 'layout'
            ]):
                page_type = "scorecard"
            elif any(keyword in page_url for keyword in ['rate', 'price', 'fee']):
                page_type = "rates"
            elif any(keyword in page_url for keyword in ['about', 'history', 'overview']):
                page_type = "about"
            elif any(keyword in page_url for keyword in ['membership', 'member', 'join']):
                page_type = "membership"
            elif any(keyword in page_url for keyword in ['tee-time', 'book', 'reserve']):
                page_type = "tee_time"
            elif any(keyword in page_url for keyword in ['amenity', 'facility', 'restaurant']):
                page_type = "amenities"

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
        """Extract important URLs for prominent display in text file"""
        urls = {
            "scorecard_url": "",
            "rates_url": "",
            "about_url": "",        # NEW
            "membership_url": "",   # NEW
            "tee_time_url": "",     # NEW
            "reservation_url": "",
            "main_website": ""
        }

        # Get main website
        if scraped_data:
            urls["main_website"] = scraped_data[0].get('url', '')

        # Find all URL types
        for page in scraped_data:
            page_url = page.get('url', '').lower()

            # Check for scorecard URL (enhanced detection)
            if not urls["scorecard_url"] and any(keyword in page_url for keyword in [
                # High priority patterns
                'scorecard', 'score-card', 'score_card',
                # Medium priority patterns
                'course-info', 'course_info', 'yardage', 'course-layout', 'course_layout',
                'hole-info', 'hole_info', 'course-map', 'course_map', 'hole-by-hole', 'hole_by_hole',
                'course-guide', 'course_guide', 'course-details', 'course_details',
                # Additional patterns
                'course-overview', 'course_overview', 'golf-course-info', 'golf_course_info',
                'tee-distances', 'tee_distances', 'distances', 'layout', 'holes',
                'course-description', 'course_description', 'hole-descriptions', 'hole_descriptions'
            ]):
                urls["scorecard_url"] = page.get('url', '')

            # Check for rates URL
            if not urls["rates_url"] and any(keyword in page_url for keyword in [
                'rate', 'price', 'pricing', 'fee', 'cost', 'green-fee'
            ]):
                urls["rates_url"] = page.get('url', '')

            # Check for about URL (NEW)
            if not urls["about_url"] and any(keyword in page_url for keyword in [
                'about', 'about-us', 'about-course', 'history', 'overview', 'our-course'
            ]):
                urls["about_url"] = page.get('url', '')

            # Check for membership URL (NEW)
            if not urls["membership_url"] and any(keyword in page_url for keyword in [
                'membership', 'member', 'join', 'become-member', 'membership-info', 'dues'
            ]):
                urls["membership_url"] = page.get('url', '')

            # Check for tee time URL (NEW)
            if not urls["tee_time_url"] and any(keyword in page_url for keyword in [
                'tee-time', 'book-now', 'reserve', 'reservation', 'online-booking', 'booking'
            ]):
                urls["tee_time_url"] = page.get('url', '')

            # Check internal links for missing URLs
            for link in page.get('internalLinks', []):
                if not urls["scorecard_url"] and link.get('isScorecard'):
                    urls["scorecard_url"] = link['href']
                if not urls["rates_url"] and link.get('isRates'):
                    urls["rates_url"] = link['href']
                if not urls["about_url"] and link.get('isAbout'):           # NEW
                    urls["about_url"] = link['href']
                if not urls["membership_url"] and link.get('isMembership'): # NEW
                    urls["membership_url"] = link['href']
                if not urls["tee_time_url"] and link.get('isTeeTime'):      # NEW
                    urls["tee_time_url"] = link['href']
                if not urls["reservation_url"] and link.get('isReservation'):
                    urls["reservation_url"] = link['href']

        return urls

    # Keep existing helper methods for social media and rates extraction
    def extract_social_media_enhanced(self, scraped_data):
        """Enhanced social media link extraction and cleaning"""
        social_links = {
            "facebook_url": "",
            "instagram_url": "",
            "twitter_url": "",
            "youtube_url": "",
            "tiktok_url": ""
        }

        all_social_links = []
        for page in scraped_data:
            if page.get('socialLinks'):
                all_social_links.extend(page['socialLinks'])

        for link in all_social_links:
            url = link.get('href', '').lower()
            clean_url = link.get('href', '')

            if 'facebook.com' in url and not social_links["facebook_url"]:
                social_links["facebook_url"] = clean_url
            elif 'instagram.com' in url and not social_links["instagram_url"]:
                social_links["instagram_url"] = clean_url
            elif ('twitter.com' in url or 'x.com' in url) and not social_links["twitter_url"]:
                social_links["twitter_url"] = clean_url
            elif ('youtube.com' in url or 'youtu.be' in url) and not social_links["youtube_url"]:
                social_links["youtube_url"] = clean_url
            elif 'tiktok.com' in url and not social_links["tiktok_url"]:
                social_links["tiktok_url"] = clean_url

        return social_links

    # Keep other existing helper methods with same implementation
    def _extract_best_course_name(self, scraped_data, headings):
        for page in scraped_data:
            if page.get('courseName') and len(page['courseName']) > 3:
                return page['courseName']
        for page in scraped_data:
            if page.get('title'):
                title = page['title']
                title = re.sub(r'\s*-\s*.*$', '', title)
                if 'golf' in title.lower() and len(title) > 5:
                    return title
        for heading in headings:
            if 'golf' in heading.lower() and len(heading) > 5:
                return heading
        return "Unknown Golf Course"

    def _extract_primary_address(self, scraped_data):
        for page in scraped_data:
            contact_info = page.get('contactInfo', {})
            addresses = contact_info.get('addresses', [])
            if addresses:
                return addresses[0]
        return ""

    def _extract_primary_phone(self, scraped_data):
        for page in scraped_data:
            contact_info = page.get('contactInfo', {})
            phones = contact_info.get('phones', [])
            if phones:
                return phones[0]
        return ""

    def _extract_primary_email(self, scraped_data):
        for page in scraped_data:
            contact_info = page.get('contactInfo', {})
            emails = contact_info.get('emails', [])
            if emails:
                return emails[0]
        return ""

    def _detect_18_hole_course(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        patterns = [
            r'18[^.]*hole',
            r'eighteen[^.]*hole',
            r'regulation[^.]*course',
            r'championship[^.]*course'
        ]
        return any(re.search(pattern, all_text, re.IGNORECASE) for pattern in patterns)

    def _detect_9_hole_course(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        patterns = [r'9[^.]*hole', r'nine[^.]*hole']
        return any(re.search(pattern, all_text, re.IGNORECASE) for pattern in patterns)

    def _detect_par3_course(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        patterns = [
            r'par[^.]*3[^.]*course',
            r'par[^.]*three[^.]*course',
            r'short[^.]*course'
        ]
        return any(re.search(pattern, all_text, re.IGNORECASE) for pattern in patterns)

    def _detect_executive_course(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        return 'executive' in all_text and 'course' in all_text

    def _detect_ocean_views(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        patterns = [
            r'ocean[^.]*view',
            r'water[^.]*view',
            r'coastal[^.]*view',
            r'beach[^.]*view'
        ]
        return any(re.search(pattern, all_text, re.IGNORECASE) for pattern in patterns)

    def _detect_scenic_views(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        patterns = [
            r'scenic[^.]*view',
            r'mountain[^.]*view',
            r'valley[^.]*view',
            r'panoramic[^.]*view',
            r'beautiful[^.]*view'
        ]
        return any(re.search(pattern, all_text, re.IGNORECASE) for pattern in patterns)

    def _detect_pro_shop(self, scraped_data):
        all_text = " ".join([page.get('allText', '') for page in scraped_data]).lower()
        if "pro shop" in all_text or "proshop" in all_text:
            return ["Pro shop available"]
        return []

    def _count_regex_extractions(self, scraped_data):
        count = 0
        for page in scraped_data:
            if page.get('socialLinks'):
                count += len(page['socialLinks'])
            if page.get('contactInfo'):
                contact = page['contactInfo']
                count += len(contact.get('phones', []))
                count += len(contact.get('emails', []))
        return count


class OpenAIAnalyzer:
    def __init__(self, api_key: str = None, preferred_model: str = None):
        self.client = OpenAI(
            api_key=api_key or os.getenv('OPENAI_API_KEY'),
            # Enable compression for requests
            default_headers={"Accept-Encoding": "gzip, deflate"}
        )
        self.primary_model = preferred_model or "gpt-4o-mini"

        self.model_costs = {
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
            "gpt-3.5-turbo-1106": {"input": 0.001, "output": 0.002},
            "gpt-3.5-turbo": {"input": 0.0015, "output": 0.002},
            "gpt-4-turbo-preview": {"input": 0.01, "output": 0.03},
            "gpt-4": {"input": 0.03, "output": 0.06}
        }

        print(f"🤖 Using model: {self.primary_model}")

        self.usage_stats = {
            self.primary_model: {"calls": 0, "tokens": 0}
        }

    def remove_empty_fields(self, data):
        """Recursively remove empty fields to reduce payload size"""
        if isinstance(data, dict):
            cleaned = {}
            for key, value in data.items():
                cleaned_value = self.remove_empty_fields(value)
                # Only include non-empty values
                if cleaned_value is not None and cleaned_value != [] and cleaned_value != {} and cleaned_value != "":
                    cleaned[key] = cleaned_value
            return cleaned if cleaned else None
        elif isinstance(data, list):
            cleaned = [self.remove_empty_fields(item) for item in data]
            # Remove None values and empty items
            return [item for item in cleaned if item is not None and item != [] and item != {} and item != ""]
        else:
            return data

    def remove_duplicate_content(self, data: Dict) -> Dict:
        """Remove duplicate content across pages to reduce payload size"""
        if 'pages' not in data or len(data['pages']) <= 1:
            return data

        print("\n🔍 DUPLICATE CONTENT ANALYSIS:")
        print("-" * 50)

        pages = data['pages']
        duplicates_found = 0
        total_chars_removed = 0

        # Track all text content we've seen
        seen_texts: Set[str] = set()

        # Process each page
        for i, page in enumerate(pages):
            content = page.get('content', {})
            page_duplicates = 0
            page_chars_removed = 0

            # Check each text field for duplicates
            for field_name in ['full_text', 'headings', 'pricing_elements',
                             'amenity_elements', 'course_elements', 'hours_elements']:

                field_data = content.get(field_name)
                if not field_data:
                    continue

                if isinstance(field_data, str):
                    # For strings, check for significant overlap with previous content
                    if self._is_duplicate_text(field_data, seen_texts, similarity_threshold=0.8):
                        original_size = len(field_data)
                        content[field_name] = ""  # Remove duplicate
                        page_chars_removed += original_size
                        page_duplicates += 1
                        print(f"    Page {i+1} {field_name}: Removed {original_size} chars (duplicate)")
                    else:
                        seen_texts.add(field_data)

                elif isinstance(field_data, list):
                    # For arrays, remove individual duplicate items
                    original_count = len(field_data)
                    deduplicated = []

                    for item in field_data:
                        if isinstance(item, str) and item:
                            if not self._is_duplicate_text(item, seen_texts, similarity_threshold=0.9):
                                deduplicated.append(item)
                                seen_texts.add(item)
                            else:
                                page_chars_removed += len(item)
                                page_duplicates += 1

                    content[field_name] = deduplicated
                    removed_count = original_count - len(deduplicated)
                    if removed_count > 0:
                        print(f"    Page {i+1} {field_name}: Removed {removed_count}/{original_count} items (duplicates)")

            # Handle structured data duplicates
            if 'structured_data' in page:
                structured = page['structured_data']

                # Deduplicate tables
                if 'tables' in structured:
                    original_tables = len(structured['tables'])
                    unique_tables = []

                    for table in structured['tables']:
                        table_str = json.dumps(table, separators=(',', ':'))
                        if not self._is_duplicate_text(table_str, seen_texts, similarity_threshold=0.95):
                            unique_tables.append(table)
                            seen_texts.add(table_str)
                        else:
                            page_chars_removed += len(table_str)
                            page_duplicates += 1

                    structured['tables'] = unique_tables
                    removed_tables = original_tables - len(unique_tables)
                    if removed_tables > 0:
                        print(f"    Page {i+1} tables: Removed {removed_tables}/{original_tables} tables (duplicates)")

                # Deduplicate lists
                if 'lists' in structured:
                    original_lists = len(structured['lists'])
                    unique_lists = []

                    for list_item in structured['lists']:
                        list_str = json.dumps(list_item, separators=(',', ':'))
                        if not self._is_duplicate_text(list_str, seen_texts, similarity_threshold=0.95):
                            unique_lists.append(list_item)
                            seen_texts.add(list_str)
                        else:
                            page_chars_removed += len(list_str)
                            page_duplicates += 1

                    structured['lists'] = unique_lists
                    removed_lists = original_lists - len(unique_lists)
                    if removed_lists > 0:
                        print(f"    Page {i+1} lists: Removed {removed_lists}/{original_lists} lists (duplicates)")

            if page_duplicates > 0:
                duplicates_found += page_duplicates
                total_chars_removed += page_chars_removed
                print(f"  Page {i+1}: Removed {page_duplicates} duplicates ({page_chars_removed:,} chars)")

        if duplicates_found > 0:
            print(f"\n✅ Deduplication complete: Removed {duplicates_found} duplicates ({total_chars_removed:,} chars)")
        else:
            print(f"✅ No significant duplicates found")

        print("-" * 50)
        return data

    def _is_duplicate_text(self, text: str, seen_texts: Set[str], similarity_threshold: float = 0.8) -> bool:
        """Check if text is similar to any previously seen text"""
        if not text or len(text) < 50:  # Skip very short texts
            return False

        # Check for exact matches first
        if text in seen_texts:
            return True

        # Check for high similarity with existing texts
        for seen_text in seen_texts:
            if len(seen_text) < 50:  # Skip comparing with short texts
                continue

            # Quick length check - if very different lengths, likely not duplicates
            length_ratio = min(len(text), len(seen_text)) / max(len(text), len(seen_text))
            if length_ratio < 0.5:
                continue

            # Check similarity
            similarity = SequenceMatcher(None, text, seen_text).ratio()
            if similarity >= similarity_threshold:
                return True

        return False

    def analyze_payload_content(self, data: Dict) -> None:
        """Analyze what's taking up space in the payload"""
        print("\n🔍 PAYLOAD CONTENT ANALYSIS:")
        print("-" * 50)

        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        total_size = len(json_str)

        # Analyze by section
        for section_name, section_data in data.items():
            if isinstance(section_data, (dict, list)):
                section_json = json.dumps(section_data, separators=(',', ':'), ensure_ascii=False)
                section_size = len(section_json)
                percentage = (section_size / total_size * 100) if total_size > 0 else 0
                print(f"  {section_name}: {section_size:,} chars ({percentage:.1f}%)")

                # Deep dive into pages section
                if section_name == 'pages' and isinstance(section_data, list):
                    for i, page in enumerate(section_data):
                        page_json = json.dumps(page, separators=(',', ':'), ensure_ascii=False)
                        page_size = len(page_json)
                        print(f"    Page {i+1}: {page_size:,} chars")

                        # Analyze page content
                        if isinstance(page, dict) and 'content' in page:
                            content = page['content']
                            for field_name, field_data in content.items():
                                if field_data:  # Only show non-empty fields
                                    field_json = json.dumps(field_data, separators=(',', ':'), ensure_ascii=False)
                                    field_size = len(field_json)
                                    print(f"      {field_name}: {field_size:,} chars")

                                    # Show sample of large text fields
                                    if field_name == 'full_text' and isinstance(field_data, str) and len(field_data) > 1000:
                                        sample = field_data[:200] + "..." if len(field_data) > 200 else field_data
                                        print(f"        Sample: {repr(sample)}")

        print(f"\nTotal payload: {total_size:,} chars (~{total_size//4:,} tokens)")
        print("-" * 50)

    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation: ~4 chars per token"""
        return len(text) // 4

    def truncate_for_tokens(self, data: Dict, max_tokens: int = 100000) -> Dict:
        """Aggressively truncate data to fit within token limits"""
        # Estimate current size
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        current_tokens = self.estimate_tokens(json_str)

        if current_tokens <= max_tokens:
            return data

        print(f"  ⚠️ Payload too large ({current_tokens:,} tokens), truncating to fit {max_tokens:,} token limit")

        # Create a copy to modify
        truncated = json.loads(json.dumps(data))  # Deep copy

        # Aggressive truncation of text fields
        for page in truncated.get('pages', []):
            content = page.get('content', {})

            # Truncate full_text more aggressively
            if content.get('full_text'):
                content['full_text'] = content['full_text'][:4000]  # 4k chars max

            # Limit array sizes more aggressively
            for field in ['headings', 'pricing_elements', 'amenity_elements', 'course_elements', 'hours_elements']:
                if content.get(field):
                    content[field] = content[field][:5]  # Max 5 items each

        # Check size again and truncate further if needed
        json_str = json.dumps(truncated, separators=(',', ':'), ensure_ascii=False)
        new_tokens = self.estimate_tokens(json_str)

        if new_tokens > max_tokens:
            print(f"  ⚠️ Still too large ({new_tokens:,} tokens), applying emergency truncation")
            # Emergency: Keep only first 2 pages
            if len(truncated.get('pages', [])) > 2:
                truncated['pages'] = truncated['pages'][:2]

        final_json = json.dumps(truncated, separators=(',', ':'), ensure_ascii=False)
        final_tokens = self.estimate_tokens(final_json)
        print(f"  ✅ Truncated to {final_tokens:,} tokens")

        return truncated

    def prune_empty_values(self, data):
        """Recursively remove empty arrays, objects, None, and empty strings"""
        if isinstance(data, dict):
            return {k: self.prune_empty_values(v) for k, v in data.items() if v not in (None, '', [], {})}
        elif isinstance(data, list):
            return [self.prune_empty_values(v) for v in data if v not in (None, '', [], {})]
        else:
            return data

    async def analyze_golf_course_data(self, analysis_ready_data: Dict) -> Dict:
        """
        Invoke OpenAI function-calling with extract_golf_course_data schema
        and return exactly the structured JSON.
        """
        # Remove empty fields first to significantly reduce payload
        print("\n🗜️  PAYLOAD OPTIMIZATION:")
        print("-" * 50)

        original_json = json.dumps(analysis_ready_data, separators=(',', ':'), ensure_ascii=False)
        original_size = len(original_json)
        print(f"  📦 Original payload: {original_size:,} chars")

        # Remove empty fields
        clean_data = self.remove_empty_fields(analysis_ready_data)
        if clean_data is None:
            clean_data = analysis_ready_data  # Fallback

        clean_json = json.dumps(clean_data, separators=(',', ':'), ensure_ascii=False)
        clean_size = len(clean_json)

        if clean_size < original_size:
            reduction_pct = ((original_size - clean_size) / original_size * 100)
            print(f"  🗜️  Empty field removal: -{original_size - clean_size:,} chars ({reduction_pct:.1f}%)")

        # Calculate original tokens
        original_tokens = self.estimate_tokens(clean_json)

        # Remove duplicate content across pages
        deduplicated_data = self.remove_duplicate_content(clean_data)

        # Calculate reduction from deduplication
        deduplicated_json = json.dumps(deduplicated_data, separators=(',', ':'), ensure_ascii=False)
        deduplicated_size = len(deduplicated_json)
        deduplicated_tokens = self.estimate_tokens(deduplicated_json)

        if deduplicated_size < clean_size:
            reduction_pct = ((clean_size - deduplicated_size) / clean_size * 100)
            token_reduction = self.estimate_tokens(clean_json) - deduplicated_tokens
            print(f"  📉 Deduplication saved: {clean_size - deduplicated_size:,} chars ({reduction_pct:.1f}%) | {token_reduction:,} tokens")

        # Analyze payload content to understand what's taking up space
        self.analyze_payload_content(deduplicated_data)

        # Use the deduplicated data
        final_data = deduplicated_data
        json_payload = deduplicated_json

        # Final safety check - if still too large, apply emergency truncation
        if deduplicated_tokens > 120000:
            print(f"  ⚠️ Payload still too large, applying emergency truncation...")

            # Emergency truncation: Keep only essential data
            for page in final_data.get('pages', []):
                content = page.get('content', {})

                # Truncate all text arrays more aggressively
                for field in ['headings', 'pricing_elements', 'amenity_elements', 'course_elements', 'hours_elements']:
                    if content.get(field):
                        content[field] = content[field][:3]  # Max 3 items each

                # Truncate full_text more
                if content.get('full_text'):
                    content['full_text'] = content['full_text'][:4000]

                # Limit structured data
                if 'structured_data' in page:
                    structured = page['structured_data']
                    if 'tables' in structured:
                        structured['tables'] = structured['tables'][:1]  # Max 1 table
                    if 'lists' in structured:
                        structured['lists'] = structured['lists'][:2]  # Max 2 lists

            # If still too many pages, keep only the most important ones
            if len(final_data.get('pages', [])) > 4:
                # Keep main page + up to 3 most important pages (scorecard, rates, amenities)
                pages = final_data['pages']
                main_page = pages[0] if pages else None
                important_pages = []

                for page in pages[1:]:
                    page_url = page.get('url', '').lower()
                    if any(keyword in page_url for keyword in ['scorecard', 'rate', 'price', 'amenity']):
                        important_pages.append(page)
                        if len(important_pages) >= 3:
                            break

                final_data['pages'] = [main_page] + important_pages if main_page else important_pages
                print(f"    📄 Reduced to {len(final_data['pages'])} most important pages")

            # Recalculate after emergency truncation
            json_payload = json.dumps(final_data, separators=(',', ':'), ensure_ascii=False)
            final_tokens = self.estimate_tokens(json_payload)
            print(f"    🚨 Emergency truncation complete: {len(json_payload):,} chars (~{final_tokens:,} tokens)")

        print(f"  📦 Final payload: {len(json_payload):,} chars (~{self.estimate_tokens(json_payload):,} tokens)")

        # Show payload reduction summary
        total_reduction = original_size - len(json_payload)
        total_reduction_pct = (total_reduction / original_size * 100) if original_size > 0 else 0
        print(f"  ✅ Total reduction: -{total_reduction:,} chars ({total_reduction_pct:.1f}%)")
        print("-" * 50)

        if self.estimate_tokens(json_payload) > 120000:
            print(f"  ⚠️ WARNING: Payload may still exceed 128K token limit!")

        functions = [
            {
                "name": "extract_golf_course_data",
                "description": "Extract all golf course details in structured format.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "general_info": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "address": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "phone": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "email": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "website": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "course_description": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "scorecard_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "about_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "membership_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "tee_time_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "course_type": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "rates_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "18_hole_course": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "9_hole_course": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "par_3_course": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "executive_course": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "ocean_views": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "scenic_views": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "boolean"}
                                    },
                                    "required": ["value"]
                                },
                                "signature_holes": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "pricing_level": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "integer",
                                            "minimum": 1,
                                            "maximum": 5,
                                            "description": "Pricing level: 1 ($0-$50), 2 ($51-$100), 3 ($101-$140), 4 ($141-$180), 5 ($181+)"
                                        },
                                        "description": {"type": "string"},
                                        "typical_18_hole_rate": {"type": "string"}
                                    },
                                    "required": ["value", "description", "typical_18_hole_rate"]
                                }
                            },
                            "required": [
                                "name", "address", "phone", "email", "website",
                                "course_description", "scorecard_url", "about_url",
                                "membership_url", "tee_time_url", "course_type",
                                "rates_url", "18_hole_course", "9_hole_course",
                                "par_3_course", "executive_course", "ocean_views",
                                "scenic_views", "signature_holes", "pricing_level"
                            ]
                        },
                        "rates": {
                            "type": "object",
                            "properties": {
                                "pricing_information": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": ["pricing_information"]
                        },
                        "amenities": {
                            "type": "object",
                            "properties": {
                                "pro_shop": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "driving_range": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "practice_green": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "short_game_practice_area": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "clubhouse": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "locker_rooms": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "showers": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "food_beverage_options": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"},
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "food_beverage_options_description": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"},
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "beverage_cart": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                },
                                "banquet_facilities": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "available": {"type": "boolean"}
                                    },
                                    "required": ["value", "available"]
                                }
                            },
                            "required": [
                                "pro_shop", "driving_range", "practice_green",
                                "short_game_practice_area", "clubhouse", "locker_rooms",
                                "showers", "food_beverage_options",
                                "food_beverage_options_description",
                                "beverage_cart", "banquet_facilities"
                            ]
                        },
                        "course_history": {
                            "type": "object",
                            "properties": {
                                "general": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "architect": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "year_built": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": ["integer", "null"]}
                                    },
                                    "required": ["value"]
                                },
                                "notable_events": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "design_features": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": [
                                "general", "architect", "year_built",
                                "notable_events", "design_features"
                            ]
                        },
                        "awards": {
                            "type": "object",
                            "properties": {
                                "recognitions": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "rankings": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "certifications": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": ["recognitions", "rankings", "certifications"]
                        },
                        "amateur_professional_events": {
                            "type": "object",
                            "properties": {
                                "amateur_tournaments": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "professional_tournaments": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "charity_events": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "contact_for_events": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": [
                                "amateur_tournaments", "professional_tournaments",
                                "charity_events", "contact_for_events"
                            ]
                        },
                        "policies": {
                            "type": "object",
                            "properties": {
                                "course_policies": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": ["course_policies"]
                        },
                        "social": {
                            "type": "object",
                            "properties": {
                                "facebook_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "instagram_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "twitter_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "youtube_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                },
                                "tiktok_url": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": [
                                "facebook_url", "instagram_url", "twitter_url",
                                "youtube_url", "tiktok_url"
                            ]
                        },
                        "sustainability": {
                            "type": "object",
                            "properties": {
                                "general": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "certifications": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                },
                                "practices": {
                                    "type": "object",
                                    "properties": {
                                        "value": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        }
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": ["general", "certifications", "practices"]
                        },
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "pages_crawled": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "integer"}
                                    },
                                    "required": ["value"]
                                },
                                "ml_extractions": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "integer"}
                                    },
                                    "required": ["value"]
                                },
                                "regex_extractions": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "integer"}
                                    },
                                    "required": ["value"]
                                },
                                "last_updated": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string", "format": "date-time"}
                                    },
                                    "required": ["value"]
                                },
                                "spider_version": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"}
                                    },
                                    "required": ["value"]
                                }
                            },
                            "required": [
                                "pages_crawled", "ml_extractions", "regex_extractions",
                                "last_updated", "spider_version"
                            ]
                        }
                    },
                    "required": [
                        "general_info", "rates", "amenities",
                        "course_history", "awards", "amateur_professional_events",
                        "policies", "social", "sustainability", "metadata"
                    ]
                }
            }
        ]

        messages = [
            {
                "role": "system",
                "content": (
                    "You are an expert at extracting golf course data with special focus on comprehensive pricing extraction and pricing level categorization.\n\n"
                    "CRITICAL FOR PRICING_INFORMATION: You must extract ALL available pricing details from pricing_elements, tables, lists, and full_text. Include:\n\n"
                    "1. GREEN FEES: 18-hole and 9-hole rates for weekdays/weekends, walking/riding\n"
                    "2. CART RENTAL: Per cart or per person fees\n"
                    "3. DISCOUNTS: Senior, military, twilight, replay, resident rates with specific amounts\n"
                    "4. TIME-BASED: Morning, afternoon, twilight rates with exact times when available\n"
                    "5. SEASONAL: Peak/off-peak, summer/winter rate variations\n"
                    "6. MEMBERSHIPS: Annual, monthly, punch cards, season passes\n"
                    "7. ADDITIONAL: Club rental, pull cart, range balls, lesson rates\n\n"
                    "Format as detailed structured text with clear categories and specific dollar amounts.\n\n"
                    "CRITICAL FOR PRICING_LEVEL: Analyze the extracted pricing to determine the appropriate pricing level and use these EXACT descriptions:\n\n"
                    "• Level 1 ($0-$50): Use description 'Ideal for most municipal/public courses and twilight specials.'\n"
                    "• Level 2 ($51-$100): Use description 'Covers mid-range daily-fee courses and early-bird/weekday rates at nicer tracks.'\n"
                    "• Level 3 ($101-$140): Use description 'Represents upper mid-tier layouts, popular resort off-peak rates, and weekend discounts.'\n"
                    "• Level 4 ($141-$180): Use description 'Premium resort/play-and-stay packages, high-end daily-fee courses, and peak-season rates.'\n"
                    "• Level 5 ($181+): Use description 'Championship-level courses, signature resort fees, and exclusive club guest rates.'\n\n"
                    "Base the pricing_level on typical 18-hole weekend rates (including cart if required). If only 9-hole rates available, double them for estimation. Use the most common/standard rate, not just the cheapest twilight rate.\n\n"
                    "CRITICAL FOR COURSE TYPE LOGIC: A course can only be ONE primary type. Use this priority logic:\n"
                    "• If 18-hole course is detected: set 18_hole_course=true, 9_hole_course=false\n"
                    "• If only 9-hole course is detected: set 9_hole_course=true, 18_hole_course=false\n"
                    "• If both mentioned but 18-hole is primary: set 18_hole_course=true, 9_hole_course=false\n"
                    "• Par-3 and executive courses can coexist with 9-hole or 18-hole designation\n"
                    "• Default if unclear: set 18_hole_course=true, 9_hole_course=false\n\n"
                    "CRITICAL FOR AMENITY DETECTION - Use this enhanced logic:\n\n"
                    "• DRIVING RANGE: Set available=true if 'driving range', 'practice range', or 'range' is mentioned ANYWHERE in course description, amenities, or content\n"
                    "• PRACTICE GREEN: Set available=true if 'putting green', 'practice green', 'putting area', or 'practice putting' is mentioned ANYWHERE\n"
                    "• SHORT GAME PRACTICE AREA: Only set available=true if practice_green is true AND there are mentions of chipping, pitching, bunkers, or short game areas\n"
                    "• CLUBHOUSE: Set available=true if 'clubhouse' is mentioned OR if restaurant/banquet facilities are available (indicates clubhouse presence)\n"
                    "• PRO SHOP: Set available=true if 'pro shop', 'proshop', 'golf shop', or 'retail' is mentioned\n\n"
                    "For course_policies, extract ALL policies including dress code, cancellation, rain checks, cart policies, walking policies, guest policies, advance booking requirements.\n\n"
                    "For awards and sustainability, extract ALL mentions of recognitions, certifications, and environmental practices.\n\n"
                    "Be extremely comprehensive. If pricing is not found, state 'Contact course directly for current rates' and include phone number. Set pricing_level to 3 (middle tier) with description 'Represents upper mid-tier layouts, popular resort off-peak rates, and weekend discounts.' if no pricing information is available.\n\n"
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
            max_tokens=12_000
        )

        # Track usage stats
        if hasattr(response, 'usage') and response.usage:
            self.usage_stats[self.primary_model]["calls"] += 1
            self.usage_stats[self.primary_model]["tokens"] += response.usage.total_tokens

            cost_info = self.model_costs.get(self.primary_model, {})
            estimated_cost = (
                (response.usage.prompt_tokens / 1000) * cost_info.get('input', 0) +
                (response.usage.completion_tokens / 1000) * cost_info.get('output', 0)
            )
            print(f"  💰 Cost: ≈${estimated_cost:.4f} ({response.usage.total_tokens} tokens)")

        func_call = response.choices[0].message.function_call
        structured_json = json.loads(func_call.arguments)
        return structured_json

    def print_usage_stats(self):
        """Print usage statistics and cost estimates"""
        print("\n📊 API Usage Summary:")
        print("-" * 50)
        total_cost = 0

        for model, stats in self.usage_stats.items():
            if stats["calls"] > 0:
                cost_info = self.model_costs.get(model, {})
                estimated_cost = (stats["tokens"] / 1000) * (cost_info.get('input', 0) + cost_info.get('output', 0)) / 2
                total_cost += estimated_cost

                print(f"  {model}:")
                print(f"    Calls: {stats['calls']}")
                print(f"    Tokens: {stats['tokens']:,}")
                print(f"    Est. Cost: ${estimated_cost:.4f}")
                print()

        if total_cost > 0:
            print(f"💰 Total Estimated Cost: ${total_cost:.4f}")
        else:
            print("💰 No API calls made")
        print("-" * 50)


async def save_results(data: Dict, filename_base: str = None):
    """Enhanced save results with clean structured data file"""
    if filename_base is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_base = f"golf_course_data_{timestamp}"

    saved_files = []

    # 1. Save the clean structured data from OpenAI (MOST IMPORTANT)
    structured_data = data.get('structured_data')
    if structured_data:
        clean_filename = f"{filename_base}_structured.json"
        async with aiofiles.open(clean_filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(structured_data, indent=2, ensure_ascii=False))
        print(f"📄 ⭐ CLEAN STRUCTURED DATA saved to: {clean_filename}")
        saved_files.append(clean_filename)

    # 2. Save the analysis-ready JSON (for debugging/reprocessing)
    analysis_json_filename = f"{filename_base}_analysis_ready.json"
    if data.get('analysis_ready_data'):
        async with aiofiles.open(analysis_json_filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data['analysis_ready_data'], indent=2, ensure_ascii=False))
        print(f"📄 Analysis-ready data saved to: {analysis_json_filename}")
        saved_files.append(analysis_json_filename)

    # 3. Save complete results JSON (for full debugging)
    json_filename = f"{filename_base}_complete.json"
    async with aiofiles.open(json_filename, 'w', encoding='utf-8') as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    saved_files.append(json_filename)

    # 4. Save enhanced TXT file (human readable report)
    txt_filename = f"{filename_base}.txt"
    try:
        formatted_text = format_data_as_text(data)
        async with aiofiles.open(txt_filename, 'w', encoding='utf-8') as f:
            await f.write(formatted_text)
        saved_files.append(txt_filename)
    except Exception as e:
        print(f"⚠️ Error formatting text report: {str(e)}")
        backup_text = f"Golf Course Analysis Report\n{'='*50}\n\nJSON data saved to: {json_filename}\n\nError occurred while formatting detailed report.\nPlease check the JSON file for complete data.\n\nError details: {str(e)}"
        async with aiofiles.open(txt_filename, 'w', encoding='utf-8') as f:
            await f.write(backup_text)
        saved_files.append(txt_filename)

    print(f"✅ Data saved to {len(saved_files)} files")

    # Return the structured JSON and readable text
    return saved_files[0], txt_filename


def format_data_as_text(data: Dict) -> str:
    """Enhanced text formatting with prominent URL display"""
    text_output = []
    text_output.append("=" * 80)
    text_output.append("COMPREHENSIVE GOLF COURSE ANALYSIS REPORT")
    text_output.append("=" * 80)
    text_output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    text_output.append("")

    def get_value(item):
        if isinstance(item, dict) and 'value' in item:
            return item['value']
        return item

    def get_available(item):
        if isinstance(item, dict) and 'available' in item:
            return item['available']
        return None

    structured_data = data.get('structured_data', {})
    if structured_data:
        general = structured_data.get('general_info', {})

        # PROMINENT URL SECTION AT TOP
        text_output.append("🔗 IMPORTANT LINKS")
        text_output.append("=" * 50)

        website = get_value(general.get('website', {}))
        if website:
            text_output.append(f"🌐 Main Website: {website}")

        scorecard_url = get_value(general.get('scorecard_url', {}))
        if scorecard_url:
            text_output.append(f"🎯 SCORECARD: {scorecard_url}")

        rates_url = get_value(general.get('rates_url', {}))
        if rates_url:
            text_output.append(f"💰 RATES: {rates_url}")

        # NEW URL DISPLAYS
        about_url = get_value(general.get('about_url', {}))
        if about_url:
            text_output.append(f"ℹ️ ABOUT: {about_url}")

        membership_url = get_value(general.get('membership_url', {}))
        if membership_url:
            text_output.append(f"👥 MEMBERSHIP: {membership_url}")

        tee_time_url = get_value(general.get('tee_time_url', {}))
        if tee_time_url:
            text_output.append(f"📅 TEE TIMES: {tee_time_url}")

        # Also check if we have these URLs from raw scraping data
        important_urls = data.get('important_urls', {})
        if important_urls:
            if important_urls.get('scorecard_url') and not scorecard_url:
                text_output.append(f"🎯 SCORECARD: {important_urls['scorecard_url']}")
            if important_urls.get('rates_url') and not rates_url:
                text_output.append(f"💰 RATES: {important_urls['rates_url']}")
            # NEW FALLBACK DISPLAYS
            if important_urls.get('about_url') and not about_url:
                text_output.append(f"ℹ️ ABOUT: {important_urls['about_url']}")
            if important_urls.get('membership_url') and not membership_url:
                text_output.append(f"👥 MEMBERSHIP: {important_urls['membership_url']}")
            if important_urls.get('tee_time_url') and not tee_time_url:
                text_output.append(f"📅 TEE TIMES: {important_urls['tee_time_url']}")
            if important_urls.get('reservation_url'):
                text_output.append(f"📅 RESERVATIONS: {important_urls['reservation_url']}")

        text_output.append("")

        # General info
        text_output.append("🏌️ GENERAL INFORMATION")
        text_output.append("-" * 50)

        name = get_value(general.get('name', {}))
        if name:
            text_output.append(f"Course Name: {name}")

        address = get_value(general.get('address', {}))
        if address:
            text_output.append(f"Address: {address}")

        phone = get_value(general.get('phone', {}))
        if phone:
            text_output.append(f"Phone: {phone}")

        email = get_value(general.get('email', {}))
        if email:
            text_output.append(f"Email: {email}")

        course_type = get_value(general.get('course_type', {}))
        if course_type:
            text_output.append(f"Course Type: {course_type}")

        # Course types
        course_types = []
        if get_value(general.get('18_hole_course', {})):
            course_types.append("18-hole")
        if get_value(general.get('9_hole_course', {})):
            course_types.append("9-hole")
        if get_value(general.get('par_3_course', {})):
            course_types.append("Par-3")
        if get_value(general.get('executive_course', {})):
            course_types.append("Executive")

        if course_types:
            text_output.append(f"Available Courses: {', '.join(course_types)}")

        # Views
        views = []
        if get_value(general.get('ocean_views', {})):
            views.append("Ocean Views")
        if get_value(general.get('scenic_views', {})):
            views.append("Scenic Views")

        if views:
            text_output.append(f"Special Features: {', '.join(views)}")

        # Course description
        course_desc = get_value(general.get('course_description', {}))
        if course_desc:
            text_output.append(f"\nCourse Description:")
            if isinstance(course_desc, list):
                for desc in course_desc:
                    text_output.append(f"  • {desc}")
            else:
                text_output.append(f"  • {course_desc}")

        # Signature holes
        signature_holes = get_value(general.get('signature_holes', {}))
        if signature_holes:
            text_output.append(f"\nSignature Holes:")
            if isinstance(signature_holes, list):
                for hole in signature_holes:
                    text_output.append(f"  • {hole}")
            else:
                text_output.append(f"  • {signature_holes}")

        # Rates section - ALWAYS display, even if no pricing found
        rates = structured_data.get('rates', {})
        pricing_info = get_value(rates.get('pricing_information', {}))

        text_output.append(f"\n💰 RATES & PRICING")
        text_output.append("-" * 50)

        # Add pricing level information first
        pricing_level_info = general.get('pricing_level', {})
        if pricing_level_info:
            level = get_value(pricing_level_info)
            level_desc = get_value(pricing_level_info.get('description', ''))
            typical_rate = get_value(pricing_level_info.get('typical_18_hole_rate', ''))

            if level:
                level_ranges = {
                    1: "$0-$50 (Municipal/Public)",
                    2: "$51-$100 (Mid-Range)",
                    3: "$101-$140 (Upper Mid-Tier)",
                    4: "$141-$180 (Premium)",
                    5: "$181+ (Championship)"
                }
                level_range = level_ranges.get(level, "Unknown")
                text_output.append(f"💵 Pricing Level: {level} - {level_range}")

                if typical_rate:
                    text_output.append(f"💵 Typical 18-Hole Rate: {typical_rate}")
                if level_desc:
                    text_output.append(f"💵 Category: {level_desc}")
                text_output.append("")

        # Display pricing information or default message
        if pricing_info and pricing_info.strip():
            text_output.append(pricing_info)
        else:
            text_output.append("Contact course directly for current rates and pricing information.")
            # Try to include phone number if available
            phone = get_value(general.get('phone', {}))
            if phone and phone.strip():
                text_output.append(f"Phone: {phone}")

        # Enhanced Amenities with Available Status
        amenities = structured_data.get('amenities', {})
        if any(get_available(v) for v in amenities.values()):
            text_output.append(f"\n🏪 AMENITIES & FACILITIES")
            text_output.append("-" * 50)

            available_amenities = []
            unavailable_amenities = []

            for amenity_key, amenity_data in amenities.items():
                amenity_available = get_available(amenity_data)
                amenity_value = get_value(amenity_data)

                # Convert field names to display names
                display_names = {
                    'pro_shop': 'Pro Shop',
                    'driving_range': 'Driving Range',
                    'practice_green': 'Practice Green',
                    'short_game_practice_area': 'Short Game Practice Area',
                    'clubhouse': 'Clubhouse',
                    'locker_rooms': 'Locker Rooms',
                    'showers': 'Showers',
                    'food_beverage_options': 'Food & Beverage Options',
                    'food_beverage_options_description': 'Food & Beverage Description',
                    'beverage_cart': 'Beverage Cart',
                    'banquet_facilities': 'Banquet Facilities'
                }

                amenity_name = display_names.get(amenity_key, amenity_key.replace('_', ' ').title())

                if amenity_available:
                    if isinstance(amenity_value, list) and amenity_value:
                        available_amenities.append(f"✅ {amenity_name}:")
                        for item in amenity_value:
                            available_amenities.append(f"     • {item}")
                    elif isinstance(amenity_value, str) and amenity_value:
                        available_amenities.append(f"✅ {amenity_name}: {amenity_value}")
                    else:
                        available_amenities.append(f"✅ {amenity_name}")
                elif amenity_available is False:
                    unavailable_amenities.append(f"❌ {amenity_name}")

            # Display available amenities first
            if available_amenities:
                text_output.append("Available Amenities:")
                text_output.extend(available_amenities)
                text_output.append("")

            # Display unavailable amenities
            if unavailable_amenities:
                text_output.append("Not Available:")
                text_output.extend(unavailable_amenities)

        # Course History
        history = structured_data.get('course_history', {})
        if any(get_value(v) for v in history.values()):
            text_output.append(f"\n📜 COURSE HISTORY")
            text_output.append("-" * 50)

            architect = get_value(history.get('architect', {}))
            if architect:
                text_output.append(f"Architect: {architect}")

            year_built = get_value(history.get('year_built', {}))
            if year_built:
                text_output.append(f"Year Built: {year_built}")

            for section in ['general', 'design_features', 'notable_events']:
                items = get_value(history.get(section, {}))
                if items:
                    text_output.append(f"\n{section.replace('_', ' ').title()}:")
                    if isinstance(items, list):
                        for item in items:
                            text_output.append(f"  • {item}")
                    else:
                        text_output.append(f"  • {items}")

        # Awards & Recognition (new section)
        awards = structured_data.get('awards', {})
        if any(get_value(v) for v in awards.values()):
            text_output.append(f"\n🏆 AWARDS & RECOGNITION")
            text_output.append("-" * 50)

            for section in ['recognitions', 'rankings', 'certifications']:
                items = get_value(awards.get(section, {}))
                if items:
                    text_output.append(f"{section.replace('_', ' ').title()}:")
                    if isinstance(items, list):
                        for item in items:
                            text_output.append(f"  • {item}")
                    else:
                        text_output.append(f"  • {items}")
                    text_output.append("")

        # Events
        events = structured_data.get('amateur_professional_events', {})
        if any(get_value(v) for v in events.values()):
            text_output.append(f"\n🏆 EVENTS & TOURNAMENTS")
            text_output.append("-" * 50)

            for event_key, event_data in events.items():
                event_value = get_value(event_data)
                if event_value:
                    event_name = event_key.replace('_', ' ').title()
                    if isinstance(event_value, list) and event_value:
                        text_output.append(f"{event_name}:")
                        for item in event_value:
                            text_output.append(f"  • {item}")
                    elif isinstance(event_value, str) and event_value:
                        text_output.append(f"{event_name}: {event_value}")

        # Policies
        policies = structured_data.get('policies', {})
        course_policies = get_value(policies.get('course_policies', {}))
        if course_policies:
            text_output.append(f"\n📋 POLICIES")
            text_output.append("-" * 50)
            text_output.append(course_policies)

        # Social Media
        social = structured_data.get('social', {})
        if any(get_value(v) for v in social.values()):
            text_output.append(f"\n📱 SOCIAL MEDIA")
            text_output.append("-" * 50)

            for platform_key, platform_data in social.items():
                platform_value = get_value(platform_data)
                if platform_value:
                    platform_name = platform_key.replace('_url', '').title()
                    text_output.append(f"{platform_name}: {platform_value}")

        # Sustainability (enhanced)
        sustainability = structured_data.get('sustainability', {})
        if any(get_value(v) for v in sustainability.values()):
            text_output.append(f"\n🌱 SUSTAINABILITY & ENVIRONMENTAL PRACTICES")
            text_output.append("-" * 50)

            for section in ['general', 'certifications', 'practices']:
                items = get_value(sustainability.get(section, {}))
                if items:
                    text_output.append(f"{section.title()}:")
                    if isinstance(items, list):
                        for item in items:
                            text_output.append(f"  • {item}")
                    else:
                        text_output.append(f"  • {items}")
                    text_output.append("")

    # Add analysis metadata
    text_output.append(f"\n📊 ANALYSIS METADATA")
    text_output.append("-" * 50)

    metadata = data.get('metadata', {})
    text_output.append(f"Pages Scraped: {metadata.get('pages_scraped', 'Unknown')}")
    text_output.append(f"Analysis Timestamp: {metadata.get('analysis_timestamp', 'Unknown')}")
    text_output.append(f"AI Analysis: Enabled")

    text_output.append("\n" + "=" * 80)
    text_output.append("END OF COMPREHENSIVE REPORT")
    text_output.append("=" * 80)

    return "\n".join(text_output)


def print_json_summary(structured_data: Dict):
    """Enhanced console summary with URL display"""
    if not structured_data:
        print("❌ No structured data available")
        return

    def safe_get_value(item, default='Unknown'):
        if isinstance(item, dict):
            if 'value' in item:
                return item.get('value', default)
            else:
                return item if item else default
        elif isinstance(item, str):
            return item if item else default
        else:
            return default

    general = structured_data.get('general_info', {})
    rates = structured_data.get('rates', {})

    name = safe_get_value(general.get('name'), 'Unknown Golf Course')
    address = safe_get_value(general.get('address'), 'Unknown')
    phone = safe_get_value(general.get('phone'), 'Unknown')
    website = safe_get_value(general.get('website'), 'Unknown')
    scorecard_url = safe_get_value(general.get('scorecard_url'), '')
    rates_url = safe_get_value(general.get('rates_url'), '')
    # NEW URL EXTRACTIONS
    about_url = safe_get_value(general.get('about_url'), '')
    membership_url = safe_get_value(general.get('membership_url'), '')
    tee_time_url = safe_get_value(general.get('tee_time_url'), '')

    print(f"\n✅ Successfully analyzed: {name}")
    print(f"📍 Location: {address}")
    print(f"📞 Phone: {phone}")
    print(f"🌐 Website: {website}")

    # Prominent URL display
    if scorecard_url:
        print(f"🎯 SCORECARD URL: {scorecard_url}")
    if rates_url:
        print(f"💰 RATES URL: {rates_url}")
    # NEW URL DISPLAYS
    if about_url:
        print(f"ℹ️ ABOUT URL: {about_url}")
    if membership_url:
        print(f"👥 MEMBERSHIP URL: {membership_url}")
    if tee_time_url:
        print(f"📅 TEE TIME URL: {tee_time_url}")

    # Show pricing info if available
    pricing_info = safe_get_value(rates.get('pricing_information', {}))
    pricing_level_info = general.get('pricing_level', {})

    if pricing_info and pricing_info != 'Unknown':
        # Show first 100 characters of pricing info
        pricing_preview = pricing_info[:100] + "..." if len(pricing_info) > 100 else pricing_info
        print(f"💰 Pricing: {pricing_preview}")

    # Show pricing level
    if pricing_level_info:
        level = safe_get_value(pricing_level_info)
        typical_rate = safe_get_value(pricing_level_info.get('typical_18_hole_rate', ''), '')

        if level:
            level_ranges = {
                1: "$0-$50 (Municipal/Public)",
                2: "$51-$100 (Mid-Range)",
                3: "$101-$140 (Upper Mid-Tier)",
                4: "$141-$180 (Premium)",
                5: "$181+ (Championship)"
            }
            level_range = level_ranges.get(level, "Unknown")
            level_display = f"Level {level} - {level_range}"
            if typical_rate:
                level_display += f" | Typical Rate: {typical_rate}"
            print(f"💵 Pricing Level: {level_display}")

    print("📄 Full detailed data saved to files")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Enhanced golf course scraper with AI analysis')
    parser.add_argument('url', help='Golf course website URL to scrape')
    parser.add_argument('--max-pages', type=int, default=10, help='Maximum pages to scrape (default: 10)')
    parser.add_argument('--output-file', type=str, required=True, help='Output file path (without extension)')
    parser.add_argument('--model', type=str, default='gpt-4o-mini',
                       choices=['gpt-4o-mini', 'gpt-3.5-turbo-1106', 'gpt-3.5-turbo', 'gpt-4-turbo-preview', 'gpt-4'],
                       help='Primary OpenAI model (default: gpt-4o-mini)')
    # ADD THESE NEW ARGUMENTS:
    parser.add_argument('--force', '--overwrite', action='store_true',
                       help='Force overwrite existing output files')
    parser.add_argument('--force-reprocess', action='store_true',
                       help='Force reprocessing even if files exist (alias for --force)')
    return parser.parse_args()


async def main():
    """Enhanced main execution function with AI analysis"""
    try:
        args = parse_arguments()

        golf_course_url = args.url
        max_pages = args.max_pages
        output_file = args.output_file

        # Handle force update flags
        force_update = args.force or args.force_reprocess

        if not os.getenv('OPENAI_API_KEY'):
            print("❌ Error: Please set your OPENAI_API_KEY environment variable")
            sys.exit(1)

        scraper = golf_course_scraper(force_update=force_update)

        try:
            # CHECK FOR EXISTING FILES FIRST
            should_proceed = await scraper.check_and_handle_existing_files(output_file)
            if not should_proceed:
                print("⏭️ Scraping skipped - files already exist")
                print("💡 Use --force flag to overwrite existing files")
                sys.exit(0)

            await scraper.initialize()

            print(f'🏌️ Starting golf course scraping for: {golf_course_url}')
            print(f'🔍 Enhanced content capture with original working pricing logic (max: {max_pages})')
            print(f'🚫 Enhanced pop-up handling and content extraction')
            print(f'🤖 OpenAI analysis: ENABLED - will return clean structured JSON with comprehensive pricing')

            # Enhanced scraping
            scraped_data = await scraper.scrape_golf_course_complete(golf_course_url, max_pages)

            if not scraped_data:
                print('❌ No data scraped. Check the website URL and network connection.')
                sys.exit(1)

            print(f'✅ Scraped {len(scraped_data)} pages with enhanced pricing extraction.')

            # Check if we found pricing data
            total_pricing_elements = sum(len(page.get('priceElements', [])) for page in scraped_data)
            print(f'💰 Found {total_pricing_elements} pricing elements across all pages.')

            # Create analysis-ready data structure
            analysis_ready_data = scraper.create_analysis_ready_json(scraped_data)

            # Extract important URLs for text file
            important_urls = scraper.extract_urls_for_text_file(scraped_data)

            # Initialize enhanced results structure
            complete_results = {
                'metadata': {
                    'analysis_timestamp': datetime.now().isoformat(),
                    'scraper_version': '4.2-force-update-support',
                    'pages_scraped': len(scraped_data),
                    'url': golf_course_url,
                    'max_pages': max_pages,
                    'force_update_used': force_update,
                    'has_scorecard_page': analysis_ready_data['metadata']['has_scorecard_page'],
                    'has_rates_page': analysis_ready_data['metadata']['has_rates_page'],
                    'has_about_page': analysis_ready_data['metadata']['has_about_page'],
                    'has_membership_page': analysis_ready_data['metadata']['has_membership_page'],
                    'has_tee_time_page': analysis_ready_data['metadata']['has_tee_time_page'],
                    'total_pricing_elements_found': total_pricing_elements
                },
                'scraped_data': scraped_data,
                'analysis_ready_data': analysis_ready_data,
                'important_urls': important_urls,
                'structured_data': None,  # This will be the CLEAN OpenAI response
                'summary': {
                    'course_name': 'Unknown',
                    'total_data_points': sum(len(page.get('headings', [])) for page in scraped_data),
                    'analysis_success': False
                }
            }

            analyzer = OpenAIAnalyzer(preferred_model=args.model)

            print('🤖 Sending data to OpenAI with original working pricing extraction...')
            print('📋 OpenAI will extract detailed pricing information from discovered elements...')

            # Analyze with enhanced data structure - returns ONLY clean structured data
            structured_data = await analyzer.analyze_golf_course_data(analysis_ready_data)
            complete_results['structured_data'] = structured_data  # This is now the clean format

            if structured_data:
                try:
                    print('✅ Received comprehensive structured JSON from OpenAI')
                    print_json_summary(structured_data)

                    # Check if analysis was successful
                    complete_results['summary']['analysis_success'] = (
                        structured_data.get('general_info', {}).get('name', {}).get('value') != 'Unknown Golf Course'
                    )

                    # Extract course name for summary
                    general_info = structured_data.get('general_info', {})
                    if isinstance(general_info.get('name'), dict):
                        name = general_info['name'].get('value', 'Unknown')
                    else:
                        name = general_info.get('name', 'Unknown')
                    if name and name != 'Unknown':
                        complete_results['summary']['course_name'] = name

                except Exception as e:
                    print(f"⚠️ Error in summary display: {str(e)}")
                    print("✅ Analysis completed but summary display failed")

            # Save enhanced results
            print('\n💾 Saving enhanced results to files...')
            try:
                main_file, txt_file = await save_results(complete_results, output_file)

                try:
                    analyzer.print_usage_stats()
                except Exception as e:
                    print(f"⚠️ Error displaying usage stats: {str(e)}")

                print(f'\n🎉 Golf course analysis complete!')
                print(f'📄 ⭐ MAIN OUTPUT (Clean Structured JSON): {main_file}')
                print(f'📄 Human Readable Report: {txt_file}')
                print(f'📄 Complete Debug Data: {output_file}_complete.json')

                if important_urls.get('scorecard_url'):
                    print(f'🎯 Scorecard found at: {important_urls["scorecard_url"]}')
                if important_urls.get('rates_url'):
                    print(f'💰 Rates found at: {important_urls["rates_url"]}')
                # NEW URL DISPLAYS
                if important_urls.get('about_url'):
                    print(f'ℹ️ About found at: {important_urls["about_url"]}')
                if important_urls.get('membership_url'):
                    print(f'👥 Membership found at: {important_urls["membership_url"]}')
                if important_urls.get('tee_time_url'):
                    print(f'📅 Tee Times found at: {important_urls["tee_time_url"]}')

                sys.exit(0)

            except Exception as e:
                print(f'⚠️ Error saving results: {str(e)}')
                print('✅ Analysis completed but file saving failed')
                sys.exit(1)

        finally:
            await scraper.close()

    except Exception as error:
        print(f'❌ Main execution error: {str(error)}')
        import traceback
        print(f"📋 Full error details:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
