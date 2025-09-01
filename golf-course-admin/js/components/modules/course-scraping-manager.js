/**
 * UPDATED COURSE SCRAPING MANAGER
 * Compatible with the new combined smart scraper script
 */

export class CourseScrapingManager {
    constructor(pipelineManager) {
        this.pipelineManager = pipelineManager;
        this.utils = pipelineManager.utils || this.createFallbackUtils();
    }

    /**
     * Create fallback utils if not provided by pipeline manager
     */
    createFallbackUtils() {
        return {
            logMessage: (type, message) => {
                const timestamp = new Date().toLocaleTimeString();
                const logMessage = `[${timestamp}] ${message}`;
                console.log(`${type.toUpperCase()}: ${logMessage}`);

                const logContent = document.getElementById('logContent');
                if (logContent) {
                    const logEntry = document.createElement('div');
                    logEntry.className = `log-entry ${type}`;
                    logEntry.innerHTML = `<span class="log-time">[${timestamp}]</span> ${message}`;
                    logContent.appendChild(logEntry);
                    logContent.scrollTop = logContent.scrollHeight;

                    const entries = logContent.querySelectorAll('.log-entry');
                    if (entries.length > 100) {
                        entries[0].remove();
                    }
                }
            }
        };
    }

    /**
     * UPDATED: Execute course website scraping using the new smart script
     */
    async executeCourseWebsiteScraping(courseNumber, script, force = false) {
        try {
            this.utils.logMessage('info', `🌐 Starting website scraping for course ${courseNumber}...`);
            if (force) {
                this.utils.logMessage('info', `🔥 Force mode enabled - will reprocess existing data`);
            }

            // Get script path from pipeline manager
            const scriptPath = this.getScrapingScriptPath();
            this.utils.logMessage('info', `📁 Using script: ${scriptPath || 'Script path not configured'}`);

            // ✅ FIXED: Pass the force parameter from the caller
            const requestBody = {
                course_number: courseNumber,
                script_name: 'run_golf_course_scraper.py',  // Your new combined script
                script_path: scriptPath,
                description: 'Course Website Scraping'
            };

            // Only add force if it's true
            if (force) {
                requestBody.force = force;
            }

            const response = await fetch('/api/run-pipeline-script', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Smart Scraper API failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();
            if (result.success) {
                this.utils.logMessage('success', `✅ Website scraping completed for course ${courseNumber}!`);
                this.utils.logMessage('info', `📊 Results: ${result.message || 'Scraping successful'}`);
                if (result.force_used) {
                    this.utils.logMessage('info', `🔥 Force update was used`);
                }

                // Check if data was uploaded to database
                const hasData = await this.checkScrapingData(courseNumber);
                if (hasData) {
                    this.utils.logMessage('success', `💾 Database updated with scraped data`);
                } else {
                    this.utils.logMessage('warning', `⚠️ Scraping completed but no database data found`);
                }
            } else {
                throw new Error(result.error || 'Website scraping failed');
            }

            return result;

        } catch (error) {
            this.utils.logMessage('error', `❌ Website scraping failed for course ${courseNumber}: ${error.message}`);
            throw error;
        }
    }

    /**
     * NEW: Check if scraping data exists in database
     */
    async checkScrapingData(courseNumber) {
        try {
            const response = await fetch(`/api/course-scraping-data/${courseNumber}`);
            return response.ok;
        } catch (error) {
            console.error('Error checking scraping data:', error);
            return false;
        }
    }

    /**
     * Get course data for website scraping
     */
    async getCourseData(courseNumber) {
        try {
            if (!this.pipelineManager.database) {
                return null;
            }

            // Try primary_data first
            let { data, error } = await this.pipelineManager.database.client
                .from('primary_data')
                .select('*')
                .eq('course_number', courseNumber)
                .maybeSingle();

            if (data) return data;

            // Fallback to initial_course_upload
            ({ data, error } = await this.pipelineManager.database.client
                .from('initial_course_upload')
                .select('*')
                .eq('course_number', courseNumber)
                .maybeSingle());

            return data;
        } catch (error) {
            console.error('Error getting course data:', error);
            return null;
        }
    }

    /**
     * Extract website URL from course data
     */
    extractWebsiteUrl(courseData) {
        // List of possible field names for website URLs
        const urlFields = [
            'website',
            'website_url',
            'url',
            'course_website',
            'web_site',
            'site_url',
            'homepage'
        ];

        for (const field of urlFields) {
            if (courseData[field]) {
                let url = courseData[field].trim();

                // Skip obviously invalid values
                if (!url || ['n/a', 'none', 'null', 'undefined', ''].includes(url.toLowerCase())) {
                    continue;
                }

                // Add protocol if missing
                if (!url.startsWith('http://') && !url.startsWith('https://')) {
                    url = 'https://' + url;
                }

                // Basic URL validation
                try {
                    new URL(url);
                    return url;
                } catch (e) {
                    continue; // Invalid URL, try next field
                }
            }
        }

        return null;
    }

    /**
     * Get scraping script path from pipeline manager
     */
    getScrapingScriptPath() {
        // Get step 9 (Course Website Scraping) information from pipeline manager
        const scrapingStep = this.pipelineManager.getPipelineSteps().find(step =>
            step.name && step.name.toLowerCase().includes('website')
        );

        if (scrapingStep && scrapingStep.script) {
            return this.pipelineManager.getScriptPath(scrapingStep.script);
        }

        // Fallback to step ID 9 if found
        if (this.pipelineManager.getScriptPath) {
            const scriptName = this.pipelineManager.getScriptName(9);
            if (scriptName) {
                return this.pipelineManager.getScriptPath(scriptName);
            }
        }

        return null;
    }

    /**
     * Get scraping step information from pipeline manager
     */
    getScrapingStepInfo() {
        const steps = this.pipelineManager.getPipelineSteps();

        // Try to find by name containing "website" or "scraping"
        let scrapingStep = steps.find(step =>
            step.name && (
                step.name.toLowerCase().includes('website') ||
                step.name.toLowerCase().includes('scraping')
            )
        );

        // Fallback to step ID 9
        if (!scrapingStep) {
            scrapingStep = steps.find(step => step.id === 9);
        }

        return scrapingStep || {
            id: 9,
            name: 'Scrape Course Websites',
            script: 'run_golf_course_scraper.py'  // ✅ Updated to new script name
        };
    }

    /**
     * Check scraping status for a course
     */
    async checkScrapingStatus(courseNumber) {
        try {
            const response = await fetch(`/api/course-scraping-data/${courseNumber}`);

            if (!response.ok) {
                if (response.status === 404) {
                    return { has_data: false, course_number: courseNumber };
                }
                throw new Error(`Status check failed: ${response.status}`);
            }

            const result = await response.json();
            return {
                has_data: true,
                course_number: courseNumber,
                data: result.data,
                last_updated: result.data?.last_updated
            };

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to check scraping status for ${courseNumber}: ${error.message}`);
            return { has_data: false, course_number: courseNumber, error: error.message };
        }
    }

    /**
     * Get scraped data for a course
     */
    async getScrapedData(courseNumber) {
        try {
            const response = await fetch(`/api/course-scraping-data/${courseNumber}`);

            if (!response.ok) {
                if (response.status === 404) {
                    return null; // No scraped data found
                }
                throw new Error(`Failed to get scraped data: ${response.status}`);
            }

            const result = await response.json();
            return result.success ? result.data : null;

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to get scraped data for ${courseNumber}: ${error.message}`);
            return null;
        }
    }

    /**
     * Bulk scraping for multiple courses (for future use)
     */
    async bulkScraping(courseNumbers, options = {}) {
        const results = [];
        const {
            maxConcurrent = 1, // Process one at a time to avoid overwhelming servers
            delayBetween = 5000 // 5 second delay between courses
        } = options;

        this.utils.logMessage('info', `🚀 Starting bulk scraping for ${courseNumbers.length} courses...`);

        for (let i = 0; i < courseNumbers.length; i++) {
            const courseNumber = courseNumbers[i];

            try {
                this.utils.logMessage('info', `📊 Processing course ${i + 1}/${courseNumbers.length}: ${courseNumber}`);

                const result = await this.executeCourseWebsiteScraping(courseNumber);
                results.push({ courseNumber, success: true, result });

                this.utils.logMessage('success', `✅ Completed ${courseNumber}`);

            } catch (error) {
                results.push({ courseNumber, success: false, error: error.message });
                this.utils.logMessage('error', `❌ Failed ${courseNumber}: ${error.message}`);
            }

            // Add delay between courses (except for the last one)
            if (i < courseNumbers.length - 1) {
                this.utils.logMessage('info', `⏸️ Waiting ${delayBetween/1000} seconds before next course...`);
                await new Promise(resolve => setTimeout(resolve, delayBetween));
            }
        }

        this.utils.logMessage('success', `🏁 Bulk scraping completed. ${results.filter(r => r.success).length}/${results.length} successful`);
        return results;
    }

    /**
     * Validate website URL before scraping
     */
    async validateWebsiteUrl(url) {
        try {
            // Basic URL validation
            new URL(url);

            // Optional: Check if URL is reachable
            const response = await fetch(url, {
                method: 'HEAD',
                mode: 'no-cors',
                signal: AbortSignal.timeout(10000) // 10 second timeout
            });

            return { valid: true, reachable: true };

        } catch (error) {
            return {
                valid: false,
                reachable: false,
                error: error.message
            };
        }
    }
}

// Export for use in modular structure
export default CourseScrapingManager;
