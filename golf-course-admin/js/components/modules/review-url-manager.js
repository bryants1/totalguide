/**
 * REVIEW URL MANAGER - Step 3: Golf Review URL Finder
 * Manages the review URL finding process for golf courses
 */

export class ReviewURLManager {
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
     * Initialize the Review URL UI
     */
    async initializeUI() {
        await this.createReviewURLUI();
        await this.populateStateDropdown();
        this.setupEventListeners();
    }

    /**
     * Create Review URL UI and inject into the section
     */
    createReviewURLUI() {
        // Find Review URL section
        let reviewURLSection = document.querySelector('.review-url-section');

        // Fallback: find by looking for sections with Review URL text
        if (!reviewURLSection) {
            const sections = document.querySelectorAll('.pipeline-section');
            for (const section of sections) {
                const h3 = section.querySelector('h3');
                if (h3 && h3.textContent.toLowerCase().includes('review url')) {
                    reviewURLSection = section;
                    break;
                }
            }
        }

        if (!reviewURLSection) {
            console.warn('Review URL section not found');
            return;
        }

        const reviewURLHTML = `
            <div class="section-header">
                <h3>🔍 Review URL Finder</h3>
                <p>Find GolfNow, GolfPass, and other review URLs for golf courses</p>
            </div>

            <div class="review-url-controls">

                <div class="url-finder-form">
                    <div class="form-group">
                        <label for="reviewUrlState">Target State (Optional)</label>
                        <select id="reviewUrlState" class="form-select">
                            <option value="">All States</option>
                            <!-- States will be populated by populateStateDropdown() -->
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="reviewUrlLimit">Limit Courses (Optional)</label>
                        <input type="number" id="reviewUrlLimit" class="form-input" placeholder="e.g., 10" min="1" max="1000">
                        <small>Leave empty to process all courses</small>
                    </div>

                    <div class="form-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="reviewUrlForce">
                            Force Update Existing URLs
                        </label>
                        <small>Check this to override existing review URLs</small>
                    </div>

                    <div class="url-finder-actions">
                        <button id="runReviewUrlBtn" class="btn-primary">
                            🚀 Find Review URLs
                        </button>
                        <button id="checkReviewUrlStatusBtn" class="btn-secondary">
                            📊 Check Status
                        </button>
                    </div>
                </div>

                <div id="reviewUrlProgress" class="url-finder-progress hidden">
                    <div class="progress-info">
                        <span id="reviewUrlProgressText">Starting review URL search...</span>
                    </div>
                    <div class="progress-bar-container">
                        <div class="progress-bar" id="reviewUrlProgressBar"></div>
                    </div>
                </div>

                <div id="reviewUrlStatus" class="status-display" style="margin: 20px 0;">
                    <div class="status-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h4 style="margin: 0;">📈 URL Finding Status</h4>
                        <span id="reviewUrlStatusLastUpdated" style="font-size: 14px; color: #666;">Last updated: Never</span>
                    </div>
                    <div id="reviewUrlStatusStats" class="status-stats" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 15px; margin-bottom: 20px;">
                        <div class="stat-card" style="text-align: center; padding: 15px; border: 1px solid #ddd; border-radius: 8px; background: white;">
                            <div class="stat-number" id="reviewUrlTotalCourses" style="font-size: 24px; font-weight: bold; color: #333;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #666;">Total Courses</div>
                        </div>
                        <div class="stat-card status-complete" style="text-align: center; padding: 15px; border: 1px solid #22c55e; border-radius: 8px; background: #f0fdf4;">
                            <div class="stat-number" id="reviewUrlProcessedCourses" style="font-size: 24px; font-weight: bold; color: #16a34a;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #166534;">With URLs</div>
                        </div>
                        <div class="stat-card status-pending" style="text-align: center; padding: 15px; border: 1px solid #f59e0b; border-radius: 8px; background: #fffbeb;">
                            <div class="stat-number" id="reviewUrlRemainingCourses" style="font-size: 24px; font-weight: bold; color: #d97706;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #92400e;">Remaining</div>
                        </div>
                        <div class="stat-card" style="text-align: center; padding: 15px; border: 1px solid #3b82f6; border-radius: 8px; background: #eff6ff;">
                            <div class="stat-number" id="reviewUrlCompletionPercent" style="font-size: 24px; font-weight: bold; color: #2563eb;">0%</div>
                            <div class="stat-label" style="font-size: 12px; color: #1d4ed8;">Complete</div>
                        </div>
                    </div>
                    <div id="reviewUrlStatusByState" class="status-by-state hidden" style="margin-top: 20px;">
                        <h5 style="margin-bottom: 10px;">Status by State:</h5>
                        <div id="reviewUrlStateStats" class="state-stats"></div>
                    </div>
                </div>

                <div id="reviewUrlResults" class="results-display hidden" style="margin: 20px 0; padding: 15px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px;">
                    <h4 style="margin-bottom: 15px;">🎯 Recent Results</h4>
                    <div id="reviewUrlResultsList" class="results-list"></div>
                </div>
            </div>
        `;

        reviewURLSection.innerHTML = reviewURLHTML;
    }

    /**
     * Populate state dropdown with available states
     */
    async populateStateDropdown() {
        try {
            this.utils.logMessage('info', '📍 Loading available states for review URLs...');

            const stateSelect = document.getElementById('reviewUrlState');
            if (!stateSelect) {
                console.warn('Review URL state select element not found');
                return;
            }

            // Try to get states from courses in database
            let availableStates = [];

            if (this.pipelineManager.database) {
                try {
                    // Get unique states from initial_course_upload table
                    const { data: statesData, error } = await this.pipelineManager.database.client
                        .from('initial_course_upload')
                        .select('state_or_region')
                        .not('state_or_region', 'is', null)
                        .not('state_or_region', 'eq', '');

                    if (!error && statesData) {
                        // Extract unique states
                        const uniqueStates = [...new Set(statesData.map(row => row.state_or_region))];
                        availableStates = uniqueStates.filter(state => state && state.trim()).sort();
                        this.utils.logMessage('success', `✅ Loaded ${availableStates.length} states from database`);
                    }
                } catch (dbError) {
                    console.warn('Database query failed:', dbError);
                }
            }

            // Fallback to common states if database query fails or returns empty
            if (availableStates.length === 0) {
                availableStates = [
                    'MA', 'CT', 'NH', 'RI', 'VT', 'ME', 'NY', 'NJ', 'PA',
                    'FL', 'CA', 'TX', 'NC', 'SC', 'VA', 'MD', 'DE', 'GA'
                ];
                this.utils.logMessage('info', '🔄 Using fallback state list');
            }

            // Generate state options HTML
            const stateOptions = availableStates
                .map(state => `<option value="${state}">${this.getStateName(state)}</option>`)
                .join('');

            // Update the select element (preserve the "All States" option)
            stateSelect.innerHTML = '<option value="">All States</option>' + stateOptions;

            this.utils.logMessage('success', `📍 Review URL state dropdown populated with ${availableStates.length} states`);

        } catch (error) {
            this.utils.logMessage('warning', `⚠️ Could not load states: ${error.message}`);
            this.utils.logMessage('info', '🔄 Using minimal fallback state list');

            // Minimal fallback for New England states
            const stateSelect = document.getElementById('reviewUrlState');
            if (stateSelect) {
                stateSelect.innerHTML = `
                    <option value="">All States</option>
                    <option value="MA">Massachusetts</option>
                    <option value="CT">Connecticut</option>
                    <option value="NH">New Hampshire</option>
                    <option value="RI">Rhode Island</option>
                    <option value="VT">Vermont</option>
                    <option value="ME">Maine</option>
                `;
            }
        }
    }

    /**
     * Convert state code to full state name
     */
    getStateName(stateCode) {
        const stateNames = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
            'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
            'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
            'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
            'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
        };

        return stateNames[stateCode] || stateCode;
    }

    setupEventListeners() {
        const runReviewUrlBtn = document.getElementById('runReviewUrlBtn');
        const checkReviewUrlStatusBtn = document.getElementById('checkReviewUrlStatusBtn');

        if (runReviewUrlBtn) {
            runReviewUrlBtn.addEventListener('click', () => this.runReviewURLFinder());
        }
        if (checkReviewUrlStatusBtn) {
            checkReviewUrlStatusBtn.addEventListener('click', () => this.checkReviewURLStatus());
        }
    }

    /**
     * Run Review URL finder for courses
     */
    async runReviewURLFinder() {
        const stateSelect = document.getElementById('reviewUrlState');
        const limitInput = document.getElementById('reviewUrlLimit');
        const forceCheckbox = document.getElementById('reviewUrlForce');
        const progressContainer = document.getElementById('reviewUrlProgress');
        const progressText = document.getElementById('reviewUrlProgressText');
        const progressBar = document.getElementById('reviewUrlProgressBar');
        const runButton = document.getElementById('runReviewUrlBtn');

        const state = stateSelect?.value || null;
        const limit = limitInput?.value ? parseInt(limitInput.value) : null;
        const force = forceCheckbox?.checked || false;

        // Get step info for logging
        const stepInfo = this.getReviewURLStepInfo();

        try {
            // Show progress
            if (progressContainer) progressContainer.classList.remove('hidden');
            if (runButton) runButton.disabled = true;
            if (progressText) progressText.textContent = `Starting ${stepInfo.name}...`;
            if (progressBar) progressBar.style.width = '10%';

            this.utils.logMessage('info', `🔍 Starting ${stepInfo.name}${state ? ` for state ${state}` : ' for all states'}`);
            if (limit) this.utils.logMessage('info', `📊 Limited to ${limit} courses`);
            this.utils.logMessage('info', `🔥 Force update: ${force}`);

            // Get script path from pipeline manager settings
            const scriptPath = this.getReviewURLScriptPath();
            if (scriptPath) {
                this.utils.logMessage('info', `📁 Using script: ${stepInfo.script}`);
            }

            // Build request body for the smart script
            const requestBody = {
                script_name: stepInfo.script,
                script_path: scriptPath,
                description: stepInfo.name
            };

            // Add state if provided
            if (state) {
                requestBody.state = state;
            }

            // Add force flag if provided
            if (force) {
                requestBody.force = force;
            }

            // Add limit if provided
            if (limit) {
                requestBody.limit = limit;
            }

            // For bulk operations (state or all courses), we don't specify a course_number
            // The smart script will handle targeting internally
            if (progressText) progressText.textContent = 'Processing courses...';
            if (progressBar) progressBar.style.width = '50%';

            // Call the generic pipeline script endpoint
            const response = await fetch('/api/run-pipeline-script', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ...requestBody,
                    course_number: 'BULK_OPERATION' // Special indicator for bulk processing
                })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`API call failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();

            if (progressText) progressText.textContent = 'URL finding completed!';
            if (progressBar) progressBar.style.width = '100%';

            if (result.success) {
                this.utils.logMessage('success', `✅ ${stepInfo.name} completed successfully!`);
                if (result.processed_courses) {
                    this.utils.logMessage('info', `📊 Processed courses: ${result.processed_courses}`);
                }
                if (result.successful_updates) {
                    this.utils.logMessage('info', `✅ Successful URL updates: ${result.successful_updates}`);
                }

                // Update status display
                await this.checkReviewURLStatus();

                // Hide progress after delay
                setTimeout(() => {
                    if (progressContainer) progressContainer.classList.add('hidden');
                }, 3000);
            } else {
                throw new Error(result.message || `${stepInfo.name} failed`);
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ ${stepInfo.name} failed: ${error.message}`);
            if (progressText) progressText.textContent = 'URL finding failed!';
            if (progressBar) progressBar.style.width = '0%';

            setTimeout(() => {
                if (progressContainer) progressContainer.classList.add('hidden');
            }, 3000);
        } finally {
            if (runButton) runButton.disabled = false;
        }
    }

    /**
     * Check Review URL finding status
     */
    async checkReviewURLStatus() {
        const stateSelect = document.getElementById('reviewUrlState');
        const state = stateSelect?.value || null;

        // Get step info for logging
        const stepInfo = this.getReviewURLStepInfo();

        try {
            this.utils.logMessage('info', `📊 Checking ${stepInfo.name} status...`);

            const response = await fetch(`/api/review-urls/status${state ? `?state=${state}` : ''}`, {
                method: 'GET'
            });

            if (!response.ok) {
                throw new Error(`Status check failed: ${response.status}`);
            }

            const result = await response.json();

            if (result.success) {
                this.updateReviewURLStatusDisplay(result);
                this.utils.logMessage('success', `✅ ${stepInfo.name} status updated`);
            } else {
                throw new Error(result.error || 'Status check failed');
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to check ${stepInfo.name} status: ${error.message}`);
        }
    }

    /**
     * Update Review URL status display
     */
    updateReviewURLStatusDisplay(statusData) {
        const stats = statusData.statistics || {};

        // Update summary stats
        const totalElement = document.getElementById('reviewUrlTotalCourses');
        const processedElement = document.getElementById('reviewUrlProcessedCourses');
        const remainingElement = document.getElementById('reviewUrlRemainingCourses');
        const percentElement = document.getElementById('reviewUrlCompletionPercent');
        const lastUpdatedElement = document.getElementById('reviewUrlStatusLastUpdated');

        if (totalElement) totalElement.textContent = stats.total_courses || 0;
        if (processedElement) processedElement.textContent = stats.courses_with_urls || 0;
        if (remainingElement) remainingElement.textContent = stats.courses_without_urls || 0;
        if (percentElement) percentElement.textContent = `${stats.completion_percentage || 0}%`;
        if (lastUpdatedElement) lastUpdatedElement.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;

        // Show URL type breakdown
        const urlTypeStats = statusData.url_type_breakdown || {};
        if (Object.keys(urlTypeStats).length > 0) {
            this.utils.logMessage('info', `📊 URL Types Found - GolfNow: ${urlTypeStats.golfnow || 0}, GolfPass: ${urlTypeStats.golfpass || 0}, TripAdvisor: ${urlTypeStats.tripadvisor || 0}`);
        }

        // Show recent activity
        const recentActivity = statusData.recent_activity || [];
        if (recentActivity.length > 0) {
            const resultsContainer = document.getElementById('reviewUrlResults');
            const resultsList = document.getElementById('reviewUrlResultsList');

            if (resultsContainer && resultsList) {
                const resultsHtml = recentActivity.map(entry => {
                    const urlsFound = [];
                    if (entry.golfnow_url) urlsFound.push('GolfNow');
                    if (entry.golfpass_url) urlsFound.push('GolfPass');
                    if (entry.tripadvisor_url) urlsFound.push('TripAdvisor');
                    if (entry.yelp_url) urlsFound.push('Yelp');
                    if (entry.google_maps_url) urlsFound.push('Google Maps');

                    return `
                        <div class="result-item" style="display: flex; justify-content: space-between; align-items: center; padding: 10px; margin: 8px 0; background: white; border: 1px solid #e5e7eb; border-radius: 6px;">
                            <div class="result-info">
                                <strong>${entry.course_number}</strong> - ${entry.course_name || 'Name not found'}
                                <br><small style="color: #666;">URLs Found: ${urlsFound.join(', ') || 'None'} | Updated: ${new Date(entry.updated_at).toLocaleString()}</small>
                            </div>
                            <div class="result-actions">
                                <button class="btn-small btn-secondary" onclick="window.pipelineManager.reviewURL.viewReviewURLData('${entry.course_number}')" style="padding: 6px 12px; background: #6b7280; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">
                                    View URLs
                                </button>
                            </div>
                        </div>
                    `;
                }).join('');

                resultsList.innerHTML = resultsHtml;
                resultsContainer.classList.remove('hidden');
            }
        }
    }

    /**
     * View Review URL data for a specific course
     */
    async viewReviewURLData(courseNumber) {
        try {
            const response = await fetch(`/api/review-urls/${courseNumber}`);

            if (!response.ok) {
                throw new Error(`Failed to get URLs: ${response.status}`);
            }

            const result = await response.json();

            if (result.success && result.data) {
                // Display the URLs in a modal or detailed view
                const data = result.data;
                const urlsFound = [];

                if (data.golfnow_url) urlsFound.push(`• GolfNow: ${data.golfnow_url}`);
                if (data.golfpass_url) urlsFound.push(`• GolfPass: ${data.golfpass_url}`);
                if (data.tripadvisor_url) urlsFound.push(`• TripAdvisor: ${data.tripadvisor_url}`);
                if (data.yelp_url) urlsFound.push(`• Yelp: ${data.yelp_url}`);
                if (data.google_maps_url) urlsFound.push(`• Google Maps: ${data.google_maps_url}`);

                const details = `
Review URLs for ${courseNumber}:
${urlsFound.length > 0 ? urlsFound.join('\n') : '• No URLs found'}

Updated: ${new Date(data.updated_at).toLocaleString()}
                `;

                alert(details); // Replace with proper modal in production
                this.utils.logMessage('info', `🔍 Viewed review URLs for ${courseNumber}`);
            } else {
                throw new Error(result.error || 'No URLs found');
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to view review URLs: ${error.message}`);
        }
    }

    /**
     * Execute Review URL finder for a single course (for pipeline integration)
     */
    async executeReviewURLFinder(courseNumber, script, force = false) {
        try {
            // Get step info from pipeline manager
            const stepInfo = this.getReviewURLStepInfo();

            this.utils.logMessage('info', `🔍 Running ${stepInfo.name} for course ${courseNumber}...`);

            const scriptPath = this.getReviewURLScriptPath();
            this.utils.logMessage('info', `🚀 Script: ${stepInfo.script || 'Unknown script'}`);
            this.utils.logMessage('info', `📁 Path: ${scriptPath || 'Script path not configured'}`);

            // Build request for single course
            const requestBody = {
                course_number: courseNumber,
                script_name: stepInfo.script,
                script_path: scriptPath,
                description: stepInfo.name
            };

            // Add force flag if provided
            if (force) {
                requestBody.force = force;
            }

            // Make API call to run review URL finder for this specific course
            const response = await fetch('/api/run-pipeline-script', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Review URL API failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();
            if (result.success) {
                this.utils.logMessage('success', `✅ ${stepInfo.name} completed successfully for course ${courseNumber}`);
                if (result.urls_found) {
                    this.utils.logMessage('info', `🔗 URLs found: ${result.urls_found.join(', ')}`);
                }
            } else {
                throw new Error(result.error || `${stepInfo.name} failed`);
            }

        } catch (error) {
            const stepInfo = this.getReviewURLStepInfo();
            this.utils.logMessage('error', `❌ ${stepInfo.name} failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get Review URL script path from pipeline manager settings
     */
    getReviewURLScriptPath() {
        // Get step 3 (Review URLs) information from pipeline manager
        const reviewURLStep = this.pipelineManager.getPipelineSteps().find(step =>
            step.name && (
                step.name.toLowerCase().includes('url') ||
                step.name.toLowerCase().includes('review')
            )
        );

        if (reviewURLStep && reviewURLStep.script) {
            return this.pipelineManager.getScriptPath(reviewURLStep.script);
        }

        // Fallback to step ID 3 if found
        if (this.pipelineManager.getScriptPath) {
            const scriptName = this.pipelineManager.getScriptName(3);
            if (scriptName) {
                return this.pipelineManager.getScriptPath(scriptName);
            }
        }

        return null;
    }

    /**
     * Get Review URL step information from pipeline manager
     */
    getReviewURLStepInfo() {
        const steps = this.pipelineManager.getPipelineSteps();

        // Try to find by name containing "url" or "review"
        let reviewURLStep = steps.find(step =>
            step.name && (
                step.name.toLowerCase().includes('url') ||
                step.name.toLowerCase().includes('review')
            )
        );

        // Fallback to step ID 3
        if (!reviewURLStep) {
            reviewURLStep = steps.find(step => step.id === 3);
        }

        return reviewURLStep || {
            id: 3,
            name: 'Golf Review URLs',
            script: 'run_get_review_urls.py'
        };
    }

    /**
     * Get course data for Review URL processing
     */
    async getCourseData(courseNumber) {
        try {
            if (!this.pipelineManager.database) {
                return null;
            }

            let { data, error } = await this.pipelineManager.database.client
                .from('primary_data')
                .select('*')
                .eq('course_number', courseNumber)
                .maybeSingle();

            if (data) return data;

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
}

// Export for use in modular structure
export default ReviewURLManager;
