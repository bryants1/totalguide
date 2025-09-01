/**
 * FIXED PIPELINE MANAGER - Uses existing admin settings API
 * Now correctly calls /api/admin-settings instead of non-existent /api/pipeline-configuration
 */
import { DataImportManager } from './modules/data-import-manager.js';
import { GooglePlacesManager } from './modules/google-places-manager.js';

export class PipelineManager {
    constructor() {
        this.container = null;
        this.courses = [];
        this.isInitialized = false;
        this.selectedCourses = new Set();
        this.refreshInterval = null;
        this.database = null;
        this.nextCourseNumber = null;
        this.adminSettings = null;
        this.scriptsDirectory = null;
        this.statesDirectory = null;
        this.utils = this.createUtils();
        this.currentProfile = 'default';

        // Initialize managers
        this.dataImport = new DataImportManager(this);
        this.googlePlaces = new GooglePlacesManager(this);

        // Pipeline steps will be loaded from admin settings API
        this.PIPELINE_STEPS = [];
    }

    setDatabase(database) {
        this.database = database;
    }

    /**
     * Refresh pipeline configuration from admin settings API
     */
    async refreshPipelineConfiguration() {
        await this.loadPipelineConfiguration();
        this.utils.logMessage('success', '🔄 Pipeline configuration refreshed');
    }

    /**
     * Load pipeline configuration from admin settings API
     */
    async loadPipelineConfiguration() {
        try {
            this.utils.logMessage('info', '⚙️ Loading pipeline configuration from admin settings...');

            const response = await fetch(`/api/admin-settings?profile=${this.currentProfile}`, {
                method: 'GET'
            });

            if (!response.ok) {
                throw new Error('Failed to fetch admin settings: ' + response.status);
            }

            const result = await response.json();

            if (result.success && result.data && Array.isArray(result.data.pipeline_steps)) {
                this.PIPELINE_STEPS = result.data.pipeline_steps;
                this.adminSettings = result.data;
                this.scriptsDirectory = result.data.scripts_directory;
                this.statesDirectory = result.data.states_directory;

                this.utils.logMessage('success', 'Loaded ' + this.PIPELINE_STEPS.length + ' pipeline steps from admin settings');

                // Log the loaded steps for debugging
                this.PIPELINE_STEPS.forEach(step => {
                    const stepType = step.manual ? 'Manual' : 'Script: ' + step.script;
                    this.utils.logMessage('info', '   Step ' + step.id + ': ' + step.name + ' (' + stepType + ')');
                });

                // Update any UI elements that depend on pipeline steps
                this.updatePipelineStepsUI();

            } else {
                throw new Error('Invalid pipeline configuration data from admin settings API');
            }

        } catch (error) {
            this.utils.logMessage('warning', '⚠️ Could not load pipeline configuration from admin settings: ' + error.message);
            this.utils.logMessage('info', '🔄 Using fallback pipeline configuration');

            // Fallback to default pipeline steps if API fails
            this.PIPELINE_STEPS = this.getDefaultPipelineSteps();
            this.utils.logMessage('info', '📋 Using ' + this.PIPELINE_STEPS.length + ' default pipeline steps');
        }
    }

    /**
     * Get default pipeline steps as fallback
     */
    getDefaultPipelineSteps() {
        return [
            { id: 1, name: 'Initial Upload', manual: true },
            { id: 2, name: 'Google Places Details', script: 'run_google_places_enrichment.py' },
            { id: 3, name: 'GolfNow & GolfPass URLs', script: 'golf_url_finder.py' },
            { id: 4, name: 'GolfNow Reviews', script: 'golfnow_reviews_batch.py' },
            { id: 5, name: 'GolfPass Reviews', script: 'run_golfpass_scraper.py' },
            { id: 6, name: 'Google Reviews', manual: true },
            { id: 7, name: 'Score & Summarize Reviews', script: 'extract_text_insights.py' },
            { id: 8, name: 'Combine Review Scores', script: 'combined_scores.py' },
            { id: 9, name: 'Scrape Course Websites', script: 'database_integrated_runner.py' },
            { id: 10, name: 'Download Scorecard Images', script: 'run_scorecard_extraction.py' },
            { id: 11, name: 'Extract Tees & Ratings', script: 'scorecard_scraper.py' },
            { id: 12, name: 'Collect Visuals & Weather', script: 'golf_course_visualizer.py' },
            { id: 13, name: 'Strategic Analysis', script: 'comprehensive_golf_analysis.py' }
        ];
    }

    /**
     * Update pipeline settings when admin settings change
     */
    updateSettings(settings) {
        console.log('🔧 PipelineManager: Updating settings', settings);

        try {
            this.adminSettings = settings;

            if (settings.scripts_directory) {
                this.scriptsDirectory = settings.scripts_directory;
                console.log('📁 Scripts directory updated: ' + settings.scripts_directory);
            }

            if (settings.states_directory) {
                this.statesDirectory = settings.states_directory;
                console.log('🗂️ States directory updated: ' + settings.states_directory);
            }

            if (settings.pipeline_steps && Array.isArray(settings.pipeline_steps)) {
                this.PIPELINE_STEPS = settings.pipeline_steps;
                console.log('📋 Pipeline steps updated: ' + settings.pipeline_steps.length + ' steps');
                this.updatePipelineStepsUI();
            }

            if (settings.profile_name) {
                this.currentProfile = settings.profile_name;
                console.log('👤 Profile updated: ' + settings.profile_name);
            }

            console.log('✅ PipelineManager: Settings updated successfully');

        } catch (error) {
            console.error('❌ PipelineManager: Error updating settings:', error);
            throw error;
        }
    }

    updatePipelineStepsUI() {
        try {
            const pipelineStageSelect = document.getElementById('pipelineStage');
            if (pipelineStageSelect && this.PIPELINE_STEPS.length > 0) {
                const currentValue = pipelineStageSelect.value;

                const optionsHtml = '<option value="full">Full Pipeline (All Steps)</option>' +
                    this.PIPELINE_STEPS.map(step => {
                        const stepType = step.manual ? '(Manual)' : '(' + step.script + ')';
                        return '<option value="step' + step.id + '">Step ' + step.id + ': ' + step.name + ' ' + stepType + '</option>';
                    }).join('');

                pipelineStageSelect.innerHTML = optionsHtml;

                if (currentValue) {
                    pipelineStageSelect.value = currentValue;
                }

                console.log('🔄 Pipeline stage dropdown updated');
            }

            const pipelineTableBody = document.getElementById('pipelineTableBody');
            if (pipelineTableBody && this.courses && this.courses.length > 0) {
                this.populateTable();
                console.log('📊 Pipeline status table refreshed');
            }

        } catch (error) {
            console.warn('⚠️ Error updating pipeline UI:', error);
        }
    }

    // Getters for admin settings
    getAdminSettings() {
        return this.adminSettings || null;
    }

    getScriptsDirectory() {
        return this.scriptsDirectory || null;
    }

    getStatesDirectory() {
        return this.statesDirectory || null;
    }

    getPipelineSteps() {
        return this.PIPELINE_STEPS || [];
    }

    getScriptName(stepId) {
        const step = this.PIPELINE_STEPS.find(s => s.id === stepId);
        return step ? step.script : null;
    }

    getScriptPath(scriptName) {
        if (!scriptName || !this.scriptsDirectory) {
            return null;
        }
        return this.scriptsDirectory + '/' + scriptName;
    }

    async executeScript(stepId, courseNumber, options = {}) {
        const stepInfo = this.PIPELINE_STEPS.find(s => s.id === stepId);
        if (!stepInfo || !stepInfo.script) {
            throw new Error('No script defined for step ' + stepId);
        }

        const scriptPath = this.getScriptPath(stepInfo.script);

        const response = await fetch('/api/run-pipeline-script', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                course_number: courseNumber,
                script_name: stepInfo.script,
                script_path: scriptPath,
                states_directory: this.statesDirectory,
                step_id: stepId,
                description: stepInfo.name,
                ...options
            })
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error('Script execution failed: ' + response.status + ' - ' + errorText);
        }

        return response.json();
    }

    async initialize(container) {
        if (this.isInitialized) return;

        this.container = container;

        // Load pipeline configuration from admin settings API first
        await this.loadPipelineConfiguration();

        await this.loadCourses();
        await this.getNextCourseNumber();

        // Create the basic interface first
        this.createPipelineInterface();

        // Setup managers (which will populate the UI and setup event listeners)
        this.dataImport.setupEventListeners();
        await this.googlePlaces.initializeUI();

        // Setup other event listeners
        this.setupOtherEventListeners();

        this.isInitialized = true;
        await this.dataImport.getNextCourseNumber();

        this.startAutoRefresh();
    }

    createUtils() {
        return {
            logMessage: (type, message) => {
                const timestamp = new Date().toLocaleTimeString();
                const logMessage = '[' + timestamp + '] ' + message;
                console.log(type.toUpperCase() + ': ' + logMessage);

                const logContent = document.getElementById('logContent');
                if (logContent) {
                    const logEntry = document.createElement('div');
                    logEntry.className = 'log-entry ' + type;
                    logEntry.innerHTML = '<span class="log-time">[' + timestamp + ']</span> ' + message;
                    logContent.appendChild(logEntry);
                    logContent.scrollTop = logContent.scrollHeight;

                    const entries = logContent.querySelectorAll('.log-entry');
                    if (entries.length > 100) {
                        entries[0].remove();
                    }
                }
            },

            setLogContainer: (container) => {
                this.logContainer = container;
            }
        };
    }

    async getNextCourseNumber() {
        if (this.dataImport) {
            await this.dataImport.getNextCourseNumber();
            this.nextCourseNumber = this.dataImport.nextCourseNumber;
        }
    }

    async loadCourses() {
        try {
            if (this.database) {
                this.courses = await this.database.getCourses();

                const pipelineStatuses = await this.database.getAllPipelineStatuses();
                const statusMap = new Map(pipelineStatuses.map(s => [s.course_number, s]));

                this.courses = this.courses.map(course => ({
                    ...course,
                    pipeline_status: statusMap.get(course.course_number) || {
                        course_number: course.course_number,
                        current_step: 1,
                        progress_percent: 0,
                        status: 'pending',
                        last_updated: new Date().toISOString(),
                        step_details: {},
                        error_message: null
                    }
                }));
            }
        } catch (error) {
            console.error('Error loading courses:', error);
            this.courses = [];
        }
    }

    createPipelineInterface() {
        if (!this.container) return;

        this.container.innerHTML =
            '<div class="pipeline-container">' +
                this.generateDataImportSection() +
                this.generateGooglePlacesSection() +
                this.generatePipelineStatusSection() +
                this.generateSingleRunnerSection() +
            '</div>';

        this.populateTable();
        this.populateCourseSelect();
    }

    generateDataImportSection() {
        return
            '<!-- Excel Import Section -->' +
            '<div class="pipeline-section excel-import-section">' +
                '<!-- Will be populated by DataImportManager -->' +
            '</div>' +

            '<!-- CSV Import Section -->' +
            '<div class="pipeline-section csv-import-section">' +
                '<!-- Will be populated by DataImportManager -->' +
            '</div>';
    }

    generateGooglePlacesSection() {
        return
            '<!-- Google Places Section -->' +
            '<div class="pipeline-section google-places-section">' +
                '<!-- Will be populated by GooglePlacesManager -->' +
            '</div>';
    }

    generatePipelineStatusSection() {
        return
            '<!-- Pipeline Status Section -->' +
            '<div class="pipeline-section">' +
                '<div class="section-header">' +
                    '<h3>🔄 Pipeline Status</h3>' +
                    '<div class="header-actions">' +
                        '<button id="refreshStatusBtn" class="btn-secondary">🔄 Refresh</button>' +
                        '<select id="statusFilter" class="form-select">' +
                            '<option value="">All Statuses</option>' +
                            '<option value="pending">Pending</option>' +
                            '<option value="running">Running</option>' +
                            '<option value="complete">Complete</option>' +
                            '<option value="error">Error</option>' +
                        '</select>' +
                    '</div>' +
                '</div>' +

                '<!-- Statistics Cards -->' +
                '<div class="pipeline-stats">' +
                    '<div class="stat-card">' +
                        '<div class="stat-number" id="totalCourses">0</div>' +
                        '<div class="stat-label">Total Courses</div>' +
                    '</div>' +
                    '<div class="stat-card status-complete">' +
                        '<div class="stat-number" id="completeCourses">0</div>' +
                        '<div class="stat-label">Complete</div>' +
                    '</div>' +
                    '<div class="stat-card status-running">' +
                        '<div class="stat-number" id="runningCourses">0</div>' +
                        '<div class="stat-label">Running</div>' +
                    '</div>' +
                    '<div class="stat-card status-error">' +
                        '<div class="stat-number" id="errorCourses">0</div>' +
                        '<div class="stat-label">Errors</div>' +
                    '</div>' +
                    '<div class="stat-card status-pending">' +
                        '<div class="stat-number" id="pendingCourses">0</div>' +
                        '<div class="stat-label">Pending</div>' +
                    '</div>' +
                '</div>' +

                '<!-- Bulk Actions -->' +
                '<div class="bulk-actions">' +
                    '<div class="bulk-selection">' +
                        '<label class="checkbox-label">' +
                            '<input type="checkbox" id="selectAllCourses">' +
                            'Select All (<span id="selectedCount">0</span>)' +
                        '</label>' +
                        '<span class="bulk-info" id="bulkInfo"></span>' +
                    '</div>' +
                    '<div class="bulk-controls">' +
                        '<button id="runSelectedBtn" class="btn-primary" disabled>▶️ Run Selected</button>' +
                        '<button id="retryErrorsBtn" class="btn-warning">🔄 Retry Errors</button>' +
                        '<button id="clearCompletedBtn" class="btn-secondary">🗑️ Clear Completed</button>' +
                    '</div>' +
                '</div>' +

                '<!-- Pipeline Status Table -->' +
                '<div class="pipeline-table-container">' +
                    '<table class="pipeline-table">' +
                        '<thead>' +
                            '<tr>' +
                                '<th><input type="checkbox" id="selectAllCheckbox"></th>' +
                                '<th>Course</th>' +
                                '<th>Status</th>' +
                                '<th>Step</th>' +
                                '<th>Progress</th>' +
                                '<th>Last Updated</th>' +
                                '<th>Actions</th>' +
                            '</tr>' +
                        '</thead>' +
                        '<tbody id="pipelineTableBody">' +
                            '<tr class="loading-row">' +
                                '<td colspan="7">Loading courses...</td>' +
                            '</tr>' +
                        '</tbody>' +
                    '</table>' +
                '</div>' +
            '</div>';
    }

    generateSingleRunnerSection() {
        return
            '<!-- Single Course Runner Section -->' +
            '<div class="pipeline-section">' +
                '<div class="section-header">' +
                    '<h3>🎯 Single Course Runner</h3>' +
                    '<p>Run pipeline for individual courses with detailed logging</p>' +
                '</div>' +

                '<div class="single-runner-controls">' +
                    '<div class="runner-form">' +
                        '<select id="pipelineCourseSelect" class="form-select">' +
                            '<option value="">Select Course...</option>' +
                        '</select>' +
                        '<select id="pipelineStage" class="form-select">' +
                            '<option value="full">Full Pipeline (All Steps)</option>' +
                        '</select>' +
                        '<label class="checkbox-label">' +
                            '<input type="checkbox" id="forceUpdate">' +
                            'Force Update' +
                        '</label>' +
                        '<button id="runSingleBtn" class="btn-primary">▶️ Run Pipeline</button>' +
                    '</div>' +

                    '<div class="runner-status" id="runnerStatus" style="display: none;">' +
                        '<div class="status-header">' +
                            '<span id="currentCourse">Course: None</span>' +
                            '<span id="currentStage">Stage: None</span>' +
                            '<span id="currentProgress">0%</span>' +
                        '</div>' +
                        '<div class="progress-bar-container">' +
                            '<div class="progress-bar" id="runnerProgressFill"></div>' +
                        '</div>' +
                    '</div>' +

                    '<div class="execution-log">' +
                        '<div class="log-header">' +
                            '<h4>📋 Execution Log</h4>' +
                            '<button id="clearLogBtn" class="btn-secondary">Clear</button>' +
                        '</div>' +
                        '<div class="log-content" id="logContent">' +
                            '<div class="log-entry info">Ready to run pipeline...</div>' +
                        '</div>' +
                    '</div>' +
                '</div>' +
            '</div>';
    }

    // Delegate methods to managers for backward compatibility
    async analyzeExcelData() {
        return this.dataImport.analyzeExcelData();
    }

    async importExcelData() {
        return this.dataImport.importExcelData();
    }

    async addManualCourse() {
        return this.dataImport.addManualCourse();
    }

    async clearAllData() {
        return this.dataImport.clearAllData();
    }

    async runGooglePlacesEnrichment() {
        return this.googlePlaces.runGooglePlacesEnrichment();
    }

    async checkGooglePlacesStatus() {
        return this.googlePlaces.checkGooglePlacesStatus();
    }

    async viewGooglePlacesData(courseNumber) {
        return this.googlePlaces.viewGooglePlacesData(courseNumber);
    }

    // Pipeline execution methods
    async executeInitialUpload(courseNumber) {
        this.logMessage('info', '📋 Step 1 (Initial Upload) is a manual process');
        this.logMessage('info', 'ℹ️ Please ensure course ' + courseNumber + ' data is in initial_course_upload table');
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    async executeGooglePlacesDetails(courseNumber, script) {
        // Delegate to GooglePlacesManager
        return this.googlePlaces.executeGooglePlacesDetails(courseNumber, script);
    }

    async executeGolfUrlFinder(courseNumber, script) {
        try {
            this.logMessage('info', '🔍 Finding Golf URLs for course ' + courseNumber + '...');
            const result = await this.executeScript(3, courseNumber, { description: 'Golf URL Finder' });
            this.logMessage('success', '✅ Golf URLs found: ' + (result.urls_found || 'unknown count'));
        } catch (error) {
            this.logMessage('error', '❌ Golf URL Finder failed: ' + error.message);
            throw error;
        }
    }

    async executeGolfNowReviews(courseNumber, script) {
        try {
            this.logMessage('info', '📊 Scraping GolfNow reviews for course ' + courseNumber + '...');
            const result = await this.executeScript(4, courseNumber, { description: 'GolfNow Reviews' });
            this.logMessage('success', '✅ GolfNow reviews scraped: ' + (result.reviews_count || 'unknown count'));
        } catch (error) {
            this.logMessage('error', '❌ GolfNow Reviews failed: ' + error.message);
            throw error;
        }
    }

    async executeGolfPassReviews(courseNumber, script) {
        try {
            this.logMessage('info', '📊 Scraping GolfPass reviews for course ' + courseNumber + '...');
            const result = await this.executeScript(5, courseNumber, { description: 'GolfPass Reviews' });
            this.logMessage('success', '✅ GolfPass reviews scraped: ' + (result.reviews_count || 'unknown count'));
        } catch (error) {
            this.logMessage('error', '❌ GolfPass Reviews failed: ' + error.message);
            throw error;
        }
    }

    async executeGoogleReviews(courseNumber) {
        this.logMessage('info', '📋 Step 6 (Google Reviews) is a manual process');
        this.logMessage('info', 'ℹ️ Please manually collect Google Reviews for course ' + courseNumber);
        await new Promise(resolve => setTimeout(resolve, 1000));
    }

    async executeReviewScoring(courseNumber, script) {
        try {
            this.logMessage('info', '🔢 Scoring and analyzing review text for course ' + courseNumber + '...');
            const result = await this.executeScript(7, courseNumber, { description: 'Review Scoring' });
            this.logMessage('success', '✅ Review text insights extracted successfully');
        } catch (error) {
            this.logMessage('error', '❌ Review Scoring failed: ' + error.message);
            throw error;
        }
    }

    async executeCombineScores(courseNumber, script) {
        try {
            this.logMessage('info', '📈 Combining review scores from all sources for course ' + courseNumber + '...');
            const result = await this.executeScript(8, courseNumber, { description: 'Combine Scores' });
            this.logMessage('success', '✅ Review scores combined successfully');
        } catch (error) {
            this.logMessage('error', '❌ Combine Scores failed: ' + error.message);
            throw error;
        }
    }

    async executeCourseWebsiteScraping(courseNumber, script) {
        try {
            this.logMessage('info', '🌐 Starting website scraping for course ' + courseNumber + '...');

            const courseData = await this.getCourseData(courseNumber);
            if (!courseData) {
                throw new Error('Course ' + courseNumber + ' not found');
            }

            const websiteUrl = this.extractWebsiteUrl(courseData);
            if (!websiteUrl) {
                throw new Error('No website URL found for course ' + courseNumber);
            }

            this.logMessage('info', '🌐 Found website: ' + websiteUrl);

            const response = await fetch('/api/run-website-scraper', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    course_number: courseNumber,
                    website_url: websiteUrl,
                    script_path: this.getScriptPath(this.getScriptName(9)),
                    max_pages: 10
                })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error('Website Scraper failed: ' + response.status + ' - ' + errorText);
            }

            const result = await response.json();
            if (result.success) {
                this.logMessage('success', '✅ Website scraping completed!');
                this.logMessage('info', '📁 Files created: ' + (result.files_created ? result.files_created.join(', ') : 'unknown'));
            } else {
                throw new Error(result.error || 'Website scraping failed');
            }

        } catch (error) {
            this.logMessage('error', '❌ Website scraping failed: ' + error.message);
            throw error;
        }
    }

    async executeScorecardImages(courseNumber, script) {
        try {
            this.logMessage('info', '📄 Downloading and processing scorecard images for course ' + courseNumber + '...');
            const result = await this.executeScript(10, courseNumber, { description: 'Scorecard Images' });
            this.logMessage('success', '✅ Scorecard images processed: ' + (result.images_processed || 'unknown count'));
        } catch (error) {
            this.logMessage('error', '❌ Scorecard Images failed: ' + error.message);
            throw error;
        }
    }

    async executeTeesRatings(courseNumber, script) {
        try {
            this.logMessage('info', '🏌️ Extracting tee and rating information for course ' + courseNumber + '...');
            const result = await this.executeScript(11, courseNumber, { description: 'Tees & Ratings' });
            this.logMessage('success', '✅ Tee and rating data extracted successfully');
        } catch (error) {
            this.logMessage('error', '❌ Extract Tees & Ratings failed: ' + error.message);
            throw error;
        }
    }

    async executeVisualsWeather(courseNumber, script) {
        try {
            this.logMessage('info', '🌤️ Collecting course visuals and weather data for course ' + courseNumber + '...');
            const result = await this.executeScript(12, courseNumber, { description: 'Visuals & Weather' });
            this.logMessage('success', '✅ Visuals and weather data collected successfully');
        } catch (error) {
            this.logMessage('error', '❌ Collect Visuals & Weather failed: ' + error.message);
            throw error;
        }
    }

    async executeStrategicAnalysis(courseNumber, script) {
        try {
            this.logMessage('info', '📊 Running comprehensive strategic analysis for course ' + courseNumber + '...');
            const result = await this.executeScript(13, courseNumber, { description: 'Strategic Analysis' });
            this.logMessage('success', '✅ Strategic analysis completed successfully');
        } catch (error) {
            this.logMessage('error', '❌ Strategic Analysis failed: ' + error.message);
            throw error;
        }
    }

    // Helper methods
    async getCourseData(courseNumber) {
        try {
            let data, error;
            const result = await this.database.client
                .from('primary_data')
                .select('*')
                .eq('course_number', courseNumber)
                .maybeSingle();

            data = result.data;
            error = result.error;

            if (data) return data;

            const result2 = await this.database.client
                .from('initial_course_upload')
                .select('*')
                .eq('course_number', courseNumber)
                .maybeSingle();

            return result2.data;
        } catch (error) {
            console.error('Error getting course data:', error);
            return null;
        }
    }

    extractWebsiteUrl(courseData) {
        const urlFields = ['website', 'website_url', 'url', 'course_website', 'web_site'];

        for (const field of urlFields) {
            if (courseData[field]) {
                let url = courseData[field].trim();
                if (url && !['n/a', 'none', 'null', ''].includes(url.toLowerCase())) {
                    if (!url.startsWith('http://') && !url.startsWith('https://')) {
                        url = 'https://' + url;
                    }
                    return url;
                }
            }
        }
        return null;
    }

    setupOtherEventListeners() {
        // Pipeline Status Event Listeners
        const refreshStatusBtn = document.getElementById('refreshStatusBtn');
        const statusFilter = document.getElementById('statusFilter');
        const selectAllCourses = document.getElementById('selectAllCourses');
        const runSelectedBtn = document.getElementById('runSelectedBtn');
        const retryErrorsBtn = document.getElementById('retryErrorsBtn');
        const clearCompletedBtn = document.getElementById('clearCompletedBtn');

        if (refreshStatusBtn) {
            refreshStatusBtn.addEventListener('click', () => this.refreshStatus());
        }
        if (statusFilter) {
            statusFilter.addEventListener('change', (e) => this.filterByStatus(e.target.value));
        }
        if (selectAllCourses) {
            selectAllCourses.addEventListener('change', (e) => this.toggleSelectAll(e.target.checked));
        }
        if (runSelectedBtn) {
            runSelectedBtn.addEventListener('click', () => this.runSelectedCourses());
        }
        if (retryErrorsBtn) {
            retryErrorsBtn.addEventListener('click', () => this.retryErrorCourses());
        }
        if (clearCompletedBtn) {
            clearCompletedBtn.addEventListener('click', () => this.clearCompletedCourses());
        }

        // Single Runner Event Listeners
        const runSingleBtn = document.getElementById('runSingleBtn');
        const clearLogBtn = document.getElementById('clearLogBtn');

        if (runSingleBtn) {
            runSingleBtn.addEventListener('click', () => this.runSingleCourse());
        }
        if (clearLogBtn) {
            clearLogBtn.addEventListener('click', () => this.clearLog());
        }
    }

    // Pipeline Status Functionality
    async refreshStatus() {
        try {
            await this.loadCourses();
            this.populateTable();
            this.populateCourseSelect();
            this.updateStatistics();
            this.logMessage('info', 'Status refreshed');
        } catch (error) {
            console.error('Error refreshing status:', error);
            this.logMessage('error', 'Failed to refresh status');
        }
    }

    populateTable() {
        const tableBody = document.getElementById('pipelineTableBody');
        if (!tableBody) return;

        if (this.courses.length === 0) {
            tableBody.innerHTML =
                '<tr class="empty-row">' +
                    '<td colspan="7">No courses found. Import some courses to get started.</td>' +
                '</tr>';
            return;
        }

        const TOTAL_STEPS = this.PIPELINE_STEPS.length;

        tableBody.innerHTML = this.courses.map(course => {
            const status = course.pipeline_status || {};
            const currentStep = Math.min(status.current_step || 1, TOTAL_STEPS);
            const stepInfo = this.PIPELINE_STEPS.find(step => step.id === currentStep) || this.PIPELINE_STEPS[0];
            const stepName = stepInfo ? stepInfo.name : 'Unknown Step';
            const progressPercent = Math.min(Math.max(status.progress_percent || 0, 0), 100);

            const stepType = stepInfo && stepInfo.script ?
                '<small>(' + stepInfo.script + ')</small>' :
                (stepInfo && stepInfo.manual ? '<small>(Manual)</small>' : '');

            const retryButton = status.status === 'error' ?
                '<button class="btn-small btn-warning" onclick="pipelineManager.retryCourse(\'' + course.course_number + '\')">🔄 Retry</button>' : '';

            return
                '<tr class="pipeline-row status-' + (status.status || 'pending') + '" data-course="' + course.course_number + '">' +
                    '<td>' +
                        '<input type="checkbox" class="course-checkbox" value="' + course.course_number + '">' +
                    '</td>' +
                    '<td>' +
                        '<div class="course-info">' +
                            '<div class="course-number">' + course.course_number + '</div>' +
                            '<div class="course-name">' + (course.course_name || 'N/A') + '</div>' +
                        '</div>' +
                    '</td>' +
                    '<td>' +
                        '<span class="status-indicator status-' + (status.status || 'pending') + '">' +
                            (status.status || 'pending').toUpperCase() +
                        '</span>' +
                    '</td>' +
                    '<td>' +
                        '<div class="step-info">' +
                            '<span class="step-number">' + currentStep + '/' + TOTAL_STEPS + '</span>' +
                            '<span class="step-name">' + stepName + '</span>' +
                            stepType +
                        '</div>' +
                    '</td>' +
                    '<td>' +
                        '<div class="progress-container">' +
                            '<div class="progress-bar-bg">' +
                                '<div class="progress-bar-fill" style="width: ' + progressPercent + '%"></div>' +
                            '</div>' +
                            '<span class="progress-text">' + progressPercent + '%</span>' +
                        '</div>' +
                    '</td>' +
                    '<td>' +
                        '<span class="last-updated">' + this.formatDate(status.last_updated) + '</span>' +
                        (status.error_message ? '<div class="error-message">' + status.error_message + '</div>' : '') +
                    '</td>' +
                    '<td>' +
                        '<div class="action-buttons">' +
                            '<button class="btn-small btn-primary" onclick="pipelineManager.runSingleCourseById(\'' + course.course_number + '\')">▶️ Run</button>' +
                            retryButton +
                        '</div>' +
                    '</td>' +
                '</tr>';
        }).join('');

        this.setupTableCheckboxes();
        this.updateStatistics();
    }

    populateCourseSelect() {
        const courseSelect = document.getElementById('pipelineCourseSelect');
        if (!courseSelect) return;

        courseSelect.innerHTML = '<option value="">Select Course...</option>' +
            this.courses.map(course =>
                '<option value="' + course.course_number + '">' + course.course_number + ' - ' + (course.course_name || 'N/A') + '</option>'
            ).join('');
    }

    setupTableCheckboxes() {
        const checkboxes = document.querySelectorAll('.course-checkbox');
        const selectAllCheckbox = document.getElementById('selectAllCheckbox');

        checkboxes.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    this.selectedCourses.add(checkbox.value);
                } else {
                    this.selectedCourses.delete(checkbox.value);
                }
                this.updateSelectionUI();
            });
        });

        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', (e) => {
                this.toggleSelectAll(e.target.checked);
            });
        }
    }

    toggleSelectAll(checked) {
        const checkboxes = document.querySelectorAll('.course-checkbox');

        checkboxes.forEach(checkbox => {
            checkbox.checked = checked;
            if (checked) {
                this.selectedCourses.add(checkbox.value);
            } else {
                this.selectedCourses.delete(checkbox.value);
            }
        });

        this.updateSelectionUI();
    }

    updateSelectionUI() {
        const selectedCount = document.getElementById('selectedCount');
        const bulkInfo = document.getElementById('bulkInfo');
        const runSelectedBtn = document.getElementById('runSelectedBtn');
        const selectAllCheckbox = document.getElementById('selectAllCheckbox');

        const count = this.selectedCourses.size;

        if (selectedCount) selectedCount.textContent = count;
        if (bulkInfo) bulkInfo.textContent = count > 0 ? count + ' courses selected' : '';
        if (runSelectedBtn) runSelectedBtn.disabled = count === 0;

        const totalCheckboxes = document.querySelectorAll('.course-checkbox').length;
        if (selectAllCheckbox) {
            if (count === 0) {
                selectAllCheckbox.indeterminate = false;
                selectAllCheckbox.checked = false;
            } else if (count === totalCheckboxes) {
                selectAllCheckbox.indeterminate = false;
                selectAllCheckbox.checked = true;
            } else {
                selectAllCheckbox.indeterminate = true;
            }
        }
    }

    updateStatistics() {
        const stats = {
            total: this.courses.length,
            complete: 0,
            running: 0,
            error: 0,
            pending: 0
        };

        this.courses.forEach(course => {
            const status = course.pipeline_status ? course.pipeline_status.status : 'pending';
            if (status && stats.hasOwnProperty(status)) {
                stats[status] = stats[status] + 1;
            } else {
                stats.pending = stats.pending + 1;
            }
        });

        const totalElement = document.getElementById('totalCourses');
        const completeElement = document.getElementById('completeCourses');
        const runningElement = document.getElementById('runningCourses');
        const errorElement = document.getElementById('errorCourses');
        const pendingElement = document.getElementById('pendingCourses');

        if (totalElement) totalElement.textContent = stats.total;
        if (completeElement) completeElement.textContent = stats.complete;
        if (runningElement) runningElement.textContent = stats.running;
        if (errorElement) errorElement.textContent = stats.error;
        if (pendingElement) pendingElement.textContent = stats.pending;
    }

    filterByStatus(status) {
        const rows = document.querySelectorAll('.pipeline-row');

        rows.forEach(row => {
            if (!status || row.classList.contains('status-' + status)) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        });
    }

    // Pipeline execution methods
    async runSelectedCourses() {
        const selectedCourseNumbers = Array.from(this.selectedCourses);
        if (selectedCourseNumbers.length === 0) return;

        this.logMessage('info', 'Starting pipeline for ' + selectedCourseNumbers.length + ' courses...');

        for (const courseNumber of selectedCourseNumbers) {
            try {
                await this.runCourseByNumber(courseNumber);
                this.logMessage('success', 'Completed pipeline for course: ' + courseNumber);
            } catch (error) {
                this.logMessage('error', 'Failed pipeline for course ' + courseNumber + ': ' + error.message);
            }
        }

        await this.refreshStatus();
    }

    async retryErrorCourses() {
        const errorCourses = this.courses.filter(c => c.pipeline_status && c.pipeline_status.status === 'error');

        if (errorCourses.length === 0) {
            this.logMessage('info', 'No courses with errors found');
            return;
        }

        this.logMessage('info', 'Retrying ' + errorCourses.length + ' error courses...');

        for (const course of errorCourses) {
            try {
                await this.runCourseByNumber(course.course_number);
                this.logMessage('success', 'Retry successful for course: ' + course.course_number);
            } catch (error) {
                this.logMessage('error', 'Retry failed for course ' + course.course_number + ': ' + error.message);
            }
        }

        await this.refreshStatus();
    }

    async clearCompletedCourses() {
        const completedCourses = this.courses.filter(c => c.pipeline_status && c.pipeline_status.status === 'complete');

        if (completedCourses.length === 0) {
            this.logMessage('info', 'No completed courses found');
            return;
        }

        try {
            for (const course of completedCourses) {
                if (this.database) {
                    await this.database.updatePipelineStatus(course.course_number, {
                        status: 'pending',
                        current_step: 1,
                        progress_percent: 0,
                        error_message: null
                    });
                }
            }

            this.logMessage('success', 'Reset ' + completedCourses.length + ' completed courses to pending');
            await this.refreshStatus();

        } catch (error) {
            this.logMessage('error', 'Failed to clear completed courses: ' + error.message);
        }
    }

    async runSingleCourse() {
        const courseSelect = document.getElementById('pipelineCourseSelect');
        const pipelineStage = document.getElementById('pipelineStage');
        const forceUpdate = document.getElementById('forceUpdate');

        const courseNumber = courseSelect ? courseSelect.value : '';
        const stage = pipelineStage ? pipelineStage.value : 'full';
        const force = forceUpdate ? forceUpdate.checked : false;

        if (!courseNumber) {
            this.logMessage('error', 'Please select a course');
            return;
        }

        try {
            await this.runCourseByNumber(courseNumber, stage, force);
            this.logMessage('success', 'Pipeline completed for course: ' + courseNumber);
            await this.refreshStatus();
        } catch (error) {
            this.logMessage('error', 'Pipeline failed for course ' + courseNumber + ': ' + error.message);
        }
    }

    async runSingleCourseById(courseNumber) {
        try {
            await this.runCourseByNumber(courseNumber);
            this.logMessage('success', 'Pipeline completed for course: ' + courseNumber);
            await this.refreshStatus();
        } catch (error) {
            this.logMessage('error', 'Pipeline failed for course ' + courseNumber + ': ' + error.message);
        }
    }

    async retryCourse(courseNumber) {
        try {
            // Clear error status first
            if (this.database) {
                await this.database.updatePipelineStatus(courseNumber, {
                    status: 'pending',
                    error_message: null
                });
            }

            await this.runCourseByNumber(courseNumber);
            this.logMessage('success', 'Retry successful for course: ' + courseNumber);
            await this.refreshStatus();
        } catch (error) {
            this.logMessage('error', 'Retry failed for course ' + courseNumber + ': ' + error.message);
        }
    }

    async runCourseByNumber(courseNumber, stage = 'full', force = false) {
        if (!this.database) {
            throw new Error('Database not connected');
        }

        // Update status to running
        await this.database.updatePipelineStatus(courseNumber, {
            status: 'running',
            current_step: 1,
            progress_percent: 0
        });

        const totalSteps = stage === 'full' ? this.PIPELINE_STEPS.length : 1;
        const startStep = stage === 'full' ? 1 : parseInt(stage.replace('step', '')) || 1;

        try {
            for (let step = startStep; step <= (stage === 'full' ? totalSteps : startStep); step++) {
                const stepInfo = this.PIPELINE_STEPS.find(s => s.id === step) || this.PIPELINE_STEPS[step - 1];
                const stepName = stepInfo ? stepInfo.name : 'Step ' + step;

                // Update current step
                await this.database.updatePipelineStatus(courseNumber, {
                    current_step: step,
                    progress_percent: Math.round(((step - 1) / totalSteps) * 100),
                    step_details: {
                        current_step: stepName,
                        script: stepInfo ? stepInfo.script : null,
                        manual: stepInfo ? stepInfo.manual : false
                    }
                });

                // Execute the actual step
                await this.executeStep(courseNumber, step, stepName, stepInfo);

                // Update progress after step completion
                await this.database.updatePipelineStatus(courseNumber, {
                    progress_percent: Math.round((step / totalSteps) * 100)
                });
            }

            // Mark as complete
            await this.database.updatePipelineStatus(courseNumber, {
                status: 'complete',
                progress_percent: 100,
                current_step: totalSteps,
                step_details: { completed_at: new Date().toISOString() }
            });

        } catch (error) {
            // Mark as error
            await this.database.setPipelineError(courseNumber, error.message);
            throw error;
        }
    }

    async executeStep(courseNumber, step, stepName, stepInfo) {
        this.logMessage('info', 'Course ' + courseNumber + ': Executing ' + stepName + ' (' + step + '/13)...');

        // Log script information
        if (stepInfo && stepInfo.script) {
            this.logMessage('info', 'Running script: ' + stepInfo.script);
        } else if (stepInfo && stepInfo.manual) {
            this.logMessage('info', 'Manual step - requires user intervention');
        }

        // Execute the appropriate step method
        switch(step) {
            case 1:
                await this.executeInitialUpload(courseNumber);
                break;
            case 2:
                await this.executeGooglePlacesDetails(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 3:
                await this.executeGolfUrlFinder(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 4:
                await this.executeGolfNowReviews(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 5:
                await this.executeGolfPassReviews(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 6:
                await this.executeGoogleReviews(courseNumber);
                break;
            case 7:
                await this.executeReviewScoring(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 8:
                await this.executeCombineScores(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 9:
                await this.executeCourseWebsiteScraping(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 10:
                await this.executeScorecardImages(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 11:
                await this.executeTeesRatings(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 12:
                await this.executeVisualsWeather(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            case 13:
                await this.executeStrategicAnalysis(courseNumber, stepInfo ? stepInfo.script : null);
                break;
            default:
                // For safety, simulate processing time
                await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 2000));
                break;
        }

        this.logMessage('success', 'Course ' + courseNumber + ': Completed ' + stepName);
    }

    // Utility Methods
    formatDate(dateString) {
        if (!dateString) return 'Never';
        const date = new Date(dateString);
        return date.toLocaleString();
    }

    logMessage(type, message) {
        const logContent = document.getElementById('logContent');
        if (!logContent) return;

        const timestamp = new Date().toLocaleTimeString();
        const logEntry = document.createElement('div');
        logEntry.className = 'log-entry ' + type;
        logEntry.innerHTML = '<span class="log-time">[' + timestamp + ']</span> ' + message;

        logContent.appendChild(logEntry);
        logContent.scrollTop = logContent.scrollHeight;

        // Keep only last 100 log entries
        const entries = logContent.querySelectorAll('.log-entry');
        if (entries.length > 100) {
            entries[0].remove();
        }
    }

    clearLog() {
        const logContent = document.getElementById('logContent');
        if (logContent) {
            logContent.innerHTML = '<div class="log-entry info">Log cleared...</div>';
        }
    }

    startAutoRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }

        // Refresh every 30 seconds
        this.refreshInterval = setInterval(() => {
            this.refreshStatus();
        }, 30000);
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
        this.isInitialized = false;
    }
}

// Export for global access
window.PipelineManager = PipelineManager;
