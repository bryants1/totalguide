/**
 * GOLF COURSE ADMIN INTERFACE - COMPLETE MAIN APPLICATION MODULE
 * Coordinates all modules and manages application state with integrated authentication
 */

import { DatabaseManager } from './database.js';
import { DataRenderer } from './components/renderer.js';
import { PipelineManager } from './components/pipeline-manager.js';
import { DataUpdater } from './data-updater.js';
import { EditModal } from './edit-modal.js';
import { TabManager } from './components/tabs.js';
import { ProgressTracker } from './components/progress.js';
import { showStatus, formatFieldName } from './utils.js';

class GolfCourseAdmin {
    constructor() {
        this.db = new DatabaseManager();
        this.renderer = new DataRenderer();
        this.updater = new DataUpdater(this.db);
        this.editModal = new EditModal(this.db);
        this.tabManager = new TabManager();
        this.progressTracker = new ProgressTracker();
        this.pipelineManager = new PipelineManager();
        this.adminSettings = null; // Will be initialized if AdminSettingsManager is available
        this.currentCourseData = {};

        // Search functionality properties
        this.allCourses = [];
        this.filteredCourses = [];
        this.searchTimeout = null;

        // Authentication state
        this.isAuthenticated = false;

        this.init();
    }

    async init() {
        // Load saved credentials and check connection state
        this.checkAuthState();

        // Set up event listeners
        this.setupEventListeners();

        // 🔹 Ensure TabManager containers exist up-front
        this.ensureTabContainers();

        // Initialize components (TabManager needs #mainTabs + #tabContent present)
        this.tabManager.init();
        this.editModal.init();
        this.progressTracker.init();
        this.initializeAdminSettings(); // Initialize admin settings if available
        this.setupTabs();
        await this.loadCourseData();   // <-- render the Overview shell immediately

        // Make methods globally available for HTML onclick handlers
        this.exposeGlobalMethods();

        console.log('Golf Course Admin initialized');
    }

    /**
     * Ensure TabManager containers exist so tabs always render.
     * Creates <div id="mainTabs"> and <div id="tabContent"> when missing.
     */
    ensureTabContainers() {
        const host = document.getElementById('courseData') || document.body;

        if (!document.getElementById('mainTabs')) {
            const mainTabs = document.createElement('div');
            mainTabs.id = 'mainTabs';
            host.prepend(mainTabs); // buttons at the top
        }

        if (!document.getElementById('tabContent')) {
            const tabContent = document.createElement('div');
            tabContent.id = 'tabContent';
            host.appendChild(tabContent); // panels below
        }
    }

    // ============================================================================
    // AUTHENTICATION & COOKIE MANAGEMENT
    // ============================================================================

    // Cookie management functions
    setCookie(name, value, days = 30) {
        const expires = new Date();
        expires.setTime(expires.getTime() + (days * 24 * 60 * 60 * 1000));
        document.cookie = `${name}=${encodeURIComponent(value)};expires=${expires.toUTCString()};path=/;SameSite=Strict`;
    }

    getCookie(name) {
        const nameEQ = name + "=";
        const ca = document.cookie.split(';');
        for (let i = 0; i < ca.length; i++) {
            let c = ca[i];
            while (c.charAt(0) === ' ') c = c.substring(1, c.length);
            if (c.indexOf(nameEQ) === 0) {
                return decodeURIComponent(c.substring(nameEQ.length, c.length));
            }
        }
        return null;
    }

    deleteCookie(name) {
        document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 UTC;path=/;`;
    }

    // Save credentials to cookies
    saveCredentials() {
        const url = document.getElementById('supabaseUrl')?.value.trim();
        const key = document.getElementById('supabaseKey')?.value.trim();

        if (url) this.setCookie('supabase_url', url);
        if (key) this.setCookie('supabase_key', key);
    }

    // Load credentials from cookies
    loadCredentials() {
        const savedUrl = this.getCookie('supabase_url');
        const savedKey = this.getCookie('supabase_key');

        const urlInput = document.getElementById('supabaseUrl');
        const keyInput = document.getElementById('supabaseKey');

        if (savedUrl && urlInput) urlInput.value = savedUrl;
        if (savedKey && keyInput) keyInput.value = savedKey;

        return { url: savedUrl, key: savedKey };
    }

    // Check authentication state on startup
    async checkAuthState() {
        const credentials = this.loadCredentials();
        const wasConnected = this.getCookie('is_connected') === 'true';

        if (wasConnected && credentials.url && credentials.key) {
            // Auto-connect with saved credentials
            this.showConnectionStatus('🔄 Auto-connecting with saved credentials...', 'info');
            setTimeout(() => {
                this.connectDatabase(true); // Auto-connect
            }, 500);
        } else {
            this.showLoginState();
        }
    }

    // Show login state
    showLoginState() {
        const loginSection = document.getElementById('loginSection');
        const connectedSection = document.getElementById('connectedSection');
        const courseSelection = document.getElementById('courseSelection');
        const courseData = document.getElementById('courseData');

        if (loginSection) loginSection.style.display = 'block';
        if (connectedSection) connectedSection.style.display = 'none';
        if (courseSelection) courseSelection.classList.add('hidden');
        if (courseData) courseData.classList.add('hidden');

        this.isAuthenticated = false;
    }

    // Show connected state
    showConnectedState() {
        const loginSection = document.getElementById('loginSection');
        const connectedSection = document.getElementById('connectedSection');
        const courseSelection = document.getElementById('courseSelection');

        if (loginSection) loginSection.style.display = 'none';
        if (connectedSection) connectedSection.style.display = 'block';
        if (courseSelection) courseSelection.classList.remove('hidden');
        if (courseData) courseData.classList.remove('hidden'); // show tabs area
        this.isAuthenticated = true;
    }

    // Show connection status message
    showConnectionStatus(message, type = 'info') {
        const statusDiv = document.getElementById('connectionStatus');
        if (statusDiv) {
            const className = type === 'error' ? 'error-message' :
                             type === 'success' ? 'success-message' : 'info-message';
            statusDiv.innerHTML = `<div class="${className}">${message}</div>`;
        }
    }

    // Clear connection status
    clearConnectionStatus() {
        const statusDiv = document.getElementById('connectionStatus');
        if (statusDiv) statusDiv.innerHTML = '';
    }

    // Disconnect and return to login
    disconnect() {
        if (confirm('Are you sure you want to disconnect? This will return you to the login screen.')) {
            this.deleteCookie('is_connected');
            this.showLoginState();
            this.clearConnectionStatus();
            this.allCourses = [];
            this.filteredCourses = [];
            console.log('Disconnected successfully');
        }
    }

    // Clear stored credentials
    clearStoredCredentials() {
        if (confirm('Are you sure you want to clear your saved Supabase credentials?')) {
            this.deleteCookie('supabase_url');
            this.deleteCookie('supabase_key');
            this.deleteCookie('is_connected');

            const urlInput = document.getElementById('supabaseUrl');
            const keyInput = document.getElementById('supabaseKey');

            if (urlInput) urlInput.value = '';
            if (keyInput) keyInput.value = '';

            this.showLoginState();
            this.showConnectionStatus('🗑️ Saved credentials cleared', 'info');
            setTimeout(() => this.clearConnectionStatus(), 3000);
        }
    }

    // ============================================================================
    // CONNECTION & DATABASE
    // ============================================================================

    async connectDatabase(isAutoConnect = false) {
        // Save credentials if not auto-connecting
        if (!isAutoConnect) {
            this.saveCredentials();
        }

        const url = document.getElementById('supabaseUrl')?.value.trim();
        const key = document.getElementById('supabaseKey')?.value.trim();

        if (!url || !key) {
            this.showConnectionStatus('❌ Please enter both URL and key', 'error');
            return;
        }

        try {
            if (!isAutoConnect) {
                this.showConnectionStatus('🔄 Connecting to database...', 'info');
            }

            await this.db.connect(url, key);

            // Load courses list
            const courses = await this.db.getCoursesList();
            this.allCourses = courses;
            this.filteredCourses = [...courses];

            // Set connected state
            this.setCookie('is_connected', 'true', 30);
            this.showConnectedState();
            this.clearConnectionStatus();

            // Populate course dropdown and setup search
            this.updateDropdownOptions(this.filteredCourses);
            this.updateSearchStats();

            console.log(isAutoConnect ? 'Auto-connected successfully!' : 'Connected successfully!');
            console.log(`Loaded ${courses.length} courses`);

        } catch (error) {
            console.error('Connection failed:', error);
            this.showConnectionStatus(`❌ Connection failed: ${error.message}`, 'error');
            this.deleteCookie('is_connected');
            this.showLoginState();
        }
    }

    // ============================================================================
    // EVENT LISTENERS
    // ============================================================================

    setupEventListeners() {
        // Connection button
        const connectBtn = document.getElementById('connectBtn');
        if (connectBtn) {
            connectBtn.addEventListener('click', () => this.connectDatabase());
        }

        // Clear credentials button
        const clearCredsBtn = document.getElementById('clearCredsBtn');
        if (clearCredsBtn) {
            clearCredsBtn.addEventListener('click', () => this.clearStoredCredentials());
        }

        // Auto-save credentials when typing (debounced)
        this.setupAutoSave();

        // Course selection dropdown
        const courseSelect = document.getElementById('courseSelect');
        if (courseSelect) {
            courseSelect.addEventListener('change', () => {
                this.loadCourseData();
                this.syncSearchWithDropdown();
            });
        }

        // Course search box
        const courseSearch = document.getElementById('courseSearch');
        if (courseSearch) {
            courseSearch.addEventListener('input', (e) => {
                this.handleSearchInput(e.target.value);
            });

            courseSearch.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    this.handleSearchEnter();
                }
            });
        }

        // Clear search button
        const clearSearchBtn = document.getElementById('clearSearchBtn');
        if (clearSearchBtn) {
            clearSearchBtn.addEventListener('click', () => {
                this.clearSearch();
            });
        }

        // Update data buttons (created dynamically)
        document.addEventListener('click', (e) => {
            if (e.target.id === 'updateDataBtn' || e.target.closest('#updateDataBtn')) {
                this.updatePrimaryData();
            }
            if (e.target.id === 'updateAllDataBtn' || e.target.closest('#updateAllDataBtn')) {
                this.updateAllCoursesData();
            }
        });

        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('table-edit-btn')) {
                e.preventDefault();
                e.stopPropagation();

                const editType = e.target.dataset.editType;
                const courseNumber = e.target.dataset.course;

                if (editType === 'tee_row') {
                    const rowData = JSON.parse(decodeURIComponent(e.target.dataset.rowData));

                    // Open tee row edit modal
                    this.editModal.openTeeRowEdit(rowData, courseNumber, () => {
                        this.loadCourseData();
                    });

                } else if (editType === 'par_row') {
                    const rowData = JSON.parse(decodeURIComponent(e.target.dataset.rowData));

                    // Open par row edit modal
                    this.editModal.openParRowEdit(rowData, courseNumber, () => {
                        this.loadCourseData();
                    });
                }
            }
        });

        // Tab switching (delegate to TabManager)
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('tab')) {
                const tabId = e.target.getAttribute('data-tab');
                if (tabId) {
                    this.showTab(tabId);
                }
            }
        });

        // Edit buttons
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('edit-btn')) {
                e.preventDefault();
                e.stopPropagation();

                const fieldName = e.target.dataset.field;
                const value = decodeURIComponent(e.target.dataset.value || '');
                const courseNumber = e.target.dataset.course;

                if (fieldName && courseNumber && this.editModal.open) {
                    this.editModal.open(fieldName, value, courseNumber, () => {
                        this.loadCourseData();
                    });
                }
            }
        });
    }

    // Setup auto-save for credentials
    setupAutoSave() {
        const urlInput = document.getElementById('supabaseUrl');
        const keyInput = document.getElementById('supabaseKey');

        let saveTimeout;
        const debouncedSave = () => {
            clearTimeout(saveTimeout);
            saveTimeout = setTimeout(() => this.saveCredentials(), 1000);
        };

        if (urlInput) urlInput.addEventListener('input', debouncedSave);
        if (keyInput) keyInput.addEventListener('input', debouncedSave);
    }

    // ============================================================================
    // SEARCH FUNCTIONALITY
    // ============================================================================

    handleSearchInput(searchTerm) {
        if (this.searchTimeout) {
            clearTimeout(this.searchTimeout);
        }

        this.searchTimeout = setTimeout(() => {
            this.filterCourses(searchTerm);
        }, 300);
    }

    filterCourses(searchTerm) {
        if (!searchTerm.trim()) {
            this.filteredCourses = [...this.allCourses];
            this.updateDropdownOptions(this.filteredCourses);
            this.updateSearchResults();
            return;
        }

        const term = searchTerm.toLowerCase().trim();

        this.filteredCourses = this.allCourses.filter(course => {
            const courseNumber = (course.course_number || '').toLowerCase();
            const courseName = (course.course_name || '').toLowerCase();
            const state = (course.state || '').toLowerCase();
            const city = (course.city || '').toLowerCase();
            const county = (course.county || '').toLowerCase();

            return courseNumber.includes(term) ||
                   courseName.includes(term) ||
                   state.includes(term) ||
                   city.includes(term) ||
                   county.includes(term);
        });

        this.updateDropdownOptions(this.filteredCourses);
        this.updateSearchResults();

        console.log(`Search for "${searchTerm}" found ${this.filteredCourses.length} courses`);
    }

    updateDropdownOptions(courses) {
        const courseSelect = document.getElementById('courseSelect');
        if (!courseSelect) return;

        const currentSelection = courseSelect.value;
        courseSelect.innerHTML = '<option value="">-- Select a course --</option>';

        courses.forEach(course => {
            const option = document.createElement('option');
            option.value = course.course_number;
            option.textContent = `${course.course_number} - ${course.course_name || 'Unknown'}`;

            if (course.course_number === currentSelection) {
                option.selected = true;
            }

            courseSelect.appendChild(option);
        });
    }

    updateSearchResults() {
        const searchResults = document.getElementById('searchResults');
        if (!searchResults) return;

        if (this.filteredCourses.length === 0) {
            searchResults.innerHTML = '<div class="search-no-results">No courses found matching your search</div>';
            searchResults.classList.remove('hidden');
            return;
        }

        if (this.filteredCourses.length === this.allCourses.length) {
            searchResults.classList.add('hidden');
            return;
        }

        const resultsHTML = `
            <div class="search-results-summary">
                Found ${this.filteredCourses.length} course${this.filteredCourses.length !== 1 ? 's' : ''}
                ${this.filteredCourses.length <= 10 ? this.renderQuickSelectButtons() : ''}
            </div>
        `;

        searchResults.innerHTML = resultsHTML;
        searchResults.classList.remove('hidden');
    }

    renderQuickSelectButtons() {
        if (this.filteredCourses.length > 10) return '';

        return `
            <div class="quick-select-buttons">
                ${this.filteredCourses.map(course => `
                    <button class="quick-select-btn" onclick="window.golfAdmin.selectCourse('${course.course_number}')">
                        ${course.course_number} - ${(course.course_name || 'Unknown').substring(0, 30)}
                    </button>
                `).join('')}
            </div>
        `;
    }

    selectCourse(courseNumber) {
        const courseSelect = document.getElementById('courseSelect');
        if (courseSelect) {
            courseSelect.value = courseNumber;
            this.loadCourseData();
            this.syncSearchWithDropdown();
        }
    }

    syncSearchWithDropdown() {
        const courseSelect = document.getElementById('courseSelect');
        const courseSearch = document.getElementById('courseSearch');

        if (!courseSelect || !courseSearch) return;

        const selectedCourse = this.allCourses.find(c => c.course_number === courseSelect.value);
        if (selectedCourse) {
            courseSearch.value = `${selectedCourse.course_number} - ${selectedCourse.course_name || ''}`;
        }
    }

    handleSearchEnter() {
        if (this.filteredCourses.length === 1) {
            this.selectCourse(this.filteredCourses[0].course_number);
        }
    }

    clearSearch() {
        const courseSearch = document.getElementById('courseSearch');
        const searchResults = document.getElementById('searchResults');

        if (courseSearch) courseSearch.value = '';
        if (searchResults) searchResults.classList.add('hidden');

        this.filteredCourses = [...this.allCourses];
        this.updateDropdownOptions(this.filteredCourses);
    }

    updateSearchStats() {
        const searchStats = document.getElementById('searchStats');
        if (!searchStats || !this.allCourses.length) return;

        const states = [...new Set(this.allCourses.map(c => c.state).filter(Boolean))];
        const cities = [...new Set(this.allCourses.map(c => c.city).filter(Boolean))];

        searchStats.innerHTML = `
            <div class="search-stats">
                <span class="stat-item">📊 ${this.allCourses.length} Primary Data Courses</span>
                <span class="stat-item">🗺️ ${states.length} States</span>
                <span class="stat-item">🏙️ ${cities.length} Cities</span>
            </div>
        `;
    }

    // ============================================================================
    // ADMIN SETTINGS INTEGRATION
    // ============================================================================

    initializeAdminSettings() {
        // Check if AdminSettingsManager is available (loaded via script tag)
        if (typeof AdminSettingsManager !== 'undefined') {
            try {
                this.adminSettings = new AdminSettingsManager();
                this.adminSettings.init();
                console.log('✅ Admin Settings Manager initialized');
            } catch (error) {
                console.warn('⚠️ Failed to initialize Admin Settings Manager:', error);
            }
        } else {
            console.log('ℹ️ AdminSettingsManager not found - admin settings disabled');
        }
    }

    updateAdminSettings(settings) {
        // Update pipeline manager with new settings
        if (this.pipelineManager) {
            this.pipelineManager.updateSettings(settings);
        }

        console.log('Admin settings updated:', settings);
    }

    getAdminSettings() {
        return this.adminSettings ? this.adminSettings.getSettings() : null;
    }

    // ============================================================================
    // CORE APPLICATION FUNCTIONALITY
    // ============================================================================

    async loadCourseData() {
      const courseNumber = document.getElementById('courseSelect')?.value;

      if (!courseNumber) {
        // NEW: show the page with placeholder content instead of hiding it
        const courseDataEl = document.getElementById('courseData');
        const primaryTab = document.getElementById('primaryTab');
        if (courseDataEl) courseDataEl.classList.remove('hidden');

        // Defensive: ensure containers & tabs exist
        this.ensureTabContainers();
        if (!this.tabManager.tabs?.length) this.setupTabs();

        if (primaryTab) {
          primaryTab.innerHTML = this.renderer.renderPrimaryData(
            null,           // no data yet
            null,           // no course number
            null,           // no tees/pars
            { showEmptyShell: true } // shell with placeholders
          );
        }
        return; // keep the rest unchanged
      }

      try {
        showStatus('<div class="loader"></div> Loading course data...', 'info', 'connectionStatus');

        const courseData = await this.db.loadAllCourseData(courseNumber);
        this.currentCourseData = courseData;

        const courseName = courseData.primary?.course_name ||
                           courseData.scraping?.name ||
                           courseData.usgolf?.course_name ||
                           `Course ${courseNumber}`;

        const courseTitle = document.getElementById('courseTitle');
        if (courseTitle) {
            courseTitle.textContent = `${courseNumber} - ${courseName}`;
        }

        this.renderAllData(courseData, courseNumber);

        const updateBtn = document.getElementById('updateDataBtn');
        if (updateBtn) {
            updateBtn.style.display = courseData.primary ? 'flex' : 'none';
        }

        const courseDataElement = document.getElementById('courseData');
        if (courseDataElement) {
            courseDataElement.classList.remove('hidden');
        }

        showStatus(`✅ Loaded data for ${courseName}`, 'success', 'connectionStatus');

      } catch (error) {
        showStatus(`❌ Error loading course data: ${error.message}`, 'error', 'connectionStatus');
        console.error('Load course data error:', error);
      }
    }

    exposeGlobalMethods() {
        // Make methods available globally
        window.connectDatabase = () => this.connectDatabase();
        window.disconnectDatabase = () => this.disconnect();
        window.clearStoredCredentials = () => this.clearStoredCredentials();
        window.loadCourseData = () => this.loadCourseData();
        window.updatePrimaryData = () => this.updatePrimaryData();
        window.updateAllCoursesData = () => this.updateAllCoursesData();
        window.showTab = (tabName) => this.showTab(tabName);
        window.openEditModal = (fieldName, currentValue, courseNumber) => {
            this.editModal.open(fieldName, currentValue, courseNumber, () => {
                this.loadCourseData();
            });
        };
        window.closeEditModal = () => this.editModal.close();

        window.formatFieldName = formatFieldName;
        window.pipelineManager = this.pipelineManager;
        if (this.adminSettings) {
            window.adminSettings = this.adminSettings; // Only expose if available
        }
        window.openTeeRowEditModal = (teeData, courseNumber) => {
            this.editModal.openTeeRowEdit(teeData, courseNumber, () => {
                this.loadCourseData();
            });
        };
        window.openParRowEditModal = (parData, courseNumber) => {
            this.editModal.openParRowEdit(parData, courseNumber, () => {
                this.loadCourseData();
            });
        };

        console.log('Global methods exposed');
    }

    // ============================================================================
    // TAB MANAGEMENT
    // ============================================================================

    setupTabs() {
        const tabs = [
            { id: 'primary', label: 'Primary Data', icon: '📊' },
            { id: 'scores', label: 'Course Scores', icon: '⭐' },
            { id: 'vector', label: 'Vector Attributes', icon: '🎯' },
            { id: 'analysis', label: 'Comprehensive Analysis', icon: '📈' },
            { id: 'reviews', label: 'Reviews', icon: '💬' },
            { id: 'usgolf', label: 'Initial Course Data', icon: '📋' },
            { id: 'scraping', label: 'Course Info', icon: '🔍' },
            { id: 'pipeline', label: 'Pipeline', icon: '🔄' }
        ];

        if (this.tabManager && this.tabManager.setupTabs) {
            this.tabManager.setupTabs(tabs);
        } else {
            this.createTabsManually(tabs);
        }
    }

    createTabsManually(tabs) {
        const tabsContainer = document.querySelector('.tabs');
        if (tabsContainer) {
            tabsContainer.innerHTML = tabs.map(tab => `
                <button class="tab ${tab.id === 'primary' ? 'active' : ''}" data-tab="${tab.id}">
                    ${tab.icon ? tab.icon + ' ' : ''}${tab.label}
                </button>
            `).join('');
        }

        tabs.forEach(tab => {
            let tabContent = document.getElementById(`${tab.id}Tab`);
            if (!tabContent) {
                const tabContentContainer = document.getElementById('tabContent') || document.querySelector('#courseData');
                if (tabContentContainer) {
                    tabContent = document.createElement('div');
                    tabContent.id = `${tab.id}Tab`;
                    tabContent.className = `tab-content ${tab.id === 'primary' ? 'active' : ''}`;
                    tabContentContainer.appendChild(tabContent);
                }
            }
        });
    }

    showTab(tabName) {
        // Delegate switching to TabManager (keeps button & panel state in sync)
        if (this.tabManager && this.tabManager.showTab) {
            this.tabManager.showTab(tabName);
        }

        // Lazy initialize pipeline tab if/when user selects it
        if (tabName === 'pipeline') {
            this.initializePipelineTab();
        }

        console.log(`Switched to tab: ${tabName}`);
    }

    async initializePipelineTab() {
        try {
            const pipelineTab = document.getElementById('pipelineTab');

            if (!pipelineTab) {
                console.error('❌ Pipeline tab element not found');
                return;
            }

            if (!pipelineTab.dataset.initialized) {
                console.log('🚀 Initializing Pipeline Manager...');

                // CRITICAL: Set the database connection BEFORE initializing
                this.pipelineManager.setDatabase(this.db);

                await this.pipelineManager.initialize(pipelineTab);

                pipelineTab.dataset.initialized = 'true';
                console.log('✅ Pipeline Manager initialized successfully');
            }

        } catch (error) {
            console.error('❌ Failed to initialize Pipeline Manager:', error);

            // Better error display
            const pipelineTab = document.getElementById('pipelineTab');
            if (pipelineTab) {
                pipelineTab.innerHTML = `
                    <div class="error-message" style="padding: 20px; text-align: center; background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 8px; margin: 20px;">
                        <h3 style="color: #721c24; margin-bottom: 10px;">❌ Failed to Initialize Pipeline Manager</h3>
                        <p style="color: #721c24; margin-bottom: 15px;"><strong>Error:</strong> ${error.message}</p>
                        <button onclick="location.reload()" class="btn-primary" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                            🔄 Reload Page
                        </button>
                    </div>
                `;
            }
        }
    }

    // ============================================================================
    // DATA RENDERING & UPDATES
    // ============================================================================

    renderAllData(courseData, courseNumber) {
        const primaryTab = document.getElementById('primaryTab');
        if (primaryTab) {
            const primaryHTML = `
                <div class="update-buttons-section">
                    <!-- (keep your existing buttons exactly as-is) -->
                </div>

                <div id="updateProgress" class="update-progress">
                    <h4 style="margin:0 0 15px 0; color:#4a7c59;">📊 Data Update Progress</h4>
                    <div id="progressContent"></div>
                </div>

                <div id="primaryDataContent">
                    ${this.renderer.renderPrimaryData(
                        courseData.primary || null,
                        courseNumber,
                        courseData.teesAndPars,
                        { showEmptyShell: !courseData.primary } // <-- key flag
                    )}
                </div>
            `;
            primaryTab.innerHTML = primaryHTML;
        }

        const tabs = [
            { id: 'scores',   data: courseData.scores,   renderer: 'renderScoresData' },
            { id: 'vector',   data: courseData.vector,   renderer: 'renderVectorData' },
            { id: 'analysis', data: courseData.analysis, renderer: 'renderAnalysisData' },
            { id: 'reviews',  data: courseData.reviews,  renderer: 'renderReviewsData' },
            { id: 'usgolf',   data: courseData.usgolf,   renderer: 'renderUSGolfData' },
            { id: 'scraping', data: courseData.scraping, renderer: 'renderScrapingData' }
        ];

        tabs.forEach(tab => {
            const tabElement = document.getElementById(`${tab.id}Tab`);
            if (tabElement && this.renderer[tab.renderer]) {
                tabElement.innerHTML = this.renderer[tab.renderer](tab.data);
            }
        });

        console.log('All data rendered');
    }

    async updatePrimaryData() {
        const courseNumber = document.getElementById('courseSelect')?.value;
        if (!courseNumber) {
            showStatus('❌ No course selected', 'error', 'connectionStatus');
            return;
        }

        const updateBtn = document.getElementById('updateDataBtn');
        const btnText = document.getElementById('updateButtonText');
        const btnSpinner = document.getElementById('updateButtonSpinner');

        if (updateBtn) updateBtn.disabled = true;
        if (btnText) btnText.textContent = 'Updating Data...';
        if (btnSpinner) btnSpinner.style.display = 'inline-block';

        this.progressTracker.show();

        try {
            showStatus('🔄 Starting data update from source tables...', 'info', 'connectionStatus');

            const result = await this.updater.updatePrimaryData(
                courseNumber,
                (field, status, message) => {
                    this.progressTracker.addItem(field, status, message);
                }
            );

            if (result.updatedCount > 0) {
                showStatus(`✅ Successfully updated ${result.updatedCount} fields from source tables`, 'success', 'connectionStatus');
                this.progressTracker.showSummary();

                setTimeout(async () => {
                    await this.loadCourseData();
                    this.progressTracker.hide();
                }, 2000);
            } else {
                showStatus('ℹ️ No updates needed - all data is current', 'info', 'connectionStatus');
                this.progressTracker.addCustomMessage('All fields are up to date', 'info');
                setTimeout(() => {
                    this.progressTracker.hide();
                }, 2000);
            }

        } catch (error) {
            showStatus(`❌ Data update failed: ${error.message}`, 'error', 'connectionStatus');
            this.progressTracker.addCustomMessage(`Update failed: ${error.message}`, 'error');
            this.progressTracker.hide();
            console.error('Update error:', error);
        } finally {
            if (updateBtn) updateBtn.disabled = false;
            if (btnText) btnText.textContent = 'Update Data from Source Tables';
            if (btnSpinner) btnSpinner.style.display = 'none';
        }
    }

    async updateAllCoursesData() {
        if (!confirm(`This will update primary data for ALL courses in the database. This may take several minutes. Continue?`)) {
            return;
        }

        const updateAllBtn = document.getElementById('updateAllDataBtn');
        const btnText = document.getElementById('updateAllButtonText');
        const btnSpinner = document.getElementById('updateAllButtonSpinner');

        if (updateAllBtn) updateAllBtn.disabled = true;
        if (btnText) btnText.textContent = 'Updating All Courses...';
        if (btnSpinner) btnSpinner.style.display = 'inline-block';

        this.progressTracker.show();
        this.progressTracker.clear();

        try {
            showStatus('🚀 Starting bulk update for all courses...', 'info', 'connectionStatus');

            const result = await this.updater.updateAllCourses((type, status, message) => {
                switch (type) {
                    case 'init':
                    case 'progress':
                    case 'complete':
                        this.progressTracker.addCustomMessage(message, status);
                        break;
                    case 'course-complete':
                        this.progressTracker.addCustomMessage(message, status);
                        break;
                }
            });

            if (result.success) {
                showStatus(
                    `✅ Bulk update completed! ${result.successCount}/${result.totalCourses} courses updated successfully. ${result.totalFieldsUpdated} total fields updated.`,
                    'success',
                    'connectionStatus'
                );

                if (result.errorCount > 0) {
                    console.warn('Bulk update errors:', result.errors);
                    this.progressTracker.addCustomMessage(
                        `⚠️ ${result.errorCount} courses had errors - check console for details`,
                        'warning'
                    );
                }

                setTimeout(() => {
                    this.progressTracker.hide();
                }, 5000);
            }

        } catch (error) {
            showStatus(`❌ Bulk update failed: ${error.message}`, 'error', 'connectionStatus');
            this.progressTracker.addCustomMessage(`Bulk update failed: ${error.message}`, 'error');
            console.error('Bulk update error:', error);

            setTimeout(() => {
                this.progressTracker.hide();
            }, 3000);
        } finally {
            if (updateAllBtn) updateAllBtn.disabled = false;
            if (btnText) btnText.textContent = 'Update All Courses Data';
            if (btnSpinner) btnSpinner.style.display = 'none';
        }
    }

    // ============================================================================
    // UTILITY METHODS
    // ============================================================================

    getCurrentCourse() {
        return document.getElementById('courseSelect')?.value || null;
    }

    getCurrentCourseData() {
        return this.currentCourseData;
    }

    isConnected() {
        return this.isAuthenticated && this.db && this.db.supabaseClient;
    }

    async refreshCurrentCourse() {
        const courseNumber = this.getCurrentCourse();
        if (courseNumber) {
            await this.loadCourseData();
        }
    }

    exportCourseData() {
        const courseNumber = this.getCurrentCourse();
        if (!courseNumber || !this.currentCourseData) {
            showStatus('❌ No course data to export', 'error', 'connectionStatus');
            return;
        }

        const dataStr = JSON.stringify(this.currentCourseData, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });

        const link = document.createElement('a');
        link.href = URL.createObjectURL(dataBlob);
        link.download = `golf-course-${courseNumber}-data-${new Date().toISOString().split('T')[0]}.json`;
        link.click();

        showStatus('✅ Course data exported successfully', 'success', 'connectionStatus');
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing Golf Course Admin...');
    try {
        const app = new GolfCourseAdmin();
        window.golfAdmin = app;
        console.log('Golf Course Admin initialized successfully');
    } catch (error) {
        console.error('Failed to initialize Golf Course Admin:', error);
        showStatus('❌ Failed to initialize application', 'error', 'connectionStatus');
    }
});

export default GolfCourseAdmin;
