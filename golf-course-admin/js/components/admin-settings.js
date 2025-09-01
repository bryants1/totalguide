/**
 * ADMIN SETTINGS MANAGER
 * Manages configuration settings for scripts, directories, and pipeline steps
 */

class AdminSettingsManager {
    constructor() {
        this.modal = null;
        this.currentSettings = this.getDefaultSettings();
        this.isInitialized = false;
    }

    init() {
        if (this.isInitialized) {
            console.log('Admin Settings Manager already initialized');
            return;
        }

        this.createAdminInterface();
        this.loadSettings();
        this.populatePipelineSteps();
        this.setupEventListeners();
        this.isInitialized = true;

        console.log('Admin Settings Manager initialized');
    }

    getDefaultSettings() {
        return {
            scriptsDirectory: '/Users/your-username/golf-scripts',
            statesDirectory: '/Users/your-username/golf-states',
            pipelineSteps: [
                { id: 1, name: 'Initial Upload', script: '', manual: true },
                { id: 2, name: 'Google Places Details', script: 'run_google_places_enrichment.py', manual: false },
                { id: 3, name: 'GolfNow & GolfPass URLs', script: 'golf_url_finder.py', manual: false },
                { id: 4, name: 'GolfNow Reviews', script: 'golfnow_reviews_batch.py', manual: false },
                { id: 5, name: 'GolfPass Reviews', script: 'run_golfpass_scraper.py', manual: false },
                { id: 6, name: 'Google Reviews', script: '', manual: true },
                { id: 7, name: 'Score & Summarize Reviews', script: 'extract_text_insights.py', manual: false },
                { id: 8, name: 'Combine Review Scores', script: 'combined_scores.py', manual: false },
                { id: 9, name: 'Scrape Course Websites', script: 'database_integrated_runner.py', manual: false },
                { id: 10, name: 'Download Scorecard Images', script: 'run_scorecard_extraction.py', manual: false },
                { id: 11, name: 'Extract Tees & Ratings', script: 'scorecard_scraper.py', manual: false },
                { id: 12, name: 'Collect Visuals & Weather', script: 'golf_course_visualizer.py', manual: false },
                { id: 13, name: 'Strategic Analysis', script: 'comprehensive_golf_analysis.py', manual: false }
            ]
        };
    }

    createAdminInterface() {
        // Check if modal already exists to prevent duplicates
        if (document.getElementById('adminSettingsModal')) {
            console.log('Admin interface already exists, skipping creation');
            this.modal = document.getElementById('adminSettingsModal');
            return;
        }

        // Create admin settings icon
        const adminIcon = document.createElement('button');
        adminIcon.className = 'admin-settings-icon';
        adminIcon.innerHTML = '⚙️';
        adminIcon.title = 'Admin Settings';
        adminIcon.onclick = () => this.open();
        document.body.appendChild(adminIcon);

        // Create admin modal
        const modalHTML = `
            <div id="adminSettingsModal" class="admin-settings-modal">
                <div class="admin-modal-content">
                    <div class="admin-modal-header">
                        <h2>⚙️ Admin Settings</h2>
                        <button class="admin-close-btn" onclick="adminSettings.close()">✕</button>
                    </div>

                    <div id="adminStatusMessage" class="admin-status-message"></div>

                    <form class="admin-settings-form" id="adminSettingsForm">
                        <!-- Global Configuration Section -->
                        <div class="admin-section">
                            <h3>📁 Directory Configuration</h3>

                            <div class="admin-form-group">
                                <label for="scriptsDirectory">Scripts Directory</label>
                                <input type="text" id="scriptsDirectory" placeholder="/path/to/scripts" value="/Users/your-username/golf-scripts">
                                <small>Base directory where all pipeline scripts are located</small>
                            </div>

                            <div class="admin-form-group">
                                <label for="statesDirectory">States Directory</label>
                                <input type="text" id="statesDirectory" placeholder="/path/to/states" value="/Users/your-username/golf-states">
                                <small>Directory containing state-specific data and configurations</small>
                            </div>
                        </div>

                        <!-- Pipeline Steps Configuration -->
                        <div class="admin-section">
                            <h3>🔄 Pipeline Steps Configuration</h3>
                            <p style="color: #6b7280; margin-bottom: 20px;">Configure the script names for each pipeline step. Manual steps don't require scripts.</p>

                            <div class="pipeline-steps-grid" id="pipelineStepsGrid">
                                <!-- Steps will be populated by JavaScript -->
                            </div>
                        </div>

                        <div class="admin-actions">
                            <button type="button" class="admin-btn admin-btn-secondary" onclick="adminSettings.resetToDefaults()">
                                🔄 Reset to Defaults
                            </button>
                            <button type="button" class="admin-btn admin-btn-success" onclick="adminSettings.testConfiguration()">
                                🧪 Test Configuration
                            </button>
                            <button type="submit" class="admin-btn admin-btn-primary">
                                💾 Save Settings
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHTML);
        this.modal = document.getElementById('adminSettingsModal');
    }

    async loadSettings() {
        try {
            // Load from database only
            if (await this.isDatabaseAvailable()) {
                console.log('🔄 Loading admin settings from database...');

                const response = await fetch('/api/admin-settings?profile=default');
                const result = await response.json();

                if (result.success) {
                    console.log('✅ Loaded admin settings from database');
                    this.currentSettings = {
                        scriptsDirectory: result.data.scripts_directory,
                        statesDirectory: result.data.states_directory,
                        pipelineSteps: result.data.pipeline_steps
                    };
                    this.populateForm();
                    return;
                } else {
                    console.warn('Failed to load admin settings from database:', result.message);
                }
            } else {
                console.log('❌ Database not available - cannot load admin settings');
                this.showMessage('Please connect to database to load admin settings', 'info');
            }

            // Use defaults if database load failed
            this.currentSettings = this.getDefaultSettings();
        } catch (error) {
            console.error('Failed to load admin settings:', error);
            this.currentSettings = this.getDefaultSettings();
            this.showMessage('Failed to load settings from database. Using defaults.', 'error');
        }

        this.populateForm();
    }

    async saveSettings() {
        try {
            const formData = this.getFormData();
            this.currentSettings = { ...this.currentSettings, ...formData };

            // Save to database only
            if (!(await this.isDatabaseAvailable())) {
                this.showMessage('❌ Database not connected. Please connect to save settings.', 'error');
                return false;
            }

            console.log('💾 Saving admin settings to database...');

            const response = await fetch('/api/admin-settings', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    profile_name: 'default',
                    scripts_directory: this.currentSettings.scriptsDirectory,
                    states_directory: this.currentSettings.statesDirectory,
                    pipeline_steps: this.currentSettings.pipelineSteps
                })
            });

            const result = await response.json();

            if (result.success) {
                console.log('✅ Saved admin settings to database');
                this.showMessage('Settings saved to database successfully!', 'success');

                // Update pipeline manager if it exists
                if (window.pipelineManager && typeof window.pipelineManager.updateSettings === 'function') {
                    window.pipelineManager.updateSettings(this.currentSettings);
                }

                // Update main app if it exists
                if (window.golfAdmin && typeof window.golfAdmin.updateAdminSettings === 'function') {
                    window.golfAdmin.updateAdminSettings(this.currentSettings);
                }

                return true;
            } else {
                throw new Error(result.message || 'Database save failed');
            }

        } catch (error) {
            console.error('Failed to save settings:', error);
            this.showMessage('Failed to save settings: ' + error.message, 'error');
            return false;
        }
    }

    populateForm() {
        const elements = {
            scriptsDirectory: document.getElementById('scriptsDirectory'),
            statesDirectory: document.getElementById('statesDirectory')
        };

        Object.keys(elements).forEach(key => {
            if (elements[key] && this.currentSettings[key] !== undefined) {
                elements[key].value = this.currentSettings[key];
            }
        });
    }

    populatePipelineSteps() {
        const grid = document.getElementById('pipelineStepsGrid');
        if (!grid) return;

        const steps = this.currentSettings.pipelineSteps || this.getDefaultSettings().pipelineSteps;

        grid.innerHTML = steps.map(step => `
            <div class="pipeline-step-row">
                <div class="step-number">${step.id}</div>
                <div class="step-name">${step.name}</div>
                <div>
                    ${step.manual ?
                        '<span class="step-type-badge step-type-manual">Manual Step</span>' :
                        `<input type="text" class="step-script-input" data-step="${step.id}" value="${step.script || ''}" placeholder="script_name.py">`
                    }
                </div>
            </div>
        `).join('');

        // No checkbox event listeners needed anymore
    }

    getFormData() {
        const formData = {
            scriptsDirectory: document.getElementById('scriptsDirectory')?.value.trim() || '',
            statesDirectory: document.getElementById('statesDirectory')?.value.trim() || '',
            pipelineSteps: [...(this.currentSettings.pipelineSteps || [])]
        };

        // Update pipeline steps scripts
        const scriptInputs = document.querySelectorAll('.step-script-input');
        scriptInputs.forEach(input => {
            const stepId = parseInt(input.dataset.step);
            const stepIndex = formData.pipelineSteps.findIndex(s => s.id === stepId);
            if (stepIndex >= 0) {
                formData.pipelineSteps[stepIndex].script = input.value.trim();
            }
        });

        return formData;
    }

    setupEventListeners() {
        const form = document.getElementById('adminSettingsForm');
        if (form) {
            form.addEventListener('submit', (e) => {
                e.preventDefault();
                this.saveSettings();
            });
        }

        // Close modal when clicking outside
        if (this.modal) {
            this.modal.addEventListener('click', (e) => {
                if (e.target === this.modal) {
                    this.close();
                }
            });
        }

        // ESC key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal?.classList.contains('show')) {
                this.close();
            }
        });
    }

    open() {
        if (!this.isInitialized) {
            this.init();
        }
        this.loadSettings();
        this.populatePipelineSteps();
        this.modal?.classList.add('show');
        document.body.style.overflow = 'hidden';
    }

    close() {
        this.modal?.classList.remove('show');
        document.body.style.overflow = '';
        this.hideMessage();
    }

    resetToDefaults() {
        if (confirm('Are you sure you want to reset all settings to defaults? This will overwrite your current configuration.')) {
            this.currentSettings = this.getDefaultSettings();
            this.populateForm();
            this.populatePipelineSteps();
            this.showMessage('Settings reset to defaults. Click Save to apply.', 'info');
        }
    }

    async testConfiguration() {
        this.showMessage('Testing configuration...', 'info');

        try {
            const formData = this.getFormData();

            // Basic validation
            if (!formData.scriptsDirectory) {
                this.showMessage('❌ Scripts directory is required', 'error');
                return;
            }

            if (!formData.statesDirectory) {
                this.showMessage('❌ States directory is required', 'error');
                return;
            }

            // Test database connection if available
            if (await this.isDatabaseAvailable()) {
                try {
                    const response = await fetch('/api/admin-settings/test', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ profile: 'default' })
                    });

                    const result = await response.json();

                    if (result.success) {
                        this.showMessage('✅ Configuration validation passed! Database connection working.', 'success');
                    } else {
                        this.showMessage(`⚠️ Configuration test failed: ${result.message}`, 'error');
                    }
                } catch (dbError) {
                    this.showMessage('✅ Basic validation passed (database test failed)', 'success');
                }
            } else {
                this.showMessage('⚠️ Database not available. Please connect to fully test configuration.', 'warning');
            }

        } catch (error) {
            this.showMessage(`❌ Configuration test failed: ${error.message}`, 'error');
        }
    }

    showMessage(message, type = 'info') {
        const messageEl = document.getElementById('adminStatusMessage');
        if (!messageEl) return;

        messageEl.textContent = message;
        messageEl.className = `admin-status-message admin-status-${type}`;
        messageEl.style.display = 'block';

        // Auto-hide after 5 seconds for success/info messages
        if (type === 'success' || type === 'info') {
            setTimeout(() => this.hideMessage(), 5000);
        }
    }

    hideMessage() {
        const messageEl = document.getElementById('adminStatusMessage');
        if (messageEl) {
            messageEl.style.display = 'none';
        }
    }

    getSettings() {
        return this.currentSettings;
    }

    updateSetting(key, value) {
        this.currentSettings[key] = value;
        this.saveSettings();
    }

    // Get script path for a specific step
    getScriptPath(stepId) {
        const step = this.currentSettings.pipelineSteps?.find(s => s.id === stepId);
        if (!step || step.manual || !step.script) {
            return null;
        }

        const scriptsDir = this.currentSettings.scriptsDirectory || '/Users/your-username/golf-scripts';
        return `${scriptsDir}/${step.script}`;
    }

    // Check if a step is manual
    isStepManual(stepId) {
        const step = this.currentSettings.pipelineSteps?.find(s => s.id === stepId);
        return step ? step.manual : false;
    }

    // Helper method to check if database is available
    async isDatabaseAvailable() {
        try {
            // Try a simple API call to check if database is working
            const response = await fetch('/api/health');
            const result = await response.json();
            return result.success && result.supabaseConnected;
        } catch (error) {
            console.warn('Database availability check failed:', error);
            return false;
        }
    }
}
