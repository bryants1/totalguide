/**
 * DATA IMPORT MANAGER - Final Complete Version
 * Handles Excel/CSV imports without upsert conflicts
 */

export class DataImportManager {
    constructor(pipelineManager) {
        this.pipelineManager = pipelineManager;
        this.database = null;
        this.nextCourseNumber = null;
        this.excelData = null;
        this.currentFile = null;
        this.uploadedFiles = {
            initial: null,
            google: null,
            reviews: null
        };
    }

    setDatabase(database) {
        this.database = database;
        this.getNextCourseNumber();
    }

    setupEventListeners() {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initializeUI());
        } else {
            this.initializeUI();
        }
    }

    initializeUI() {
        const excelSection = document.querySelector('.excel-import-section');
        if (excelSection) {
            excelSection.innerHTML = this.generateExcelImportSection();
        }

        const csvSection = document.querySelector('.csv-import-section');
        if (csvSection) {
            csvSection.innerHTML = this.generateManualEntrySection();
        }

        this.setupFileHandlers();
        this.getNextCourseNumber();
    }

    generateExcelImportSection() {
        return `
            <div class="section-header">
                <h3>📊 Excel Data Import</h3>
                <p>Import course data from Excel files (USGolfData.xlsx, google_places_data.xlsx, review_urls.xlsx)</p>
                <button id="promoteDataBtn" class="btn-secondary">🚀 Promote to Primary</button>
            </div>

            <div class="import-files-grid">
                <!-- USGolfData.xlsx -->
                <div class="file-upload-card">
                    <h4>📁 Initial Course Upload</h4>
                    <div class="file-drop-zone" id="initialDropZone">
                        <input type="file" id="initialFileInput" accept=".xlsx,.xls" style="display: none;">
                        <div class="upload-prompt">
                            <span class="upload-icon">📤</span>
                            <p>Drop USGolfData.xlsx here</p>
                            <button class="link-btn" onclick="document.getElementById('initialFileInput').click()">Browse</button>
                        </div>
                        <div id="initialFileStatus" class="file-status"></div>
                    </div>
                </div>

                <!-- google_places_data.xlsx -->
                <div class="file-upload-card">
                    <h4>📍 Google Places Data</h4>
                    <div class="file-drop-zone" id="googleDropZone">
                        <input type="file" id="googleFileInput" accept=".xlsx,.xls" style="display: none;">
                        <div class="upload-prompt">
                            <span class="upload-icon">📤</span>
                            <p>Drop google_places_data.xlsx here</p>
                            <button class="link-btn" onclick="document.getElementById('googleFileInput').click()">Browse</button>
                        </div>
                        <div id="googleFileStatus" class="file-status"></div>
                    </div>
                </div>

                <!-- review_urls.xlsx -->
                <div class="file-upload-card">
                    <h4>🔗 Review URLs</h4>
                    <div class="file-drop-zone" id="reviewDropZone">
                        <input type="file" id="reviewFileInput" accept=".xlsx,.xls" style="display: none;">
                        <div class="upload-prompt">
                            <span class="upload-icon">📤</span>
                            <p>Drop review_urls.xlsx here</p>
                            <button class="link-btn" onclick="document.getElementById('reviewFileInput').click()">Browse</button>
                        </div>
                        <div id="reviewFileStatus" class="file-status"></div>
                    </div>
                </div>
            </div>

            <div id="uploadActions" class="upload-actions hidden">
                <div class="upload-summary">
                    <h4>📋 Ready to Upload</h4>
                    <ul id="uploadSummary"></ul>
                </div>
                <div class="action-buttons">
                    <button id="validateDataBtn" class="btn-secondary">🔍 Validate Data</button>
                    <button id="uploadAllBtn" class="btn-primary">📥 Upload All to Database</button>
                    <button id="clearAllBtn" class="btn-secondary">🗑️ Clear All</button>
                </div>
            </div>

            <div id="uploadProgress" class="upload-progress hidden">
                <h4>📊 Upload Progress</h4>
                <div class="progress-bar">
                    <div id="progressFill" class="progress-fill"></div>
                </div>
                <div id="progressStatus" class="progress-status"></div>
                <div id="progressLog" class="progress-log"></div>
            </div>
        `;
    }

    generateManualEntrySection() {
        return `
            <div class="section-header">
                <h3>➕ Manual Course Entry</h3>
                <p>Add individual courses manually</p>
            </div>

            <div class="manual-entry-form">
                <div class="form-row">
                    <div class="form-group">
                        <label>Course Number</label>
                        <input type="text" id="manualCourseNumber" placeholder="e.g., MA-1234-1">
                        <small>Next available: <span id="nextCourseNumber">Loading...</span></small>
                    </div>
                    <div class="form-group">
                        <label>Course Name</label>
                        <input type="text" id="manualCourseName" placeholder="e.g., Pebble Beach Golf Links">
                    </div>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label>Street Address</label>
                        <input type="text" id="manualAddress" placeholder="e.g., 1700 17 Mile Dr">
                    </div>
                    <div class="form-group">
                        <label>City</label>
                        <input type="text" id="manualCity" placeholder="e.g., Pebble Beach">
                    </div>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label>State</label>
                        <input type="text" id="manualState" placeholder="e.g., CA" maxlength="2">
                    </div>
                    <div class="form-group">
                        <label>Zip Code</label>
                        <input type="text" id="manualZipCode" placeholder="e.g., 93953">
                    </div>
                    <div class="form-group">
                        <label>County</label>
                        <input type="text" id="manualCounty" placeholder="e.g., Monterey">
                    </div>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label>Phone Number</label>
                        <input type="text" id="manualPhone" placeholder="e.g., (831) 624-3811">
                    </div>
                    <div class="form-group">
                        <label>Website</label>
                        <input type="text" id="manualWebsite" placeholder="e.g., www.pebblebeach.com">
                    </div>
                </div>

                <div class="form-actions">
                    <button id="addManualCourseBtn" class="btn-primary">➕ Add Course</button>
                    <button id="clearFormBtn" class="btn-secondary">🔄 Clear Form</button>
                </div>
            </div>
        `;
    }

    setupFileHandlers() {
        // File input handlers
        const initialInput = document.getElementById('initialFileInput');
        const initialDropZone = document.getElementById('initialDropZone');
        if (initialInput) {
            initialInput.addEventListener('change', (e) => this.handleFileSelect(e, 'initial'));
        }
        if (initialDropZone) {
            this.setupDropZone(initialDropZone, 'initial');
        }

        const googleInput = document.getElementById('googleFileInput');
        const googleDropZone = document.getElementById('googleDropZone');
        if (googleInput) {
            googleInput.addEventListener('change', (e) => this.handleFileSelect(e, 'google'));
        }
        if (googleDropZone) {
            this.setupDropZone(googleDropZone, 'google');
        }

        const reviewInput = document.getElementById('reviewFileInput');
        const reviewDropZone = document.getElementById('reviewDropZone');
        if (reviewInput) {
            reviewInput.addEventListener('change', (e) => this.handleFileSelect(e, 'reviews'));
        }
        if (reviewDropZone) {
            this.setupDropZone(reviewDropZone, 'reviews');
        }

        // Action buttons
        const validateBtn = document.getElementById('validateDataBtn');
        if (validateBtn) {
            validateBtn.addEventListener('click', () => this.validateData());
        }

        const uploadBtn = document.getElementById('uploadAllBtn');
        if (uploadBtn) {
            uploadBtn.addEventListener('click', () => this.uploadAllData());
        }

        const clearBtn = document.getElementById('clearAllBtn');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.clearAllFiles());
        }

        // Manual entry buttons
        const addManualBtn = document.getElementById('addManualCourseBtn');
        if (addManualBtn) {
            addManualBtn.addEventListener('click', () => this.addManualCourse());
        }

        const clearFormBtn = document.getElementById('clearFormBtn');
        if (clearFormBtn) {
            clearFormBtn.addEventListener('click', () => this.clearManualForm());
        }

        // Promote button
        const promoteBtn = document.getElementById('promoteDataBtn');
        if (promoteBtn) {
            promoteBtn.addEventListener('click', async () => {
                this.logProgress('info', '🚀 Starting promotion to primary_data...');

                if (!this.pipelineManager) {
                    this.logProgress('error', 'Pipeline manager not available');
                    return;
                }

                if (!this.pipelineManager.dataPromoter) {
                    this.logProgress('warning', 'Data promoter not initialized, attempting to create...');

                    try {
                        const module = await import('./primary-data-promoter.js');
                        this.pipelineManager.dataPromoter = new module.PrimaryDataPromoter(this.pipelineManager);
                        this.pipelineManager.dataPromoter.setDatabase(this.database || this.pipelineManager.database);
                        this.logProgress('success', 'Data promoter created successfully');
                    } catch (err) {
                        this.logProgress('error', `Failed to create data promoter: ${err.message}`);
                        return;
                    }
                }

                try {
                    const result = await this.pipelineManager.dataPromoter.promoteAllMissingCourses();
                    this.logProgress('success', `✅ Promotion complete! Created: ${result.created}, Updated: ${result.updated}, Failed: ${result.failed}`);
                } catch (error) {
                    this.logProgress('error', `❌ Promotion failed: ${error.message}`);
                }
            });
        }
    }

    setupDropZone(dropZone, type) {
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('drag-over');
        });

        dropZone.addEventListener('dragleave', () => {
            dropZone.classList.remove('drag-over');
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.processFile(files[0], type);
            }
        });
    }

    handleFileSelect(event, type) {
        const file = event.target.files[0];
        if (file) {
            this.processFile(file, type);
        }
    }

    async processFile(file, type) {
        if (!file.name.match(/\.(xlsx|xls)$/)) {
            this.logMessage('error', 'Please upload an Excel file (.xlsx or .xls)');
            return;
        }

        try {
            const data = await this.readExcelFile(file);
            this.uploadedFiles[type] = {
                name: file.name,
                data: data,
                rowCount: data.length
            };

            const statusDiv = document.getElementById(`${type}FileStatus`);
            if (statusDiv) {
                statusDiv.innerHTML = `✅ ${file.name} (${data.length} rows)`;
                statusDiv.classList.add('success');
            }

            this.updateUploadActions();

        } catch (error) {
            this.logMessage('error', `Failed to read ${file.name}: ${error.message}`);
        }
    }

    readExcelFile(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();

            reader.onload = (e) => {
                try {
                    const data = new Uint8Array(e.target.result);
                    const workbook = XLSX.read(data, { type: 'array' });
                    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
                    const jsonData = XLSX.utils.sheet_to_json(firstSheet);
                    resolve(jsonData);
                } catch (error) {
                    reject(error);
                }
            };

            reader.onerror = () => reject(new Error('Failed to read file'));
            reader.readAsArrayBuffer(file);
        });
    }

    updateUploadActions() {
        const hasFiles = Object.values(this.uploadedFiles).some(f => f !== null);
        const uploadActions = document.getElementById('uploadActions');

        if (uploadActions) {
            if (hasFiles) {
                uploadActions.classList.remove('hidden');
                const summary = document.getElementById('uploadSummary');
                if (summary) {
                    summary.innerHTML = Object.entries(this.uploadedFiles)
                        .filter(([_, file]) => file !== null)
                        .map(([type, file]) => `<li>${this.getFileTypeLabel(type)}: ${file.name} (${file.rowCount} rows)</li>`)
                        .join('');
                }
            } else {
                uploadActions.classList.add('hidden');
            }
        }
    }

    getFileTypeLabel(type) {
        const labels = {
            initial: 'Initial Course Upload',
            google: 'Google Places Data',
            reviews: 'Review URLs'
        };
        return labels[type] || type;
    }

    async validateData() {
        const progressDiv = document.getElementById('uploadProgress');
        const logDiv = document.getElementById('progressLog');

        if (progressDiv) progressDiv.classList.remove('hidden');
        if (logDiv) logDiv.innerHTML = '';

        try {
            if (this.uploadedFiles.initial) {
                this.logProgress('info', 'Validating USGolfData...');
                const initialData = this.uploadedFiles.initial.data;

                const requiredFields = ['cCourseNumber'];
                const missingFields = requiredFields.filter(field =>
                    !initialData[0].hasOwnProperty(field)
                );

                if (missingFields.length > 0) {
                    throw new Error(`Missing required fields in USGolfData: ${missingFields.join(', ')}`);
                }

                this.logProgress('success', `✅ USGolfData validated: ${initialData.length} courses`);
            }

            if (this.uploadedFiles.google) {
                this.logProgress('info', 'Validating Google Places data...');
                const googleData = this.uploadedFiles.google.data;

                if (!googleData[0].hasOwnProperty('cCourseNumber')) {
                    throw new Error('Missing cCourseNumber field in Google Places data');
                }

                this.logProgress('success', `✅ Google Places data validated: ${googleData.length} records`);
            }

            if (this.uploadedFiles.reviews) {
                this.logProgress('info', 'Validating Review URLs...');
                const reviewData = this.uploadedFiles.reviews.data;

                if (!reviewData[0].hasOwnProperty('cCourseNumber')) {
                    throw new Error('Missing cCourseNumber field in Review URLs');
                }

                this.logProgress('success', `✅ Review URLs validated: ${reviewData.length} records`);
            }

            this.logProgress('success', '✅ All data validated successfully!');

        } catch (error) {
            this.logProgress('error', `❌ Validation failed: ${error.message}`);
        }
    }

    async uploadAllData() {
        // Get database connection
        if (!this.database && this.pipelineManager && this.pipelineManager.database) {
            this.database = this.pipelineManager.database;
        }

        if (!this.database || !this.database.client) {
            this.logMessage('error', 'Database not connected. Please ensure you are connected to Supabase.');

            if (window.pipelineManager && window.pipelineManager.database) {
                this.database = window.pipelineManager.database;
                this.logMessage('info', 'Found database connection from global pipeline manager');
            } else {
                this.logMessage('error', 'Could not find database connection. Please check your connection.');
                return;
            }
        }

        const progressDiv = document.getElementById('uploadProgress');
        if (progressDiv) progressDiv.classList.remove('hidden');

        try {
            // Calculate total steps including promotion step
            const hasReviewUrls = this.uploadedFiles.reviews !== null;
            let totalSteps = Object.values(this.uploadedFiles).filter(f => f !== null).length;
            if (hasReviewUrls) totalSteps++; // Add promotion step
            let currentStep = 0;

            // 1. Upload Initial Course Data
            if (this.uploadedFiles.initial) {
                currentStep++;
                this.updateProgress(currentStep, totalSteps, 'Uploading initial course data...');
                await this.uploadInitialCourseData(this.uploadedFiles.initial.data);
            }

            // 2. Upload Google Places Data
            if (this.uploadedFiles.google) {
                currentStep++;
                this.updateProgress(currentStep, totalSteps, 'Uploading Google Places data...');
                await this.uploadGooglePlacesData(this.uploadedFiles.google.data);
            }

            // 3. PROMOTE TO PRIMARY_DATA if review URLs need to be uploaded
            if (hasReviewUrls) {
                currentStep++;
                this.updateProgress(currentStep, totalSteps, 'Creating primary_data records...');

                // Ensure promoter exists
                if (!this.pipelineManager?.dataPromoter) {
                    this.logProgress('info', 'Loading data promoter...');
                    try {
                        const module = await import('./primary-data-promoter.js');
                        this.pipelineManager.dataPromoter = new module.PrimaryDataPromoter(this.pipelineManager);
                        this.pipelineManager.dataPromoter.setDatabase(this.database);
                    } catch (err) {
                        this.logProgress('warning', `Could not load promoter: ${err.message}`);
                    }
                }

                if (this.pipelineManager?.dataPromoter) {
                    await this.pipelineManager.dataPromoter.promoteAllMissingCourses();
                    this.logProgress('success', '✅ Primary data records created');
                } else {
                    this.logProgress('warning', '⚠️ Skipping promotion - promoter not available');
                }
            }

            // 4. Upload Review URLs (now that primary_data exists)
            if (this.uploadedFiles.reviews) {
                currentStep++;
                this.updateProgress(currentStep, totalSteps, 'Uploading Review URLs...');
                await this.uploadReviewUrls(this.uploadedFiles.reviews.data);
            }

            this.logProgress('success', '🎉 All data uploaded successfully!');

            // Refresh course list
            if (this.pipelineManager && this.pipelineManager.loadCourses) {
                await this.pipelineManager.loadCourses();
            }

        } catch (error) {
            this.logProgress('error', `❌ Upload failed: ${error.message}`);
            console.error('Upload error details:', error);
        }
    }

    async uploadInitialCourseData(data) {
        let inserted = 0;
        let updated = 0;
        let failed = 0;
        const batchSize = 50;

        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            this.logProgress('info', `Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(data.length/batchSize)}...`);

            for (const row of batch) {
                const record = {
                    course_number: row['cCourseNumber'],
                    course_name: row['CoursesMasterT::CourseName'] || row['CourseName'],
                    club_number: this.cleanInteger(row['ClubNumber']),
                    course_number_only: this.cleanInteger(row['CourseNumber']),
                    street_address: row['CoursesMasterT::StreetAddress'],
                    city: row['CoursesMasterT::City'],
                    county: row['CoursesMasterT::County'],
                    state_or_region: row['CoursesMasterT::StateorRegion'],
                    zip_code: row['CoursesMasterT::Zip'],
                    phone_number: row['CoursesMasterT::PhoneNumber'],
                    website_url: row['CoursesMasterT::URL    4598'],
                    architect: row['CoursesMasterT::Architect'],
                    year_built_founded: this.cleanYear(row['CoursesMasterT::YearBuiltFounded']),
                    status_type: row['CoursesMasterT::StatusPublicPrivateResort'],
                    guest_policy: row['CoursesMasterT::GuestPolicy'],
                    email_address: row['CoursesMasterT::EmailAddress'],
                    total_par: this.cleanInteger(row['Par']),
                    total_holes: this.cleanInteger(row['Holes'] || row['CoursesMasterT::TotalHoles']),
                    course_rating: this.cleanNumeric(row['Rating']),
                    slope_rating: this.cleanInteger(row['Slope']),
                    total_length: this.cleanInteger(row['Length']),
                    created_at: new Date().toISOString()
                };

                try {
                    // First check if the record exists
                    const { data: existing } = await this.database.client
                        .from('initial_course_upload')
                        .select('course_number')
                        .eq('course_number', record.course_number)
                        .single();

                    if (existing) {
                        // Update existing record
                        const { error: updateError } = await this.database.client
                            .from('initial_course_upload')
                            .update(record)
                            .eq('course_number', record.course_number);

                        if (updateError) {
                            failed++;
                            console.error(`Update failed for ${record.course_number}:`, updateError);
                        } else {
                            updated++;
                        }
                    } else {
                        // Insert new record
                        const { error: insertError } = await this.database.client
                            .from('initial_course_upload')
                            .insert(record);

                        if (insertError) {
                            failed++;
                            console.error(`Insert failed for ${record.course_number}:`, insertError);
                        } else {
                            inserted++;
                        }
                    }
                } catch (err) {
                    failed++;
                    console.error(`Error processing ${record.course_number}:`, err);
                }
            }
        }

        this.logProgress('success', `✅ Processed ${data.length} courses: ${inserted} new, ${updated} updated, ${failed} failed`);
    }

    async uploadGooglePlacesData(data) {
        let inserted = 0;
        let updated = 0;
        let failed = 0;
        const batchSize = 50;

        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            this.logProgress('info', `Processing Google batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(data.length/batchSize)}...`);

            for (const row of batch) {
                const record = {
                    course_number: row['cCourseNumber'],
                    place_id: row['PlaceID'],
                    display_name: row['DisplayName'],
                    formatted_address: row['FormattedAddress'],
                    street_number: row['StreetNumber'],
                    route: row['Route'],
                    street_address: row['StreetAddress'],
                    city: row['City'],
                    state: row['State'],
                    zip_code: row['ZipCode'],
                    county: row['County'],
                    country: row['Country'],
                    latitude: this.cleanNumeric(row['Latitude']),
                    longitude: this.cleanNumeric(row['Longitude']),
                    primary_type: row['PrimaryType'],
                    website: row['Website'],
                    phone: row['Phone'],
                    opening_hours: row['OpeningHours'],
                    user_rating_count: this.cleanInteger(row['UserRatingCount']),
                    photo_reference: row['PhotoRef'],
                    google_maps_link: row['GoogleMapsLink'],
                    created_at: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                };

                try {
                    const { data: existing } = await this.database.client
                        .from('google_places_data')
                        .select('course_number')
                        .eq('course_number', record.course_number)
                        .single();

                    if (existing) {
                        const { error: updateError } = await this.database.client
                            .from('google_places_data')
                            .update(record)
                            .eq('course_number', record.course_number);

                        if (updateError) {
                            failed++;
                            console.error(`Update failed for ${record.course_number}:`, updateError);
                        } else {
                            updated++;
                        }
                    } else {
                        const { error: insertError } = await this.database.client
                            .from('google_places_data')
                            .insert(record);

                        if (insertError) {
                            failed++;
                            console.error(`Insert failed for ${record.course_number}:`, insertError);
                        } else {
                            inserted++;
                        }
                    }
                } catch (err) {
                    failed++;
                    console.error(`Error processing ${record.course_number}:`, err);
                }
            }
        }

        this.logProgress('success', `✅ Processed ${data.length} Google Places records: ${inserted} new, ${updated} updated, ${failed} failed`);
    }

    async uploadReviewUrls(data) {
        let inserted = 0;
        let updated = 0;
        let failed = 0;
        const batchSize = 50;

        for (let i = 0; i < data.length; i += batchSize) {
            const batch = data.slice(i, i + batchSize);
            this.logProgress('info', `Processing Review URLs batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(data.length/batchSize)}...`);

            for (const row of batch) {
                const record = {
                    course_number: row['cCourseNumber'],
                    golf_now_url: row['GolfNow URL'],
                    golf_pass_url: row['GolfPassURL'],
                    created_at: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                };

                try {
                    const { data: existing } = await this.database.client
                        .from('review_urls')
                        .select('course_number')
                        .eq('course_number', record.course_number)
                        .single();

                    if (existing) {
                        const { error: updateError } = await this.database.client
                            .from('review_urls')
                            .update(record)
                            .eq('course_number', record.course_number);

                        if (updateError) {
                            failed++;
                            console.error(`Update failed for ${record.course_number}:`, updateError);
                        } else {
                            updated++;
                        }
                    } else {
                        const { error: insertError } = await this.database.client
                            .from('review_urls')
                            .insert(record);

                        if (insertError) {
                            if (insertError.code === '23503') {
                                this.logProgress('warning', `⚠️ ${record.course_number}: No primary_data record exists`);
                            }
                            failed++;
                            console.error(`Insert failed for ${record.course_number}:`, insertError);
                        } else {
                            inserted++;
                        }
                    }
                } catch (err) {
                    failed++;
                    console.error(`Error processing ${record.course_number}:`, err);
                }
            }
        }

        this.logProgress('success', `✅ Processed ${data.length} Review URLs: ${inserted} new, ${updated} updated, ${failed} failed`);
        if (failed > 0) {
            this.logProgress('warning', `⚠️ ${failed} records failed - may need to run "Promote to Primary" first`);
        }
    }

    cleanInteger(value) {
        if (value === null || value === undefined || value === '') return null;
        if (typeof value === 'string' && value.includes('.')) {
            return parseInt(parseFloat(value));
        }
        return parseInt(value) || null;
    }

    cleanNumeric(value) {
        if (value === null || value === undefined || value === '') return null;
        return parseFloat(value) || null;
    }

    cleanYear(value) {
        if (!value) return null;

        const valueStr = String(value);

        if (valueStr.includes('/')) {
            const parts = valueStr.split('/');
            return this.cleanInteger(parts[0]);
        }

        if (valueStr.includes('-') && valueStr.length > 4) {
            const parts = valueStr.split('-');
            return this.cleanInteger(parts[0]);
        }

        return this.cleanInteger(value);
    }

    updateProgress(current, total, message) {
        const percent = Math.round((current / total) * 100);
        const progressFill = document.getElementById('progressFill');
        const statusDiv = document.getElementById('progressStatus');

        if (progressFill) {
            progressFill.style.width = `${percent}%`;
        }

        if (statusDiv) {
            statusDiv.textContent = `${message} (${percent}%)`;
        }
    }

    logProgress(type, message) {
        const logDiv = document.getElementById('progressLog');
        if (logDiv) {
            const entry = document.createElement('div');
            entry.className = `log-entry ${type}`;
            entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
            logDiv.appendChild(entry);
            logDiv.scrollTop = logDiv.scrollHeight;
        }

        if (this.pipelineManager && this.pipelineManager.utils) {
            this.pipelineManager.utils.logMessage(type, message);
        }
    }

    logMessage(type, message) {
        this.logProgress(type, message);
    }

    clearAllFiles() {
        this.uploadedFiles = {
            initial: null,
            google: null,
            reviews: null
        };

        ['initial', 'google', 'reviews'].forEach(type => {
            const statusDiv = document.getElementById(`${type}FileStatus`);
            if (statusDiv) {
                statusDiv.innerHTML = '';
                statusDiv.classList.remove('success');
            }
        });

        const uploadActions = document.getElementById('uploadActions');
        if (uploadActions) {
            uploadActions.classList.add('hidden');
        }

        const progressDiv = document.getElementById('uploadProgress');
        if (progressDiv) {
            progressDiv.classList.add('hidden');
        }
    }

    async addManualCourse() {
        if (!this.database) {
            this.logMessage('error', 'Database not connected');
            return;
        }

        const courseData = {
            course_number: document.getElementById('manualCourseNumber')?.value,
            course_name: document.getElementById('manualCourseName')?.value,
            street_address: document.getElementById('manualAddress')?.value,
            city: document.getElementById('manualCity')?.value,
            state_or_region: document.getElementById('manualState')?.value,
            zip_code: document.getElementById('manualZipCode')?.value,
            county: document.getElementById('manualCounty')?.value,
            phone_number: document.getElementById('manualPhone')?.value,
            website_url: document.getElementById('manualWebsite')?.value,
            created_at: new Date().toISOString()
        };

        if (!courseData.course_number || !courseData.course_name) {
            this.logMessage('error', 'Course Number and Name are required');
            return;
        }

        try {
            // First add to initial_course_upload
            const { error: initialError } = await this.database.client
                .from('initial_course_upload')
                .insert([courseData]);

            if (initialError) throw initialError;

            // Then create primary_data record
            const primaryData = {
                course_number: courseData.course_number,
                course_name: courseData.course_name,
                street_address: courseData.street_address,
                city: courseData.city,
                state: courseData.state_or_region,
                zip_code: courseData.zip_code,
                county: courseData.county,
                phone: courseData.phone_number,
                website: courseData.website_url,
                created_at: new Date().toISOString()
            };

            const { error: primaryError } = await this.database.client
                .from('primary_data')
                .insert([primaryData]);

            if (primaryError && primaryError.code !== '23505') {
                console.warn('Could not create primary_data record:', primaryError);
            }

            this.logMessage('success', `✅ Course ${courseData.course_number} added successfully`);
            this.clearManualForm();

            if (this.pipelineManager && this.pipelineManager.loadCourses) {
                await this.pipelineManager.loadCourses();
            }

        } catch (error) {
            this.logMessage('error', `Failed to add course: ${error.message}`);
        }
    }

    clearManualForm() {
        const fields = [
            'manualCourseNumber', 'manualCourseName', 'manualAddress',
            'manualCity', 'manualState', 'manualZipCode', 'manualCounty',
            'manualPhone', 'manualWebsite'
        ];

        fields.forEach(fieldId => {
            const field = document.getElementById(fieldId);
            if (field) field.value = '';
        });
    }

    async getNextCourseNumber() {
        if (!this.database && this.pipelineManager && this.pipelineManager.database) {
            this.database = this.pipelineManager.database;
        }

        if (!this.database || !this.database.client) {
            console.log('Database not available yet for getting next course number');
            setTimeout(() => this.getNextCourseNumber(), 1000);
            return;
        }

        try {
            const { data, error } = await this.database.client
                .from('initial_course_upload')
                .select('course_number')
                .order('course_number', { ascending: false })
                .limit(1);

            if (error) throw error;

            if (data && data.length > 0) {
                const lastNumber = data[0].course_number;
                const parts = lastNumber.match(/([A-Z]+)-(\d+)-(\d+)/);

                if (parts) {
                    const state = parts[1];
                    const courseNum = parseInt(parts[2]);
                    this.nextCourseNumber = `${state}-${courseNum + 1}-1`;
                } else {
                    this.nextCourseNumber = 'MA-2000-1';
                }
            } else {
                this.nextCourseNumber = 'MA-1000-1';
            }

            const nextNumberSpan = document.getElementById('nextCourseNumber');
            if (nextNumberSpan) {
                nextNumberSpan.textContent = this.nextCourseNumber;
            }

        } catch (error) {
            console.error('Error getting next course number:', error);
        }
    }
}

// Add CSS for the layout
const style = document.createElement('style');
style.textContent = `
.import-files-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 20px;
    margin: 20px 0;
}

.file-upload-card {
    background: white;
    border-radius: 8px;
    padding: 15px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.file-upload-card h4 {
    margin: 0 0 10px 0;
    color: #333;
}

.file-drop-zone {
    border: 2px dashed #ddd;
    border-radius: 8px;
    padding: 20px;
    text-align: center;
    transition: all 0.3s ease;
}

.file-drop-zone.drag-over {
    border-color: #4a7c59;
    background: #f0f7f4;
}

.file-status {
    margin-top: 10px;
    font-size: 14px;
}

.file-status.success {
    color: #28a745;
}

.upload-actions {
    background: white;
    border-radius: 8px;
    padding: 20px;
    margin: 20px 0;
}

.upload-summary {
    margin-bottom: 15px;
}

.upload-summary ul {
    margin: 10px 0;
    padding-left: 20px;
}

.action-buttons {
    display: flex;
    gap: 10px;
}

.upload-progress {
    background: white;
    border-radius: 8px;
    padding: 20px;
    margin: 20px 0;
}

.progress-bar {
    height: 20px;
    background: #f0f0f0;
    border-radius: 10px;
    overflow: hidden;
    margin: 10px 0;
}

.progress-fill {
    height: 100%;
    background: linear-gradient(135deg, #4a7c59, #2d5a3d);
    transition: width 0.3s ease;
}

.progress-log {
    max-height: 200px;
    overflow-y: auto;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 10px;
    margin-top: 10px;
    font-size: 12px;
    font-family: monospace;
}

.log-entry {
    margin: 2px 0;
}

.log-entry.success { color: #28a745; }
.log-entry.error { color: #dc3545; }
.log-entry.info { color: #17a2b8; }
.log-entry.warning { color: #ffc107; }

.manual-entry-form {
    background: white;
    border-radius: 8px;
    padding: 20px;
}

.form-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 15px;
    margin-bottom: 15px;
}

.form-group label {
    display: block;
    margin-bottom: 5px;
    font-weight: 500;
    color: #333;
}

.form-group input {
    width: 100%;
    padding: 8px;
    border: 1px solid #ddd;
    border-radius: 4px;
}

.form-group small {
    display: block;
    margin-top: 5px;
    color: #666;
    font-size: 12px;
}
`;
document.head.appendChild(style);
