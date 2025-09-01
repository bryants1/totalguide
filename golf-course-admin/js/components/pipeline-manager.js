/**
 * PIPELINE MANAGER (Option A - DB-safe)
 * - No top-level await/import side-effects
 * - Renders Excel/CSV containers correctly (return lines fixed)
 * - Initializes DataImportManager so Excel upload works
 * - Database is injected via setDatabase(db)
 */

import { DataImportManager } from './modules/data-import-manager.js';
import { PrimaryDataPromoter } from './modules/primary-data-promoter.js';

export class PipelineManager {
  constructor() {
    this.container = null;
    this.database = null;
    this.utils = this._createUtils();
    this.dataPromoter = new PrimaryDataPromoter(this);

    // Core manager (required for Excel/CSV)
    this.dataImport = new DataImportManager(this);

    // Optional managers (loaded lazily in initialize)
    this.googlePlaces = null;
    this.reviewURL = null;
    this.courseScraping = null;

    this.PIPELINE_STEPS = [];
  }

  setDatabase(database) {
    this.database = database;
    if (this.dataPromoter) {
    this.dataPromoter.setDatabase(database);
}
  }


  _createUtils() {
    return {
      logMessage: (type, message) => {
        const timestamp = new Date().toLocaleTimeString();
        const pref = `[${timestamp}]`;
        if (type === 'error') console.error(pref, message);
        else if (type === 'warn') console.warn(pref, message);
        else console.log(pref, message);

        const logContent = document.getElementById('logContent');
        if (logContent) {
          const logEntry = document.createElement('div');
          logEntry.className = `log-entry ${type}`;
          logEntry.innerHTML = `<span class="log-time">${pref}</span> ${message}`;
          logContent.appendChild(logEntry);
          logContent.scrollTop = logContent.scrollHeight;
          const entries = logContent.querySelectorAll('.log-entry');
          if (entries.length > 100) entries[0].remove();
        }
      }
    };
  }

  async initialize(container) {
    this.container = container;
    if (!this.container) {
      this.utils.logMessage('error', 'Pipeline container not found');
      return;
    }

    // Build the base UI (Excel/CSV section must be present for DataImportManager)
    this.container.innerHTML =
      '<div class="pipeline-container">' +
        this._generateSingleRunnerSection() +
        this._generateDataImportSection() +     // Excel/CSV containers
        this._generateGooglePlacesSection() +   // optional, will be populated if manager loads
        this._generateReviewURLSection() +      // optional
        this._generatePipelineStatusSection() +
      '</div>';

    // Initialize Excel/CSV manager (required)
    try {
      this.dataImport.setupEventListeners();
      this.utils.logMessage('success', '📥 DataImportManager initialized');
    } catch (err) {
      this.utils.logMessage('error', `DataImportManager failed: ${err.message}`);
    }

    // Lazily load optional managers - NO top-level await, no module-level side effects
    await this._lazyInitOptionals();

    // Populate simple status table if DB is available
    await this._populateTable();
  }

  async _lazyInitOptionals() {
    // Google Places
    try {
      const mod = await import('./modules/google-places-manager.js');
      if (mod && mod.GooglePlacesManager) {
        this.googlePlaces = new mod.GooglePlacesManager(this);
        if (this.googlePlaces.initializeUI) {
          await this.googlePlaces.initializeUI();
          this.utils.logMessage('success', '🌐 GooglePlacesManager initialized');
        }
      }
    } catch (e) {
      this.utils.logMessage('warn', `GooglePlacesManager not loaded: ${e.message}`);
    }

    // Review URL
    try {
      const mod = await import('./modules/review-url-manager.js');
      if (mod && mod.ReviewURLManager) {
        this.reviewURL = new mod.ReviewURLManager(this);
        if (this.reviewURL.initializeUI) {
          await this.reviewURL.initializeUI();
          this.utils.logMessage('success', '🔗 ReviewURLManager initialized');
        }
      }
    } catch (e) {
      this.utils.logMessage('warn', `ReviewURLManager not loaded: ${e.message}`);
    }

    // Course Scraping (optional; not used in this minimal build)
    try {
      const mod = await import('./modules/course-scraping-manager.js');
      if (mod && mod.CourseScrapingManager) {
        this.courseScraping = new mod.CourseScrapingManager(this);
        this.utils.logMessage('success', '🌐 CourseScrapingManager available');
      }
    } catch (e) {
      this.utils.logMessage('warn', `CourseScrapingManager not loaded: ${e.message}`);
    }
  }

  // ====== UI generators (return on the same line) ======
  _generateDataImportSection() {
    return `
      <!-- Excel Import Section -->
      <div class="pipeline-section excel-import-section">
        <!-- Will be populated by DataImportManager -->
      </div>

      <!-- CSV Import Section -->
      <div class="pipeline-section csv-import-section">
        <!-- Will be populated by DataImportManager -->
      </div>
    `;
  }

  _generateGooglePlacesSection() {
    return `
      <!-- Google Places Section -->
      <div class="pipeline-section google-places-section">
        <!-- Will be populated by GooglePlacesManager -->
      </div>
    `;
  }

  _generateReviewURLSection() {
    return `
      <!-- Review URL Section -->
      <div class="pipeline-section review-url-section">
        <!-- Will be populated by ReviewURLManager -->
      </div>
    `;
  }

  _generatePipelineStatusSection() {
    return `
      <!-- Pipeline Status Section -->
      <div class="pipeline-section">
        <div class="section-header">
          <h3>🔄 Pipeline Status</h3>
          <div class="header-actions">
            <button id="refreshStatusBtn" class="btn-secondary">🔄 Refresh</button>
            <select id="statusFilter" class="form-select">
              <option value="">All Statuses</option>
              <option value="pending">Pending</option>
              <option value="running">Running</option>
              <option value="complete">Complete</option>
              <option value="error">Error</option>
            </select>
          </div>
        </div>

        <div class="pipeline-stats">
          <div class="stat-card"><div class="stat-number" id="totalCourses">0</div><div class="stat-label">Total Courses</div></div>
          <div class="stat-card status-complete"><div class="stat-number" id="completeCourses">0</div><div class="stat-label">Complete</div></div>
          <div class="stat-card status-running"><div class="stat-number" id="runningCourses">0</div><div class="stat-label">Running</div></div>
          <div class="stat-card status-error"><div class="stat-number" id="errorCourses">0</div><div class="stat-label">Errors</div></div>
          <div class="stat-card status-pending"><div class="stat-number" id="pendingCourses">0</div><div class="stat-label">Pending</div></div>
        </div>

        <div class="bulk-actions">
          <div class="bulk-selection">
            <label class="checkbox-label"><input type="checkbox" id="selectAllCourses"> Select All (<span id="selectedCount">0</span>)</label>
            <span class="bulk-info" id="bulkInfo"></span>
          </div>
          <div class="bulk-controls">
            <button id="runSelectedBtn" class="btn-primary" disabled>▶️ Run Selected</button>
            <button id="retryErrorsBtn" class="btn-warning">🔄 Retry Errors</button>
            <button id="clearCompletedBtn" class="btn-secondary">🗑️ Clear Completed</button>
          </div>
        </div>

        <div class="pipeline-table-container">
          <table class="pipeline-table">
            <thead>
              <tr>
                <th><input type="checkbox" id="selectAllCheckbox"></th>
                <th>Course</th>
                <th>Status</th>
                <th>Step</th>
                <th>Progress</th>
                <th>Last Updated</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="pipelineTableBody">
              <tr class="loading-row">
                <td colspan="7">Loading courses...</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    `;
  }

  _generateSingleRunnerSection() {
    return `
      <!-- Single Course Runner Section -->
      <div class="pipeline-section">
        <div class="section-header">
          <h3>🎯 Single Course Runner</h3>
          <p>Run pipeline for individual courses with detailed logging</p>
        </div>

        <div class="single-runner-controls">
          <div class="runner-form">
            <select id="pipelineCourseSelect" class="form-select">
              <option value="">Select Course...</option>
            </select>
            <select id="pipelineStage" class="form-select">
              <option value="full">Full Pipeline (All Steps)</option>
            </select>
            <label class="checkbox-label"><input type="checkbox" id="forceUpdate"> Force Update</label>
            <button id="runSingleBtn" class="btn-primary">▶️ Run Pipeline</button>
          </div>

          <div class="runner-status" id="runnerStatus" style="display:none;">
            <div class="status-header">
              <span id="currentCourse">Course: None</span>
              <span id="currentStage">Stage: None</span>
              <span id="currentProgress">0%</span>
            </div>
            <div class="progress-bar-container"><div class="progress-bar" id="runnerProgressFill"></div></div>
          </div>

          <div class="execution-log">
            <div class="log-header">
              <h4>📋 Execution Log</h4>
              <button id="clearLogBtn" class="btn-secondary">Clear</button>
            </div>
            <div class="log-content" id="logContent">
              <div class="log-entry info">Ready to run pipeline...</div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  // ====== Minimal table plumbing (uses DB only if setDatabase was called) ======
  async _populateTable() {
    if (!this.database) return; // do nothing until app injects DB

    try {
      const courses = await this.database.getCourses();
      const tableBody = document.getElementById('pipelineTableBody');
      if (!tableBody) return;

      if (!courses || courses.length === 0) {
        tableBody.innerHTML = `
          <tr class="empty-row">
            <td colspan="7">No courses found. Import some courses to get started.</td>
          </tr>`;
        return;
      }

      tableBody.innerHTML = courses.map(c => `
        <tr class="pipeline-row status-pending" data-course="${c.course_number}">
          <td><input type="checkbox" class="course-checkbox" value="${c.course_number}"></td>
          <td>
            <div class="course-info">
              <div class="course-number">${c.course_number}</div>
              <div class="course-name">${c.course_name || 'N/A'}</div>
            </div>
          </td>
          <td><span class="status-indicator status-pending">PENDING</span></td>
          <td><div class="step-info"><span class="step-number">—</span><span class="step-name">—</span></div></td>
          <td><span class="progress-text">0%</span></td>
          <td><span class="last-updated">Never</span></td>
          <td><div class="action-buttons"><button class="btn-small btn-primary" onclick="pipelineManager?.runSingleCourseById?.('${c.course_number}')">▶️ Run</button></div></td>
        </tr>
      `).join('');

      this._setupTableCheckboxes();

    } catch (err) {
      this.utils.logMessage('error', `Failed to load courses for table: ${err.message}`);
    }
  }

  _setupTableCheckboxes() {
    const checkboxes = document.querySelectorAll('.course-checkbox');
    const selectAllCheckbox = document.getElementById('selectAllCheckbox');
    const selected = new Set();

    const updateSelectionUI = () => {
      const selectedCount = document.getElementById('selectedCount');
      const runSelectedBtn = document.getElementById('runSelectedBtn');
      if (selectedCount) selectedCount.textContent = selected.size;
      if (runSelectedBtn) runSelectedBtn.disabled = selected.size === 0;

      if (selectAllCheckbox) {
        const total = checkboxes.length;
        if (selected.size === 0) { selectAllCheckbox.indeterminate = false; selectAllCheckbox.checked = false; }
        else if (selected.size === total) { selectAllCheckbox.indeterminate = false; selectAllCheckbox.checked = true; }
        else { selectAllCheckbox.indeterminate = true; }
      }
    };

    checkboxes.forEach(cb => {
      cb.addEventListener('change', () => {
        if (cb.checked) selected.add(cb.value);
        else selected.delete(cb.value);
        updateSelectionUI();
      });
    });

    if (selectAllCheckbox) {
      selectAllCheckbox.addEventListener('change', (e) => {
        checkboxes.forEach(cb => {
          cb.checked = e.target.checked;
          if (e.target.checked) selected.add(cb.value);
          else selected.delete(cb.value);
        });
        updateSelectionUI();
      });
    }

    updateSelectionUI();
  }

  // Placeholder hook for older inline handlers
  async runSingleCourseById(courseNumber) {
    this.utils.logMessage('info', `▶️ Requested run for ${courseNumber} (hook not implemented in minimal Option A)`);
  }
}

// Export globally if legacy onclicks rely on it
window.PipelineManager = PipelineManager;
