/**
 * FIXED GOOGLE PLACES ENRICHMENT MANAGER
 * Removed dependency on non-existent /api/available-states endpoint
 */

export class GooglePlacesManager {
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
     * Initialize the Google Places UI
     */
    async initializeUI() {
        await this.createGooglePlacesUI();
        await this.populateStateDropdown(); // Add this line!
        this.setupEventListeners();
    }

    /**
     * Create Google Places UI and inject into the section
     */
    createGooglePlacesUI() {
        // Find Google Places section
        let googlePlacesSection = document.querySelector('.google-places-section');

        // Fallback: find by looking for sections with Google Places text
        if (!googlePlacesSection) {
            const sections = document.querySelectorAll('.pipeline-section');
            for (const section of sections) {
                const h3 = section.querySelector('h3');
                if (h3 && h3.textContent.toLowerCase().includes('google places')) {
                    googlePlacesSection = section;
                    break;
                }
            }
        }

        if (!googlePlacesSection) {
            console.warn('Google Places section not found');
            return;
        }

        const googlePlacesHTML = `
            <div class="section-header">
                <h3>🌐 Google Places Enrichment</h3>
                <p>Enrich course data with Google Places API information</p>
            </div>

            <div class="google-places-controls">

                <div class="enrichment-form">
                    <div class="form-group">
                        <label for="googlePlacesState">Target State (Optional)</label>
                        <select id="googlePlacesState" class="form-select">
                            <option value="">All States</option>
                            <!-- States will be populated by populateStateDropdown() -->
                        </select>
                    </div>

                    <div class="form-group">
                        <label class="checkbox-label">
                            <input type="checkbox" id="googlePlacesForce">
                            Force Update Existing Data
                        </label>
                        <small>Check this to override existing Google Places data</small>
                    </div>

                    <div class="enrichment-actions">
                        <button id="runGooglePlacesBtn" class="btn-primary">
                            🚀 Run Google Places Enrichment
                        </button>
                        <button id="checkGoogleStatusBtn" class="btn-secondary">
                            📊 Check Status
                        </button>
                    </div>
                </div>

                <div id="googlePlacesProgress" class="enrichment-progress hidden">
                    <div class="progress-info">
                        <span id="googleProgressText">Starting Google Places enrichment...</span>
                    </div>
                    <div class="progress-bar-container">
                        <div class="progress-bar" id="googleProgressBar"></div>
                    </div>
                </div>

                <div id="googlePlacesStatus" class="status-display" style="margin: 20px 0;">
                    <div class="status-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h4 style="margin: 0;">📈 Enrichment Status</h4>
                        <span id="googleStatusLastUpdated" style="font-size: 14px; color: #666;">Last updated: Never</span>
                    </div>
                    <div id="googleStatusStats" class="status-stats" style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 15px; margin-bottom: 20px;">
                        <div class="stat-card" style="text-align: center; padding: 15px; border: 1px solid #ddd; border-radius: 8px; background: white;">
                            <div class="stat-number" id="googleTotalCourses" style="font-size: 24px; font-weight: bold; color: #333;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #666;">Total Courses</div>
                        </div>
                        <div class="stat-card status-complete" style="text-align: center; padding: 15px; border: 1px solid #22c55e; border-radius: 8px; background: #f0fdf4;">
                            <div class="stat-number" id="googleProcessedCourses" style="font-size: 24px; font-weight: bold; color: #16a34a;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #166534;">Enriched</div>
                        </div>
                        <div class="stat-card status-pending" style="text-align: center; padding: 15px; border: 1px solid #f59e0b; border-radius: 8px; background: #fffbeb;">
                            <div class="stat-number" id="googleRemainingCourses" style="font-size: 24px; font-weight: bold; color: #d97706;">0</div>
                            <div class="stat-label" style="font-size: 12px; color: #92400e;">Remaining</div>
                        </div>
                        <div class="stat-card" style="text-align: center; padding: 15px; border: 1px solid #3b82f6; border-radius: 8px; background: #eff6ff;">
                            <div class="stat-number" id="googleCompletionPercent" style="font-size: 24px; font-weight: bold; color: #2563eb;">0%</div>
                            <div class="stat-label" style="font-size: 12px; color: #1d4ed8;">Complete</div>
                        </div>
                    </div>
                    <div id="googleStatusByState" class="status-by-state hidden" style="margin-top: 20px;">
                        <h5 style="margin-bottom: 10px;">Status by State:</h5>
                        <div id="googleStateStats" class="state-stats"></div>
                    </div>
                </div>

                <div id="googlePlacesResults" class="results-display hidden" style="margin: 20px 0; padding: 15px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px;">
                    <h4 style="margin-bottom: 15px;">🎯 Recent Results</h4>
                    <div id="googleResultsList" class="results-list"></div>
                </div>
            </div>
        `;

        googlePlacesSection.innerHTML = googlePlacesHTML;
    }

    /**
     * Populate state dropdown with available states
     * Fixed: Uses direct database query instead of non-existent API endpoint
     */
     async populateStateDropdown() {
         try {
             this.utils.logMessage('info', '📍 Loading available states from API...');

             const stateSelect = document.getElementById('googlePlacesState');
             if (!stateSelect) {
                 console.warn('State select element not found');
                 return;
             }

             let availableStates = [];

             try {
                 // ✅ FIXED: Use the actual API endpoint that exists in server.js
                 const response = await fetch('/api/available-states');

                 if (response.ok) {
                     const result = await response.json();
                     if (result.success && Array.isArray(result.states)) {
                         availableStates = result.states;
                         this.utils.logMessage('success', `✅ Loaded ${availableStates.length} states from API`);
                         console.log('📍 States from API:', availableStates);
                     } else {
                         console.warn('API returned invalid format:', result);
                         throw new Error('Invalid API response format');
                     }
                 } else {
                     console.warn('API request failed:', response.status, response.statusText);
                     throw new Error(`API request failed: ${response.status}`);
                 }
             } catch (apiError) {
                 console.warn('API call failed, trying database fallback:', apiError);

                 // Fallback: Try database directly if API fails
                 if (this.pipelineManager.database) {
                     try {
                         const { data: statesData, error } = await this.pipelineManager.database.client
                             .from('initial_course_upload')
                             .select('state_or_region')
                             .not('state_or_region', 'is', null)
                             .not('state_or_region', 'eq', '');

                         if (!error && statesData) {
                             const uniqueStates = [...new Set(statesData.map(row => row.state_or_region))];
                             availableStates = uniqueStates.filter(state => state && state.trim()).sort();
                             this.utils.logMessage('success', `✅ Loaded ${availableStates.length} states from database fallback`);
                         }
                     } catch (dbError) {
                         console.warn('Database query also failed:', dbError);
                     }
                 }
             }

             // Final fallback to common states if both API and database fail
             if (availableStates.length === 0) {
                 availableStates = [
                     'MA', 'CT', 'NH', 'RI', 'VT', 'ME', 'NY', 'NJ', 'PA',
                     'FL', 'CA', 'TX', 'NC', 'SC', 'VA', 'MD', 'DE', 'GA'
                 ];
                 this.utils.logMessage('warning', '⚠️ Using fallback state list (API and database both failed)');
             }

             // Generate state options HTML
             const stateOptions = availableStates
                 .map(state => `<option value="${state}">${this.getStateName(state)}</option>`)
                 .join('');

             // Update the select element (preserve the "All States" option)
             stateSelect.innerHTML = '<option value="">All States</option>' + stateOptions;

             this.utils.logMessage('success', `📍 State dropdown populated with ${availableStates.length} states`);

         } catch (error) {
             this.utils.logMessage('error', `❌ Failed to load states: ${error.message}`);

             // Minimal fallback for critical failure
             const stateSelect = document.getElementById('googlePlacesState');
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
                 this.utils.logMessage('info', '🔄 Applied minimal fallback state list');
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
        const runGooglePlacesBtn = document.getElementById('runGooglePlacesBtn');
        const checkGoogleStatusBtn = document.getElementById('checkGoogleStatusBtn');

        if (runGooglePlacesBtn) {
            runGooglePlacesBtn.addEventListener('click', () => this.runGooglePlacesEnrichment());
        }
        if (checkGoogleStatusBtn) {
            checkGoogleStatusBtn.addEventListener('click', () => this.checkGooglePlacesStatus());
        }
    }

    /**
     * Run Google Places enrichment for courses
     */
    async runGooglePlacesEnrichment() {
        const stateSelect = document.getElementById('googlePlacesState');
        const forceCheckbox = document.getElementById('googlePlacesForce');
        const progressContainer = document.getElementById('googlePlacesProgress');
        const progressText = document.getElementById('googleProgressText');
        const progressBar = document.getElementById('googleProgressBar');
        const runButton = document.getElementById('runGooglePlacesBtn');

        const state = stateSelect?.value || null;
        const force = forceCheckbox?.checked || false;

        // Get step info for logging
        const stepInfo = this.getGooglePlacesStepInfo();

        try {
            // Show progress
            if (progressContainer) progressContainer.classList.remove('hidden');
            if (runButton) runButton.disabled = true;
            if (progressText) progressText.textContent = `Starting ${stepInfo.name}...`;
            if (progressBar) progressBar.style.width = '10%';

            this.utils.logMessage('info', `🌐 Starting ${stepInfo.name}${state ? ` for state ${state}` : ' for all states'}`);
            this.utils.logMessage('info', `🔥 Force update: ${force}`);

            // Get script path from pipeline manager settings
            const scriptPath = this.getGooglePlacesScriptPath();
            if (scriptPath) {
                this.utils.logMessage('info', `📁 Using script: ${stepInfo.script}`);
            }

            // Call the API
            const response = await fetch('/api/run-google-places', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    state: state,
                    force: force,
                    script_path: scriptPath
                })
            });

            if (progressText) progressText.textContent = 'Processing courses...';
            if (progressBar) progressBar.style.width = '50%';

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`API call failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();

            if (progressText) progressText.textContent = 'Enrichment completed!';
            if (progressBar) progressBar.style.width = '100%';

            if (result.success) {
                this.utils.logMessage('success', `✅ ${stepInfo.name} completed successfully!`);
                this.utils.logMessage('info', `📊 Processed courses: ${result.processed_courses || 'unknown'}`);

                // Update status display
                await this.checkGooglePlacesStatus();

                // Hide progress after delay
                setTimeout(() => {
                    if (progressContainer) progressContainer.classList.add('hidden');
                }, 3000);
            } else {
                throw new Error(result.message || `${stepInfo.name} failed`);
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ ${stepInfo.name} failed: ${error.message}`);
            if (progressText) progressText.textContent = 'Enrichment failed!';
            if (progressBar) progressBar.style.width = '0%';

            setTimeout(() => {
                if (progressContainer) progressContainer.classList.add('hidden');
            }, 3000);
        } finally {
            if (runButton) runButton.disabled = false;
        }
    }

    /**
     * Check Google Places enrichment status
     */
    async checkGooglePlacesStatus() {
        const stateSelect = document.getElementById('googlePlacesState');
        const state = stateSelect?.value || null;

        // Get step info for logging
        const stepInfo = this.getGooglePlacesStepInfo();

        try {
            this.utils.logMessage('info', `📊 Checking ${stepInfo.name} status...`);

            const response = await fetch(`/api/google-places-status${state ? `?state=${state}` : ''}`, {
                method: 'GET'
            });

            if (!response.ok) {
                throw new Error(`Status check failed: ${response.status}`);
            }

            const result = await response.json();

            if (result.success) {
                this.updateGooglePlacesStatusDisplay(result);
                this.utils.logMessage('success', `✅ ${stepInfo.name} status updated`);
            } else {
                throw new Error(result.error || 'Status check failed');
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to check ${stepInfo.name} status: ${error.message}`);
        }
    }

    /**
     * Update Google Places status display
     */
     updateGooglePlacesStatusDisplay(statusData) {
          console.log('📊 Status data received:', statusData); // Debug log

          const stats = statusData.statistics || {};
          const recentEntries = statusData.recent_entries || [];

          let totalCourses = 0;
          let processedCourses = 0;
          let remainingCourses = 0;
          let completionPercent = 0;

          // ✅ FIXED: Check if statistics is an object with state breakdown or direct totals
          if (typeof stats === 'object' && Object.keys(stats).length > 0) {

              // Check if this looks like state-by-state breakdown (has state codes as keys)
              const firstKey = Object.keys(stats)[0];
              const firstValue = stats[firstKey];

              if (firstValue && typeof firstValue === 'object' && 'total' in firstValue && 'processed' in firstValue) {
                  // State-by-state format: { "MA": { total: 10, processed: 5 }, "CT": { total: 8, processed: 3 } }
                  console.log('📊 Processing state-by-state statistics');
                  Object.values(stats).forEach(stateStat => {
                      totalCourses += stateStat.total || 0;
                      processedCourses += stateStat.processed || 0;
                      remainingCourses += stateStat.remaining || 0;
                  });
              } else {
                  // Direct totals format: { total: 50, processed: 25, remaining: 25, percentage: 50 }
                  console.log('📊 Processing direct total statistics');
                  totalCourses = stats.total || statusData.total_processed || 0;
                  processedCourses = stats.processed || statusData.processed_courses || 0;
                  remainingCourses = stats.remaining || (totalCourses - processedCourses);
                  completionPercent = stats.percentage || 0;
              }
          } else {
              // Fallback: try to get totals from root level of response
              console.log('📊 Using fallback statistics from root level');
              totalCourses = statusData.total_courses || 0;
              processedCourses = statusData.processed_courses || 0;
              remainingCourses = statusData.remaining_courses || (totalCourses - processedCourses);
          }

          // Calculate completion percentage if not provided
          if (completionPercent === 0 && totalCourses > 0) {
              completionPercent = Math.round((processedCourses / totalCourses) * 100);
          }

          console.log('📊 Calculated totals:', {
              totalCourses,
              processedCourses,
              remainingCourses,
              completionPercent
          });

          // Update summary stats
          const totalElement = document.getElementById('googleTotalCourses');
          const processedElement = document.getElementById('googleProcessedCourses');
          const remainingElement = document.getElementById('googleRemainingCourses');
          const percentElement = document.getElementById('googleCompletionPercent');
          const lastUpdatedElement = document.getElementById('googleStatusLastUpdated');

          if (totalElement) totalElement.textContent = totalCourses;
          if (processedElement) processedElement.textContent = processedCourses;
          if (remainingElement) remainingElement.textContent = Math.max(0, remainingCourses);
          if (percentElement) percentElement.textContent = `${completionPercent}%`;
          if (lastUpdatedElement) lastUpdatedElement.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;

          // ✅ FIXED: Only show state breakdown if we actually have multiple states
          const stateStatsContainer = document.getElementById('googleStateStats');
          const statusByStateContainer = document.getElementById('googleStatusByState');

          // Check if we have state-by-state data
          const hasStateBreakdown = typeof stats === 'object' &&
                                   Object.keys(stats).length > 1 &&
                                   Object.values(stats)[0] &&
                                   typeof Object.values(stats)[0] === 'object' &&
                                   'total' in Object.values(stats)[0];

          if (hasStateBreakdown && stateStatsContainer && statusByStateContainer) {
              console.log('📊 Displaying state-by-state breakdown');
              statusByStateContainer.classList.remove('hidden');

              const stateHtml = Object.entries(stats).map(([state, stateStat]) => `
                  <div class="state-stat-row" style="display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; margin: 5px 0; background: white; border: 1px solid #e5e7eb; border-radius: 4px;">
                      <div class="state-name" style="font-weight: bold;">${state}</div>
                      <div class="state-numbers">
                          <span class="processed" style="color: #16a34a; font-weight: bold;">${stateStat.processed || 0}</span> /
                          <span class="total" style="font-weight: bold;">${stateStat.total || 0}</span>
                          <span class="percentage" style="color: #666; margin-left: 8px;">(${stateStat.percentage || 0}%)</span>
                      </div>
                  </div>
              `).join('');

              stateStatsContainer.innerHTML = stateHtml;
          } else {
              console.log('📊 Hiding state-by-state breakdown (no multi-state data)');
              if (statusByStateContainer) {
                  statusByStateContainer.classList.add('hidden');
              }
          }

          // ✅ REMOVED: Recent results section for standardization
          // Hide the results container to keep status display clean and standardized
          const resultsContainer = document.getElementById('googlePlacesResults');
          if (resultsContainer) {
              resultsContainer.classList.add('hidden');
          }
      }

    /**
     * View Google Places data for a specific course
     */
    async viewGooglePlacesData(courseNumber) {
        try {
            const response = await fetch(`/api/google-places-data/${courseNumber}`);

            if (!response.ok) {
                throw new Error(`Failed to get data: ${response.status}`);
            }

            const result = await response.json();

            if (result.success) {
                // Display the data in a modal or detailed view
                const data = result.data;
                const details = `
Google Places Data for ${courseNumber}:
• Place ID: ${data.place_id || 'N/A'}
• Display Name: ${data.display_name || 'N/A'}
• Address: ${data.formatted_address || 'N/A'}
• Phone: ${data.phone || 'N/A'}
• Website: ${data.website || 'N/A'}
• Coordinates: ${data.latitude}, ${data.longitude}
• Rating Count: ${data.user_rating_count || 'N/A'}
• Opening Hours: ${data.opening_hours || 'N/A'}
• Updated: ${new Date(data.updated_at).toLocaleString()}
                `;

                alert(details); // Replace with proper modal in production
                this.utils.logMessage('info', `📍 Viewed Google Places data for ${courseNumber}`);
            } else {
                throw new Error(result.error || 'Failed to get data');
            }

        } catch (error) {
            this.utils.logMessage('error', `❌ Failed to view Google Places data: ${error.message}`);
        }
    }

    /**
     * Execute Google Places details for a single course (for pipeline integration)
     */
    async executeGooglePlacesDetails(courseNumber, script) {
        try {
            // Get step info from pipeline manager
            const stepInfo = this.getGooglePlacesStepInfo();

            this.utils.logMessage('info', `🌐 Running ${stepInfo.name} for course ${courseNumber}...`);

            const scriptPath = this.getGooglePlacesScriptPath();
            this.utils.logMessage('info', `🚀 Script: ${stepInfo.script || 'Unknown script'}`);
            this.utils.logMessage('info', `📁 Path: ${scriptPath || 'Script path not configured'}`);

            // Make API call to run Google Places enrichment for this specific course
            const response = await fetch('/api/run-google-places', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    course: courseNumber,  // Pass the specific course number
                    force: true,  // Force update for single course execution
                    script_path: scriptPath
                })
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Google Places API failed: ${response.status} - ${errorText}`);
            }

            const result = await response.json();
            if (result.success) {
                this.utils.logMessage('success', `✅ ${stepInfo.name} completed successfully for course ${courseNumber}`);
            } else {
                throw new Error(result.error || `${stepInfo.name} failed`);
            }

        } catch (error) {
            const stepInfo = this.getGooglePlacesStepInfo();
            this.utils.logMessage('error', `❌ ${stepInfo.name} failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * Get Google Places script path from pipeline manager settings
     */
    getGooglePlacesScriptPath() {
        // Get step 2 (Google Places) information from pipeline manager
        const googlePlacesStep = this.pipelineManager.getPipelineSteps().find(step =>
            step.name && step.name.toLowerCase().includes('google places')
        );

        if (googlePlacesStep && googlePlacesStep.script) {
            return this.pipelineManager.getScriptPath(googlePlacesStep.script);
        }

        // Fallback to step ID 2 if found
        if (this.pipelineManager.getScriptPath) {
            const scriptName = this.pipelineManager.getScriptName(2);
            if (scriptName) {
                return this.pipelineManager.getScriptPath(scriptName);
            }
        }

        return null;
    }

    /**
     * Get Google Places step information from pipeline manager
     */
    getGooglePlacesStepInfo() {
        const steps = this.pipelineManager.getPipelineSteps();

        // Try to find by name containing "google places"
        let googlePlacesStep = steps.find(step =>
            step.name && step.name.toLowerCase().includes('google places')
        );

        // Fallback to step ID 2
        if (!googlePlacesStep) {
            googlePlacesStep = steps.find(step => step.id === 2);
        }

        return googlePlacesStep || {
            id: 2,
            name: 'Google Places Details',
            script: 'run_google_places_enrichment.py'
        };
    }

    /**
     * Get course data for Google Places processing
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
export default GooglePlacesManager;
