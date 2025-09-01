/**
 * EDIT MODAL MODULE - COMPLETE VERSION
 * Handles field editing modal functionality with full input types and validation
 */

import { formatFieldName, showStatus } from './utils.js';

export class EditModal {
    constructor(databaseManager) {
        this.db = databaseManager;
        this.currentEditField = null;
        this.currentCourseNumber = null;
        this.onSaveCallback = null;
        this.modal = null;
        this.form = null;
    }

    init() {
        this.modal = document.getElementById('editModal');
        this.form = document.getElementById('editForm');

        if (!this.modal || !this.form) {
            console.error('Edit modal elements not found');
            return;
        }

        // Set up event listeners
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Form submission
        this.form.addEventListener('submit', (e) => this.handleSubmit(e));

        // Click outside to close
        this.modal.addEventListener('click', (e) => {
            if (e.target === this.modal) {
                this.close();
            }
        });

        // Escape key to close
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.classList.contains('show')) {
                this.close();
            }
        });
    }

    open(fieldName, currentValue, courseNumber, onSaveCallback) {
        this.currentEditField = fieldName;
        this.currentCourseNumber = courseNumber;
        this.onSaveCallback = onSaveCallback;

        // Decode the value if it was URI encoded
        try {
            currentValue = decodeURIComponent(currentValue);
        } catch (e) {
            console.warn('Failed to decode value:', currentValue);
        }

        document.getElementById('editModalTitle').textContent = `Edit ${formatFieldName(fieldName)}`;

        // Remove existing input
        const existingInput = document.getElementById('editFieldValue');
        if (existingInput) {
            existingInput.remove();
        }

        // Create appropriate input type based on field
        const inputElement = this.createInputElement(fieldName, currentValue);

        // Insert the input element
        const labelElement = document.querySelector('#editForm label');
        labelElement.parentNode.insertBefore(inputElement, labelElement.nextSibling);

        this.modal.classList.add('show');

        // Focus on input
        setTimeout(() => {
            inputElement.focus();
        }, 100);
    }

    close() {
        this.modal.classList.remove('show');
        this.currentEditField = null;
        this.currentCourseNumber = null;
        this.onSaveCallback = null;
    }
    openTeeRowEdit(teeData, courseNumber, onSaveCallback) {
        this.currentEditField = 'tee_row';
        this.currentCourseNumber = courseNumber;
        this.onSaveCallback = onSaveCallback;
        this.currentTeeData = teeData;

        document.getElementById('editModalTitle').textContent = `Edit ${teeData.tee_name || 'Unknown Tee'} Yardages`;

        // Remove existing input
        const existingInput = document.getElementById('editFieldValue');
        if (existingInput) {
            existingInput.remove();
        }

        // Create tee editing form
        const formContainer = this.createTeeEditForm(teeData);

        // Insert the form
        const labelElement = document.querySelector('#editForm label');
        labelElement.parentNode.insertBefore(formContainer, labelElement.nextSibling);

        this.modal.classList.add('show');
    }

    // Add this method to EditModal class
    openParRowEdit(parData, courseNumber, onSaveCallback) {
        this.currentEditField = 'par_row';
        this.currentCourseNumber = courseNumber;
        this.onSaveCallback = onSaveCallback;
        this.currentParData = parData;

        document.getElementById('editModalTitle').textContent = 'Edit Par Values';

        // Remove existing input
        const existingInput = document.getElementById('editFieldValue');
        if (existingInput) {
            existingInput.remove();
        }

        // Create par editing form
        const formContainer = this.createParEditForm(parData);

        // Insert the form
        const labelElement = document.querySelector('#editForm label');
        labelElement.parentNode.insertBefore(formContainer, labelElement.nextSibling);

        this.modal.classList.add('show');
    }

    async handleTeeRowSubmit() {
        const formData = new FormData(this.form);
        const updateData = {
            id: this.currentTeeData.id,
            course_number: this.currentCourseNumber,
            tee_name: formData.get('tee_name') || null,
            out_9: parseInt(formData.get('out_9')) || null,
            in_9: parseInt(formData.get('in_9')) || null,
            total_yardage: parseInt(formData.get('total_yardage')) || null
        };

        // Add all hole yardages
        for (let i = 1; i <= 18; i++) {
            const value = formData.get(`hole_${i}`);
            updateData[`hole_${i}`] = value ? parseInt(value) : null;
        }

        await this.db.updateTeeRow(updateData);
        showStatus(`✅ Successfully updated ${updateData.tee_name} yardages`, 'success', 'connectionStatus');
    }

    async handleParRowSubmit() {
        const formData = new FormData(this.form);
        const updateData = {
            id: this.currentParData.id,
            course_number: this.currentCourseNumber,
            out_9: parseInt(formData.get('out_9')) || null,
            in_9: parseInt(formData.get('in_9')) || null,
            total_par: parseInt(formData.get('total_par')) || null,
            verified: formData.get('verified') === 'true'
        };

        // Add all hole pars
        for (let i = 1; i <= 18; i++) {
            const value = formData.get(`hole_${i}`);
            updateData[`hole_${i}`] = value ? parseInt(value) : null;
        }

        await this.db.updateParRow(updateData);
        showStatus('✅ Successfully updated par values', 'success', 'connectionStatus');
    }

    // Add this method to EditModal class
    createTeeEditForm(teeData) {
        const container = document.createElement('div');
        container.id = 'editFieldValue';
        container.style.width = '100%';

        container.innerHTML = `
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(80px, 1fr)); gap: 10px; margin-bottom: 15px;">
                <div>
                    <label style="font-size: 12px; color: #666;">Tee Name</label>
                    <input type="text" name="tee_name" value="${teeData.tee_name || ''}" style="width: 100%; padding: 5px;">
                </div>
            </div>

            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 5px 0; color: #333;">Front 9 Yardages</h4>
                <div style="display: grid; grid-template-columns: repeat(9, 1fr); gap: 8px;">
                    ${[1,2,3,4,5,6,7,8,9].map(hole => `
                        <div>
                            <label style="font-size: 12px; color: #666;">${hole}</label>
                            <input type="number" name="hole_${hole}" value="${teeData[`hole_${hole}`] || ''}"
                                   style="width: 100%; padding: 5px; font-size: 13px;" min="50" max="700">
                        </div>
                    `).join('')}
                </div>
            </div>

            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 5px 0; color: #333;">Back 9 Yardages</h4>
                <div style="display: grid; grid-template-columns: repeat(9, 1fr); gap: 8px;">
                    ${[10,11,12,13,14,15,16,17,18].map(hole => `
                        <div>
                            <label style="font-size: 12px; color: #666;">${hole}</label>
                            <input type="number" name="hole_${hole}" value="${teeData[`hole_${hole}`] || ''}"
                                   style="width: 100%; padding: 5px; font-size: 13px;" min="50" max="700">
                        </div>
                    `).join('')}
                </div>
            </div>

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px;">
                <div>
                    <label style="font-size: 12px; color: #666;">Out 9 Total</label>
                    <input type="number" name="out_9" value="${teeData.out_9 || ''}"
                           style="width: 100%; padding: 5px;" min="500" max="4000">
                </div>
                <div>
                    <label style="font-size: 12px; color: #666;">In 9 Total</label>
                    <input type="number" name="in_9" value="${teeData.in_9 || ''}"
                           style="width: 100%; padding: 5px;" min="500" max="4000">
                </div>
                <div>
                    <label style="font-size: 12px; color: #666;">Total Yardage</label>
                    <input type="number" name="total_yardage" value="${teeData.total_yardage || ''}"
                           style="width: 100%; padding: 5px;" min="1000" max="8000">
                </div>
            </div>
        `;

        return container;
    }

    // Add this method to EditModal class
    createParEditForm(parData) {
        const container = document.createElement('div');
        container.id = 'editFieldValue';
        container.style.width = '100%';

        container.innerHTML = `
            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 5px 0; color: #333;">Front 9 Par Values</h4>
                <div style="display: grid; grid-template-columns: repeat(9, 1fr); gap: 8px;">
                    ${[1,2,3,4,5,6,7,8,9].map(hole => `
                        <div>
                            <label style="font-size: 12px; color: #666;">${hole}</label>
                            <select name="hole_${hole}" style="width: 100%; padding: 5px; font-size: 13px;">
                                <option value="">-</option>
                                <option value="3" ${parData[`hole_${hole}`] == 3 ? 'selected' : ''}>3</option>
                                <option value="4" ${parData[`hole_${hole}`] == 4 ? 'selected' : ''}>4</option>
                                <option value="5" ${parData[`hole_${hole}`] == 5 ? 'selected' : ''}>5</option>
                                <option value="6" ${parData[`hole_${hole}`] == 6 ? 'selected' : ''}>6</option>
                            </select>
                        </div>
                    `).join('')}
                </div>
            </div>

            <div style="margin-bottom: 15px;">
                <h4 style="margin: 10px 0 5px 0; color: #333;">Back 9 Par Values</h4>
                <div style="display: grid; grid-template-columns: repeat(9, 1fr); gap: 8px;">
                    ${[10,11,12,13,14,15,16,17,18].map(hole => `
                        <div>
                            <label style="font-size: 12px; color: #666;">${hole}</label>
                            <select name="hole_${hole}" style="width: 100%; padding: 5px; font-size: 13px;">
                                <option value="">-</option>
                                <option value="3" ${parData[`hole_${hole}`] == 3 ? 'selected' : ''}>3</option>
                                <option value="4" ${parData[`hole_${hole}`] == 4 ? 'selected' : ''}>4</option>
                                <option value="5" ${parData[`hole_${hole}`] == 5 ? 'selected' : ''}>5</option>
                                <option value="6" ${parData[`hole_${hole}`] == 6 ? 'selected' : ''}>6</option>
                            </select>
                        </div>
                    `).join('')}
                </div>
            </div>

            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;">
                <div>
                    <label style="font-size: 12px; color: #666;">Out 9 Par</label>
                    <input type="number" name="out_9" value="${parData.out_9 || ''}"
                           style="width: 100%; padding: 5px;" min="27" max="45">
                </div>
                <div>
                    <label style="font-size: 12px; color: #666;">In 9 Par</label>
                    <input type="number" name="in_9" value="${parData.in_9 || ''}"
                           style="width: 100%; padding: 5px;" min="27" max="45">
                </div>
                <div>
                    <label style="font-size: 12px; color: #666;">Total Par</label>
                    <input type="number" name="total_par" value="${parData.total_par || ''}"
                           style="width: 100%; padding: 5px;" min="54" max="90">
                </div>
                <div>
                    <label style="font-size: 12px; color: #666;">Verified</label>
                    <select name="verified" style="width: 100%; padding: 5px;">
                        <option value="false" ${!parData.verified ? 'selected' : ''}>No</option>
                        <option value="true" ${parData.verified ? 'selected' : ''}>Yes</option>
                    </select>
                </div>
            </div>
        `;

        return container;
    }


    createInputElement(fieldName, currentValue) {
        const booleanFields = [
            'is_18_hole', 'is_9_hole', 'is_par_3_course', 'is_executive_course',
            'has_ocean_views', 'has_scenic_views', 'has_driving_range',
            'has_practice_green', 'has_short_game_area', 'has_clubhouse',
            'has_pro_shop', 'has_locker_rooms', 'has_showers', 'has_beverage_cart',
            'has_banquet_facilities', 'process'
        ];

        const textAreaFields = [
            'course_description', 'food_beverage_options', 'food_beverage_description',
            'signature_holes', 'pricing_information', 'course_policies',
            'course_history_general', 'notable_events', 'design_features', 'recognitions',
            'rankings', 'certifications', 'amateur_tournaments', 'professional_tournaments',
            'charity_events', 'sustainability_general', 'sustainability_certifications',
            'sustainability_practices', 'driving_range_details', 'practice_green_details',
            'short_game_area_details', 'clubhouse_details', 'pro_shop_details',
            'locker_room_details', 'shower_details', 'beverage_cart_details',
            'banquet_facilities_details', 'food_beverage_description', 'event_contact',
            'pricing_level_description', 'formatted_address', 'opening_hours'
        ];

        const numberFields = [
            'total_holes', 'total_par', 'course_rating', 'slope_rating', 'total_length',
            'year_built_founded', 'latitude', 'longitude', 'user_rating_count', 'typical_18_hole_rate'
        ];

        let inputElement;

        if (booleanFields.includes(fieldName)) {
            inputElement = document.createElement('select');
            inputElement.innerHTML = `
                <option value="">Select...</option>
                <option value="true" ${currentValue === 'true' || currentValue === true ? 'selected' : ''}>Yes</option>
                <option value="false" ${currentValue === 'false' || currentValue === false ? 'selected' : ''}>No</option>
            `;
        } else if (fieldName === 'course_type') {
            inputElement = document.createElement('select');
            const courseTypeOptions = ['Public', 'Private', 'Semi-Private', 'Municipal', 'Resort'];
            inputElement.innerHTML = `
                <option value="">Select...</option>
                ${courseTypeOptions.map(option =>
                    `<option value="${option}" ${currentValue === option ? 'selected' : ''}>${option}</option>`
                ).join('')}
            `;
        } else if (fieldName === 'status_type') {
            inputElement = document.createElement('select');
            const statusTypeOptions = ['Public', 'Private', 'Semi-Private', 'Municipal'];
            inputElement.innerHTML = `
                <option value="">Select...</option>
                ${statusTypeOptions.map(option =>
                    `<option value="${option}" ${currentValue === option ? 'selected' : ''}>${option}</option>`
                ).join('')}
            `;
        } else if (fieldName === 'pricing_level') {
            inputElement = document.createElement('select');
            inputElement.innerHTML = `
                <option value="">Select...</option>
                <option value="1" ${currentValue == '1' ? 'selected' : ''}>Level 1 (Budget)</option>
                <option value="2" ${currentValue == '2' ? 'selected' : ''}>Level 2 (Value)</option>
                <option value="3" ${currentValue == '3' ? 'selected' : ''}>Level 3 (Mid-Range)</option>
                <option value="4" ${currentValue == '4' ? 'selected' : ''}>Level 4 (Premium)</option>
                <option value="5" ${currentValue == '5' ? 'selected' : ''}>Level 5 (Luxury)</option>
            `;
        } else if (fieldName === 'primary_type') {
            inputElement = document.createElement('select');
            const primaryTypeOptions = [
                'tourist_attraction', 'establishment', 'point_of_interest',
                'golf_course', 'recreational_facility', 'sports_complex'
            ];
            inputElement.innerHTML = `
                <option value="">Select...</option>
                ${primaryTypeOptions.map(option =>
                    `<option value="${option}" ${currentValue === option ? 'selected' : ''}>${option.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}</option>`
                ).join('')}
            `;
        } else if (textAreaFields.includes(fieldName)) {
            inputElement = document.createElement('textarea');
            inputElement.value = currentValue || '';
            inputElement.rows = 6;
            inputElement.style.minHeight = '150px';
            inputElement.style.resize = 'vertical';
        } else {
            inputElement = document.createElement('input');

            if (numberFields.includes(fieldName)) {
                inputElement.type = 'number';
                this.setNumberFieldAttributes(inputElement, fieldName);
            } else if (fieldName.includes('url') || fieldName === 'website' || fieldName === 'website_url') {
                inputElement.type = 'url';
                inputElement.placeholder = 'https://example.com';
            } else if (fieldName === 'email_address' || fieldName === 'email') {
                inputElement.type = 'email';
                inputElement.placeholder = 'email@example.com';
            } else if (fieldName === 'phone' || fieldName === 'phone_number') {
                inputElement.type = 'tel';
                inputElement.placeholder = '(555) 123-4567';
            } else if (fieldName === 'zip_code') {
                inputElement.type = 'text';
                inputElement.pattern = '[0-9]{5}(-[0-9]{4})?';
                inputElement.placeholder = '12345 or 12345-6789';
            } else if (fieldName === 'place_id') {
                inputElement.type = 'text';
                inputElement.placeholder = 'Google Places ID';
                inputElement.style.fontFamily = 'monospace';
            } else {
                inputElement.type = 'text';
            }

            inputElement.value = currentValue || '';
        }

        inputElement.id = 'editFieldValue';
        inputElement.name = 'value';
        inputElement.style.width = '100%';

        // Add helpful descriptions for certain fields
        this.addFieldDescription(inputElement, fieldName);

        return inputElement;
    }

    setNumberFieldAttributes(inputElement, fieldName) {
        if (fieldName === 'latitude') {
            inputElement.step = 'any';
            inputElement.min = -90;
            inputElement.max = 90;
            inputElement.placeholder = 'e.g. 40.7128';
        } else if (fieldName === 'longitude') {
            inputElement.step = 'any';
            inputElement.min = -180;
            inputElement.max = 180;
            inputElement.placeholder = 'e.g. -74.0060';
        } else if (fieldName === 'course_rating') {
            inputElement.step = '0.1';
            inputElement.min = 60;
            inputElement.max = 80;
            inputElement.placeholder = 'e.g. 72.5';
        } else if (fieldName === 'slope_rating') {
            inputElement.min = 55;
            inputElement.max = 155;
            inputElement.placeholder = 'e.g. 125';
        } else if (fieldName === 'typical_18_hole_rate') {
            inputElement.step = '0.01';
            inputElement.min = 0;
            inputElement.placeholder = 'e.g. 85.00';
        } else if (fieldName === 'total_holes') {
            inputElement.min = 1;
            inputElement.max = 36;
            inputElement.placeholder = 'e.g. 18';
        } else if (fieldName === 'total_par') {
            inputElement.min = 27;
            inputElement.max = 144;
            inputElement.placeholder = 'e.g. 72';
        } else if (fieldName === 'total_length') {
            inputElement.min = 1000;
            inputElement.max = 8500;
            inputElement.placeholder = 'e.g. 6800 (yards)';
        } else if (fieldName === 'year_built_founded') {
            inputElement.min = 1800;
            inputElement.max = new Date().getFullYear();
            inputElement.placeholder = 'e.g. 1925';
        } else if (fieldName === 'user_rating_count') {
            inputElement.min = 0;
            inputElement.placeholder = 'e.g. 150';
        }
    }

    addFieldDescription(inputElement, fieldName) {
        let description = '';

        switch (fieldName) {
            case 'course_description':
                description = 'Detailed description of the golf course, its features, and character.';
                break;
            case 'signature_holes':
                description = 'Notable holes that define the course, with hole numbers and descriptions.';
                break;
            case 'pricing_information':
                description = 'Detailed pricing structure including rates, discounts, and packages.';
                break;
            case 'course_policies':
                description = 'Course rules, dress code, cancellation policies, etc.';
                break;
            case 'place_id':
                description = 'Google Places unique identifier (starts with ChIJ...)';
                break;
            case 'formatted_address':
                description = 'Complete address as formatted by Google Places.';
                break;
            case 'opening_hours':
                description = 'Course operating hours, can include seasonal variations.';
                break;
        }

        if (description) {
            const helpText = document.createElement('small');
            helpText.style.display = 'block';
            helpText.style.marginTop = '5px';
            helpText.style.color = '#6c757d';
            helpText.style.fontStyle = 'italic';
            helpText.textContent = description;

            inputElement.parentNode.appendChild(helpText);
        }
    }

    async handleSubmit(e) {
        e.preventDefault();

        if (!this.currentEditField || !this.currentCourseNumber) {
            showStatus('❌ Edit session expired. Please try again.', 'error', 'connectionStatus');
            this.close();
            return;
        }

        const saveButton = document.querySelector('.save-btn');
        const saveText = document.getElementById('saveButtonText');
        const saveSpinner = document.getElementById('saveButtonSpinner');

        // Show loading state
        saveButton.disabled = true;
        saveText.style.display = 'none';
        saveSpinner.style.display = 'inline-block';

        try {
            if (this.currentEditField === 'tee_row') {
                await this.handleTeeRowSubmit();
            } else if (this.currentEditField === 'par_row') {
                await this.handleParRowSubmit();
            } else {
                // Handle regular field edit (existing code)
                const inputElement = document.getElementById('editFieldValue');
                let newValue = inputElement.value;

                if (inputElement.tagName === 'TEXTAREA') {
                    newValue = newValue.trim();
                } else if (inputElement.type === 'text' || inputElement.type === 'url' || inputElement.type === 'email' || inputElement.type === 'tel') {
                    newValue = newValue.trim();
                }

                newValue = this.convertValue(newValue, this.currentEditField);

                const validation = this.validateValue(newValue, this.currentEditField);
                if (!validation.valid) {
                    showStatus(`❌ ${validation.message}`, 'error', 'connectionStatus');
                    return;
                }

                const targetTable = this.currentEditField === 'process' ? 'initial_course_upload' : 'primary_data';
                const isManual = targetTable === 'primary_data';

                await this.db.updateField(
                    targetTable,
                    this.currentCourseNumber,
                    this.currentEditField,
                    newValue,
                    isManual
                );

                showStatus(`✅ Successfully updated ${formatFieldName(this.currentEditField)}`, 'success', 'connectionStatus');
            }

            // Call the save callback to refresh data
            if (this.onSaveCallback) {
                this.onSaveCallback();
            }

            this.close();

        } catch (error) {
            showStatus(`❌ Failed to update: ${error.message}`, 'error', 'connectionStatus');
        } finally {
            // Reset button state
            saveButton.disabled = false;
            saveText.style.display = 'inline';
            saveSpinner.style.display = 'none';
        }
    }

    convertValue(value, fieldName) {
        const booleanFields = [
            'is_18_hole', 'is_9_hole', 'is_par_3_course', 'is_executive_course',
            'has_ocean_views', 'has_scenic_views', 'has_driving_range',
            'has_practice_green', 'has_short_game_area', 'has_clubhouse',
            'has_pro_shop', 'has_locker_rooms', 'has_showers', 'has_beverage_cart',
            'has_banquet_facilities'
        ];

        const numberFields = [
            'total_holes', 'total_par', 'course_rating', 'slope_rating', 'total_length',
            'year_built_founded', 'latitude', 'longitude', 'user_rating_count', 'typical_18_hole_rate'
        ];

        if (booleanFields.includes(fieldName)) {
            if (value === 'true') return true;
            else if (value === 'false') return false;
            else return null;
        } else if (numberFields.includes(fieldName)) {
            if (value === '') {
                return null;
            } else {
                const numValue = Number(value);
                if (isNaN(numValue)) {
                    throw new Error('Please enter a valid number');
                }
                return numValue;
            }
        } else if (value === '') {
            return null;
        }

        return value;
    }

    validateValue(value, fieldName) {
        // URL validation
        if ((fieldName.includes('url') || fieldName === 'website' || fieldName === 'website_url') && value) {
            try {
                new URL(value);
                if (!value.startsWith('http://') && !value.startsWith('https://')) {
                    return { valid: false, message: 'URL must start with http:// or https://' };
                }
            } catch {
                return { valid: false, message: 'Please enter a valid URL' };
            }
        }

        // Email validation
        if ((fieldName === 'email_address' || fieldName === 'email') && value) {
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            if (!emailRegex.test(value)) {
                return { valid: false, message: 'Please enter a valid email address' };
            }
        }

        // Phone validation (basic)
        if ((fieldName === 'phone' || fieldName === 'phone_number') && value) {
            const phoneRegex = /^[\d\s\-\(\)\+\.]+$/;
            if (!phoneRegex.test(value) || value.length < 10) {
                return { valid: false, message: 'Please enter a valid phone number' };
            }
        }

        // ZIP code validation
        if (fieldName === 'zip_code' && value) {
            const zipRegex = /^\d{5}(-\d{4})?$/;
            if (!zipRegex.test(value)) {
                return { valid: false, message: 'Please enter a valid ZIP code (12345 or 12345-6789)' };
            }
        }

        // Latitude validation
        if (fieldName === 'latitude' && value !== null) {
            const lat = Number(value);
            if (lat < -90 || lat > 90) {
                return { valid: false, message: 'Latitude must be between -90 and 90' };
            }
        }

        // Longitude validation
        if (fieldName === 'longitude' && value !== null) {
            const lng = Number(value);
            if (lng < -180 || lng > 180) {
                return { valid: false, message: 'Longitude must be between -180 and 180' };
            }
        }

        // Course rating validation
        if (fieldName === 'course_rating' && value !== null) {
            const rating = Number(value);
            if (rating < 60 || rating > 80) {
                return { valid: false, message: 'Course rating must be between 60 and 80' };
            }
        }

        // Slope rating validation
        if (fieldName === 'slope_rating' && value !== null) {
            const slope = Number(value);
            if (slope < 55 || slope > 155) {
                return { valid: false, message: 'Slope rating must be between 55 and 155' };
            }
        }

        // Year validation
        if (fieldName === 'year_built_founded' && value !== null) {
            const year = Number(value);
            const currentYear = new Date().getFullYear();
            if (year < 1800 || year > currentYear) {
                return { valid: false, message: `Year must be between 1800 and ${currentYear}` };
            }
        }

        // Place ID validation
        if (fieldName === 'place_id' && value) {
            if (!value.startsWith('ChIJ') && !value.startsWith('EiQ') && !value.startsWith('GhIJ')) {
                return { valid: false, message: 'Google Place ID should start with ChIJ, EiQ, or GhIJ' };
            }
        }

        return { valid: true };
    }

    // Method to programmatically open modal (useful for testing)
    openForField(fieldName, courseNumber, currentValue = '', onSave = null) {
        this.open(fieldName, currentValue, courseNumber, onSave);
    }

    // Method to check if modal is open
    isOpen() {
        return this.modal && this.modal.classList.contains('show');
    }

    // Method to get current field being edited
    getCurrentField() {
        return this.currentEditField;
    }

    // Method to get current course number
    getCurrentCourse() {
        return this.currentCourseNumber;
    }
}
