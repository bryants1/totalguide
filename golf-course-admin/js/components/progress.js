/**
 * PROGRESS TRACKER COMPONENT
 * Handles progress display for data updates
 */

import { formatFieldName } from '../utils.js';

export class ProgressTracker {
    constructor() {
        this.container = null;
        this.contentContainer = null;
        this.items = [];
    }

    init() {
        this.container = document.getElementById('updateProgress');
        this.contentContainer = document.getElementById('progressContent');

        if (!this.container || !this.contentContainer) {
            console.warn('Progress containers not found');
        }
    }

    show() {
        if (this.container) {
            this.container.classList.add('show');
            this.clear();
        }
    }

    hide() {
        if (this.container) {
            this.container.classList.remove('show');
        }
    }

    clear() {
        if (this.contentContainer) {
            this.contentContainer.innerHTML = '';
            this.items = [];
        }
    }

    addItem(field, status, message) {
        if (!this.contentContainer) {
            this.init();
            if (!this.contentContainer) return;
        }

        const item = {
            field,
            status,
            message,
            timestamp: new Date()
        };

        this.items.push(item);

        const itemElement = document.createElement('div');
        itemElement.className = 'progress-item';
        itemElement.innerHTML = `
            <span>${formatFieldName(field)}</span>
            <span class="progress-status ${status}">${message}</span>
        `;

        this.contentContainer.appendChild(itemElement);

        // Auto-scroll to bottom
        this.contentContainer.scrollTop = this.contentContainer.scrollHeight;

        // Add animation
        this.animateNewItem(itemElement);
    }

    animateNewItem(element) {
        element.style.opacity = '0';
        element.style.transform = 'translateX(-20px)';

        requestAnimationFrame(() => {
            element.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            element.style.opacity = '1';
            element.style.transform = 'translateX(0)';
        });
    }

    updateItem(field, status, message) {
        const existingItemIndex = this.items.findIndex(item => item.field === field);

        if (existingItemIndex !== -1) {
            // Update existing item
            this.items[existingItemIndex] = {
                ...this.items[existingItemIndex],
                status,
                message,
                timestamp: new Date()
            };

            // Update DOM element
            const itemElements = this.contentContainer.querySelectorAll('.progress-item');
            const itemElement = itemElements[existingItemIndex];

            if (itemElement) {
                const statusElement = itemElement.querySelector('.progress-status');
                if (statusElement) {
                    statusElement.className = `progress-status ${status}`;
                    statusElement.textContent = message;
                }
            }
        } else {
            // Add new item if not found
            this.addItem(field, status, message);
        }
    }

    getStats() {
        const stats = {
            total: this.items.length,
            updated: 0,
            skipped: 0,
            protected: 0,
            checking: 0,
            errors: 0
        };

        this.items.forEach(item => {
            switch (item.status) {
                case 'updated':
                    stats.updated++;
                    break;
                case 'skipped':
                    stats.skipped++;
                    break;
                case 'protected':
                    stats.protected++;
                    break;
                case 'checking':
                    stats.checking++;
                    break;
                case 'error':
                    stats.errors++;
                    break;
            }
        });

        return stats;
    }

    showSummary() {
        const stats = this.getStats();

        const summaryElement = document.createElement('div');
        summaryElement.className = 'progress-summary';
        summaryElement.innerHTML = `
            <div style="background: linear-gradient(135deg, #f8f9fa, #e9ecef); border: 1px solid #dee2e6; border-radius: 12px; padding: 20px; margin-top: 20px;">
                <h4 style="margin: 0 0 15px 0; color: #4a7c59;">📊 Update Summary</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 15px;">
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #28a745;">${stats.updated}</div>
                        <div style="font-size: 0.85rem; color: #6c757d;">Updated</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #6c757d;">${stats.skipped}</div>
                        <div style="font-size: 0.85rem; color: #6c757d;">Skipped</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #7b1fa2;">${stats.protected}</div>
                        <div style="font-size: 0.85rem; color: #6c757d;">Protected</div>
                    </div>
                    ${stats.errors > 0 ? `
                    <div style="text-align: center;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #dc3545;">${stats.errors}</div>
                        <div style="font-size: 0.85rem; color: #6c757d;">Errors</div>
                    </div>
                    ` : ''}
                </div>
            </div>
        `;

        this.contentContainer.appendChild(summaryElement);
        this.contentContainer.scrollTop = this.contentContainer.scrollHeight;
    }

    exportLog() {
        const logData = {
            timestamp: new Date().toISOString(),
            stats: this.getStats(),
            items: this.items
        };

        const dataStr = JSON.stringify(logData, null, 2);
        const dataBlob = new Blob([dataStr], { type: 'application/json' });

        const link = document.createElement('a');
        link.href = URL.createObjectURL(dataBlob);
        link.download = `golf-admin-update-log-${new Date().toISOString().split('T')[0]}.json`;
        link.click();
    }

    addCustomMessage(message, type = 'info') {
        if (!this.contentContainer) return;

        const messageElement = document.createElement('div');
        messageElement.className = 'progress-message';
        messageElement.innerHTML = `
            <div style="padding: 12px; margin: 10px 0; border-radius: 8px; font-size: 14px; font-weight: 500; background: ${this.getMessageBackground(type)}; color: ${this.getMessageColor(type)};">
                ${message}
            </div>
        `;

        this.contentContainer.appendChild(messageElement);
        this.contentContainer.scrollTop = this.contentContainer.scrollHeight;
    }

    getMessageBackground(type) {
        switch (type) {
            case 'success': return 'linear-gradient(135deg, #d4edda, #c3e6cb)';
            case 'error': return 'linear-gradient(135deg, #f8d7da, #f5c6cb)';
            case 'warning': return 'linear-gradient(135deg, #fff3cd, #ffeaa7)';
            default: return 'linear-gradient(135deg, #d1ecf1, #bee5eb)';
        }
    }

    getMessageColor(type) {
        switch (type) {
            case 'success': return '#155724';
            case 'error': return '#721c24';
            case 'warning': return '#856404';
            default: return '#0c5460';
        }
    }

    setProgress(current, total) {
        if (!this.container) return;

        // Create or update progress bar
        let progressBar = this.container.querySelector('.progress-bar');
        if (!progressBar) {
            progressBar = document.createElement('div');
            progressBar.className = 'progress-bar';
            progressBar.innerHTML = `
                <div style="background: #e9ecef; border-radius: 10px; height: 8px; margin: 15px 0;">
                    <div class="progress-fill" style="background: linear-gradient(135deg, #4a7c59, #2d5a3d); height: 100%; border-radius: 10px; transition: width 0.3s ease; width: 0%;"></div>
                </div>
                <div class="progress-text" style="font-size: 12px; color: #6c757d; text-align: center;"></div>
            `;
            this.container.insertBefore(progressBar, this.contentContainer);
        }

        const percentage = total > 0 ? Math.round((current / total) * 100) : 0;
        const progressFill = progressBar.querySelector('.progress-fill');
        const progressText = progressBar.querySelector('.progress-text');

        if (progressFill) {
            progressFill.style.width = `${percentage}%`;
        }

        if (progressText) {
            progressText.textContent = `${current} of ${total} fields processed (${percentage}%)`;
        }
    }

    destroy() {
        this.hide();
        this.clear();
        this.container = null;
        this.contentContainer = null;
        this.items = [];
    }
}
