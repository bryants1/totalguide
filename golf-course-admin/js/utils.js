/**
 * UTILITY FUNCTIONS MODULE
 * Common helper functions used throughout the application
 */

export function showStatus(message, type, containerId) {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = `<div class="status ${type}">${message}</div>`;
    }
}

export function formatDisplayValue(value) {
    if (value == null || value === '') return 'N/A';
    if (typeof value === 'boolean') {
        return `<span class="boolean-indicator ${value ? 'true' : 'false'}">${value ? 'Yes' : 'No'}</span>`;
    }
    return value;
}

export function getSourceBadge(source) {
    if (!source) return '';

    // Map source names to shorter, cleaner versions
    const sourceMap = {
        'google_places_data': 'G',
        'course_info': 'C',
        'initial_course_upload': 'I',
        'manual': 'M'
    };

    const cleanSource = source.replace(/[^a-zA-Z0-9]/g, '_');
    const shortName = sourceMap[cleanSource] || source.charAt(0).toUpperCase();

    return `<span class="source-badge source-${cleanSource}" title="${source}">${shortName}</span>`;
}

export function isUrl(value) {
    if (!value || typeof value !== 'string') return false;
    try {
        new URL(value);
        return value.startsWith('http://') || value.startsWith('https://');
    } catch {
        return false;
    }
}

export function truncateUrl(url) {
    if (!url) return '';
    if (url.length > 40) {
        return url.substring(0, 37) + '...';
    }
    return url;
}

export function formatFieldName(fieldName) {
    return fieldName
        .replace(/_/g, ' ')
        .replace(/\b\w/g, l => l.toUpperCase())
        .replace(/Has /g, '')
        .replace(/Is /g, '');
}

export function formatDate(dateString) {
    if (!dateString) return 'N/A';
    try {
        return new Date(dateString).toLocaleDateString();
    } catch {
        return 'Invalid Date';
    }
}

export function getScoreColor(score) {
    if (score == null) return null;
    if (score >= 8) return '#16a34a';
    if (score >= 6) return '#f59e0b';
    if (score >= 4) return '#f97316';
    return '#dc2626';
}

export function getRatingColor(rating) {
    if (rating == null) return null;
    if (rating >= 4.5) return '#16a34a';
    if (rating >= 4.0) return '#22c55e';
    if (rating >= 3.5) return '#f59e0b';
    if (rating >= 3.0) return '#f97316';
    return '#dc2626';
}

export function getInsightColor(score) {
    if (score == null) return null;
    if (score >= 0.8) return '#16a34a';
    if (score >= 0.6) return '#22c55e';
    if (score >= 0.4) return '#f59e0b';
    if (score >= 0.2) return '#f97316';
    return '#dc2626';
}

export function sanitizeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

export function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

export function throttle(func, limit) {
    let inThrottle;
    return function() {
        const args = arguments;
        const context = this;
        if (!inThrottle) {
            func.apply(context, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    }
}

export function deepClone(obj) {
    if (obj === null || typeof obj !== 'object') return obj;
    if (obj instanceof Date) return new Date(obj.getTime());
    if (obj instanceof Array) return obj.map(item => deepClone(item));
    if (obj instanceof Object) {
        const clonedObj = {};
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                clonedObj[key] = deepClone(obj[key]);
            }
        }
        return clonedObj;
    }
}

export function generateId() {
    return Math.random().toString(36).substr(2, 9);
}

export function capitalizeFirst(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1);
}

export function pluralize(word, count) {
    if (count === 1) return word;

    // Simple pluralization rules
    if (word.endsWith('y')) {
        return word.slice(0, -1) + 'ies';
    } else if (word.endsWith('s') || word.endsWith('sh') || word.endsWith('ch')) {
        return word + 'es';
    } else {
        return word + 's';
    }
}

export function formatNumber(num, decimals = 0) {
    if (num == null || isNaN(num)) return 'N/A';
    return Number(num).toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

export function formatCurrency(amount, currency = 'USD') {
    if (amount == null || isNaN(amount)) return 'N/A';
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: currency
    }).format(amount);
}

export function formatPercentage(value, decimals = 1) {
    if (value == null || isNaN(value)) return 'N/A';
    return (value * 100).toFixed(decimals) + '%';
}

export function escapeRegex(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function validateEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

export function validateUrl(url) {
    try {
        new URL(url);
        return true;
    } catch {
        return false;
    }
}

export function validatePhoneNumber(phone) {
    // Basic phone number validation (US format)
    const phoneRegex = /^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$/;
    return phoneRegex.test(phone);
}

export function convertArrayToText(arr) {
    if (!Array.isArray(arr)) return arr;
    return arr.join('\n');
}

export function convertTextToArray(text) {
    if (!text || typeof text !== 'string') return [];
    return text.split('\n').map(line => line.trim()).filter(line => line.length > 0);
}

export function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export class EventEmitter {
    constructor() {
        this.events = {};
    }

    on(event, callback) {
        if (!this.events[event]) {
            this.events[event] = [];
        }
        this.events[event].push(callback);
    }

    off(event, callback) {
        if (!this.events[event]) return;
        this.events[event] = this.events[event].filter(cb => cb !== callback);
    }

    emit(event, ...args) {
        if (!this.events[event]) return;
        this.events[event].forEach(callback => callback(...args));
    }
}

export class Logger {
    static log(message, ...args) {
        console.log(`[Golf Admin] ${message}`, ...args);
    }

    static warn(message, ...args) {
        console.warn(`[Golf Admin] ${message}`, ...args);
    }

    static error(message, ...args) {
        console.error(`[Golf Admin] ${message}`, ...args);
    }

    static debug(message, ...args) {
        if (process.env.NODE_ENV === 'development') {
            console.debug(`[Golf Admin] ${message}`, ...args);
        }
    }
}
