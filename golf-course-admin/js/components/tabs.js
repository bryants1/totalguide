/**
 * TAB MANAGER COMPONENT
 * Handles tab switching and content management
 */

export class TabManager {
    constructor() {
        this.tabs = [];
        this.activeTab = null;
        this.tabsContainer = null;
        this.contentContainer = null;
    }

    init() {
        this.tabsContainer = document.getElementById('mainTabs');
        this.contentContainer = document.getElementById('tabContent');

        if (!this.tabsContainer || !this.contentContainer) {
            console.error('Tab containers not found');
            return;
        }
    }

    setupTabs(tabsConfig) {
        this.tabs = tabsConfig;
        this.renderTabs();
        this.setupTabContent();

        // Activate first tab by default
        if (this.tabs.length > 0) {
            this.showTab(this.tabs[0].id);
        }
    }

    renderTabs() {
        if (!this.tabsContainer) return;

        this.tabsContainer.innerHTML = this.tabs.map(tab => `
            <button
                class="tab"
                data-tab="${tab.id}"
            >
                ${tab.icon ? tab.icon + ' ' : ''}${tab.label}
            </button>
        `).join('');
    }

    setupTabContent() {
        if (!this.contentContainer) return;

        // CREATE TAB CONTENT DIVS, NOT TAB BUTTONS!
        this.contentContainer.innerHTML = this.tabs.map(tab => `
            <div id="${tab.id}Tab" class="tab-content">
                <div class="status info">Loading ${tab.label.toLowerCase()}...</div>
            </div>
        `).join('');
    }

    showTab(tabId) {
        // REMOVED: All pipeline cleanup code

        // Remove active class from all tabs
        this.tabsContainer.querySelectorAll('.tab').forEach(tab => {
            tab.classList.remove('active');
        });

        // Remove active class from all tab contents
        this.contentContainer.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });

        // Add active class to clicked tab
        const activeTabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (activeTabButton) {
            activeTabButton.classList.add('active');
        }

        // Add active class to corresponding content
        const activeTabContent = document.getElementById(`${tabId}Tab`);
        if (activeTabContent) {
            activeTabContent.classList.add('active');
        }

        // Update active tab
        this.activeTab = tabId;

        // Store active tab in localStorage for persistence
        try {
            localStorage.setItem('golf-admin-active-tab', tabId);
        } catch (e) {
            // localStorage not available, ignore
        }

        // REMOVED: All pipeline initialization code

        // Animate the transition
        this.animateTabTransition(tabId);
    }

    setTabContent(tabId, content) {
        const tabContent = document.getElementById(`${tabId}Tab`);
        if (tabContent) {
            tabContent.innerHTML = content;
        }
    }

    getActiveTab() {
        return this.activeTab;
    }

    disableTab(tabId) {
        const tabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (tabButton) {
            tabButton.disabled = true;
            tabButton.classList.add('disabled');
        }
    }

    enableTab(tabId) {
        const tabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (tabButton) {
            tabButton.disabled = false;
            tabButton.classList.remove('disabled');
        }
    }

    addTabBadge(tabId, text, className = '') {
        const tabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (tabButton) {
            // Remove existing badge
            const existingBadge = tabButton.querySelector('.tab-badge');
            if (existingBadge) {
                existingBadge.remove();
            }

            // Add new badge
            const badge = document.createElement('span');
            badge.className = `tab-badge ${className}`;
            badge.textContent = text;
            tabButton.appendChild(badge);
        }
    }

    removeTabBadge(tabId) {
        const tabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (tabButton) {
            const badge = tabButton.querySelector('.tab-badge');
            if (badge) {
                badge.remove();
            }
        }
    }

    highlightTab(tabId, highlight = true) {
        const tabButton = this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
        if (tabButton) {
            if (highlight) {
                tabButton.classList.add('highlight');
            } else {
                tabButton.classList.remove('highlight');
            }
        }
    }

    getTabButton(tabId) {
        return this.tabsContainer.querySelector(`[data-tab="${tabId}"]`);
    }

    getTabContent(tabId) {
        return document.getElementById(`${tabId}Tab`);
    }

    // Restore active tab from localStorage
    restoreActiveTab() {
        try {
            const savedTab = localStorage.getItem('golf-admin-active-tab');
            if (savedTab && this.tabs.find(tab => tab.id === savedTab)) {
                this.showTab(savedTab);
                return true;
            }
        } catch (e) {
            // localStorage not available, ignore
        }
        return false;
    }

    // Add keyboard navigation
    setupKeyboardNavigation() {
        document.addEventListener('keydown', (e) => {
            if (e.altKey && e.key >= '1' && e.key <= '9') {
                const tabIndex = parseInt(e.key) - 1;
                if (this.tabs[tabIndex]) {
                    e.preventDefault();
                    this.showTab(this.tabs[tabIndex].id);
                }
            }
        });
    }

    // Animate tab transitions
    animateTabTransition(tabId) {
        const content = this.getTabContent(tabId);
        if (content) {
            content.style.opacity = '0';
            content.style.transform = 'translateX(20px)';

            requestAnimationFrame(() => {
                content.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
                content.style.opacity = '1';
                content.style.transform = 'translateX(0)';
            });
        }
    }

    destroy() {
        // Clean up event listeners and references
        this.tabs = [];
        this.activeTab = null;
        this.tabsContainer = null;
        this.contentContainer = null;
    }
}
