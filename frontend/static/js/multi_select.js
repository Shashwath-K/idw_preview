/**
 * PramanaMultiSelect — Reusable checkbox-based multi-select dropdown.
 *
 * Usage:
 *   const ms = new PramanaMultiSelect('#regionFilter', {
 *       placeholder: 'All Regions',
 *       maxPills: 2
 *   });
 *   ms.setOptions(['Karnataka', 'Tamil Nadu', 'Andhra Pradesh']);
 *   ms.getValues();  // => ['Karnataka', 'Tamil Nadu']
 */
(function (root) {
    'use strict';

    const MAX_PILLS_DEFAULT = 2;

    class PramanaMultiSelect {
        constructor(selector, opts = {}) {
            this.el = typeof selector === 'string' ? document.querySelector(selector) : selector;
            if (!this.el) return;

            this.placeholder = opts.placeholder || this.el.dataset.placeholder || 'Select...';
            this.maxPills = opts.maxPills ?? MAX_PILLS_DEFAULT;
            this.onChange = opts.onChange || null;
            this.items = [];
            this.selected = new Set();

            this._build();
            this._bindGlobal();
        }

        /* ---- Public API ---- */

        setOptions(items) {
            this.items = items.map(item =>
                typeof item === 'object' ? item : { value: String(item), label: String(item) }
            );
            this.selected.clear();
            this._renderOptions();
            this._syncTrigger();
        }

        getValues() {
            return Array.from(this.selected);
        }

        getCSV() {
            const vals = this.getValues();
            return vals.length ? vals.join(',') : '';
        }

        reset() {
            this.selected.clear();
            this._renderOptions();
            this._syncTrigger();
        }

        selectAll() {
            this.items.forEach(it => this.selected.add(it.value));
            this._renderOptions();
            this._syncTrigger();
        }

        /* ---- Build DOM ---- */

        _build() {
            this.el.style.display = 'none';

            this.wrapper = document.createElement('div');
            this.wrapper.className = 'pms-wrapper';
            this.el.parentNode.insertBefore(this.wrapper, this.el.nextSibling);

            // Trigger button
            this.trigger = document.createElement('div');
            this.trigger.className = 'pms-trigger';
            this.trigger.innerHTML = `
                <span class="pms-pills"></span>
                <span class="pms-placeholder">${this.placeholder}</span>
                <span class="pms-caret"><i class="fas fa-chevron-down"></i></span>
            `;
            this.wrapper.appendChild(this.trigger);

            // Dropdown
            this.dropdown = document.createElement('div');
            this.dropdown.className = 'pms-dropdown';
            this.dropdown.innerHTML = `
                <div class="pms-search-box"><input type="text" placeholder="Search…"></div>
                <label class="pms-select-all"><input type="checkbox"> Select All</label>
                <div class="pms-options"></div>
                <div class="pms-no-results">No matches found</div>
            `;
            this.wrapper.appendChild(this.dropdown);

            this.pillsEl = this.trigger.querySelector('.pms-pills');
            this.placeholderEl = this.trigger.querySelector('.pms-placeholder');
            this.searchInput = this.dropdown.querySelector('.pms-search-box input');
            this.selectAllCb = this.dropdown.querySelector('.pms-select-all input');
            this.optionsCont = this.dropdown.querySelector('.pms-options');
            this.noResults = this.dropdown.querySelector('.pms-no-results');

            this._bindEvents();
        }

        _bindEvents() {
            this.trigger.addEventListener('click', (e) => {
                e.stopPropagation();
                this._toggle();
            });

            this.searchInput.addEventListener('input', () => this._filterOptions());
            this.searchInput.addEventListener('click', (e) => e.stopPropagation());

            this.selectAllCb.addEventListener('change', () => {
                if (this.selectAllCb.checked) {
                    this._getVisibleItems().forEach(it => this.selected.add(it.value));
                } else {
                    this._getVisibleItems().forEach(it => this.selected.delete(it.value));
                }
                this._renderOptions();
                this._syncTrigger();
                this._fireChange();
            });

            this.dropdown.addEventListener('click', (e) => e.stopPropagation());
        }

        _bindGlobal() {
            document.addEventListener('click', () => this._close());
        }

        /* ---- Open / Close ---- */

        _toggle() {
            if (this.dropdown.classList.contains('open')) {
                this._close();
            } else {
                this._open();
            }
        }

        _open() {
            // Close all other multi-selects first
            document.querySelectorAll('.pms-dropdown.open').forEach(d => d.classList.remove('open'));
            document.querySelectorAll('.pms-trigger.open').forEach(t => t.classList.remove('open'));

            this.dropdown.classList.add('open');
            this.trigger.classList.add('open');
            this.searchInput.value = '';
            this._filterOptions();
            this.searchInput.focus();
        }

        _close() {
            this.dropdown.classList.remove('open');
            this.trigger.classList.remove('open');
        }

        /* ---- Render ---- */

        _renderOptions() {
            this.optionsCont.innerHTML = '';
            this.items.forEach(item => {
                const div = document.createElement('label');
                div.className = 'pms-option';
                div.dataset.value = item.value;
                div.dataset.label = item.label.toLowerCase();

                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.checked = this.selected.has(item.value);
                cb.addEventListener('change', () => {
                    if (cb.checked) {
                        this.selected.add(item.value);
                    } else {
                        this.selected.delete(item.value);
                    }
                    this._syncTrigger();
                    this._syncSelectAll();
                    this._fireChange();
                });

                const span = document.createElement('span');
                span.textContent = item.label;

                div.appendChild(cb);
                div.appendChild(span);
                this.optionsCont.appendChild(div);
            });
            this._syncSelectAll();
        }

        _syncTrigger() {
            const vals = this.getValues();
            this.pillsEl.innerHTML = '';

            if (vals.length === 0) {
                this.placeholderEl.style.display = '';
                this.placeholderEl.textContent = this.placeholder;
                return;
            }

            this.placeholderEl.style.display = 'none';
            const show = vals.slice(0, this.maxPills);
            const extra = vals.length - show.length;

            show.forEach(v => {
                const item = this.items.find(i => i.value === v);
                const pill = document.createElement('span');
                pill.className = 'pms-pill';
                pill.innerHTML = `${item ? item.label : v} <span class="pms-pill-remove" data-val="${v}">&times;</span>`;
                pill.querySelector('.pms-pill-remove').addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.selected.delete(v);
                    this._renderOptions();
                    this._syncTrigger();
                    this._fireChange();
                });
                this.pillsEl.appendChild(pill);
            });

            if (extra > 0) {
                const badge = document.createElement('span');
                badge.className = 'pms-pill-overflow';
                badge.textContent = `+${extra}`;
                this.pillsEl.appendChild(badge);
            }
        }

        _syncSelectAll() {
            const visible = this._getVisibleItems();
            const allChecked = visible.length > 0 && visible.every(it => this.selected.has(it.value));
            this.selectAllCb.checked = allChecked;
        }

        _filterOptions() {
            const query = this.searchInput.value.toLowerCase().trim();
            let visibleCount = 0;

            this.optionsCont.querySelectorAll('.pms-option').forEach(opt => {
                const matches = !query || opt.dataset.label.includes(query);
                opt.classList.toggle('hidden', !matches);
                if (matches) visibleCount++;
            });

            this.noResults.style.display = visibleCount === 0 ? 'block' : 'none';
            this._syncSelectAll();
        }

        _getVisibleItems() {
            const visible = [];
            this.optionsCont.querySelectorAll('.pms-option:not(.hidden)').forEach(opt => {
                visible.push({ value: opt.dataset.value, label: opt.dataset.label });
            });
            return visible;
        }

        _fireChange() {
            if (this.onChange) this.onChange(this.getValues());
            // Also dispatch a native change event on the original select
            this.el.dispatchEvent(new Event('change', { bubbles: true }));
        }
    }

    // Expose globally
    root.PramanaMultiSelect = PramanaMultiSelect;

})(window);
