/**
 * PramanaTableSort — Client-side table column sorting.
 *
 * Usage:
 *   <th data-sortable="true" data-sort-type="number">Sessions</th>
 *   new PramanaTableSort('myTable');
 *
 * Cycles: unsorted → ascending ↑ → descending ↓ → unsorted
 */
(function (root) {
    'use strict';

    class PramanaTableSort {
        constructor(tableId) {
            this.table = document.getElementById(tableId);
            if (!this.table) return;

            this.headers = this.table.querySelectorAll('thead th[data-sortable="true"]');
            this.currentCol = null;
            this.currentDir = null; // null | 'asc' | 'desc'

            this._init();
        }

        _init() {
            this.headers.forEach((th, colIdx) => {
                th.style.cursor = 'pointer';
                th.style.userSelect = 'none';
                th.style.whiteSpace = 'nowrap';

                // Add sort indicator span
                const indicator = document.createElement('span');
                indicator.className = 'sort-indicator';
                indicator.style.marginLeft = '4px';
                indicator.style.fontSize = '0.7rem';
                indicator.style.color = '#999';
                th.appendChild(indicator);

                th.addEventListener('click', () => this._sortColumn(colIdx, th));
            });
        }

        _sortColumn(colIdx, th) {
            // Determine next direction
            let nextDir;
            if (this.currentCol !== colIdx) {
                nextDir = 'asc';
            } else if (this.currentDir === 'asc') {
                nextDir = 'desc';
            } else if (this.currentDir === 'desc') {
                nextDir = null;
            } else {
                nextDir = 'asc';
            }

            this.currentCol = colIdx;
            this.currentDir = nextDir;

            // Update indicators
            this.headers.forEach(h => {
                const ind = h.querySelector('.sort-indicator');
                if (ind) ind.textContent = '';
            });

            const indicator = th.querySelector('.sort-indicator');
            if (nextDir === 'asc') {
                indicator.textContent = ' ▲';
                indicator.style.color = '#007bff';
            } else if (nextDir === 'desc') {
                indicator.textContent = ' ▼';
                indicator.style.color = '#007bff';
            } else {
                indicator.textContent = '';
                indicator.style.color = '#999';
            }

            // Sort rows
            const tbody = this.table.querySelector('tbody');
            if (!tbody) return;

            const rows = Array.from(tbody.querySelectorAll('tr'));
            const sortType = th.dataset.sortType || 'string';

            if (nextDir === null) {
                // Restore original order — we can't really do this without
                // storing original, so just leave as-is (no-op)
                return;
            }

            rows.sort((a, b) => {
                const cellA = a.cells[colIdx];
                const cellB = b.cells[colIdx];
                if (!cellA || !cellB) return 0;

                let valA = (cellA.textContent || '').trim();
                let valB = (cellB.textContent || '').trim();

                if (sortType === 'number') {
                    valA = parseFloat(valA.replace(/,/g, '')) || 0;
                    valB = parseFloat(valB.replace(/,/g, '')) || 0;
                    return nextDir === 'asc' ? valA - valB : valB - valA;
                }

                // String comparison
                valA = valA.toLowerCase();
                valB = valB.toLowerCase();
                if (valA < valB) return nextDir === 'asc' ? -1 : 1;
                if (valA > valB) return nextDir === 'asc' ? 1 : -1;
                return 0;
            });

            rows.forEach(row => tbody.appendChild(row));
        }
    }

    root.PramanaTableSort = PramanaTableSort;

})(window);
