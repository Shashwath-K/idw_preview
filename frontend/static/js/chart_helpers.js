/**
 * PramanaCharts — Factory helpers for Chart.js with consistent styling.
 *
 * Features:
 *  - External legends (HTML div, not inside canvas)
 *  - Truncated X-axis labels with full text in tooltips
 *  - Linear trendlines for line charts
 *  - Pie charts with separate legend divs
 */
(function (root) {
    'use strict';

    const PALETTE = [
        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
        '#06b6d4', '#ec4899', '#14b8a6', '#f97316', '#6366f1'
    ];

    const PramanaCharts = {
        instances: {},

        /**
         * Truncate a label to maxLen chars.
         */
        _truncate(label, maxLen) {
            if (!label) return '';
            return label.length > maxLen ? label.substring(0, maxLen) + '…' : label;
        },

        /**
         * Compute linear regression trendline from data points.
         * Returns array of y-values (same length as input).
         */
        _linearTrend(data) {
            const n = data.length;
            if (n < 2) return data.slice();
            let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            for (let i = 0; i < n; i++) {
                const y = typeof data[i] === 'number' ? data[i] : 0;
                sumX += i;
                sumY += y;
                sumXY += i * y;
                sumX2 += i * i;
            }
            const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            const intercept = (sumY - slope * sumX) / n;
            return data.map((_, i) => Math.round(slope * i + intercept));
        },

        /**
         * Render an HTML legend into a target div.
         */
        _renderExternalLegend(chart, legendDivId) {
            const legendDiv = document.getElementById(legendDivId);
            if (!legendDiv || !chart) return;

            const items = chart.options.plugins.legend.labels.generateLabels(chart);
            legendDiv.innerHTML = '';
            legendDiv.style.display = 'flex';
            legendDiv.style.flexWrap = 'wrap';
            legendDiv.style.gap = '8px';
            legendDiv.style.fontSize = '0.75rem';
            legendDiv.style.padding = '6px 0';

            items.forEach((item, idx) => {
                const el = document.createElement('span');
                el.style.display = 'inline-flex';
                el.style.alignItems = 'center';
                el.style.gap = '4px';
                el.style.cursor = 'pointer';
                el.style.opacity = item.hidden ? '0.4' : '1';

                const swatch = document.createElement('span');
                swatch.style.display = 'inline-block';
                swatch.style.width = '10px';
                swatch.style.height = '10px';
                swatch.style.borderRadius = '2px';
                swatch.style.background = item.fillStyle || item.strokeStyle;

                const label = document.createElement('span');
                label.textContent = item.text;
                label.style.color = '#333';

                el.appendChild(swatch);
                el.appendChild(label);

                el.addEventListener('click', () => {
                    chart.toggleDataVisibility(idx);
                    chart.update();
                    PramanaCharts._renderExternalLegend(chart, legendDivId);
                });

                legendDiv.appendChild(el);
            });
        },

        /**
         * Shared base options for all charts.
         */
        _baseOptions(opts = {}) {
            const labelMaxLen = opts.labelMaxLength || 12;
            return {
                maintainAspectRatio: false,
                responsive: true,
                plugins: {
                    legend: {
                        display: !opts.externalLegendId,
                        position: 'bottom',
                        labels: { color: '#333', boxWidth: 12, padding: 8, font: { size: 11 } }
                    },
                    tooltip: {
                        callbacks: {
                            title: function (ctx) {
                                // Show full label in tooltip
                                if (opts._fullLabels && ctx[0]) {
                                    return opts._fullLabels[ctx[0].dataIndex] || ctx[0].label;
                                }
                                return ctx[0]?.label;
                            }
                        }
                    }
                },
                scales: opts.noScales ? undefined : {
                    y: {
                        beginAtZero: true,
                        grid: { display: true, color: 'rgba(0,0,0,0.04)' },
                        ticks: { color: '#555', precision: 0, font: { size: 11 } }
                    },
                    x: {
                        grid: { display: false },
                        ticks: {
                            color: '#555',
                            font: { size: 10 },
                            maxRotation: 45,
                            callback: function (val, idx) {
                                const label = this.getLabelForValue(val);
                                return PramanaCharts._truncate(label, labelMaxLen);
                            }
                        }
                    }
                }
            };
        },

        /**
         * Create or replace a Bar Chart.
         */
        createBarChart(canvasId, data, opts = {}) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return null;
            const ctx = canvas.getContext('2d');

            if (this.instances[canvasId]) this.instances[canvasId].destroy();

            const labels = data.map(d => d.label);
            const values = data.map(d => d.value);
            const colors = opts.colors || PALETTE.slice(0, labels.length);

            const baseOpts = this._baseOptions({ ...opts, _fullLabels: labels });

            const chart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: opts.datasetLabel || 'Value',
                        data: values,
                        backgroundColor: typeof colors === 'string' ? colors : colors,
                        borderRadius: 4,
                        maxBarThickness: 40
                    }]
                },
                options: baseOpts
            });

            this.instances[canvasId] = chart;
            if (opts.externalLegendId) {
                this._renderExternalLegend(chart, opts.externalLegendId);
            }
            return chart;
        },

        /**
         * Create or replace a Line Chart with optional trendline.
         */
        createLineChart(canvasId, data, opts = {}) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return null;
            const ctx = canvas.getContext('2d');

            if (this.instances[canvasId]) this.instances[canvasId].destroy();

            const labels = data.map(d => d.label);
            const values = data.map(d => d.value);

            const datasets = [{
                label: opts.datasetLabel || 'Value',
                data: values,
                borderColor: opts.lineColor || PALETTE[0],
                backgroundColor: (opts.lineColor || PALETTE[0]) + '18',
                fill: opts.fill !== false,
                tension: 0.3,
                pointRadius: 3,
                pointHoverRadius: 5,
                borderWidth: 2
            }];

            // Add trendline
            if (opts.showTrendline !== false && values.length >= 3) {
                const trendData = this._linearTrend(values);
                datasets.push({
                    label: 'Trend',
                    data: trendData,
                    borderColor: '#ef4444',
                    borderDash: [6, 3],
                    borderWidth: 1.5,
                    pointRadius: 0,
                    fill: false,
                    tension: 0
                });
            }

            const baseOpts = this._baseOptions({ ...opts, _fullLabels: labels });

            const chart = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: baseOpts
            });

            this.instances[canvasId] = chart;
            if (opts.externalLegendId) {
                this._renderExternalLegend(chart, opts.externalLegendId);
            }
            return chart;
        },

        /**
         * Create or replace a Pie/Doughnut Chart with external legend.
         */
        createPieChart(canvasId, data, opts = {}) {
            const canvas = document.getElementById(canvasId);
            if (!canvas) return null;
            const ctx = canvas.getContext('2d');

            if (this.instances[canvasId]) this.instances[canvasId].destroy();

            const labels = data.map(d => d.label);
            const values = data.map(d => d.value);
            const colors = opts.colors || PALETTE.slice(0, labels.length);

            const chart = new Chart(ctx, {
                type: opts.doughnut ? 'doughnut' : 'pie',
                data: {
                    labels,
                    datasets: [{
                        data: values,
                        backgroundColor: colors,
                        borderWidth: 1,
                        borderColor: '#fff'
                    }]
                },
                options: {
                    maintainAspectRatio: false,
                    responsive: true,
                    plugins: {
                        legend: {
                            display: !opts.externalLegendId,
                            position: 'right',
                            labels: { color: '#333', boxWidth: 10, padding: 6, font: { size: 11 } }
                        },
                        tooltip: {
                            callbacks: {
                                label: function (ctx) {
                                    const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                                    const pct = total > 0 ? ((ctx.raw / total) * 100).toFixed(1) : 0;
                                    return `${ctx.label}: ${ctx.raw.toLocaleString()} (${pct}%)`;
                                }
                            }
                        }
                    }
                }
            });

            this.instances[canvasId] = chart;
            if (opts.externalLegendId) {
                this._renderExternalLegend(chart, opts.externalLegendId);
            }
            return chart;
        },

        /**
         * Destroy a chart by canvas ID.
         */
        destroy(canvasId) {
            if (this.instances[canvasId]) {
                this.instances[canvasId].destroy();
                delete this.instances[canvasId];
            }
        }
    };

    root.PramanaCharts = PramanaCharts;

})(window);
