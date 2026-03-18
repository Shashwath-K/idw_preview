(function () {
    const charts = {};

    document.addEventListener("DOMContentLoaded", async () => {
        syncRangeLabels();
        bindFilters();
        try {
            await loadFilterOptions();
            await refreshPage();
        } catch (error) {
            console.error(error);
        }
    });

    function getPage() {
        return document.body.dataset.page || "dashboard";
    }

    function bindFilters() {
        const filterIds = ["startYear", "endYear", "regionFilter", "programFilter"];
        filterIds.forEach((id) => {
            const element = document.getElementById(id);
            if (!element) {
                return;
            }

            element.addEventListener("input", () => {
                syncRangeLabels();
                refreshPage().catch(console.error);
            });

            element.addEventListener("change", () => {
                syncRangeLabels();
                refreshPage().catch(console.error);
            });
        });
    }

    function syncRangeLabels() {
        const start = document.getElementById("startYear");
        const end = document.getElementById("endYear");
        const startLabel = document.getElementById("startYearLabel");
        const endLabel = document.getElementById("endYearLabel");

        if (start && end && Number(start.value) > Number(end.value)) {
            if (document.activeElement === start) {
                end.value = start.value;
            } else {
                start.value = end.value;
            }
        }

        if (start && startLabel) {
            startLabel.textContent = start.value;
        }

        if (end && endLabel) {
            endLabel.textContent = end.value;
        }
    }

    async function loadFilterOptions() {
        const [yearOptions, regionOptions, programOptions] = await Promise.all([
            fetchJSON("/session/filter-options"),
            fetchJSON("/region/options"),
            fetchJSON("/exposure/programs"),
        ]);

        const years = yearOptions.years || [];
        if (years.length) {
            const minYear = Math.min(...years);
            const maxYear = Math.max(...years);
            configureRange("startYear", minYear, maxYear, minYear);
            configureRange("endYear", minYear, maxYear, maxYear);
        }

        populateSelect("regionFilter", "All Regions", regionOptions.regions || []);
        populateSelect("programFilter", "All Programs", programOptions.programs || []);
        syncRangeLabels();
    }

    function configureRange(id, min, max, value) {
        const input = document.getElementById(id);
        if (!input) {
            return;
        }

        input.min = String(min);
        input.max = String(max);
        input.value = String(value);
    }

    function populateSelect(id, placeholder, options) {
        const select = document.getElementById(id);
        if (!select) {
            return;
        }

        const currentValue = select.value;
        select.innerHTML = "";

        const allOption = document.createElement("option");
        allOption.value = "";
        allOption.textContent = placeholder;
        select.appendChild(allOption);

        options.forEach((optionValue) => {
            const option = document.createElement("option");
            option.value = optionValue;
            option.textContent = optionValue;
            if (optionValue === currentValue) {
                option.selected = true;
            }
            select.appendChild(option);
        });
    }

    function getFilters() {
        const params = new URLSearchParams();
        const start = document.getElementById("startYear")?.value;
        const end = document.getElementById("endYear")?.value;
        const region = document.getElementById("regionFilter")?.value;
        const program = document.getElementById("programFilter")?.value;

        if (start) {
            params.set("start", start);
        }
        if (end) {
            params.set("end", end);
        }
        if (region) {
            params.set("region", region);
        }
        if (program) {
            params.set("program", program);
        }

        return params.toString();
    }

    async function refreshPage() {
        const page = getPage();

        if (page === "dashboard") {
            await loadDashboard();
            return;
        }

        if (page === "sessions") {
            await loadSessionsPage();
            return;
        }

        if (page === "region") {
            await loadRegionPage();
            return;
        }

        if (page === "instructor") {
            await loadInstructorPage();
            return;
        }

        if (page === "programs") {
            await loadProgramsPage();
        }
    }

    async function loadDashboard() {
        const filters = getFilters();
        const [sessionKpis, exposureKpis, instructorKpis, regionImpact, monthlySessions, instructorProductivity, programMetrics] = await Promise.all([
            fetchJSON(`/session/kpis?${filters}`),
            fetchJSON(`/exposure/kpis?${filters}`),
            fetchJSON(`/instructor/kpis?${filters}`),
            fetchJSON(`/region/impact?${filters}`),
            fetchJSON(`/session/monthly?${filters}`),
            fetchJSON(`/instructor/productivity?${filters}`),
            fetchJSON(`/exposure/program-metrics?${filters}`),
        ]);

        setText("dashboardTotalSessions", sessionKpis.metrics.total_sessions);
        setText("dashboardTotalStudents", exposureKpis.metrics.total_students);
        setText("dashboardTotalInstructors", instructorKpis.metrics.total_instructors);
        setText("dashboardTotalPrograms", exposureKpis.metrics.total_programs);

        renderBarChart("regionImpactChart", regionImpact.data, "Students Reached", "#007bff");
        renderLineChart("monthlySessionsChart", monthlySessions.data, "Sessions", "#17a2b8");
        renderHorizontalBarChart("instructorProductivityChart", instructorProductivity.data, "Sessions Conducted", "#fd7e14");
        renderDoughnutChart("programMetricsChart", programMetrics.data);
    }

    async function loadSessionsPage() {
        const filters = getFilters();
        const [kpis, monthly, byRegion] = await Promise.all([
            fetchJSON(`/session/kpis?${filters}`),
            fetchJSON(`/session/monthly?${filters}`),
            fetchJSON(`/session/by-region?${filters}`),
        ]);

        setText("sessionTotalSessions", kpis.metrics.total_sessions);
        setText("sessionTotalInstructors", kpis.metrics.total_instructors);
        setText("sessionActiveRegions", kpis.metrics.active_regions);
        setText("sessionPrograms", kpis.metrics.total_programs);

        renderLineChart("sessionMonthlyChart", monthly.data, "Sessions", "#20c997");
        renderBarChart("sessionRegionChart", byRegion.data, "Sessions", "#6610f2");
    }

    async function loadRegionPage() {
        const filters = getFilters();
        const [kpis, impact, monthly] = await Promise.all([
            fetchJSON(`/region/kpis?${filters}`),
            fetchJSON(`/region/impact?${filters}`),
            fetchJSON(`/region/monthly-impact?${filters}`),
        ]);

        setText("regionStudentsReached", kpis.metrics.total_students_reached);
        setText("regionStates", kpis.metrics.total_states);
        setText("regionPrograms", kpis.metrics.total_programs);
        setText("regionAverageImpact", kpis.metrics.avg_students_per_state_period);

        renderBarChart("regionStateChart", impact.data, "Students Reached", "#e83e8c");
        renderLineChart("regionMonthlyChart", monthly.data, "Students Reached", "#007bff");
    }

    async function loadInstructorPage() {
        const filters = getFilters();
        const [kpis, productivity, monthly] = await Promise.all([
            fetchJSON(`/instructor/kpis?${filters}`),
            fetchJSON(`/instructor/productivity?${filters}`),
            fetchJSON(`/instructor/monthly?${filters}`),
        ]);

        setText("instructorTotal", kpis.metrics.total_instructors);
        setText("instructorSessions", kpis.metrics.sessions_conducted);
        setText("instructorAverageSessions", kpis.metrics.avg_sessions_per_instructor);
        setText("instructorStudentsReached", kpis.metrics.total_students_reached);

        renderHorizontalBarChart("instructorTopChart", productivity.data, "Sessions Conducted", "#ffc107");
        renderLineChart("instructorMonthlyChart", monthly.data, "Sessions Conducted", "#28a745");
    }

    async function loadProgramsPage() {
        const filters = getFilters();
        const [kpis, metrics, distribution] = await Promise.all([
            fetchJSON(`/exposure/kpis?${filters}`),
            fetchJSON(`/exposure/program-metrics?${filters}`),
            fetchJSON(`/exposure/program-distribution?${filters}`),
        ]);

        setText("programTotalStudents", kpis.metrics.total_students);
        setText("programTotalPrograms", kpis.metrics.total_programs);
        setText("programRegions", kpis.metrics.total_regions);
        setText("programAverageStudents", kpis.metrics.avg_students_per_exposure);

        renderBarChart("programReachChart", metrics.data, "Students Reached", "#dc3545");
        renderDoughnutChart("programDistributionChart", distribution.data);
    }

    function setText(id, value) {
        const element = document.getElementById(id);
        if (!element) {
            return;
        }

        const numericValue = Number(value);
        element.textContent = Number.isFinite(numericValue) ? numericValue.toLocaleString() : (value ?? "-");
    }

    function renderBarChart(id, points, label, color) {
        renderChart(id, "bar", points, label, {
            backgroundColor: color,
            borderRadius: 6,
        });
    }

    function renderHorizontalBarChart(id, points, label, color) {
        renderChart(id, "bar", points, label, {
            backgroundColor: color,
            indexAxis: "y",
            borderRadius: 6,
        });
    }

    function renderLineChart(id, points, label, color) {
        renderChart(id, "line", points, label, {
            borderColor: color,
            backgroundColor: `${color}33`,
            tension: 0.35,
            fill: true,
        });
    }

    function renderDoughnutChart(id, points) {
        const palette = ["#007bff", "#17a2b8", "#28a745", "#ffc107", "#dc3545", "#6610f2", "#fd7e14", "#20c997"];
        renderChart(id, "doughnut", points, "Programs", {
            backgroundColor: points.map((_, index) => palette[index % palette.length]),
            borderWidth: 1,
        });
    }

    function renderChart(id, type, points, label, datasetOptions) {
        const canvas = document.getElementById(id);
        if (!canvas) {
            return;
        }

        if (charts[id]) {
            charts[id].destroy();
        }

        charts[id] = new Chart(canvas, {
            type,
            data: {
                labels: points.map((point) => point.label),
                datasets: [{
                    label,
                    data: points.map((point) => point.value),
                    ...datasetOptions,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: type === "doughnut",
                        position: "bottom",
                    },
                },
                scales: type === "doughnut" ? {} : {
                    y: {
                        beginAtZero: true,
                    },
                },
            },
        });
    }

    async function fetchJSON(url) {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`Request failed for ${url}`);
        }
        return response.json();
    }
})();
