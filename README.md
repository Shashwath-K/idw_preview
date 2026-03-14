# Pramana Dashboard

This project implements a Data Warehouse Analytics Dashboard built on top of a PostgreSQL star schema using FastAPI and AdminLTE.

## Project Overview

The application provides a production-style analytics interface for the `pramana` PostgreSQL data warehouse. It exposes read-only FastAPI endpoints over existing `dim_*` and `fact_*` tables and renders a professional AdminLTE user interface with Chart.js visualizations for sessions, regional impact, instructor productivity, and program metrics.

## Architecture

The solution follows a layered architecture:

`PostgreSQL -> FastAPI API -> AdminLTE UI -> Chart.js`

- PostgreSQL stores the star schema and serves as the source of truth.
- FastAPI exposes read-only analytics endpoints.
- Jinja templates render the AdminLTE 3 interface.
- Vanilla JavaScript calls the API and updates KPI cards and charts dynamically.

## Database Schema

The dashboard assumes that the `pramana` database already exists and contains warehouse tables in a star schema layout, including:

- `fact_session_event`
- `fact_exposure`
- `fact_monthly_region_impact`
- `fact_instructor_productivity`
- `dim_location`
- `dim_program`
- `dim_instructor`

The implementation does not create tables or modify the schema. All SQL is read-only and centralized in the backend service layer.

## Backend

The backend is built with FastAPI and `psycopg2`. It is organized into:

- `config.py` for application paths and database configuration
- `db.py` for PostgreSQL connection management
- `routers/` for HTTP endpoints
- `services/` for read-only SQL query logic
- `models/` for Pydantic response models

## Frontend

The frontend uses AdminLTE 3, Bootstrap, Jinja2 templates, Chart.js, and vanilla JavaScript.

The UI includes:

- A main dashboard page with KPI cards and four charts
- Dedicated pages for sessions, region impact, instructor productivity, and program metrics
- Shared filters for year range, region, and program
- Live chart refresh when filter values change

## API Layer

Representative endpoints include:

- `GET /session/count`
- `GET /session/kpis`
- `GET /session/monthly`
- `GET /region/impact`
- `GET /region/monthly-impact`
- `GET /instructor/productivity`
- `GET /exposure/program-metrics`

Query parameters support filtered analysis using:

- `start`
- `end`
- `region`
- `program`

## How to Run

1. Create and activate a Python virtual environment.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Start the application from the `pramana_dashboard` directory:

   ```bash
   uvicorn backend.app:app --reload
   ```

4. Open the application in a browser:

   `http://127.0.0.1:8000`

If your PostgreSQL credentials differ from the defaults, set these environment variables before starting the server:

- `PRAMANA_DB_NAME`
- `PRAMANA_DB_USER`
- `PRAMANA_DB_PASSWORD`
- `PRAMANA_DB_HOST`
- `PRAMANA_DB_PORT`

## Folder Structure

```text
pramana_dashboard/
├── backend/
│   ├── app.py
│   ├── config.py
│   ├── db.py
│   ├── models/
│   ├── routers/
│   └── services/
├── frontend/
│   ├── static/
│   └── templates/
├── requirements.txt
└── README.md
```

## Future Improvements

- Add authentication and role-based access control
- Introduce caching for high-volume aggregation queries
- Add automated tests for API and template rendering
- Vendor AdminLTE assets locally for fully offline deployments
- Expand the filter model to support district, school, and instructor drill-down
