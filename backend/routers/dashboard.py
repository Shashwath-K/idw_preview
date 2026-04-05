from fastapi import APIRouter, Query
from backend.services import overview_service
from backend.services import region_service

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

@router.get("/filters")
def get_filters():
    from backend.services.query_utils import fetch_all
    from backend.config import DATAMART_SCHEMA_NAME
    
    # 1. Fetch Regions via service
    regions = region_service.get_region_options()
    
    # 2. Fetch Programs via standardized query helper
    prog_rows = fetch_all(f"SELECT DISTINCT program_name FROM {DATAMART_SCHEMA_NAME}.dim_program WHERE program_name IS NOT NULL ORDER BY program_name")
    programs = [r["program_name"] for r in prog_rows if r.get("program_name")]
    
    return {
        "regions": regions,
        "programs": programs
    }


@router.get("/data")
def get_data(
    start_year: str | None = Query(None),
    end_year: str | None = Query(None),
    region: str | None = Query(None),
    program: str | None = Query(None)
):
    start = int(start_year) if start_year else None
    end = int(end_year) if end_year else None

    kpis = overview_service.get_overview_kpis(start, end, region, program)
    charts = overview_service.get_overview_charts(start, end, region, program)

    formatted_charts = {
        "instructors_by_region": {
            "labels": [item["label"] for item in charts["instructors_by_region"]],
            "datasets": [{
                "label": "Instructors",
                "data": [item["value"] for item in charts["instructors_by_region"]],
                "backgroundColor": "#3b82f6"
            }]
        },
        "drivers_by_region": {
            "labels": ["N/A"],
            "datasets": [{
                "label": "Drivers",
                "data": [0],
                "backgroundColor": "#10b981"
            }]
        },
        "programs_by_region": {
            "labels": [item["label"] for item in charts["programs_by_region"]],
            "datasets": [{
                "label": "Programs",
                "data": [item["value"] for item in charts["programs_by_region"]],
                "backgroundColor": "#f59e0b"
            }]
        }
    }

    return {
        "kpis": kpis,
        "charts": formatted_charts
    }

@router.get("/export")
def export_data(
    start_year: str | None = Query(None),
    end_year: str | None = Query(None),
    region: str | None = Query(None),
    program: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    start = int(start_year) if start_year else None
    end = int(end_year) if end_year else None
    targets = overview_service.get_program_targets(start, end, region, program, limit=100000, offset=0)
    
    formatted_table = []
    for row in targets["table"]:
        formatted_table.append({
            "Program": row["label"],
            "Donor": row["donor"],
            "Sessions Actual": row["completed_sessions"],
            "Sessions Target": row["target_sessions"],
            "Progress %": row["progress_pct"],
            "Students Reached": row["students_reached"],
            "End Date": row["end_date"],
            "Status": row["status"]
        })
    return json_to_excel_streaming_response(formatted_table, "overview_report.xlsx")
