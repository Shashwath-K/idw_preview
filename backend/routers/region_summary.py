from fastapi import APIRouter, Query
from backend.services import region_summary_service

router = APIRouter(prefix="/region-summary", tags=["region-summary"])

@router.get("/filters")
def get_filters():
    return region_summary_service.get_region_summary_filters()

@router.get("/data")
def get_data(
    program_type: str | None = Query(default=None),
    year: str | None = Query(default=None),
    month: str | None = Query(default=None),
    limit: int = Query(default=15),
    offset: int = Query(default=0)
):
    return region_summary_service.get_region_summary_data(
        program_type=program_type,
        year=year,
        month=month,
        limit=limit,
        offset=offset
    )

@router.get("/export")
def export_data(
    program_type: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = region_summary_service.get_region_summary_data(program_type, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "region_summary_report.xlsx")
