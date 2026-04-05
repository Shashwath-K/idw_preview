from fastapi import APIRouter, Query
from backend.services import region_summary_service

router = APIRouter(prefix="/region-summary", tags=["region-summary"])

@router.get("/filters")
def get_filters(region: str | None = Query(default=None)):
    return region_summary_service.get_region_summary_filters(region_name=region)

@router.get("/data")
def get_data(
    region: str | None = Query(default=None),
    program_type: str | None = Query(default=None),
    year: str | None = Query(default=None),
    month: str | None = Query(default=None),
    limit: int = Query(default=15),
    offset: int = Query(default=0)
):
    return region_summary_service.get_region_summary_data(
        region=region,
        program_type=program_type,
        year=year,
        month=month,
        limit=limit,
        offset=offset
    )

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    program_type: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = region_summary_service.get_region_summary_data(region, program_type, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "region_summary_report.xlsx")
