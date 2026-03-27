from fastapi import APIRouter, Query
from backend.services import instructor_summary_service

router = APIRouter(prefix="/instructor-summary", tags=["instructor-summary"])

@router.get("/filters")
def get_filters():
    return instructor_summary_service.get_instructor_summary_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    return instructor_summary_service.get_instructor_summary_data(
        region=region,
        area=area,
        year=year,
        month=month,
        limit=limit,
        offset=offset
    )

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = instructor_summary_service.get_instructor_summary_data(region, area, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "instructor_summary_report.xlsx")
