from fastapi import APIRouter, Query
from backend.services import attendance_service

router = APIRouter(prefix="/attendance", tags=["attendance"])

@router.get("/filters")
def get_filters():
    return attendance_service.get_attendance_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    return attendance_service.get_attendance_data(region, area, year, month, limit, offset)

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = attendance_service.get_attendance_data(region, area, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "attendance_report.xlsx")
