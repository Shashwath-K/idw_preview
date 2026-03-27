from fastapi import APIRouter, Query
from backend.services import work_day_service

router = APIRouter(prefix="/work-day", tags=["work-day"])

@router.get("/filters")
def get_filters():
    return work_day_service.get_work_day_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    return work_day_service.get_work_day_data(region, area, year, month, limit, offset)

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = work_day_service.get_work_day_data(region, area, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "work_day_report.xlsx")
