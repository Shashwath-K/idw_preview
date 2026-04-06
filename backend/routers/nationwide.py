from fastapi import APIRouter, Query
from backend.services import nationwide_service

router = APIRouter(prefix="/nationwide-dashboard", tags=["nationwide-dashboard"])

@router.get("/filters")
def get_filters():
    return nationwide_service.get_nationwide_filters()

@router.get("/data")
def get_data(
    start_year: str | None = Query(None),
    end_year:   str | None = Query(None),
    region:     str | None = Query(None),
    limit:      int        = Query(default=15),
    offset:     int        = Query(default=0),
):
    return nationwide_service.get_nationwide_data(start_year, end_year, region, limit, offset)

@router.get("/export")
def export_data(
    start_year: str | None = Query(None),
    end_year:   str | None = Query(None),
    region:     str | None = Query(None),
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = nationwide_service.get_nationwide_data(start_year, end_year, region, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "nationwide_dashboard.xlsx")
