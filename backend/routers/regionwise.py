from fastapi import APIRouter, Query
from backend.services import regionwise_service

router = APIRouter(prefix="/regionwise-dashboard", tags=["regionwise-dashboard"])

@router.get("/filters")
def get_filters(region: str | None = Query(None)):
    return regionwise_service.get_regionwise_filters(region_name=region)

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area:   str | None = Query(None),
    year:   str | None = Query(None),
    month:  str | None = Query(None),
    limit:  int        = Query(default=15),
    offset: int        = Query(default=0),
):
    return regionwise_service.get_regionwise_data(region, area, year, month, limit, offset)

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    area:   str | None = Query(None),
    year:   str | None = Query(None),
    month:  str | None = Query(None),
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = regionwise_service.get_regionwise_data(region, area, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "regionwise_dashboard.xlsx")
