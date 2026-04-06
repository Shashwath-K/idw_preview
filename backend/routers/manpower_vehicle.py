from fastapi import APIRouter, Query
from backend.services import manpower_vehicle_service

router = APIRouter(prefix="/manpower-vehicle-dashboard", tags=["manpower-vehicle-dashboard"])

@router.get("/filters")
def get_filters():
    return manpower_vehicle_service.get_manpower_vehicle_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    year:   str | None = Query(None),
    month:  str | None = Query(None),
    limit:  int        = Query(default=15),
    offset: int        = Query(default=0),
):
    return manpower_vehicle_service.get_manpower_vehicle_data(region, year, month, limit, offset)

@router.get("/export")
def export_data(
    region: str | None = Query(None),
    year:   str | None = Query(None),
    month:  str | None = Query(None),
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = manpower_vehicle_service.get_manpower_vehicle_data(region, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "manpower_vehicle_dashboard.xlsx")
