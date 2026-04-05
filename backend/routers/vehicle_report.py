from fastapi import APIRouter, Query, HTTPException
from backend.services.vehicle_report_service import get_vehicle_report_filters, get_vehicle_report_data

router = APIRouter(prefix="/api/vehicle-report", tags=["Vehicle Report"])

@router.get("/filters")
def vehicle_filters(region_name: str = Query(None)):
    return get_vehicle_report_filters(region_name)

from typing import Optional

@router.get("/data")
def vehicle_data(
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    # Convert empty strings to None and valid strings to int
    y = int(year) if year and year.strip() else None
    m = int(month) if month and month.strip() else None
    
    res = get_vehicle_report_data(region, area, y, m, limit, offset)
    if "error" in res:
        raise HTTPException(status_code=500, detail=res["error"])
    return res

@router.get("/debug")
def vehicle_debug():
    return get_vehicle_report_data()
