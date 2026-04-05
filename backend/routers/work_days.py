from fastapi import APIRouter, Query, Request
from backend.services.work_days_service import get_work_days_filters, get_work_days_data
from typing import Optional

router = APIRouter(prefix="/api/work-days", tags=["Work Days Report"])

@router.get("/filters")
async def work_days_filters(region_id: Optional[str] = Query(None)):
    # Handle empty string from frontend
    reg_id = int(region_id) if region_id and region_id.strip() else None
    return get_work_days_filters(reg_id)

@router.get("/data")
async def work_days_data(
    region: Optional[str] = Query(None),
    area: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    limit: int = 15,
    offset: int = 0
):
    # Safe conversion of parameters
    reg_id = int(region) if region and region.strip() else None
    ar_id = int(area) if area and area.strip() else None
    yr = int(year) if year and year.strip() else None
    mn = int(month) if month and month.strip() else None
    
    return get_work_days_data(reg_id, ar_id, yr, mn, limit, offset)
