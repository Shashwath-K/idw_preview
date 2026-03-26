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
    month: int | None = Query(None)
):
    return {"table": attendance_service.get_attendance_data(region, area, year, month)}
