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
    month: str | None = Query(None)
):
    return {"table": work_day_service.get_work_day_data(region, area, year, month)}
