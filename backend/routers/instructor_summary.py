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
    month: int | None = Query(None)
):
    return instructor_summary_service.get_instructor_summary_data(
        region=region,
        area=area,
        year=year,
        month=month
    )
