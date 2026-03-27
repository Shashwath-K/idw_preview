from fastapi import APIRouter, Query
from backend.services import arealead_summary_service

router = APIRouter(prefix="/arealead-summary", tags=["arealead-summary"])

@router.get("/filters")
def get_filters():
    return arealead_summary_service.get_arealead_summary_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    return arealead_summary_service.get_arealead_summary_data(region, area, year, month, limit, offset)
