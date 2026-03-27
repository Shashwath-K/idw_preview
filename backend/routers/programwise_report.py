from fastapi import APIRouter, Query
from backend.services import programwise_report_service

router = APIRouter(prefix="/programwise-report", tags=["programwise-report"])

@router.get("/filters")
def get_filters():
    return programwise_report_service.get_programwise_report_filters()

@router.get("/data")
def get_data(
    category: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    return {"table": programwise_report_service.get_programwise_report_data(category, year, month)}
