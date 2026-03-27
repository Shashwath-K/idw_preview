from fastapi import APIRouter, Query
from backend.services import school_visit_service

router = APIRouter(prefix="/school-visit", tags=["school-visit"])

@router.get("/filters")
def get_filters():
    return school_visit_service.get_school_visit_filters()

@router.get("/data")
def get_data(
    region: str | None = Query(None),
    area: str | None = Query(None),
    program: str | None = Query(None),
    year: str | None = Query(None),
    month: str | None = Query(None)
):
    return {"table": school_visit_service.get_school_visit_data(region, area, program, year, month)}
