from fastapi import APIRouter, Query

from backend.models.schemas import KPIBundle, SeriesBundle
from backend.services import instructor_service


router = APIRouter(prefix="/instructor", tags=["instructor"])


@router.get("/kpis", response_model=KPIBundle)
def instructor_kpis(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "metrics": instructor_service.get_instructor_kpis(start=start, end=end, region=region, program=program)
    }


@router.get("/productivity", response_model=SeriesBundle)
def instructor_productivity(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=25),
):
    return {
        "title": "Instructor Productivity",
        "data": instructor_service.get_instructor_productivity(
            start=start, end=end, region=region, program=program, limit=limit
        ),
    }


@router.get("/monthly", response_model=SeriesBundle)
def monthly_instructor_activity(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Monthly Instructor Activity",
        "data": instructor_service.get_monthly_instructor_activity(
            start=start, end=end, region=region, program=program
        ),
    }
