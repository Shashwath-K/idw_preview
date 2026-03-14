from fastapi import APIRouter, Query

from backend.models.schemas import CountResponse, KPIBundle, OptionsResponse, SeriesBundle
from backend.services import exposure_service


router = APIRouter(prefix="/exposure", tags=["exposure"])


@router.get("/total-students", response_model=CountResponse)
def total_students(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "count": exposure_service.get_total_students(start=start, end=end, region=region, program=program)
    }


@router.get("/kpis", response_model=KPIBundle)
def exposure_kpis(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {"metrics": exposure_service.get_exposure_kpis(start=start, end=end, region=region, program=program)}


@router.get("/program-metrics", response_model=SeriesBundle)
def program_metrics(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=25),
):
    return {
        "title": "Program Metrics",
        "data": exposure_service.get_program_metrics(
            start=start, end=end, region=region, program=program, limit=limit
        ),
    }


@router.get("/program-distribution", response_model=SeriesBundle)
def program_distribution(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Program Distribution",
        "data": exposure_service.get_program_distribution(start=start, end=end, region=region, program=program),
    }


@router.get("/programs", response_model=OptionsResponse)
def program_options():
    return {"programs": exposure_service.get_program_options()}
