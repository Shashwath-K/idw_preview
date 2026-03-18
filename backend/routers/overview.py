from fastapi import APIRouter, Query

from backend.models.schemas import KPIBundle, SeriesBundle
from backend.services import overview_service


router = APIRouter(prefix="/overview", tags=["overview"])


@router.get("/kpis", response_model=KPIBundle)
def overview_kpis(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {"metrics": overview_service.get_overview_kpis(start=start, end=end, region=region, program=program)}


@router.get("/program-targets")
def program_targets(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Program-level targets vs actuals",
        "data": overview_service.get_program_targets(start=start, end=end, region=region, program=program),
    }


@router.get("/sessions-by-activity", response_model=SeriesBundle)
def sessions_by_activity(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Sessions by activity type",
        "data": overview_service.get_sessions_by_activity(start=start, end=end, region=region, program=program),
    }


@router.get("/sessions-by-donor", response_model=SeriesBundle)
def sessions_by_donor(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Sessions by donor",
        "data": overview_service.get_sessions_by_donor(start=start, end=end, region=region, program=program),
    }
