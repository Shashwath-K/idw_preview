from fastapi import APIRouter, Query

from backend.models.schemas import KPIBundle, OptionsResponse, SeriesBundle
from backend.services import region_service


router = APIRouter(prefix="/region", tags=["region"])


@router.get("/kpis", response_model=KPIBundle)
def region_kpis(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {"metrics": region_service.get_region_kpis(start=start, end=end, region=region, program=program)}


@router.get("/impact", response_model=SeriesBundle)
def region_impact(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Region Impact",
        "data": region_service.get_region_impact(start=start, end=end, region=region, program=program),
    }


@router.get("/monthly-impact", response_model=SeriesBundle)
def monthly_region_impact(
    start: int | None = Query(default=None),
    end: int | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
):
    return {
        "title": "Monthly Region Impact",
        "data": region_service.get_monthly_region_impact(start=start, end=end, region=region, program=program),
    }


@router.get("/options", response_model=OptionsResponse)
def region_options():
    return {"regions": region_service.get_region_options()}
