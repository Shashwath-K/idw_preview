from fastapi import APIRouter, Query

from backend.models.schemas import KPIBundle, SeriesBundle
from backend.services import instructor_service


router = APIRouter(prefix="/instructor", tags=["instructor"])


@router.get("/kpis", response_model=KPIBundle)
def instructor_kpis(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
):
    return {
        "metrics": instructor_service.get_instructor_kpis(start=start, end=end, region=region, program=program, instructor=instructor)
    }


@router.get("/session-log")
def instructor_session_log(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
    limit: int = Query(default=8, ge=1, le=25),
):
    return {
        "title": "Instructor Session Log",
        "data": instructor_service.get_instructor_session_log(
            start=start, end=end, region=region, program=program, instructor=instructor, limit=limit
        ),
    }


@router.get("/type-breakdown", response_model=SeriesBundle)
def instructor_type_breakdown(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
):
    return {
        "title": "Sessions by Instructor Type",
        "data": instructor_service.get_sessions_by_instructor_type(
            start=start, end=end, region=region, program=program, instructor=instructor
        ),
    }


@router.get("/multi-program")
def instructor_multi_program(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
):
    return {
        "title": "Multi-program Instructors",
        "data": instructor_service.get_multi_program_instructors(
            start=start, end=end, region=region, program=program, instructor=instructor, limit=limit
        ),
    }


@router.get("/productivity", response_model=SeriesBundle)
def instructor_productivity(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=25),
):
    return {
        "title": "Instructor Productivity",
        "data": instructor_service.get_instructor_productivity(
            start=start, end=end, region=region, program=program, instructor=instructor, limit=limit
        ),
    }


@router.get("/monthly", response_model=SeriesBundle)
def monthly_instructor_activity(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    region: str | None = Query(default=None),
    program: str | None = Query(default=None),
    instructor: str | None = Query(default=None),
):
    return {
        "title": "Monthly Instructor Activity",
        "data": instructor_service.get_monthly_instructor_activity(
            start=start, end=end, region=region, program=program, instructor=instructor
        ),
    }


@router.get("/types")
def instructor_type_options():
    return {"types": instructor_service.get_instructor_type_options()}
