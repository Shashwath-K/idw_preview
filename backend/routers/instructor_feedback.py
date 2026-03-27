from fastapi import APIRouter, Query
from backend.services import instructor_feedback_service

router = APIRouter(prefix="/instructor-feedback", tags=["instructor-feedback"])

@router.get("/filters")
def get_filters():
    return instructor_feedback_service.get_instructor_feedback_filters()

@router.get("/data")
def get_data(
    instructor_name: str | None = Query(None),
    year: str | None = Query(None),
    limit: int = Query(15),
    offset: int = Query(0)
):
    return instructor_feedback_service.get_instructor_feedback_data(instructor_name, year, limit, offset)

@router.get("/export")
def export_data(
    instructor_name: str | None = Query(None),
    year: str | None = Query(None)
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = instructor_feedback_service.get_instructor_feedback_data(instructor_name, year, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "instructor_feedback_report.xlsx")
