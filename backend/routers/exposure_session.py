from fastapi import APIRouter, Query
from backend.services import exposure_session_service

router = APIRouter(prefix="/exposure-session-dashboard", tags=["exposure-session-dashboard"])

@router.get("/filters")
def get_filters():
    return exposure_session_service.get_exposure_session_filters()

@router.get("/data")
def get_data(
    region:  str | None = Query(None),
    program: str | None = Query(None),
    year:    str | None = Query(None),
    month:   str | None = Query(None),
    limit:   int        = Query(default=15),
    offset:  int        = Query(default=0),
):
    return exposure_session_service.get_exposure_session_data(region, program, year, month, limit, offset)

@router.get("/export")
def export_data(
    region:  str | None = Query(None),
    program: str | None = Query(None),
    year:    str | None = Query(None),
    month:   str | None = Query(None),
):
    from backend.services.export_utils import json_to_excel_streaming_response
    data = exposure_session_service.get_exposure_session_data(region, program, year, month, limit=100000, offset=0)
    return json_to_excel_streaming_response(data["table"], "exposure_session_dashboard.xlsx")
