from fastapi import APIRouter, Query
from backend.services import data_quality_service

router = APIRouter(prefix="/data-quality", tags=["data-quality"])


@router.get("/report")
def get_report():
    return data_quality_service.get_data_quality_report()


@router.get("/alerts")
def get_alerts(limit: int = Query(50)):
    return data_quality_service.get_recent_alerts(limit)


@router.post("/check")
def run_check():
    return data_quality_service.run_data_quality_check()
