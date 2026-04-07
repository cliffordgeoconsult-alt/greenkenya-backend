from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.report import ReportCreate, ReportResponse
from app.schemas.report_validation import ValidationCreate
from app.services.report_service import create_report, validate_report

router = APIRouter()


@router.post("/", response_model=ReportResponse)
def submit_report(payload: ReportCreate, db: Session = Depends(get_db)):
    return create_report(db, payload)

@router.post("/{report_id}/validate")
def validate(report_id: str, payload: ValidationCreate, db: Session = Depends(get_db)):
    return validate_report(db, report_id, payload)