# app/api/endpoints/reports.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.report import ReportCreate, ReportResponse
from app.schemas.report_validation import ValidationCreate
from app.services.report_service import create_report, validate_report
from app.models.report import CommunityReport
from app.models.report_validation import ReportValidation

router = APIRouter()


@router.post("/", response_model=ReportResponse)
def submit_report(payload: ReportCreate, db: Session = Depends(get_db)):
    return create_report(db, payload)

@router.post("/{report_id}/validate")
def validate(report_id: str, payload: ValidationCreate, db: Session = Depends(get_db)):
    return validate_report(db, report_id, payload)

@router.get("/", response_model=list[ReportResponse])
def get_reports(
    status: str = None,
    report_type: str = None,
    limit: int = None,
    db: Session = Depends(get_db)
):
    query = db.query(CommunityReport)

    if status:
        query = query.filter(CommunityReport.status == status)

    if report_type:
        query = query.filter(CommunityReport.report_type == report_type)

    query = query.order_by(CommunityReport.created_at.desc())

    if limit:
        query = query.limit(limit)

    return query.all()

@router.get("/{report_id}", response_model=ReportResponse)
def get_report(report_id: str, db: Session = Depends(get_db)):
    return db.query(CommunityReport).filter(CommunityReport.id == report_id).first()

@router.delete("/{report_id}")
def delete_report(report_id: str, db: Session = Depends(get_db)):
    report = db.query(CommunityReport).filter(CommunityReport.id == report_id).first()

    if not report:
        return {"error": "Report not found"}

    # DELETE VALIDATIONS FIRST
    db.query(ReportValidation).filter(ReportValidation.report_id == report_id).delete()

    # THEN DELETE REPORT
    db.delete(report)
    db.commit()

    return {"message": "Report deleted"}