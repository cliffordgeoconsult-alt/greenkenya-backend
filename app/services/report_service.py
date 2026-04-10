# app/services/report_service.py
from sqlalchemy.orm import Session
from app.models.report import CommunityReport
from app.utils.geo import to_grid
from app.models.report_validation import ReportValidation


def create_report(db: Session, data):
    grid = to_grid(data.latitude, data.longitude)

    # DESCRIPTION VALIDATION
    if data.report_type == "other":
        if not data.description or len(data.description.strip()) < 10:
            raise ValueError("Description required for 'other' reports")

    # IMAGE VALIDATION
    if data.images:
        if len(data.images) > 2:
            raise ValueError("Maximum 2 images allowed")

        for img in data.images:
            if not img.lower().endswith((".jpg", ".jpeg", ".png")):
                raise ValueError("Only JPG and PNG allowed")

    report = CommunityReport(
        report_type=data.report_type,
        description=data.description,
        location_grid=grid,
        latitude=data.latitude,
        longitude=data.longitude,
        images=data.images,
        status="pending",
        confidence_score=0.2,
        report_mode="anonymous"
    )

    db.add(report)
    db.commit()
    db.refresh(report)

    return report

def validate_report(db, report_id, data):
    report = db.query(CommunityReport).filter(CommunityReport.id == report_id).first()

    if not report:
        return None

    validation = ReportValidation(
        report_id=report_id,
        vote=data.vote,
        evidence_url=data.evidence_url
    )

    db.add(validation)

    # SIMPLE LOGIC (we improve later)
    if data.vote == "confirm":
        report.confidence_score += 0.1
    elif data.vote == "reject":
        report.confidence_score -= 0.1

    # Clamp values
    report.confidence_score = max(0, min(1, report.confidence_score))

    # Update status
    if report.confidence_score > 0.7:
        report.status = "verified"
    elif report.confidence_score < 0.3:
        report.status = "rejected"
    else:
        report.status = "under_verification"

    db.commit()
    db.refresh(report)

    return report