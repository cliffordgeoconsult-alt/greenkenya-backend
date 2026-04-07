from sqlalchemy.orm import Session
from app.models.report import CommunityReport
from app.utils.geo import to_grid
from app.models.report_validation import ReportValidation
from app.models.report import CommunityReport


def create_report(db: Session, data):
    grid = to_grid(data.latitude, data.longitude)

    report = CommunityReport(
        report_type=data.report_type,
        description=data.description,
        location_grid=grid,
        latitude=data.latitude,
        longitude=data.longitude,
        evidence_url=data.evidence_url,
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