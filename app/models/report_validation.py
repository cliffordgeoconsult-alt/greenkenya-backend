from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from app.db.base import Base


class ReportValidation(Base):
    __tablename__ = "report_validations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    report_id = Column(UUID(as_uuid=True), ForeignKey("community_reports.id"))

    vote = Column(String, nullable=False)  # confirm / reject
    evidence_url = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)