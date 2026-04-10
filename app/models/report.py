# app/models/report.py
from sqlalchemy import Column, String, DateTime, Float, Text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
from sqlalchemy import JSON
from app.db.base import Base


class CommunityReport(Base):
    __tablename__ = "community_reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    report_type = Column(String, nullable=False)  # forest_loss, dumping, pollution
    description = Column(Text, nullable=True)

    # SAFETY: store generalized location (NOT exact lat/lon)
    location_grid = Column(String, nullable=False)

    # Optional raw coords (ONLY if you want internally, but be careful)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    images = Column(JSON, nullable=True)  # max 2 images

    status = Column(String, default="pending")  # pending, verified, rejected
    confidence_score = Column(Float, default=0.2)

    report_mode = Column(String, default="anonymous")  # anonymous / verified

    created_at = Column(DateTime, default=datetime.utcnow)