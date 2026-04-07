from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime


class ReportCreate(BaseModel):
    report_type: str
    description: Optional[str] = None
    latitude: float
    longitude: float
    evidence_url: Optional[str] = None


class ReportResponse(BaseModel):
    id: UUID
    report_type: str
    description: Optional[str]
    location_grid: str
    status: str
    confidence_score: float
    created_at: datetime

    class Config:
        from_attributes = True