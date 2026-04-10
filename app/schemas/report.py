# app/schemas/report.py
from pydantic import BaseModel
from typing import Optional
from uuid import UUID
from datetime import datetime
from typing import List

class ReportCreate(BaseModel):
    report_type: str
    description: Optional[str] = None
    latitude: float
    longitude: float
    images: Optional[List[str]] = None # List of image URLs (max 2)


class ReportResponse(BaseModel):
    id: UUID
    report_type: str
    description: Optional[str]
    location_grid: str
    status: str
    confidence_score: float
    created_at: datetime
    images: Optional[List[str]] = None

    class Config:
        from_attributes = True