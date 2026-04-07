# app/schemas/report_validation.py
from pydantic import BaseModel
from typing import Optional

class ValidationCreate(BaseModel):
    vote: str  # "confirm" or "reject"
    evidence_url: Optional[str] = None