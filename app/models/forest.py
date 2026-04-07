# app/models/forest.py
from sqlalchemy import Column, String, Float, DateTime, Integer
from geoalchemy2 import Geometry
from sqlalchemy.sql import func
import uuid
from app.db.base import Base

class Forest(Base):
    __tablename__ = "forests"

    forest_id = Column(
        String,
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    forest_code = Column(String, unique=True)
    area_ha = Column(Float)
    county = Column(String)

    baseline_year = Column(Integer)
    source = Column(String)
    confidence = Column(Float)

    geometry = Column(
        Geometry("MULTIPOLYGON", srid=4326, spatial_index=True)
    )

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )