# app/models/county.py
from sqlalchemy import Column, String
from geoalchemy2 import Geometry
from app.db.base import Base
class County(Base):
    __tablename__ = "counties"

    county_id = Column(String, primary_key=True)

    name = Column(String, index=True)

    geometry = Column(Geometry("MULTIPOLYGON", srid=4326))