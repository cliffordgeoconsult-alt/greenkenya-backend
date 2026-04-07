# app/models/ward.py
# This file defines the Ward model, which represents a ward in the database. Each ward has an id, name, and a foreign key to the county it belongs to.
from sqlalchemy import Column, String, ForeignKey
from app.db.base import Base

class Ward(Base):
    __tablename__ = "wards"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)

    county_id = Column(String, ForeignKey("counties.county_id"))