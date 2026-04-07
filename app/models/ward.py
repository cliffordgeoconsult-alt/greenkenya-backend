from sqlalchemy import Column, String
from app.db.base import Base


class Ward(Base):
    __tablename__ = "wards"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)