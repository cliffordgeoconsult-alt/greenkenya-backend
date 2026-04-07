# app/db/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
DATABASE_URL = "postgresql://postgres:greenmap@localhost:5432/greenkenya"
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()