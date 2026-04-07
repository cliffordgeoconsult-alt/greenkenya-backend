from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_counties

router = APIRouter()

@router.get("/")
def get_counties_endpoint(db: Session = Depends(get_db)):
    return get_counties(db)