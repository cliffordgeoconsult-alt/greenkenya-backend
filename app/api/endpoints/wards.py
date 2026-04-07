from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.admin_service import get_wards

router = APIRouter()

@router.get("/")
def get_wards_endpoint(db: Session = Depends(get_db)):
    return get_wards(db)