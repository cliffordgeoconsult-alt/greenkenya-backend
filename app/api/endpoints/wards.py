# api/endpoints/wards.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_wards():
    return {"message": "List of wards will appear here"}