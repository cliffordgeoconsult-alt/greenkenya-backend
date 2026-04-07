# app/api/router.py
from fastapi import APIRouter
from app.api.endpoints import counties, subcounties, wards, forests, reports

api_router = APIRouter()

api_router.include_router(
    counties.router,
    prefix="/counties",
    tags=["Counties"]
)

api_router.include_router(
    subcounties.router,
    prefix="/subcounties",
    tags=["Subcounties"]
)

api_router.include_router(
    wards.router,
    prefix="/wards",
    tags=["Wards"]
)
api_router.include_router(
    forests.router,
    prefix="/forests",
    tags=["Forests"]
)
api_router.include_router(
    reports.router, 
    prefix="/reports", 
    tags=["Reports"]
)