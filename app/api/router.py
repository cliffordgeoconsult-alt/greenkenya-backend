# app/api/router.py
from fastapi import APIRouter
from app.api.endpoints import counties, subcounties, wards, forests, reports, waste, ai, carbon

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
api_router.include_router(
    waste.router,
    prefix="/wastes",
    tags=["Waste"]
)

api_router.include_router(
    ai.router,
    prefix="/ai",
    tags=["AI"]
)

api_router.include_router(
    carbon.router,
    prefix="/carbon",
    tags=["Carbon"]
)