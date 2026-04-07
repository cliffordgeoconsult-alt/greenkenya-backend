# app/main.py
# This is the main entry point for the Green Kenya API application. 
# It sets up the FastAPI app, includes the API router, and defines a root endpoint to confirm that the backend is running.
# app/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware 
from app.api.router import api_router
from app.db.base import Base
from app.db.session import engine

app = FastAPI(
    title="Green Kenya API",
    description="Environmental Intelligence Platform for Kenya",
    version="1.0.0"
)

# ADD CORS RIGHT HERE (VERY IMPORTANT POSITION)
origins = [
    "http://localhost:5175",
    "http://127.0.0.1:5175",
    "*"  # temporary for debugging
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Creating tables
# Base.metadata.create_all(bind=engine)

# Routers come AFTER middleware
app.include_router(api_router, prefix="/api/v1")


@app.get("/")
def root():
    return {"message": "Green Kenya Backend Running"}