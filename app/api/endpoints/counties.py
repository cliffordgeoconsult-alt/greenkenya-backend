from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_counties():
    return {"message": "List of counties will appear here"}