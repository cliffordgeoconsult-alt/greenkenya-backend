from pydantic import BaseModel


class ForestCreate(BaseModel):
    forest_code: str
    area_ha: float
    county: str
    geometry: dict

    baseline_year: int
    source: str
    confidence: float


class ForestResponse(BaseModel):
    forest_id: str
    forest_code: str
    area_ha: float
    county: str

    baseline_year: int
    source: str
    confidence: float