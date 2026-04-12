# app/services/forest_service.py
from sqlalchemy.orm import Session
from app.models.forest import Forest

def create_forest(db: Session, forest):

    new_forest = Forest(
        forest_code=forest.forest_code,
        area_ha=forest.area_ha,
        county=forest.county,
        geometry=forest.geometry,
        baseline_year=forest.baseline_year,
        source=forest.source,
        confidence=forest.confidence
    )

    db.add(new_forest)
    db.commit()
    db.refresh(new_forest)

    return new_forest


def get_forests(db: Session):
    return db.query(Forest).all()