# app/services/admin_loader.py
import geopandas as gpd
from sqlalchemy.orm import Session
from app.models.county import County
def load_counties(db: Session, path):
    gdf = gpd.read_file(path)
    for _, row in gdf.iterrows():
        county = County(
            county_id=str(row["id"]),
            name=row["name"],
            geometry=row["geometry"].wkt
        )
        db.add(county)
    db.commit()