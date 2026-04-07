import geopandas as gpd
from app.db.session import SessionLocal
from app.models.county import County

db = SessionLocal()

gdf = gpd.read_file("data/kenya_counties.geojson")

for _, row in gdf.iterrows():

    county = County(
        county_id=str(row["id"]),
        name=row["name"],
        geometry=row["geometry"].wkt
    )

    db.add(county)

db.commit()

print("Counties loaded.")