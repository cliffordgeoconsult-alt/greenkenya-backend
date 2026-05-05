# app/services/admin_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text

TARGET_COUNTIES = [
    "NAIROBI",
    "NAKURU",
    "KISUMU",
    "MOMBASA",
    "KISII",
    "NYERI",
    "TANA RIVER",
    "TAITA TAVETA",
    "NAROK"
]

# -------------------------
# COUNTIES
# -------------------------
# def get_counties(db: Session):
#     query = """
#     SELECT
#         id,
#         name,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_county
#     """

#     result = db.execute(text(query))

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_counties(db: Session):
    query = """
    SELECT
        id,
        name,
        ST_AsGeoJSON(geometry) as geojson
    FROM admin_county
    WHERE UPPER(name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {"county_names": TARGET_COUNTIES}
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "geometry": row.geojson
        }
        for row in result
    ]

# -------------------------
# SUBCOUNTIES
# -------------------------
# def get_subcounties(db: Session):
#     query = """
#     SELECT
#         id,
#         name,
#         county_id,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_subcounty
#     """

#     result = db.execute(text(query))

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "county_id": row.county_id,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_subcounties(db: Session):
    query = """
    SELECT
        sc.id,
        sc.name,
        sc.county_id,
        ST_AsGeoJSON(sc.geometry) as geojson
    FROM admin_subcounty sc
    JOIN admin_county c ON sc.county_id = c.id
    WHERE UPPER(c.name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {"county_names": TARGET_COUNTIES}
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "geometry": row.geojson
        }
        for row in result
    ]


# def get_subcounties_by_county(db: Session, county_id: str):
#     query = """
#     SELECT 
#         id,
#         name,
#         county_id,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_subcounty
#     WHERE county_id = :county_id
#     """

#     result = db.execute(text(query), {"county_id": county_id})

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "county_id": row.county_id,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_subcounties_by_county(db: Session, county_id: str):
    query = """
    SELECT 
        sc.id,
        sc.name,
        sc.county_id,
        ST_AsGeoJSON(sc.geometry) as geojson
    FROM admin_subcounty sc
    JOIN admin_county c ON sc.county_id = c.id
    WHERE sc.county_id = :county_id
    AND UPPER(c.name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {
            "county_id": county_id,
            "county_names": TARGET_COUNTIES
        }
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "geometry": row.geojson
        }
        for row in result
    ]


# -------------------------
# WARDS
# -------------------------
# def get_wards(db: Session):
#     query = """
#     SELECT
#         id,
#         name,
#         county_id,
#         subcounty_id,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_ward
#     """

#     result = db.execute(text(query))

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "county_id": row.county_id,
#             "subcounty_id": row.subcounty_id,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_wards(db: Session):
    query = """
    SELECT
        w.id,
        w.name,
        w.county_id,
        w.subcounty_id,
        ST_AsGeoJSON(w.geometry) as geojson
    FROM admin_ward w
    JOIN admin_county c ON w.county_id = c.id
    WHERE UPPER(c.name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {"county_names": TARGET_COUNTIES}
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "subcounty_id": row.subcounty_id,
            "geometry": row.geojson
        }
        for row in result
    ]

# def get_wards_by_county(db: Session, county_id: str):
#     query = """
#     SELECT
#         id,
#         name,
#         county_id,
#         subcounty_id,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_ward
#     WHERE county_id = :county_id
#     """

#     result = db.execute(text(query), {"county_id": county_id})

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "county_id": row.county_id,
#             "subcounty_id": row.subcounty_id,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_wards_by_county(db: Session, county_id: str):
    query = """
    SELECT
        w.id,
        w.name,
        w.county_id,
        w.subcounty_id,
        ST_AsGeoJSON(w.geometry) as geojson
    FROM admin_ward w
    JOIN admin_county c ON w.county_id = c.id
    WHERE w.county_id = :county_id
    AND UPPER(c.name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {
            "county_id": county_id,
            "county_names": TARGET_COUNTIES
        }
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "subcounty_id": row.subcounty_id,
            "geometry": row.geojson
        }
        for row in result
    ]


# def get_wards_by_subcounty(db: Session, subcounty_id: str):
#     query = """
#     SELECT
#         id,
#         name,
#         county_id,
#         subcounty_id,
#         ST_AsGeoJSON(geometry) as geojson
#     FROM admin_ward
#     WHERE subcounty_id = :subcounty_id
#     """

#     result = db.execute(text(query), {"subcounty_id": subcounty_id})

#     return [
#         {
#             "id": row.id,
#             "name": row.name,
#             "county_id": row.county_id,
#             "subcounty_id": row.subcounty_id,
#             "geometry": row.geojson
#         }
#         for row in result
#     ]
def get_wards_by_subcounty(db: Session, subcounty_id: str):
    query = """
    SELECT
        w.id,
        w.name,
        w.county_id,
        w.subcounty_id,
        ST_AsGeoJSON(w.geometry) as geojson
    FROM admin_ward w
    JOIN admin_county c ON w.county_id = c.id
    WHERE w.subcounty_id = :subcounty_id
    AND UPPER(c.name) = ANY(:county_names)
    """

    result = db.execute(
        text(query),
        {
            "subcounty_id": subcounty_id,
            "county_names": TARGET_COUNTIES
        }
    )

    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "subcounty_id": row.subcounty_id,
            "geometry": row.geojson
        }
        for row in result
    ]