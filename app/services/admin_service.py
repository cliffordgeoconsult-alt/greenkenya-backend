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

# Urban heat pilot (LST / land-cover context) — subset of counties with real EO only.
UHI_TARGET_COUNTIES = [
    "NAIROBI",
    "NAKURU",
    "KISUMU",
    "MOMBASA",
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


def get_uhi_counties(db: Session):
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
        {"county_names": UHI_TARGET_COUNTIES},
    )
    return [
        {"id": row.id, "name": row.name, "geometry": row.geojson}
        for row in result
    ]


def get_forest_reserves_intersecting_uhi_counties(db: Session) -> list[dict]:
    """Distinct forest reserves whose geometry intersects any UHI pilot county."""
    query = """
    SELECT DISTINCT r.reserve_id, ST_AsGeoJSON(r.geometry) AS gj
    FROM forest_reserves r
    INNER JOIN admin_county c ON ST_Intersects(r.geometry, c.geometry)
    WHERE UPPER(c.name) = ANY(:county_names)
    """
    result = db.execute(
        text(query),
        {"county_names": UHI_TARGET_COUNTIES},
    )
    return [
        {"reserve_id": str(row.reserve_id), "geometry": row.gj} for row in result
    ]


def count_forest_reserves_intersecting_uhi_counties(db: Session) -> int:
    """Count reserves intersecting UHI pilot counties (no geometry payload)."""
    row = db.execute(
        text("""
            SELECT COUNT(DISTINCT r.reserve_id)
            FROM forest_reserves r
            INNER JOIN admin_county c ON ST_Intersects(r.geometry, c.geometry)
            WHERE UPPER(c.name) = ANY(:county_names)
        """),
        {"county_names": UHI_TARGET_COUNTIES},
    ).scalar()
    return int(row or 0)


def get_uhi_wards(db: Session):
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
        {"county_names": UHI_TARGET_COUNTIES},
    )
    return [
        {
            "id": row.id,
            "name": row.name,
            "county_id": row.county_id,
            "subcounty_id": row.subcounty_id,
            "geometry": row.geojson,
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