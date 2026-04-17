import ee

# =========================
# CLUSTER EXTRACTION
# =========================
def extract_clusters(mask, aoi):
    """
    Converts a binary waste mask into meaningful clusters (potential dumpsites)
    and filters them based on size, strength, and realism.
    """

    # =========================
    # VECTORIZE MASK
    # =========================
    vectors = mask.reduceToVectors(
        geometry=aoi,
        scale=20,  # balanced resolution
        geometryType="polygon",
        eightConnected=False,  # 🔥 prevents over-merging
        maxPixels=1e9
    )

    # =========================
    # ADD AREA (m²)
    # =========================
    vectors = vectors.map(
        lambda f: f.set("area_m2", f.geometry().area(1))
    )

    # =========================
    # CONVERT TO HECTARES
    # =========================
    vectors = vectors.map(
        lambda f: f.set(
            "area_ha",
            ee.Number(f.get("area_m2")).divide(10000)
        )
    )

    # =========================
    # FILTER: MIN SIZE
    # =========================
    MIN_AREA_HA = 1  # remove tiny noise
    vectors = vectors.filter(
        ee.Filter.gte("area_ha", MIN_AREA_HA)
    )

    # =========================
    # FILTER: MAX SIZE (🔥 CRITICAL)
    # =========================
    MAX_AREA_HA = 20  # remove unrealistic mega-clusters
    vectors = vectors.filter(
        ee.Filter.lte("area_ha", MAX_AREA_HA)
    )

    # =========================
    # FILTER: STRONG SIGNAL ONLY
    # =========================
    vectors = vectors.filter(
        ee.Filter.eq("label", 1)
    )

    # =========================
    # FILTER: PIXEL DENSITY
    # =========================
    vectors = vectors.filter(
        ee.Filter.gt("count", 30)
    )

    # =========================
    # CONVERT TO CENTROIDS
    # =========================
    vectors = vectors.map(
        lambda f: f.setGeometry(
            f.geometry().centroid(1)
        )
    )

    # =========================
    # LIMIT RESULTS
    # =========================
    return vectors.limit(200)