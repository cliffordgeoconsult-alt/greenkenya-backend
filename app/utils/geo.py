def to_grid(lat: float, lon: float) -> str:
    # Simple version (you can improve later)
    lat_grid = round(lat, 2)
    lon_grid = round(lon, 2)
    return f"{lat_grid}_{lon_grid}"