#!/usr/bin/env python3
# Fully offline (after first run): Finds ZIP at lat/lon + nearby ZIPs via GeoNames US.zip (cached).
# Standard library only. No API keys. No Nominatim.
#
# Usage:
#   python3 zip_radius_offline.py --lat 37.5483 --lon -121.9886 --radius 10 --units mi
#   # or from Python:
#   #   from zip_radius_offline import get_zip_and_nearby
#   #   get_zip_and_nearby(37.5483, -121.9886, 10, "mi")

import sys, os, io, csv, ssl, math, time, argparse, zipfile
from urllib import request, error

# GeoNames postal dataset (free, no key). We cache it after the first download.
GEONAMES_BASE = "https://download.geonames.org/export/zip"
GEONAMES_FILE = "US.zip"

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".zip_radius_cache")
GEONAMES_DIR = os.path.join(CACHE_DIR, "geonames_us")
GEONAMES_ZIP = os.path.join(GEONAMES_DIR, GEONAMES_FILE)
GEONAMES_TXT = os.path.join(GEONAMES_DIR, "US.txt")  # inside the zip

# ------------- math helpers -------------
def km_to_miles(km): return km * 0.621371
def miles_to_km(mi): return mi / 0.621371

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

# ------------- data fetch/cache -------------
def http_get_bytes(url, timeout=60, context=None):
    req = request.Request(url, headers={"User-Agent": "ZIPRadiusOffline/1.0"})
    with request.urlopen(req, timeout=timeout, context=context) as resp:
        return resp.read()

def ensure_geonames_us(context=None, max_age_days=180):
    """
    Ensure GeoNames US.txt exists locally. Download US.zip if missing or stale.
    Returns path to US.txt.
    """
    os.makedirs(GEONAMES_DIR, exist_ok=True)

    if os.path.exists(GEONAMES_TXT):
        age_days = (time.time() - os.path.getmtime(GEONAMES_TXT)) / 86400.0
        if age_days < max_age_days:
            return GEONAMES_TXT

    # Download zip
    url = f"{GEONAMES_BASE}/{GEONAMES_FILE}"
    data = http_get_bytes(url, context=context)
    with open(GEONAMES_ZIP, "wb") as f:
        f.write(data)

    # Extract US.txt into cache dir
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        with zf.open("US.txt") as ztxt, open(GEONAMES_TXT, "wb") as out:
            out.write(ztxt.read())

    return GEONAMES_TXT

def load_geonames_rows(context=None):
    """
    GeoNames US.txt (tab-separated) columns:
    0 country_code, 1 postal_code, 2 place_name, 3 admin1_name, 4 admin1_code,
    5 admin2_name, 6 admin2_code, 7 admin3_name, 8 admin3_code, 9 lat, 10 lon, 11 accuracy
    """
    path = ensure_geonames_us(context=context)
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for rec in reader:
            if len(rec) < 11:  # skip malformed
                continue
            pc = (rec[1] or "").strip()
            place = (rec[2] or "").strip()
            state = (rec[4] or "").strip()  # two-letter state code
            try:
                zlat = float(rec[9]); zlon = float(rec[10])
            except ValueError:
                continue
            if pc:
                rows.append({
                    "zip": pc.zfill(5) if pc.isdigit() else pc,
                    "city": place,
                    "state": state,
                    "lat": zlat,
                    "lon": zlon
                })
    if not rows:
        raise RuntimeError("No rows parsed from GeoNames US.txt.")
    return rows

# ------------- core lookups -------------
def nearest_zip(lat, lon, rows):
    """Return (zip, distance_km, city, state, zlat, zlon) by nearest centroid."""
    best = None
    best_d = float("inf")
    for r in rows:
        d = haversine_km(lat, lon, r["lat"], r["lon"])
        if d < best_d:
            best_d = d; best = r
    return (best["zip"], best_d, best["city"], best["state"], best["lat"], best["lon"])

def nearby_zips_by_radius(lat, lon, radius_km, rows):
    """
    Pure client-side: Haversine filter on GeoNames rows.
    Returns sorted list by distance_km.
    """
    # quick bbox prune
    dlat = radius_km / 111.0
    lon_scale = max(0.000001, math.cos(math.radians(lat)))
    dlon = radius_km / (111.0 * lon_scale)
    lat_min, lat_max = lat - dlat, lat + dlat
    lon_min, lon_max = lon - dlon, lon + dlon

    out = []
    for r in rows:
        if r["lat"] < lat_min or r["lat"] > lat_max or r["lon"] < lon_min or r["lon"] > lon_max:
            continue
        dkm = haversine_km(lat, lon, r["lat"], r["lon"])
        if dkm <= radius_km + 1e-9:
            out.append({**r, "dist_km": dkm})
    out.sort(key=lambda x: x["dist_km"])
    return out

# ------------- high-level helper -------------
def get_zip_and_nearby(lat, lon, radius, units="mi", show=50, insecure=False):
    """
    Returns (zip_here, nearby_list). Fully offline after first dataset download.
    """
    # Only used on first download; otherwise totally offline
    ctx = ssl._create_unverified_context() if insecure else ssl.create_default_context()

    rows = load_geonames_rows(context=ctx)

    # Determine "ZIP you're in" by nearest centroid
    z, dkm, city, state, zlat, zlon = nearest_zip(lat, lon, rows)
    print(f"Nearest ZIP (centroid) at ({lat:.6f}, {lon:.6f}): {z}  —  {dkm:.2f} km away")

    radius_km = radius if units == "km" else miles_to_km(radius)
    nearby = nearby_zips_by_radius(lat, lon, radius_km, rows)

    print()
    if units == "mi":
        print(f"Nearby ZIPs within {radius:.2f} mi ({radius_km:.2f} km):")
    else:
        print(f"Nearby ZIPs within {radius_km:.2f} km:")

    for i, r in enumerate(nearby[:show], 1):
        d_out = km_to_miles(r["dist_km"]) if units == "mi" else r["dist_km"]
        print(f"{i:>3}) {r['zip']}  {r['city']}, {r['state']}  —  {d_out:.2f} {units}")

    return z, nearby

# ------------- CLI -------------
def main():
    ap = argparse.ArgumentParser(description="Offline ZIP at lat/lon + nearby ZIPs (GeoNames cache).")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--radius", type=float, default=10.0)
    ap.add_argument("--units", choices=["km","mi"], default="mi")
    ap.add_argument("--show", type=int, default=50)
    ap.add_argument("--insecure", action="store_true", help="Skip SSL verification for first download only")
    args = ap.parse_args()

    get_zip_and_nearby(args.lat, args.lon, args.radius, args.units, args.show, insecure=args.insecure)

if __name__ == "__main__":
    main()
