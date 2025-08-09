#!/usr/bin/env python3
# Standard library only. Reverse geocode ZIP via Nominatim; nearby ZIPs via GeoNames US.zip (cached).

import json, sys, time, math, argparse, ssl, os, csv, io, zipfile
from urllib import request, parse, error

USER_AGENT = "AmanZipLookup/1.0 (aman.sribrahma@gmail.com)"  # put your real email
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"

# GeoNames postal dataset (no key). We'll download US.zip and cache it.
GEONAMES_BASE = "https://download.geonames.org/export/zip"
GEONAMES_FILE = "US.zip"

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".zip_radius_cache")
GEONAMES_DIR = os.path.join(CACHE_DIR, "geonames_us")
GEONAMES_ZIP = os.path.join(GEONAMES_DIR, GEONAMES_FILE)
GEONAMES_TXT = os.path.join(GEONAMES_DIR, "US.txt")  # inside the zip

def http_get(url, params=None, headers=None, timeout=60, context=None):
    if params:
        url = f"{url}?{parse.urlencode(params)}"
    req = request.Request(url, headers=headers or {})
    with request.urlopen(req, timeout=timeout, context=context) as resp:
        return resp.read()

def reverse_zip_from_latlon(lat, lon, retries=3, context=None):
    params = {"lat": f"{lat}", "lon": f"{lon}", "format": "json", "addressdetails": 1, "zoom": 18}
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    last_err = None
    for _ in range(retries):
        try:
            raw = http_get(NOMINATIM_URL, params=params, headers=headers, context=context)
            data = json.loads(raw.decode("utf-8"))
            postcode = (data.get("address") or {}).get("postcode")
            if not postcode:
                raise RuntimeError("No postcode in Nominatim response.")
            return postcode.split("-")[0]
        except error.HTTPError as e:
            ra = e.headers.get("Retry-After")
            wait = int(ra) if ra and ra.isdigit() else 2
            time.sleep(wait); last_err = f"HTTP {e.code}: {e.reason}"
        except json.JSONDecodeError:
            last_err = "Non-JSON response from Nominatim."; time.sleep(1)
        except Exception as ex:
            last_err = str(ex); time.sleep(1)
    raise RuntimeError(f"Nominatim failed: {last_err}")

def km_to_miles(km): return km * 0.621371
def miles_to_km(mi): return mi / 0.621371

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def ensure_geonames_us(context=None):
    os.makedirs(GEONAMES_DIR, exist_ok=True)
    # If we already have US.txt and it's reasonably recent (<180 days), keep it
    if os.path.exists(GEONAMES_TXT):
        age_days = (time.time() - os.path.getmtime(GEONAMES_TXT)) / 86400.0
        if age_days < 180:
            return GEONAMES_TXT
    # Download the zip
    url = f"{GEONAMES_BASE}/{GEONAMES_FILE}"
    headers = {"User-Agent": USER_AGENT}
    data = http_get(url, headers=headers, context=context)
    with open(GEONAMES_ZIP, "wb") as f:
        f.write(data)
    # Extract US.txt
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        with zf.open("US.txt") as ztxt, open(GEONAMES_TXT, "wb") as out:
            out.write(ztxt.read())
    return GEONAMES_TXT

def load_geonames_rows(context=None):
    """
    GeoNames US.txt is tab-separated with columns:
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
            state = (rec[4] or "").strip()  # admin1_code is the 2-letter state code
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

def nearby_zips_by_radius(lat, lon, radius_km, context=None):
    rows = load_geonames_rows(context=context)
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

def get_zip_and_nearby(lat, lon, radius, units="mi", show=50, insecure=False):
    """
    High-level helper for console use. Returns (zip_here, nearby_list).
    """
    ctx = ssl._create_unverified_context() if insecure else ssl.create_default_context()
    radius_km = radius if units == "km" else miles_to_km(radius)

    try:
        zip_here = reverse_zip_from_latlon(lat, lon, context=ctx)
    except Exception as e:
        zip_here = None
        print(f"[warn] Could not get ZIP from Nominatim: {e}", file=sys.stderr)

    if zip_here:
        print(f"ZIP at ({lat:.6f}, {lon:.6f}): {zip_here}")
    else:
        print(f"ZIP at ({lat:.6f}, {lon:.6f}): <unknown>")

    try:
        nearby = nearby_zips_by_radius(lat, lon, radius_km, context=ctx)
    except Exception as e:
        print(f"[error] Nearby ZIP lookup failed: {e}", file=sys.stderr)
        return zip_here, []

    print()
    if units == "mi":
        print(f"Nearby ZIPs within {radius:.2f} mi ({radius_km:.2f} km):")
    else:
        print(f"Nearby ZIPs within {radius_km:.2f} km:")

    for i, r in enumerate(nearby[:show], 1):
        d_out = km_to_miles(r["dist_km"]) if units == "mi" else r["dist_km"]
        print(f"{i:>3}) {r['zip']}  {r['city']}, {r['state']}  —  {d_out:.2f} {units}")

    return zip_here, nearby

def main():
    ap = argparse.ArgumentParser(description="ZIP at lat/lon + nearby ZIPs (no external packages).")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--radius", type=float, default=10.0)
    ap.add_argument("--units", choices=["km","mi"], default="mi")
    ap.add_argument("--show", type=int, default=50)
    ap.add_argument("--insecure", action="store_true", help="Skip SSL verification if macOS certs are broken")
    args = ap.parse_args()

    get_zip_and_nearby(args.lat, args.lon, args.radius, args.units, args.show, insecure=args.insecure)

if __name__ == "__main__":
    main()
