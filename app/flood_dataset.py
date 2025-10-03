import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import math
import json
from tqdm import tqdm
from random import uniform, randint
import time
from api_config import load_api_config
from arg_parser import parse_arguments, print_filter_settings

# ================= CONFIG =================
# Load API configuration
api_config = load_api_config()
NOAA_TOKEN = api_config['noaa_token']
USGS_API_KEY = api_config['usgs_api_key']

# DATA STORAGE PATHS
RAW_DATA_DIR = api_config['raw_data_dir']
CACHE_DIR = os.path.join(RAW_DATA_DIR, "cache")
NWS_CACHE_DIR = os.path.join(CACHE_DIR, "nws")
PRECIPITATION_CACHE_FILE = os.path.join(CACHE_DIR, "precipitation.json")
ELEVATION_CACHE_FILE = os.path.join(CACHE_DIR, "elevation.json")
GAGE_HEIGHT_CACHE_FILE = os.path.join(CACHE_DIR, "gage_height.json")
USGS_CACHE_FILE = os.path.join(CACHE_DIR, "usgs_stations.json")
OUTPUT_FILE = os.path.join(RAW_DATA_DIR, "flood_dataset.csv")

os.makedirs(NWS_CACHE_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# ---------- Argument parsing ----------
args = parse_arguments()
TARGET_STATE = args['state']
MONTH_LIMIT = args['months']
YEARS_BACK = args['years']

# Print filter settings
print_filter_settings(TARGET_STATE, MONTH_LIMIT, YEARS_BACK)

# Calculate years range after getting YEARS_BACK
current_year = datetime.now().year
YEARS = range(current_year - YEARS_BACK, current_year + 1)  # last N years inclusive
MAX_DISTANCE_KM = 25

# ---------- Helper Functions ----------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))

# ---------- USGS Station Cache ----------
USGS_STATIONS = []

def load_usgs_stations():
    global USGS_STATIONS

    print("Downloading USGS station list using new OGC API...")
    USGS_STATIONS = []
    
    # Determine which states to download based on target
    state_name_to_fips = {
        "Texas": "48", "California": "06", "Florida": "12", "Louisiana": "22",
        "Georgia": "13", "South Carolina": "45", "North Carolina": "37", "Virginia": "51",
        "Illinois": "17", "Indiana": "18", "Kentucky": "21", "Tennessee": "47",
        "Alabama": "01", "Arkansas": "05"
    }
    
    if TARGET_STATE and TARGET_STATE in state_name_to_fips:
        # Only download stations for the target state
        state_codes = [state_name_to_fips[TARGET_STATE]]
        print(f"  Downloading stations for {TARGET_STATE} only (FIPS: {state_codes[0]})")
    else:
        # Download all major flood-prone states
        state_codes = ["48", "06", "12", "22", "13", "45", "37", "51", "17", "18", "21", "47", "01", "05"]
        print(f"  Downloading stations for all major flood-prone states ({len(state_codes)} states)")
    
    for state_code in state_codes:
        try:
            print(f"  Getting stations for state {state_code}...")
            endpoint = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/monitoring-locations/items"
            params = {
                "state_code": state_code,
                "site_type_code": "ST",  # Stream stations
                "limit": 5000  # Get up to 5000 stations per state
            }
            
            # Add API key if available for higher rate limits
            if USGS_API_KEY:
                params["api_key"] = USGS_API_KEY
            
            try:
                r = requests.get(endpoint, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                
                if (data.get("numberReturned") == 0):
                    print(f"    No stations found for state {state_code}")
                    print("❌ Cannot proceed without water stations. Exiting program.")
                    sys.exit(1)
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    print(f"    Rate limited downloading stations for state {state_code}, waiting 3 seconds...")
                    time.sleep(3)
                    try:
                        r = requests.get(endpoint, params=params, timeout=30)
                        r.raise_for_status()
                        data = r.json()
                    except Exception as retry_e:
                        print(f"    Warning: Retry failed for state {state_code}: {retry_e}")
                        continue
                else:
                    print(f"    Warning: HTTP error downloading stations for state {state_code}: {e}")
                    continue
            except Exception as e:
                print(f"    Warning: Failed to download stations for state {state_code}: {e}")
                continue
            
            for feature in data.get("features", []):
                try:
                    print(feature)
                    props = feature["properties"]
                    geom = feature["geometry"]
                    
                    # Extract coordinates (GeoJSON format: [longitude, latitude])
                    if geom and geom["type"] == "Point" and len(geom["coordinates"]) >= 2:
                        lon, lat = geom["coordinates"][0], geom["coordinates"][1]
                        
                        USGS_STATIONS.append({
                            "id": props["monitoring_location_number"],
                            "lat": float(lat),
                            "lon": float(lon)
                        })
                except (KeyError, ValueError, TypeError):
                    # Skip stations with missing/invalid data
                    continue
                    
        except Exception as e:
            print(f"  Warning: Failed to get stations for state {state_code}: {e}")
            continue
    
    with open(USGS_CACHE_FILE, "w") as f:
        json.dump(USGS_STATIONS, f)
    print(f"Downloaded and cached {len(USGS_STATIONS)} USGS stations from new OGC API.")

def find_nearest_usgs_station(lat, lon):
    nearest = None
    min_dist = MAX_DISTANCE_KM
    for station in USGS_STATIONS:
        dist = haversine(lat, lon, station["lat"], station["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = station["id"]
    return nearest

def get_usgs_gage_height(station_id, date):
    if not station_id:
        return None
    
    # Check cache first
    cache_key = f"{station_id},{date.strftime('%Y-%m-%d')}"
    cache_data = load_cache(GAGE_HEIGHT_CACHE_FILE)
    
    if cache_key in cache_data:
        return cache_data[cache_key]
    
    # Use new OGC API for gage height data
    endpoint = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"
    params = {
        "monitoring_location_id": f"USGS-{station_id}",
        "parameter_code": "00065",  # Gage height
        "time": date.strftime("%Y-%m-%d"),
        "limit": 1
    }
    
    # Add API key if available for higher rate limits
    if USGS_API_KEY:
        params["api_key"] = USGS_API_KEY
    
    result = None
    try:
        r = requests.get(endpoint, params=params, timeout=20)
        r.raise_for_status()
        
        # Monitor rate limits (first call only to avoid spam)
        if not hasattr(get_usgs_gage_height, '_rate_limit_logged'):
            rate_limit = r.headers.get('X-RateLimit-Limit')
            rate_remaining = r.headers.get('X-RateLimit-Remaining')
            if rate_limit and rate_remaining:
                key_status = "with API key" if USGS_API_KEY else "without API key"
                print(f"    USGS API rate limit ({key_status}): {rate_remaining}/{rate_limit} requests remaining")
            get_usgs_gage_height._rate_limit_logged = True
        
        data = r.json()
        
        # Parse GeoJSON response format
        features = data.get("features", [])
        if features:
            # Get the first (most relevant) result
            feature = features[0]
            properties = feature.get("properties", {})
            value = properties.get("value")
            if value is not None:
                result = float(value)
                
    except requests.exceptions.HTTPError as e:
        if "429" in str(e):
            print(f"    Rate limited by USGS API for station {station_id}, waiting 2 seconds...")
            time.sleep(2)
            # Retry once after rate limit (with same params including API key)
            try:
                r = requests.get(endpoint, params=params, timeout=20)
                r.raise_for_status()
                data = r.json()
                features = data.get("features", [])
                if features:
                    feature = features[0]
                    properties = feature.get("properties", {})
                    value = properties.get("value")
                    if value is not None:
                        result = float(value)
            except Exception as retry_e:
                print(f"    Warning: Retry failed for station {station_id}: {retry_e}")
                return None
        else:
            print(f"    Warning: HTTP error for station {station_id}: {e}")
            return None
    except Exception as e:
        print(f"    Warning: Failed to get gage height for station {station_id}: {e}")
        return None
    
    # Cache the result (even if None)
    cache_data[cache_key] = result
    save_cache(GAGE_HEIGHT_CACHE_FILE, cache_data)
    
    return result

# ---------- Cache Helper Functions ----------
def load_cache(cache_file):
    """Load cache from file, return empty dict if file doesn't exist."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"    Warning: Failed to load cache {cache_file}: {e}")
    return {}

def save_cache(cache_file, cache_data):
    """Save cache data to file."""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"    Warning: Failed to save cache {cache_file}: {e}")

# ---------- NOAA Precipitation ----------
def get_precipitation(lat, lon, date):
    # Check cache first
    cache_key = f"{lat:.4f},{lon:.4f},{date.strftime('%Y-%m-%d')}"
    cache_data = load_cache(PRECIPITATION_CACHE_FILE)
    
    if cache_key in cache_data:
        return cache_data[cache_key]
    
    endpoint = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    params = {
        "datasetid": "GHCND",
        "datatypeid": "PRCP",
        "startdate": (date - timedelta(days=1)).strftime("%Y-%m-%d"),
        "enddate": date.strftime("%Y-%m-%d"),
        "units": "metric",
        "limit": 1000,
        "extent": f"{lat-0.25},{lon-0.25},{lat+0.25},{lon+0.25}"
    }
    headers = {"token": NOAA_TOKEN}
    
    result = None
    try:
        r = requests.get(endpoint, params=params, headers=headers, timeout=10)
        
        if r.status_code == 429:
            print(f"    Rate limited by NOAA API for lat={lat}, lon={lon}, waiting 2 seconds...")
            time.sleep(2)
            try:
                r = requests.get(endpoint, params=params, headers=headers, timeout=10)
            except Exception as retry_e:
                print(f"    Warning: NOAA API retry failed for lat={lat}, lon={lon}: {retry_e}")
                return None
        
        if r.status_code == 200:
            data = r.json().get("results", [])
            result = sum(item["value"] for item in data)
        elif r.status_code != 429:  # Don't double-log 429 errors
            print(f"    Warning: NOAA API returned status {r.status_code} for lat={lat}, lon={lon}")
            
    except Exception as e:
        print(f"    Warning: Failed to get precipitation for lat={lat}, lon={lon}: {e}")
        return None
    
    # Cache the result (even if None)
    cache_data[cache_key] = result
    save_cache(PRECIPITATION_CACHE_FILE, cache_data)
    
    return result

# ---------- USGS Elevation ----------
def get_elevation(lat, lon):
    # Check cache first
    cache_key = f"{lat:.4f},{lon:.4f}"
    cache_data = load_cache(ELEVATION_CACHE_FILE)
    
    if cache_key in cache_data:
        return cache_data[cache_key]
    
    endpoint = "https://epqs.nationalmap.gov/v1/json"
    params = {"x": lon, "y": lat, "units": "Meters", "wkid": 4326, "includeDate": "false"}
    
    result = None
    try:
        r = requests.get(endpoint, params=params, timeout=10, allow_redirects=True)
        
        if r.status_code == 429:
            print(f"    Rate limited by USGS Elevation API for lat={lat}, lon={lon}, waiting 2 seconds...")
            time.sleep(2)
            try:
                r = requests.get(endpoint, params=params, timeout=10, allow_redirects=True)
            except Exception as retry_e:
                print(f"    Warning: USGS Elevation API retry failed for lat={lat}, lon={lon}: {retry_e}")
                return None
        
        if r.status_code == 200:
            data = r.json()
            # Handle new API response format
            if "value" in data:
                result = data["value"]
            elif "elevation" in data:
                result = data["elevation"]
            else:
                print(f"    Warning: Unexpected elevation API response format: {data}")
                return None
        elif r.status_code != 429:  # Don't double-log 429 errors
            print(f"    Warning: USGS Elevation API returned status {r.status_code} for lat={lat}, lon={lon}")
            
    except Exception as e:
        print(f"    Warning: Failed to get elevation for lat={lat}, lon={lon}: {e}")
        return None
    
    # Cache the result (even if None)
    cache_data[cache_key] = result
    save_cache(ELEVATION_CACHE_FILE, cache_data)
    
    return result

# ---------- IEM Historical Flood Alerts ----------
def fetch_historical_flood_alerts(year, month):
    cache_file = os.path.join(NWS_CACHE_DIR, f"{year}-{month:02d}.json")
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            return json.load(f)
    
    # Weather forecast offices by state
    state_wfos = {
        "Texas": ["EWX", "FWD", "HGX", "SJT", "LUB", "AMA", "CRP", "BRO", "EPZ"],
        "California": ["LOX", "SGX", "OXR", "MTR", "STO", "HNX", "EKA", "REV"], 
        "Florida": ["MFL", "TBW", "MLB", "JAX", "TAE", "KEY"],
        "Louisiana": ["LIX", "SHV", "LCH"],
        "Georgia": ["FFC"],
        "South Carolina": ["CHS"], 
        "North Carolina": ["RAH"],
        "Virginia": ["LWX"],
        "Illinois": ["LOT"],
        "Indiana": ["IND"],
        "Kentucky": ["JKL"],
        "Tennessee": ["MEG"],
        "Alabama": ["BMX"],
        "Arkansas": ["LZK"]
    }
    
    # Determine which weather offices to query
    if TARGET_STATE and TARGET_STATE in state_wfos:
        wfos = state_wfos[TARGET_STATE]
        print(f"    Fetching alerts from {len(wfos)} {TARGET_STATE} weather offices: {', '.join(wfos)}")
    else:
        # Flatten all weather offices for nationwide search
        wfos = [office for offices in state_wfos.values() for office in offices]
        print(f"    Fetching alerts from {len(wfos)} weather offices nationwide")
    
    alerts = []
    for wfo in wfos:
        try:
            print(f"    Fetching alerts from {wfo} weather office...")
            endpoint = "https://mesonet.agron.iastate.edu/json/vtec_events.py"
            params = {
                "wfo": wfo,
                "phenomena": "FL",  # Flood phenomena 
                "year": year
            }
            try:
                r = requests.get(endpoint, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except requests.exceptions.HTTPError as e:
                if "429" in str(e):
                    print(f"    Rate limited by IEM API for {wfo}, waiting 3 seconds...")
                    time.sleep(3)
                    try:
                        r = requests.get(endpoint, params=params, timeout=30)
                        r.raise_for_status()
                        data = r.json()
                    except Exception as retry_e:
                        print(f"    Warning: IEM API retry failed for {wfo}: {retry_e}")
                        continue
                else:
                    print(f"    Warning: IEM API HTTP error for {wfo}: {e}")
                    continue
            except Exception as e:
                print(f"    Warning: IEM API request failed for {wfo}: {e}")
                continue
            
            # Small delay between weather office requests
            time.sleep(0.2)
            
            for event in data.get("events", []):
                try:
                    # Filter by month
                    issue_date = datetime.strptime(event["issue"][:10], "%Y-%m-%d")
                    if issue_date.month != month:
                        continue
                    
                    # Debug: Print what we found
                    if len(alerts) < 3:  # Only print first few for debugging
                        print(f"      Found flood alert: {event.get('locations', 'No location')} on {issue_date.date()}")
                    
                    # Filter by state if specified
                    locations = event.get("locations", "")
                    if TARGET_STATE:
                        # Handle state name to abbreviation mapping
                        state_abbrevs = {
                            "Texas": "TX", "California": "CA", "Florida": "FL", "Louisiana": "LA",
                            "Georgia": "GA", "South Carolina": "SC", "North Carolina": "NC", 
                            "Virginia": "VA", "Illinois": "IL", "Indiana": "IN", "Kentucky": "KY",
                            "Tennessee": "TN", "Alabama": "AL", "Arkansas": "AR"
                        }
                        state_to_find = state_abbrevs.get(TARGET_STATE, TARGET_STATE)
                        if f"[{state_to_find}]" not in locations and TARGET_STATE not in locations:
                            continue
                    
                    # Convert to format compatible with existing code
                    alert = {
                        "properties": {
                            "event": f"{event.get('ph_name', 'Flood')} {event.get('sig_name', 'Warning')}",
                            "areaDesc": locations,
                            "severity": event.get("sig_name", "Warning"),
                            "certainty": "Observed",  # IEM data is historical/observed
                            "urgency": "Past",
                            "onset": event.get("issue"),
                        },
                        "geometry": None,  # Will be determined by centroid calculation
                        "issue_timestamp": event.get("issue"),
                        "area_sq_miles": event.get("area", 0)
                    }
                    alerts.append(alert)
                    
                except Exception as e:
                    print(f"      Warning: Failed to process flood event {event.get('eventid', 'unknown')}: {e}")
                    continue
                    
        except Exception as e:
            print(f"    Warning: Failed to get alerts from {wfo}: {e}")
            continue
    
    with open(cache_file, "w") as f:
        json.dump(alerts, f)
    return alerts

def get_alert_centroid(alert):
    geom = alert.get("geometry")
    if geom:
        # Handle NWS-style geometry data
        if geom["type"] == "Point":
            return geom["coordinates"][1], geom["coordinates"][0]
        elif geom["type"] == "Polygon":
            coords = geom["coordinates"][0]
            lats = [c[1] for c in coords]
            lons = [c[0] for c in coords]
            return sum(lats)/len(lats), sum(lons)/len(lons)
    
    # Handle IEM-style location names like "Fayette [TX]"
    area_desc = alert.get("properties", {}).get("areaDesc", "")
    if area_desc:
        return get_coordinates_from_location(area_desc)
    
    return None, None

def get_coordinates_from_location(location_str):
    """Extract approximate coordinates from location string like 'Fayette [TX]'"""
    # Simple county centroid mapping for major flood-prone counties in Texas
    county_coords = {
        "Harris": (29.7604, -95.3698),    # Houston area
        "Travis": (30.2672, -97.7431),    # Austin area
        "Bexar": (29.4241, -98.4936),     # San Antonio area
        "Dallas": (32.7767, -96.7970),    # Dallas area
        "Tarrant": (32.7555, -97.3308),   # Fort Worth area
        "Fayette": (29.8947, -96.9344),   # Fayette County
        "DeWitt": (29.0374, -97.2842),    # DeWitt County
        "Wilson": (29.1213, -98.1281),    # Wilson County
        "Val Verde": (29.3605, -100.8965), # Val Verde County
        "Kerr": (30.0474, -99.3420),      # Kerr County
        "Bandera": (29.7574, -99.0717),   # Bandera County
        "Kinney": (29.3505, -100.4440),   # Kinney County
        "Uvalde": (29.2097, -99.7864),    # Uvalde County
        "Llano": (30.7591, -98.6723),     # Llano County
    }
    
    # Extract county name from "County [STATE]" format
    import re
    match = re.search(r'([A-Z][a-z]+)\s*\[', location_str)
    if match:
        county = match.group(1)
        if county in county_coords:
            return county_coords[county]
    
    # If no specific mapping, return approximate Texas center
    if "[TX]" in location_str:
        return (31.0, -99.0)  # Center of Texas
    
    return None, None

# ---------- Build Dataset ----------
def build_dataset():
    load_usgs_stations()
    records = []

    for year in YEARS:
        for month in range(1, MONTH_LIMIT + 1):
            alerts = fetch_historical_flood_alerts(year, month)
            desc = TARGET_STATE or "all areas"
            print(f"{year}-{month:02d}: {len(alerts)} flood-related alerts found in {desc}")

            for alert in tqdm(alerts):
                try:
                    lat, lon = get_alert_centroid(alert)
                    if lat is None:
                        continue
                    start_date = datetime.strptime(alert["properties"]["onset"][:10], "%Y-%m-%d")
                    precip = get_precipitation(lat, lon, start_date)
                    time.sleep(0.1)  # Small delay between API calls
                    elevation = get_elevation(lat, lon)
                    time.sleep(0.1)  # Small delay between API calls
                    station_id = find_nearest_usgs_station(lat, lon)
                    gage_height = get_usgs_gage_height(station_id, start_date)
                    time.sleep(0.1)  # Small delay to respect USGS API rate limits

                    records.append({
                        "year": year,
                        "month": month,
                        "lat": lat,
                        "lon": lon,
                        "event": alert["properties"]["event"],
                        "area": alert["properties"]["areaDesc"],
                        "severity": alert["properties"]["severity"],
                        "certainty": alert["properties"]["certainty"],
                        "urgency": alert["properties"]["urgency"],
                        "precip_24h_mm": precip,
                        "elevation_m": elevation,
                        "usgs_station_id": station_id,
                        "usgs_gage_height_ft": gage_height,
                        "flood_occurred": 1
                    })
                except Exception as e:
                    print(f"      Warning: Failed to process positive flood sample: {e}")
                    continue

            # Negative samples
            for alert in alerts:
                try:
                    lat, lon = get_alert_centroid(alert)
                    if lat is None:
                        continue
                    start_date = datetime.strptime(alert["properties"]["onset"][:10], "%Y-%m-%d")
                    neg_lat = lat + uniform(-0.5, 0.5)
                    neg_lon = lon + uniform(-0.5, 0.5)
                    neg_date = start_date + timedelta(days=randint(1, 28))

                    precip = get_precipitation(neg_lat, neg_lon, neg_date)
                    time.sleep(0.1)  # Small delay between API calls
                    elevation = get_elevation(neg_lat, neg_lon)
                    time.sleep(0.1)  # Small delay between API calls
                    station_id = find_nearest_usgs_station(neg_lat, neg_lon)
                    gage_height = get_usgs_gage_height(station_id, neg_date)
                    time.sleep(0.1)  # Small delay to respect USGS API rate limits

                    records.append({
                        "year": neg_date.year,
                        "month": neg_date.month,
                        "lat": neg_lat,
                        "lon": neg_lon,
                        "event": "None",
                        "area": "None",
                        "severity": "None",
                        "certainty": "None",
                        "urgency": "None",
                        "precip_24h_mm": precip,
                        "elevation_m": elevation,
                        "usgs_station_id": station_id,
                        "usgs_gage_height_ft": gage_height,
                        "flood_occurred": 0
                    })
                except Exception as e:
                    print(f"      Warning: Failed to process negative flood sample: {e}")
                    continue

    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Dataset saved to {OUTPUT_FILE}, {len(df)} rows total")

if __name__ == "__main__":
    # Check if --help was requested before building dataset
    if "--help" in sys.argv or "-h" in sys.argv:
        # Import here to avoid circular imports
        from arg_parser import parse_arguments
        # Create a temporary parser just to show help
        import argparse
        parser = argparse.ArgumentParser(description="Build flood dataset with historical data from NOAA, NWS, and USGS APIs")
        parser.add_argument("--state", type=str, default=None, help="Name of state to filter")
        parser.add_argument("--months", type=int, default=12, help="Number of months per year to process (1-12)")
        parser.add_argument("--years", type=int, default=3, help="Number of years to process (1-3)")
        parser.print_help()
        sys.exit(0)
    
    build_dataset()

