
class USGSStationRepo:
    def __init__(self, config_parser):
        self.optionA = config_parser.get("root", "AProperty")
USGS_STATIONS = []

def load_usgs_stations(state, config_parser):
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

    if state and state in state_name_to_fips:
        # Only download stations for the target state
        state_codes = [state_name_to_fips[state]]
        print(f"  Downloading stations for {state} only (FIPS: {state_codes[0]})")
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
            params["api_key"] = config.get('USGS_API_KEY')

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
