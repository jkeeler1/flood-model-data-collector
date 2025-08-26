import os
import configparser
from typing import Dict, Optional


def load_api_config() -> Dict[str, str]:
    config = configparser.ConfigParser()
    config_file = "config.ini"
    
    if not os.path.exists(config_file):
        print(f"Config file '{config_file}' not found. Please copy 'config.ini.template' to '{config_file}' and update with your values.")
        print("Using default values...")
        return {
            'noaa_token': '',
            'usgs_api_key': '',
            'raw_data_dir': "/Users/jkeeler/dev/ai/models/flood_model/raw_data"
        }
    
    config.read(config_file)
    
    return {
        'noaa_token': config.get('API_KEYS', 'noaa_token', fallback=''),
        'usgs_api_key': config.get('API_KEYS', 'usgs_api_key', fallback=''),
        'raw_data_dir': config.get('DATA_PATHS', 'raw_data_dir', fallback="/Users/jkeeler/dev/ai/models/flood_model/raw_data")
    }
