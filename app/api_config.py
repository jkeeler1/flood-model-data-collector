import os
import configparser
from typing import Dict, Optional


def load_api_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config_file = "config.ini"
    
    if not os.path.exists(config_file):
        print(f"Config file '{config_file}' not found. Please copy 'config.ini.template' to '{config_file}' and update with your values.")
        print("Using default values...")
    
    config.read(config_file)
    return config

