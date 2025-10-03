"""
Flood Model - Main Entry Point

This script runs the flood dataset builder to collect historical flood data
from NOAA, NWS, and USGS APIs for machine learning model training.
"""
from app.flood_dataset import FloodDatasetBuilder
from flood_dataset import build_dataset

def main():
    """Main entry point for the flood model dataset builder."""
    print("🌊 Starting Flood Dataset Builder...")
    print("Collecting historical flood data from NOAA, NWS, and USGS APIs")
    print("-" * 60)
    
    try:
        FloodDatasetBuilder builder = new FloodDatasetBuilder()
        build_dataset()
        print("\n✅ Dataset building completed successfully!")
    except Exception as e:
        print(f"\n❌ Error building dataset: {e}")
        raise

if __name__ == '__main__':
    main()
