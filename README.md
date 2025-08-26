# Flood Dataset Builder

I created this project because I wanted to understand how AI models were
created.  In no way am I implying this is the right way to create a model. 

This project collects historical flood-related data for the U.S. from NOAA, NWS, and USGS. It 
generates a dataset with **positive and negative samples** that can be used for flood risk modeling.


## Setup

1. **Create a virtual environment**

```bash
cd flood_model_data_collector
python3 -m venv .venv
source .venv/bin/activate
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

2. **Setup your environment variables**

Setup your config.ini from the template file 
```python
cp config.ini.template config.ini
```

## Usage

Run the dataset builder with optional filters:

### 1. Full US for last 3 years (default)

```bash
python app/flood_dataset.py
```

### 2. Single county (e.g., Travis County), first 2 months for testing

```bash
python app/flood_dataset.py --county "Travis" --months 2
```

### 3. Single state (e.g., Texas), full 12 months

```bash
python app/flood_dataset.py --state "Texas" --months 12
```

---

## Data Storage and Caching

- **Raw data storage:** `/Users/jkeeler/dev/ai/models/flood_model/raw_data`
- **Caching:** NWS alerts and USGS stations are cached in `raw_data/cache/` to avoid re-downloading
- **Negative samples:** Generated automatically by shifting locations and dates from positive flood alerts

---
