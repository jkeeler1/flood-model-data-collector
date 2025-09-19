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
pip3 install -r requirements.txt
```

3. **Setup your environment variables**

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

### 2. Single county (e.g., Travis County), first 2 months of the last 3 years

```bash
python app/flood_dataset.py --county "Travis" --state 'Texas' --months 2 --years 3
```

### 3. Single state (e.g., Texas), full 12 months for the last 1 year

```bash
python app/flood_dataset.py --state "Texas" --months 12 --years 1
```

