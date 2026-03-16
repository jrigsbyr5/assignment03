"""
    Script to transform raw AirNow data files into BigQuery-compatible formats.

    This script reads the raw .dat files downloaded by 01_extract.py and converts
    them into CSV, JSON-L, and Parquet formats suitable for loading into
    BigQuery as external tables.

    Hourly observation data is converted to: CSV, JSON-L, Parquet
    Site location data is converted to: CSV, JSON-L, GeoParquet (with point geometry)

    Usage:
        python scripts/02_prepare.py
"""

import pathlib
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

HOURLY_COLUMNS = [
    'valid_date',
    'valid_time',
    'aqsid',
    'site_name',
    'gmt_offset',
    'parameter_name',
    'reporting_units',
    'value',
    'data_source',
]


# --- Hourly observation data ---

def load_hourly_dataframe(date_str):
    """Helper function to load all 24 hourly files into one dataframe."""

    raw_dir = DATA_DIR / "raw" / date_str
    files = sorted(raw_dir.glob("HourlyData_*.dat"))

    dfs = []

    for f in files:
        df = pd.read_csv(
            f,
            sep="|",
            header=None,
            names=HOURLY_COLUMNS,
            encoding="latin1"
        )
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def prepare_hourly_csv(date_str):
    """Convert raw hourly .dat files for a date to a single CSV file.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them into a single dataset, assigns column names,
    and writes to data/prepared/hourly/<date>.csv.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    df = load_hourly_dataframe(date_str)

    out_dir = DATA_DIR / "prepared" / "hourly"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{date_str}.csv"
    df.to_csv(out_path, index=False)


def prepare_hourly_jsonl(date_str):
    """Convert raw hourly .dat files for a date to newline-delimited JSON.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them, and writes one JSON object per line to
    data/prepared/hourly/<date>.jsonl.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    df = load_hourly_dataframe(date_str)

    out_dir = DATA_DIR / "prepared" / "hourly"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{date_str}.jsonl"
    df.to_json(out_path, orient="records", lines=True)


def prepare_hourly_parquet(date_str):
    """Convert raw hourly .dat files for a date to Parquet format.

    Reads all 24 HourlyData_*.dat files from data/raw/<date>/,
    combines them, and writes to data/prepared/hourly/<date>.parquet.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format.
    """
    df = load_hourly_dataframe(date_str)

    out_dir = DATA_DIR / "prepared" / "hourly"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{date_str}.parquet"
    df.to_parquet(out_path, index=False)


# --- Site location data ---

def load_site_locations():
    """Load the latest site locations file."""

    raw_root = DATA_DIR / "raw"
    latest_date = sorted(raw_root.iterdir())[-1]

    site_file = latest_date / "Monitoring_Site_Locations_V2.dat"

    df = pd.read_csv(site_file, sep="|", encoding="latin1")

    # deduplicate by AQSID
    df = df.drop_duplicates(subset="AQSID")

    return df


def prepare_site_locations_csv():
    """Convert monitoring site locations to CSV."""
    df = load_site_locations()

    out_dir = DATA_DIR / "prepared" / "sites"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "site_locations.csv"
    df.to_csv(out_path, index=False)


def prepare_site_locations_jsonl():
    """Convert monitoring site locations to newline-delimited JSON."""
    df = load_site_locations()

    out_dir = DATA_DIR / "prepared" / "sites"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "site_locations.jsonl"
    df.to_json(out_path, orient="records", lines=True)


def prepare_site_locations_geoparquet():
    """Convert monitoring site locations to GeoParquet with point geometry."""
    df = load_site_locations()

    geometry = [Point(xy) for xy in zip(df["Longitude"], df["Latitude"])]

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    out_dir = DATA_DIR / "prepared" / "sites"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "site_locations.geoparquet"
    gdf.to_parquet(out_path)


if __name__ == '__main__':
    import datetime

    # Prepare site locations (only need to do this once)
    print('Preparing site locations...')
    prepare_site_locations_csv()
    prepare_site_locations_jsonl()
    prepare_site_locations_geoparquet()

    # Prepare hourly data for each day in July 2024 (backfill)
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing hourly data for {date_str}...')
        prepare_hourly_csv(date_str)
        prepare_hourly_jsonl(date_str)
        prepare_hourly_parquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')