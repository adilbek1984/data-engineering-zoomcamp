"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: bigquery-default #duckdb-default

materialization:
  type: table
  strategy: create+replace # append, replace, create+replace

@bruin"""

import os
import io
import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def generate_month_range(start_date: str, end_date: str) -> list[tuple[int, int]]:
    """
    Generates list of (year, month) tuples between start_date and end_date inclusive.
    Dates must be in format YYYY-MM-DD.
    Example:
    generate_month_range("2022-01-15", "2022-03-10")
    -> [(2022, 1), (2022, 2), (2022, 3)]
    """

    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")

    start = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(day=1)

    months = []
    current = start

    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def materialize():
    # Get start and end dates from environment variables
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")

    # Get taxi_type
    bruin_vars = json.loads(os.environ["BRUIN_VARS"])
    taxi_types = bruin_vars.get("taxi_types")
    print(f"Taxi types: {taxi_types}")

    # Generate list of months to process
    months = generate_month_range(start_date, end_date)

    # Download and combine parquet files
    all_dataframes = []
    errors = []
    # base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    extracted_at = datetime.now()

    for taxi_type in taxi_types:
        for year, month in months:
            print(f"Downloading {year}-{month:02d}: {taxi_type}")
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            # logger.info(f"Fetching {url}")

            try:
                response = requests.get(url, timeout=300)
                response.raise_for_status()

                df = pd.read_parquet(io.BytesIO(response.content))

                # Normalize column names to lowercase with underscores to avoid collisions
                # e.g., 'Airport_fee' and 'airport_fee' both become 'airport_fee'
                df.columns = df.columns.str.lower().str.replace(' ', '_')

                df["taxi_type"] = taxi_type
                df["extracted_at"] = extracted_at

                all_dataframes.append(df)
                print(f"Successfully downloaded {year}-{month:02d}: {len(df)} rows")

            except requests.exceptions.RequestException as e:
                error_msg = f"Error downloading {taxi_type} {year}-{month:02d}: {e}"
                print(error_msg)
                errors.append(error_msg)

            except Exception as e:
                error_msg = f"Error processing {taxi_type} {year}-{month:02d}: {e}"
                print(error_msg)
                errors.append(error_msg)

    if not all_dataframes:
        error_summary = "\n".join(errors) if errors else "No errors recorded"
        raise ValueError(
            f"No dataframes to combine. Failed to download all files. \nErrors:\n{error_summary}"
        )

    if errors:
        print(
            f"\nWarning: {len(errors)} file(s) failed to download, but continuing with {len(all_dataframes)} success downloads"
        )

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Total rows combined: {len(combined_df)}")
    return combined_df
