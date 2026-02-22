"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import os
import io
import json
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def generate_month_range(start_date: str, end_date: str):
    start = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(day=1)

    months = []
    current = start
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def safe_read_parquet(content: bytes) -> pd.DataFrame:
    """
    Читаем parquet через pandas.
    Полностью удаляем timezone и приводим datetime к строке.
    """
    df = pd.read_parquet(io.BytesIO(content))

    # 1️⃣ Убираем timezone
    for col in df.select_dtypes(include=["datetimetz"]):
        df[col] = df[col].dt.tz_localize(None)

    # 2️⃣ Превращаем ВСЕ datetime в string
    for col in df.select_dtypes(include=["datetime64[ns]"]):
        df[col] = df[col].astype(str)

    return df


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    months = generate_month_range(start_date, end_date)

    all_dfs = []
    errors = []

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BruinPipeline/1.0)"
    }

    for taxi_type in taxi_types:
        for year, month in months:
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            logger.info(f"Fetching {url}")

            try:
                r = requests.get(url, headers=headers, timeout=300)
                r.raise_for_status()

                df = safe_read_parquet(r.content)

                df.columns = df.columns.str.lower().str.replace(" ", "_")
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow().isoformat()

                all_dfs.append(df)
                logger.info(f"Loaded {year}-{month:02d} {taxi_type}: {len(df)} rows")

            except Exception as e:
                msg = f"Failed {taxi_type} {year}-{month:02d}: {e}"
                logger.warning(msg)
                errors.append(msg)

    if not all_dfs:
        logger.warning("No data fetched for interval. Returning empty dataframe.")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)

    logger.info(f"Total rows: {len(combined)}")

    return combined