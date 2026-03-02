"""DLT source + pipeline for NYC taxi data served by the provided REST API.

API details:
- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Pagination: page & per_page parameters; stop when an empty page is returned
- Page size: 1000 (default)

This file defines a `@dlt.source` that yields a `trips` resource by
iterating pages until no records are returned. It uses simple JSON
pagination (incremental page counter) and is defensive about a few
possible response shapes (list or dict with `data`/`results`).
"""

import dlt
import requests
from typing import Generator, Any


DEFAULT_BASE_URL = (
    "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
)


@dlt.source
def taxi_api_source(base_url: str = DEFAULT_BASE_URL, page_size: int = 1000):
    """DLT source for the taxi API.

    Args:
        base_url: API base URL.
        page_size: number of records to request per page (default 1000).
    """

    @dlt.resource(name="trips")
    def trips() -> Generator[dict[str, Any], None, None]:
        page = 1
        session = requests.Session()

        while True:
            params = {"page": page, "per_page": page_size}
            resp = session.get(base_url, params=params, timeout=30)
            resp.raise_for_status()

            data = resp.json()

            # support common response shapes: list, or dict with 'data'/'results'/'items'
            if isinstance(data, dict):
                items = data.get("data") or data.get("results") or data.get("items") or []
            elif isinstance(data, list):
                items = data
            else:
                # unknown shape - try to treat as empty
                items = []

            # stop when an empty page is returned
            if not items:
                break

            for item in items:
                yield item

            page += 1

    yield trips()


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_api_source())
    print(load_info)
