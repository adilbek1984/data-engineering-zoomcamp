"""A simple `dlt` pipeline that ingests data from the Open Library REST API.

This pipeline defines a single REST resource `books` that calls
`https://openlibrary.org/api/books` and returns the response for the
requested `bibkeys` (e.g. ISBNs). Incremental loading / state is skipped
for now.
"""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source(bibkeys: str = "ISBN:0451526538"):
    """Define dlt resources from Open Library API.

    Args:
        bibkeys: comma-separated Open Library bibkeys (for example
            "ISBN:0451526538,ISBN:0201558025").
    """
    config: RESTAPIConfig = {
        "client": {
            # Open Library base URL
            "base_url": "https://openlibrary.org",
            # No authentication required for the public API
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    # endpoint path relative to base_url
                    "path": "api/books",
                    # default query parameters used for the call
                    "params": {"bibkeys": bibkeys, "format": "json", "jscmd": "data"},
                    # select the full response as the page data (avoids jsonpath
                    # detection producing invalid JSONPath strings like 'ISBN:...')
                    "data_selector": "$",
                },
                # this endpoint returns a JSON object keyed by bibkey; leave
                # record/response parsing to the rest helper (raw response
                # will be stored) — adjust `data_selector` here if you want
                # to extract nested records.
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="open_library_pipeline",
    destination="duckdb",
    # keep `drop_sources` while developing so repeated runs are clean
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    # run the pipeline; supply custom `bibkeys` by passing the argument to
    # `open_library_rest_api_source()` if needed.
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)
