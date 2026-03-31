# Copilot instructions for this repository

Short, action-oriented guidance for AI coding agents working on the Bruin "my-taxi-pipeline" example inside this repo.

## Big picture (what to know first)
- This repo contains multiple learning modules; focus for AI agents: [05-data-platforms/bruin/my-taxi-pipeline](../05-data-platforms/bruin/my-taxi-pipeline/).
- Pipeline architecture: assets (Python/SQL/YAML) live under `pipeline/assets/` and are executed by the `bruin` CLI using the pipeline definition `pipeline/pipeline.yml`.
- Key runtime pieces: `.bruin.yml` (connections/secrets), `pipeline/pipeline.yml` (pipeline metadata, variables, default connections), and assets under `pipeline/assets/` (ingestion → staging → reports).

## Critical developer workflows & commands
- Validate config without running: `bruin validate ./pipeline/pipeline.yml`
- Run pipeline (full refresh/backfill example):
```
bruin run ./pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-02-01
```
- Run a single asset or inspect lineage:
```
bruin run ./pipeline/assets/ingestion/trips.py
bruin lineage ./pipeline/assets/ingestion/trips.py
```
- Connection helpers: `bruin connections list` and `bruin connections ping <name>`

## Project-specific conventions and patterns
- Assets are colocated next to `pipeline.yml` under `pipeline/assets/` (SQL, Python, and YAML seed/asset descriptors).
- Python assets use an `@bruin` header to declare metadata (see `pipeline/assets/ingestion/trips.py`). Typical keys: `name`, `connection`, `materialization`, and `image`.
- Two Python styles used in templates:
  - Materialized Python asset: include `materialization:` in the header and implement `materialize()` which returns a DataFrame.
  - Script-style asset: omit `materialization:` and perform manual writes in the script (then no `materialize()` function required).
- Dependency placement: put Python deps in the nearest `requirements.txt` (template has one at pipeline root). `trips.py` comments explicitly reference this.

## Environment variables & pipeline variables (how assets receive config)
- Bruin built-ins available to Python assets: `BRUIN_START_DATE`, `BRUIN_END_DATE`, `BRUIN_START_DATETIME`, `BRUIN_END_DATETIME`.
- Pipeline variables defined in `pipeline/pipeline.yml` are passed as JSON in `BRUIN_VARS` to Python assets. Example variable used in this pipeline: `taxi_types`.

## Integration points & where to look for examples
- Ingestion example: [pipeline/assets/ingestion/trips.py](../05-data-platforms/bruin/my-taxi-pipeline/pipeline/assets/ingestion/trips.py) — contains TODO markers describing how to implement `materialize()` and which env vars to use.
- Pipeline config example: [pipeline/pipeline.yml](../05-data-platforms/bruin/my-taxi-pipeline/pipeline/pipeline.yml) — shows `default_connections` and `variables` usage.
- Seeds & lookups: `pipeline/assets/ingestion/payment_lookup.asset.yml` and accompanying CSV seed files are canonical examples for seed assets.

## What to change vs what to preserve
- Preserve the `@bruin` metadata style and `pipeline.yml` variable schema when editing assets — these drive runtime behavior.
- When implementing ingestion code prefer append-only writes; deduplication and quality checks belong in staging SQL assets.

## Quick TODO markers to act on now
- `pipeline/pipeline.yml`: set `name`, `schedule`, `start_date`, and `variables.taxi_types.default` (file contains TODOs).
- `pipeline/assets/ingestion/trips.py`: implement `materialize()` or convert to script-style asset as noted in file comments.

If any part of this guidance is unclear or you'd like different emphasis (e.g., more examples for Python materialization or seed assets), tell me which section to expand.  