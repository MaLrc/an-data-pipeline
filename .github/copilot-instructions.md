# Copilot / AI agent instructions — an-data-pipeline

Summary
- This repo is a small ETL pipeline: a Python-based "ingestion" layer that downloads, caches and ingests source files into a local DuckDB, and a dbt project that transforms `stg.*` tables into `int.*` and `mart.*` models.
- Storage: local DuckDB at `data/database/database.duckdb` (dev). Production DB is configured via `dbt/profiles.yml` and expects a `MOTHERDUCK_TOKEN`.

Quick start (commands you can run)
- Install dependencies: `pip install -e .` (uses `pyproject.toml`) or create a venv and install packages listed there.
- Ingestion (download + ingest): `make ingest` or `python ./ingestion/run.py` (supports flags — see CLI options in `ingestion/run.py`).
  - Example flags (as in `Makefile`): `--force_download_files`, `--skip_check_cached_files`, `--skip_unzip_files`, `--skip_drop_stg_table`.
- Run dbt transforms (from repo root): `cd dbt && dbt run --target dev` (or use `make transform-run` which uses the `Makefile` wrapper).
- Full pipeline: `make pipeline-full-refresh` (ingest then dbt run --full-refresh).

Architecture & data flow (what to know)
- Config-driven ingestion: datasets are defined in `ingestion/config/config.yaml`. Each dataset entry contains: `file_url`, `file_type` (`csv`|`json`), `cache_dataset_folder`, `cache_file_name`, `cache_sub_folder`, `stg_table`.
- Ingestion steps (see `ingestion/datasets/dataset.py`): 1) check remote `.md5`, 2) download to `data/cache/<dataset>`, 3) unzip if needed (`utils/files.py`), 4) drop staging table, 5) ingest into DuckDB staging schema (`stg.*`) using `DuckDbClient.ingest_from_files()`.
- DuckDB interactions: `ingestion/utils/duck.py` creates `stg` schema and `stg.cache_logs` (MERGE used to upsert cache metadata). JSON ingestion uses `read_text('**/*.json')` and CSV ingestion uses `read_csv('**/*.csv', header=true, filename=true)`.
- dbt transforms: source tables are declared in `dbt/models/sources.yml` (source `local_duckdb`, schema `stg`). Models are organized into `stg/`, `int/`, `mart/`. `stg` models are views in dev and `ephemeral` in prod (see `dbt_project.yml` config); `int` models are incremental and typically define a `unique_key`.

Project-specific conventions & gotchas
- Environment variables:
  - `ENV` must be `dev` or `prod` (enforced by `ingestion/config/config.py`).
  - `MOTHERDUCK_TOKEN` is required by `dbt/profiles.yml` when using the production (`prod`) target.
  - The repository uses `.env` via `python-dotenv` (loaded in `ingestion/run.py`), and `Makefile` commands optionally use `uv run --env-file .env`.
- dbt default target in `dbt/profiles.yml` is `prod` — *switch to `dev` when working locally* (or pass `--target dev` to `dbt run`).
- The ingestion layer relies on comparing remote `<url>.md5` with a stored value in `stg.cache_logs`. Sometimes `.md5` responses contain trailing newlines — treat them consistently when comparing.
- When adding a dataset:
  1) Add an entry in `ingestion/config/config.yaml` with `stg_table` and `cache_*` fields.
  2) Add the corresponding `raw_*` source in `dbt/models/sources.yml` and create `models/stg/stg_<name>.sql` to expose the raw table.
  3) Add `int/` and `mart/` models to transform the raw JSON/CSV (examples: `models/int/int_scrutins.sql`).

Code patterns & style notes for AI edits
- Error handling in ingestion is often logged and functions return booleans (e.g., `Dataset.process_dataset()` returns True/False). Prefer following this pattern for small failures rather than raising everywhere.
- Logging uses `loguru` across scripts. Use `logger.info`, `logger.warning`, `logger.error`, `logger.success` consistently.
- DuckDB ingestion relies on SQL string composition (watch for SQL injection risk when introducing interpolated variables — prefer safe formatting where possible).
- dbt models often use `json_extract_string` and project macros (see `dbt/macros/json_extract_if_not_null.sql`) to handle JSON fields — follow existing extraction patterns.

Integration & external dependencies
- Downloads come from `data.assemblee-nationale.fr`. Tests or local development might mock network downloads if you need deterministic behavior.
- DB targets:
  - dev: local DuckDB file (`data/database/database.duckdb`).
  - prod: MotherDuck (`md:an_data_pipeline`) using `MOTHERDUCK_TOKEN`.

Where to look first when debugging
- Ingestion logs and cache: `data/cache/` (check file existence), `stg.cache_logs` (DuckDB table). Look at `ingestion/run.py`, `ingestion/datasets/dataset.py`, and `ingestion/utils/files.py`.
- dbt failures: `dbt/target/run_results.json` and `dbt/target/manifest.json` contain verbose diagnostic info.

PR/Change guidance for contributors (short)
- For dataset additions: update ingestion config, add `stg` model (exposes raw table), add `int`/`mart` models, run `make ingest` locally, then `cd dbt && dbt run --target dev && dbt test --target dev`.
- Keep `dbt/profiles.yml` secure: do not commit production tokens; use environment variables for secrets.

If anything is unclear or you want more detail in a specific area (e.g., testing, CI, or adding a specific dataset example), tell me which section to expand. 

---
References: `ingestion/`, `ingestion/config/config.yaml`, `ingestion/run.py`, `ingestion/datasets/dataset.py`, `ingestion/utils/*.py`, `dbt/` (models, macros, `dbt_project.yml`, `profiles.yml`), `Makefile`, `pyproject.toml`.