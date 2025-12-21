-include .env
export

ingest:
	uv run ./ingestion/run.py

ingest-only-download:
	uv run ./ingestion/run.py \
	--force_download_files \
	--skip_check_cached_files \
	--skip_unzip_files \
	--skip_drop_stg_table \
	--skip_ingest_from_files

ingest-already-cached-files:
	uv run ./ingestion/run.py \
	--skip_check_cached_files	\
	--skip_download_files

transform-run:
	cd ./dbt && uv run --env-file .env dbt run

transform-buid:
	cd ./dbt && uv run --env-file .env dbt build

transform-test:
	cd ./dbt && uv run --env-file .env dbt test

pipeline-incremental-refresh:
	uv run ./ingestion/run.py
	cd ./dbt && uv run --env-file .env dbt run

pipeline-full-refresh:
	uv run ./ingestion/run.py
	cd ./dbt && uv run --env-file .env dbt run --full-refresh

install:
	uv sync