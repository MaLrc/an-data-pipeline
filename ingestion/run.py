import os
from dotenv import load_dotenv
from config.config import load_config, get_environment, CACHE_FOLDER
from pathlib import Path
from datasets.dataset import Dataset
from utils.duck import DuckDbClient
from loguru import logger
import click

load_dotenv()
config = load_config('ingestion/config/config.yaml')
get_environment()

@click.command()
@click.option('--skip_check_cached_files', is_flag=True)
@click.option('--skip_download_files', is_flag=True)
@click.option('--force_download_files', is_flag=True)
@click.option('--skip_unzip_files', is_flag=True)
@click.option('--skip_drop_stg_table', is_flag=True)
@click.option('--skip_ingest_from_files', is_flag=True)
def cli(skip_check_cached_files, skip_download_files, force_download_files, skip_unzip_files, skip_drop_stg_table, skip_ingest_from_files)->None:
  duck_client= DuckDbClient()
  datasets_list = config.datasets
  for ds_name, ds_config in datasets_list.items() :
    ds = Dataset(
      ds_name,
      ds_config, 
      duck_client=duck_client
    )
    logger.info(f'Start processing dataset : {ds_name}')
    process_status = ds.process_dataset(
      check_cached_files_task = not skip_check_cached_files,
      download_files_task = not skip_download_files,
      force_download_files = force_download_files,
      unzip_files_task = not skip_unzip_files,
      drop_stg_table_task = not skip_drop_stg_table,
      ingest_from_files_task = not skip_ingest_from_files,
    )
    if process_status:
      logger.success(f'Ingestion Done for dataset {ds_name}')
    else : 
      logger.warning(f'Ingestion Skipped for dataset {ds_name}')
      
if __name__ == '__main__':
    cli()