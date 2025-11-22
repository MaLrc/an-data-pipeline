import os
from dotenv import load_dotenv
from pprint import pprint
from config.config import load_config, get_environment, CACHE_FOLDER
from pathlib import Path
from datasets.dataset import Dataset
from utils.duck import DuckDbClient
from loguru import logger

load_dotenv()
config = load_config('ingestion/config/config.yaml')
get_environment()

duck_client= DuckDbClient()

datasets_list = config.datasets

for ds_name, ds_config in datasets_list.items() :
  ds = Dataset(
    ds_name,
    ds_config, 
    duck_client=duck_client
  )
  logger.info(f'Start processing dataset : {ds_name}')
  ds.process_dataset()