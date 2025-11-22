import os
from pydantic import BaseModel, ValidationError
from typing import Union, Optional, Literal
from pathlib import Path
from yaml import safe_load
from loguru import logger

ROOT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
CACHE_FOLDER = Path(ROOT_FOLDER, 'data/cache')
DATABASE_FOLDER = Path(ROOT_FOLDER, 'data/database')
DUCKDB_FILE = Path(DATABASE_FOLDER, 'database.duckdb')

os.makedirs(CACHE_FOLDER, exist_ok=True)
os.makedirs(DATABASE_FOLDER, exist_ok=True)

class DatasetConfig(BaseModel):
  file_url: str
  file_type: Literal['csv', 'json']
  cache_dataset_folder: str
  cache_file_name: str
  cache_sub_folder: str
  stg_table: str

class DatabaseConfig(BaseModel):
  host: str
  type: str
  path: str

class Config(BaseModel):
  # database: DatabaseConfig
  datasets: dict[str, DatasetConfig]
  
def load_config(yaml_file_path: Union[str, Path]) -> Config:
  try:
    with open(yaml_file_path, 'r') as cfg:
      config_dict = safe_load(cfg)
    return Config(**config_dict)
  except ValidationError as e:
    logger.error(f"Validation error for config file {yaml_file_path} : {e}")
    raise
  except Exception as e:
    logger.error(f"Unexpected error when loading config file {yaml_file_path} : {e}")
    raise

def get_environment(default="dev"):
  env = os.getenv("ENV", default)
  logger.info(f"Running on env {env}")
  if env not in ["dev", "prod"]:
    logger.error(f"Invalid environment: {env}. Must be 'dev' or 'prod'.")
    raise ValueError(f"Invalid environment: {env}. Must be 'dev' or 'prod'.")
  return env