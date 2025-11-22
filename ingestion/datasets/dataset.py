import os
from pathlib import Path
from typing import Union
from utils.files import download_file_from_https, unzip_files, get_file_checksum_from_https
from utils.duck import DuckDbClient
from config.config import CACHE_FOLDER, DatasetConfig
from loguru import logger

class Dataset():
  def __init__(self, dataset_name:str, dataset_config:DatasetConfig, duck_client:DuckDbClient) -> None:
    self.dataset_name = dataset_name
    self.dataset_config = dataset_config
    self.duck_client = duck_client
    self.cache_folder_path = Path(
      CACHE_FOLDER,
      self.dataset_config.cache_dataset_folder
    )
    self.cache_file_path = Path(
      self.cache_folder_path, 
      self.dataset_config.cache_file_name
    )
    pass
  
  def download_file_from_https(self):
    os.makedirs(self.cache_folder_path, exist_ok=True)
    return download_file_from_https(
      url=self.dataset_config.file_url,
      filepath=self.cache_file_path
    )
  
  def unzip_files(self, members:Union[list[str], None]=None) -> bool:
    return unzip_files(
      zip_file=self.cache_file_path,
      extract_folder=self.cache_folder_path,
      members=members
    )
      
  def process_dataset(self):
    cached_file_md5 = self.duck_client.get_dataset_cache_md5(self.dataset_config.file_url)
    online_file_md5 = get_file_checksum_from_https(self.dataset_config.file_url)
    if cached_file_md5 != online_file_md5 or os.path.exists(self.cache_file_path) == False:
      try: 
        self.download_file_from_https()
        self.duck_client.insert_cache_log(
          file_url=self.dataset_config.file_url,
          file_md5=online_file_md5
        )
      except Exception as e:
        logger.error(f'Error when downloading file from {self.dataset_config.file_url}. \n Error : {e}')
    else:
      logger.info(f'Skipping download for dataset {self.dataset_name}.')
    
    if Path(self.dataset_config.cache_file_name).suffix == ".zip":
      self.unzip_files()

    logger.info(f'Drop stg table {self.dataset_config.stg_table}')
    self.duck_client.drop_table('stg', self.dataset_config.stg_table)
    
    self.duck_client.ingest_from_files(
      files_type=self.dataset_config.file_type,
      table_name=self.dataset_config.stg_table,
      cache_path=self.cache_folder_path,
      cache_sub_folder=self.dataset_config.cache_sub_folder
    )
      
    return True