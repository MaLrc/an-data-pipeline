import duckdb
from config.config import DUCKDB_FILE
from loguru import logger
from pathlib import Path
from utils.files import path_after_cache
import os

class DuckDbClient():
  def __init__(self) -> None:
    self.duck_conn = self.create_connection(DUCKDB_FILE)
    self.duck_conn.execute('CREATE SCHEMA IF NOT EXISTS stg;')
    self.duck_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stg.cache_logs (
        file_url VARCHAR,
        file_md5 VARCHAR,
        date_T_download DATETIME
        );
        """
      )
    pass
  
  def create_connection(self, duck_file):
    conn = duckdb.connect(duck_file)
    logger.success(f'DuckDb connected to {DUCKDB_FILE}')
    return conn
  
  def get_connection(self):
    return self.duck_conn
  
  def check_table_exists(self, table_name):
    query = f"""
      SELECT COUNT(*)
      FROM information_schema.tables
      WHERE table_name = '{table_name}'
    """
    self.duck_conn.execute(query)
    result = self.duck_conn.fetchone()
    return result[0] == 1 if result is not None else False
  
  def drop_table(self, schema, table_name):
    query = f"""
      DROP TABLE IF EXISTS {schema}.{table_name};
    """
    self.duck_conn.execute(query)
    result = self.duck_conn.fetchone()
    return result[0] == 1 if result is not None else False
  
  def get_dataset_cache_log(self, file_url):
    query = f"""
    SELECT * 
    FROM stg.cache_logs
    WHERE file_url = '{file_url}';
    """
    return self.duck_conn.execute(query).fetchall()
  
  def get_dataset_cache_md5(self, file_url):
    query = f"""
    SELECT file_md5
    FROM stg.cache_logs
    WHERE file_url = '{file_url}';
    """
    result = self.duck_conn.execute(query).fetchone()
    return result[0] if result is not None else False
  
  def insert_cache_log(self, file_url, file_md5):
    query=f"""
    MERGE INTO stg.cache_logs
      USING(
        SELECT 
          '{file_url}' AS file_url,
          '{file_md5}' AS file_md5,
          NOW() AS date_T_download
      ) as upserts
      ON (upserts.file_url = stg.cache_logs.file_url)
      WHEN MATCHED THEN UPDATE
      WHEN NOT MATCHED THEN INSERT;
    """
    
    self.duck_conn.execute(query)
    
  def ingest_from_files(self, files_type, table_name, cache_path, cache_sub_folder):   
    if self.check_table_exists(table_name):
      query = f'INSERT INTO stg.{table_name} '
    else :
      query = f'CREATE TABLE IF NOT EXISTS stg.{table_name} AS '

    if files_type == 'json':
      query_select = f"""
        SELECT 
          json(content) as json,
          filename,
      """
      query_from = f"FROM read_text('{cache_path}/{cache_sub_folder + '/' if cache_sub_folder != '' else ''}**/*.json');"
    elif files_type == 'csv':
      query_select = f"""
        SELECT 
          *,
      """
      query_from = f"FROM read_csv('{cache_path}/{cache_sub_folder + '/' if cache_sub_folder != '' else ''}**/*.csv', header=true, delim=',', filename=true);"
    else:
      raise ValueError(f'Supported files type are json or csv')
    
    query_select_de = f"""
        NOW() AS date_T_insert
    """
    
    logger.info(f'Starting inserting data into stg.{table_name}')
    try :
      self.duck_conn.execute(query + query_select + query_select_de + query_from)
      logger.info(f'Done inserting data into stg.{table_name}')
    except Exception as e:
      logger.error(f'Error on inserting data into stg.{table_name}. \n Error : {e}')
    return True