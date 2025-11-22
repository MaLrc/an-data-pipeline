import os
import shutil
from pathlib import Path
from typing import Union
from zipfile import ZipFile, ZipInfo
import requests
from tqdm import tqdm
from loguru import logger

tqdm_common = {
  "ncols": 100,
  "bar_format": "{l_bar}{bar}| {n_fmt}/{total_fmt}"
}

def clear_cache(cache_folder, recreate_folder: bool = True):
  """Clear the cache folder."""
  shutil.rmtree(cache_folder)
  if recreate_folder:
    os.makedirs(cache_folder, exist_ok=True)
    
def path_after_cache(path):
  cache_index = path.parts.index('cache')
  relevant_parts = path.parts[cache_index:]
  return Path(*relevant_parts)

def download_file_from_https(url: str, filepath: Union[str, Path]) -> str:
  """
  Downloads a file from a https link to a local file.

  Args:
      url (str): The url where to download the file.
      filepath (Union[str, Path]): The path to the local file.

  Returns:
      str: Downloaded file filename.
  """
  logger.info(f"Downloading file into {filepath}")
  response = requests.get(
      url, stream=True, headers={"Accept-Encoding": "gzip, deflate"}
  )
  response.raise_for_status()
  response_size = int(response.headers.get("content-length", 0))
  filepath = Path(filepath)
  with open(filepath, "wb") as f:
    with tqdm(
      total=response_size,
      unit="B",
      unit_scale=True,
      desc=filepath.name,
      leave=False,
      **tqdm_common,
    ) as pbar:
      for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
        pbar.update(len(chunk))

  return filepath.name

def get_file_checksum_from_https(url: str) -> str:
  response = requests.get(
    f"{url}.md5"
  )
  return response.text

def unzip_files(zip_file: Union[str, Path], extract_folder:Union[str, Path], members:Union[list[str], None]=None) -> bool:
  """
  Extract all files to a local folder.

  Args:
      zip_file (Union[str, Path]): The Path of the zip file to extract.
      extract_folder (Union[str, Path]): The path to the local folder.
      members (list[str], optional): List of specific files to extract. Defaults to [].

  Returns:
      bool: True if success, False if error
  """
  with ZipFile(zip_file, 'r') as zf:
    try:
      logger.info(f'Unziping {len(zf.namelist())} file(s) from {str(zip_file)}')
      zf.extractall(
        path=extract_folder,
        members=[m for m in members if m in zf.namelist()] if members else zf.namelist()
      )
      return True
    except KeyError as e:
      logger.warning(e)
      return False
    except Exception as e:
      logger.error(f'Unexpected error : {e}')
      raise

def get_zip_file_namelist(zip_file: Union[str, Path]) -> list[str]:
  """
  Return a list of files name in a zip file.

  Args:
      zip_file (Union[str, Path]): The Path of the zip file to extract.

  Returns:
      list[str]: List of files name.
  """
  return ZipFile(zip_file, 'r').namelist()

def get_zip_file_infolist(zip_file: Union[str, Path]) -> list[ZipInfo]:
  """
  Return a list containing a ZipInfo object for each member of the archive.

  Args:
      zip_file (Union[str, Path]): The Path of the zip file to extract.

  Returns:
      list[ZipInfo]: List of ZipInfo object.
  """
  return ZipFile(zip_file, 'r').infolist()