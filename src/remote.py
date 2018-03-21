# -*- coding: utf-8 -*-
"""Download and unpack CSV file from Rosstat web site:

   download(year)
   unpack(year)

"""

import os
import subprocess
from pathlib import Path

import requests

from settings import UNPACK_RAR_EXE, Storage, LocalCSV


def _download(url, path):
    r = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return path

def unrar(rar_file_path: str, folder:str, unrar_executable=UNPACK_RAR_EXE):
    def mask_with_end_separator(folder):
        """UnRAR wants its folder argument with '/'
        """
        return ("{}{}".format(folder, os.sep) 
                if not folder.endswith(os.sep)
                else folder)    
    folder = mask_with_end_separator(folder)
    job = [unrar_executable, 'e', rar_file_path, folder, '-y']
    exit_code = subprocess.check_call(job)
    return exit_code
    

def rar_content(rar_file_path: str, unrar_executable=UNPACK_RAR_EXE):
    """Return single filename stored in RAR archive."""
    job = [unrar_executable, 'lb', rar_file_path]
    return subprocess.check_output(job).decode("utf-8").strip()

def unpacked_csv_path(year):
    s = Storage(year)
    content = rar_content(s.rar_path)
    filenames = content.split('\r\n')
    try:
        filename = [fn for fn in content.split('\r\n') if fn.startswith('data')][0]       
    except IndexError:
        raise ValueError(f'No datafile in {filenames}')
    return os.path.join(s.rar_folder, filename)


def download(year, force=False):
    s = Storage(year)
    url, path = s.url, s.rar_path
    prefix = f'{year}:'
    if os.path.exists(path) and not force:
        print(prefix, "Already downloaded", path)
    else:
        print(prefix, "Downloading", url)
        _download(url, path)
        print(prefix, "Saved", path)
    
def unpack(year: int, force=False):
   # results in data/raw/YYYY.csv
   s = Storage(year)
   if not Path(s.rar_path).exists():
       raise FileNotFoundError(s.rar_path)
   unpacked = unpacked_csv_path(year)    
   saved = LocalCSV(year).raw_path
   prefix = f'{year}:'
   if os.path.exists(saved) and not force:
        print(prefix, 'Already unpacked raw CSV file as', saved)
   else:  
       # cannot unpack to existing file, delete it
       if os.path.exists(unpacked):
           os.remove(unpacked)
       unrar(s.rar_path, s.rar_folder) 
       # moving csv file to data/raw/YYYY.csv
       os.rename(unpacked, saved)
       print(prefix, 'Extracted {s.rar_path} as:\n{saved}')       
     
if __name__ == "__main__":
    download(2016)
    unpack(2016)