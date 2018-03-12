# -*- coding: utf-8 -*-
"""Download and unpack CSV file from Rosstat web site:

   download(year)
   unpack(year)

"""

import os
import subprocess
from pathlib import Path

import requests

from settings import UNPACK_RAR_EXE, Storage


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
    filename = [fn for fn in content.split('\r\n') if fn.startswith('data')][0]       
    return os.path.join(s.rar_folder, filename)

def download(year, force=False):
    s = Storage(year)
    url, path = s.url, s.rar_path
    if os.path.exists(path) and not force:
        print("Already downloaded", path)
    else:
        print("Downloading", url)
        _download(url, path)
        print("Saved as", path)
    
def unpack(year: int, force=False):
   # results in data/raw/YYYY.csv
   s = Storage(year)
   if not Path(s.rar_path).exists():
       raise FileNotFoundError(s.rar_path)
   unpacked = unpacked_csv_path(year)    
   saved = s.raw_csv_path
   if os.path.exists(saved) and not force:
        print("Already unpacked as", saved)
   else:  
       # cannot unpack to existing file
       if os.path.exists(unpacked):
           os.remove(unpacked)
       unrar(s.rar_path, s.rar_folder) 
       # moving to data/raw/YYYY.csv
       os.rename(unpacked, saved)
       print(f'Extracted {s.rar_path} as:\n{saved}')       
     
if __name__ == "__main__":
    download(2016)
    unpack(2016)