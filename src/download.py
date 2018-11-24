"""Download csv file from Rosstat web site."""

import requests
import os
from settings import url, url_local_path


def _download(url, path):
    r = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return path


def download(year, force=False):    
    _url = url(year) 
    _path = url_local_path(year)
    if os.path.exists(_path) and not force:
        print(year, "Already downloaded", _path)
    else:
        print(year, "Downloading", _url)
        _download(_url, _path)
        print(year, "Saved", _path)
        
if __name__ == "__main__":
    download(2012)
    #download(2013)
    #download(2014)
    #download(2015)
    #download(2016)
    #download(2017)