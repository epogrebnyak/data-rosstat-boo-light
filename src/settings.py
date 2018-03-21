"""Directory structure and other configuration."""

import platform
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
        
# RAR executable
IS_WINDOWS = (platform.system() == 'Windows')

if IS_WINDOWS:
    UNPACK_RAR_EXE = str(PROJECT_ROOT / 'bin' / 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'    


def make_data_path(subfolder: str, filename: str) -> str:
    """Return <repo root>/data/<subfolder>/<filename>.
       Creates subfolders if they do not exist.
    """
    folder = PROJECT_ROOT / 'data' / subfolder
    if not folder.exists():
        folder.mkdir(parents=True)
    return str (folder / filename)


class Storage:
    """Archives to download from Rosstat and filenames for them."""
    
    urls = { 
        0: 'http://s3.eu-central-1.amazonaws.com/boo2012/data_reference.rar',    
     2012: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20161021t000000-structure-20121231t000000.rar',
     2013: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20161021t000000-structure-20131231t000000.rar',
     2014: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20161021t000000-structure-20141231t000000.rar',
     2015: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20161021t000000-structure-20151231t000000.rar',
     2016: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2016/data-20171023t000000-structure-20161231t000000.rar'
    }
    
    allowed_years = list(urls.keys())
    
    def __init__(self, year: int):
        try:
            self.url = self.urls[year]
        except KeyError:
            raise ValueError(f'{year} not in allowed years: {self.allowed_years}')

    @property    
    def rar_path(self):
        filename = self.url.split('/')[-1]
        return make_data_path('external', filename)    
    
    @property      
    def rar_folder(self):
        return make_data_path('external', '')
    
    
class LocalCSV:
    def __init__(self, year):
        self.csv_filename = f'{year}.csv'
        
    @property    
    def raw_path(self):
        return make_data_path('raw', self.csv_filename)

    @property            
    def interim_path(self):
        return make_data_path('interim', self.csv_filename)
    
    @property        
    def processed_path(self):    
        return make_data_path('processed', self.csv_filename)


def tempfile(filename: str):
    return make_data_path('temp', filename)    