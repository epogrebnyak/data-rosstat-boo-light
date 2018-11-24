"""Directory structure and paths."""

import platform
from pathlib import Path

URL = { 
        0: 'http://s3.eu-central-1.amazonaws.com/boo2012/data_reference.rar',    
     2012: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2012/data-20181029t000000-structure-20121231t000000.csv',
     2013: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2013/data-20181029t000000-structure-20131231t000000.csv',
     2014: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2014/data-20181029t000000-structure-20141231t000000.csv',
     2015: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2015/data-20181029t000000-structure-20151231t000000.csv',
     2016: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2016/data-20181029t000000-structure-20161231t000000.csv',
     2017: 'http://www.gks.ru/opendata/storage/7708234640-bdboo2017/data-20181029t000000-structure-20171231t000000.csv',
    }


__all__ = ['url',
           'url_local_path',
           'csv_path_raw',
           'csv_path_interim',
           'csv_path_processed',
          ]


PROJECT_ROOT = Path(__file__).parents[1]
IS_WINDOWS = (platform.system() == 'Windows')


# RAR executable
if IS_WINDOWS:
    UNPACK_RAR_EXE = str(PROJECT_ROOT / 'bin' / 'unrar.exe')
else:
    UNPACK_RAR_EXE = 'unrar'    

def url(year: int):
    return URL[year]

assert url(2012) == URL[2012] 

def make_subfolder(subfolder: str):
    """Return /data/<subfolder> path. Creates subfolders if they do not exist."""
    check_subfolder(subfolder)
    folder = PROJECT_ROOT / 'data' / subfolder
    if not folder.exists():
        folder.mkdir(parents=True)
    return folder


def check_subfolder(name:str):
    if name not in ['external', 'raw', 'interim', 'processed']:
        raise ValueError(f"worng subfolder name: {name}")


def url_filename(url):
    return url.split('/')[-1]


assert url_filename(URL[2012]) == 'data-20181029t000000-structure-20121231t000000.csv'


def url_local_path(year: int):
    return make_subfolder('external') / url_filename(URL[year])


def csv_filename(year: int):
    return  f'{year}.csv'
       

def _path_as_string(year, subfolder):
    return str(make_subfolder(subfolder) / csv_filename(year))

    
def csv_path_raw(year):
    return _path_as_string(year, 'raw')


def csv_path_interim(year):
    return _path_as_string(year, 'interim')


def csv_path_processed(year):    
    return _path_as_string(year, 'processed')


assert csv_path_raw(2012).endswith('\\data\\raw\\2012.csv')
assert csv_path_interim(2012).endswith('\\data\\interim\\2012.csv')
assert csv_path_processed(2012).endswith('\\data\\processed\\2012.csv')


#def tempfile(filename: str):
#    return make_data_path('temp', filename)    