from settings import PROJECT_ROOT

def data_folder(root=PROJECT_ROOT):
    folder = root / 'data' 
    if not folder.exists():
        folder.mkdir(parents=True)
    return folder

def path(tag, year: int):    
    return data_folder() / f'{tag}-{year}.csv'

def raw(year: int):
    return path("rosstat", year)

def clean(year: int):
    return path("clean", year)    
