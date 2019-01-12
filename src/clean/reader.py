from collections import OrderedDict
import itertools

import pandas as pd

import csv_io
import file
import util

import row_parser
from inspect_columns import Columns

COLUMNS = Columns.COLUMNS 
NCOL = len(COLUMNS)
COLUMNS_2 =  row_parser.get_parsed_colnames()
DTYPES_2 = row_parser.get_colname_dtypes()

def all_rows(year):
    return csv_io.yield_rows(file.raw(year))    

def has_valid_length(row, n=NCOL):
    return len(row) == n

def raw_rows(year):
    return filter(has_valid_length, all_rows(year))

def make_dict(columns):
    def make(row):
        return OrderedDict(zip(columns, row))
    return make

def dicts0(year):
    return map(make_dict(COLUMNS), raw_rows(year))

def has_inn(d):
    return d['inn']
    
def dicts1(year):        
    return filter(has_inn, dicts0(year))

def rows(year):
    return map(row_parser.row_dict_to_list, dicts1(year))
    
def dicts(year):
    return map(make_dict(COLUMNS_2), rows(year))

def length(gen):
    return sum(1 for _ in gen)

class Dataset:    
    colnames = COLUMNS_2    
    dtypes = DTYPES_2
    
    def __init__(self, year):
        self.year = year
        self._echo = util.messenger(self.year)
        self.filename = file.clean(year)

    def rows(self):
        return rows(self.year)
        
    def dicts(self):
        return dicts(self.year)
        
    def echo(self, *args):
        return self._echo(*args)
        
    def slice(self, i, j):
        return list(itertools.islice(self.dicts(), i, j))

    def inn(self, *inns):
        result = []
        inns_ = [str(i) for i in inns]
        for d in self.dicts():
            i = d['inn'] 
            if i in inns_:
                result.append(d)
                inns_.remove(i)
            if inns_==[]:
                break
        return result
    
    def nth(self, n):
        return self.slice(n, n+1)[0]
    
    def count(self):
        self.echo("Please wait - count may take very long time")
        return sum(1 for _ in self.dicts())
    
    def save(self):
        self.echo(f"Saving CSV file to {self.filename}")
        csv_io.save_rows(path = self.filename,
                         stream = self.rows(),
                         column_names = self.colnames)
        
    def pd_read(self):
        self.echo("Reading dataframe...")
        with open(self.filename, 'r', encoding='utf-8') as f:
            return pd.read_csv(f, dtype=self.dtypes)

def pd_read(year):
    echo = util.messenger(year)
    echo("Reading dataframe...")
    with open(file.clean(year), 'r', encoding='utf-8') as f:
        return pd.read_csv(f, dtype=DTYPES_2)

def __desc__(year: int):
    echo = util.messenger(year)
    echo ("Please wait - count may take very long time")
    messages = [("Total rows:            ", all_rows),
                ("Rows with valid length:", raw_rows),
                ("Rows with INN:         ", dicts1)                 
                ]
    echo = util.messenger(year)
    for (m, gen) in messages:
        echo (m, length(gen(year)))

# (2012) Counts take very, very long time
# (2012) Total rows:             765813
# (2012) Rows with valid length: 765813
# (2012) Rows with INN         : 765813

# (2017) Total rows:             2358756
# (2017) Rows with valid length: 2358756
# (2017) Rows with INN:          2358756  
  
d12 = Dataset(2012)
#print(d12.inn('2446000322'))
# df = pd_read(2012)
big20 = df[~df.ok1.isin([67,65])].sort_values(['sales'], ascending=False).head(50)[['title','ok1','inn', 'ta']]

# -- exclude = 
"""
341047  Межрегиональный общественный Фонд содействия н...   74  7710244903   
319412                                    КОНЦЕРН "ЛЕВИН"   74  7707089648   

КОНЦЕРН "ЛЕВИН"   74  7707089648 
"""  
