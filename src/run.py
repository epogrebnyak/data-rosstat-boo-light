from download import download
from reader import Dataset

download(2012)
dataset = Dataset(2012)
dataset.to_csv()
df = dataset.read_dataframe()
df[:1000].to_excel("2012_1000.xls")

"""
WARNING: this script operates in files up to 1,6 Gb in size. 
         This may slow down your machine and fail in case of 
         memory overflow or disk space shortage.         
         
         In specific, Dataset(year).read_dataframe() is known 
         to exhaust memory.
         
"""

# from remote import download, unpack
# from reader import Dataset

# #YEARS = [2012, 2013, 2014, 2015, 2016]
# YEARS = [2012]

# for year in YEARS:
    # download(year)
    # unpack(year)
    # Dataset(year).to_csv()    
    
# df = dict()    
# for year in YEARS:
    # df[year] = Dataset(year).read_dataframe()
    
# TODO: make testable with year = 0 + travis_ci
