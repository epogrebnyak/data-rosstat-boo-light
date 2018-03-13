# 2012-2016 Russian enterprises financial reports

The code allows to collect corporate reports and store them as CSV files for further analysis in R/pandas/Eviews.

<!---
**Easy path:** novice users can download smaller subsets of Rosstat data as csv/xlsx files (fewer variables, less companies, size 3-5Mb to 10-20Mb per year)

**Hard way:** a more experienced user can reproduce a clean version of full Rosstat dataset on a local computer (300Mb-1.6Gb per year)

Parent repo (heavy commit history, hard to replicate):
- https://github.com/epogrebnyak/data-rosstat-boo-2013
-->

Source data
===========
- For every year in 2012-2016 we have a file with column names and archived CSV with data. 
- Column names are practically the same for all 4 years.
- Each data file is 1-2 Gb when unpacked, >250 columns, 0.7 to 2+ mln rows.

Source dataset is a bit dirty:
 - a small part of rows uses different monetary units (rub and mln run instead of thousand rub)
 - several rows are corrupted in source files (see "Known bugs" below)

Latest dataset (2016):
- http://www.gks.ru/opendata/dataset/7708234640-bdboo2016

Usage
=====
Use code below to obtain all datasets. Supported years are 2012-2016
but older files are smaller, try running 2012 or 2013 first.

```python
from remote import download, unpack
from reader import Dataset

YEARS = [2012, 2013, 2014, 2015, 2016]

for year in YEARS:
    download(year)
    unpack(year)
    Dataset(year).to_csv()   

df = Dataset(2012).read_df()
```

Note: you will be operating with large datasets, creating files may take 2-3 mins on a fast computer
and much longer on laptops and older machines. 

Data pipline 
============

How do we obtain processed CSV files?

#### 1. Download and unrar raw csv
- Download rar file  
- Unpack raw csv from rar file  

#### 2. Make local processed csv file  
- Purge broken lines from raw csv (company has no INN field, wrong number of columns)
- Transform data:
  - adjust numeric values to '000 rub
  - produce file with fewer columns 
  - add new text columns (okved codes, company title, region by inn, year)
- Keep INN and region codes as strings
- Add headers to CSV
- Save as local CSV file

#### 3. Read local csv file as pandas dataframe
- Read dataframe using ```pd.read_csv``` with dtypes (it loads file faster)


#### Subsets: parts dataset
- Dataframe like ```df=Dataset(year).read_df()``` still very big, a lot of noise and slow to explore  
- Subsets allow creating row slices of dataset, column names stay the same acr 

```python  
from reader import Subset
inns = ['2224102690', '2204026804', '2222057509', '2204026730', '2207007165']
Subset(2016, inns).to_csv('sample1.csv')
```

Known issues
============

1\. Key field INN must be 10 digits, but sometimes starts with 0, trying to keep it as string, not int.
Alternatively, push all to INNs to int. In practice when doing df.merge(on='inn') I loose some matches,
probably due to typing of inns.

2\. Reading source csv file:
  - one line with elements exceeding number of columns  
  - several lines without INN field
  - CSV may have last empty row

3\. Full-length datasets are out of memory in pandas on many computers.

4\. Latest revisions of dataset wrongly mix units, there are fake large companies bigger than Gazprom or Rosneft.
