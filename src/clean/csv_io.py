"""Generic CSV file read and write operations."""

import csv
#from logs import print_elapsed_time

FMT = dict(lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

def check(enc):
    if enc not in ['utf-8', 'windows-1251']:
        raise ValueError("Encoding not supported: " + str(enc))

def yield_rows(path, enc='windows-1251', sep=";"):
    """Emit CSV rows by filename."""
    with open(path, 'r', encoding=enc) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=sep)
        for row in spamreader:
            yield row

def _open(path): 
    return open(path, 'w', encoding="utf-8")

#@print_elapsed_time
def save_rows(path, stream, cols=None):
    with _open(path) as file:
        writer = csv.writer(file, **FMT)
        if cols:
            writer.writerow(cols)
        writer.writerows(stream)
    print("Saved file:", path)


#@print_elapsed_time
def save_dicts(path, dict_stream, column_names):
    with _open(path) as file:
        writer = csv.DictWriter(file, fieldnames=column_names, **FMT)
        for d in dict_stream: 
            print('Wrote to file:', d)
            writer.writerow(d)
            
            