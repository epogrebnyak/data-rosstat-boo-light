"""Generic CSV file read and write operations."""

import csv
from logs import print_elapsed_time

def yield_csv_rows(path, enc='windows-1251', sep=";"):
    """Emit CSV rows by filename."""
    if enc not in ['utf-8', 'windows-1251']:
        raise ValueError("Encoding not supported: " + str(enc))
    with open(path, 'r', encoding=enc) as csvfile:
        spamreader = csv.reader(csvfile, delimiter=sep)
        for row in spamreader:
            yield row


@print_elapsed_time
def rows_to_csv(path, stream, cols=None):
    # output to csv file
    with open(path, 'w', encoding="utf-8") as file:
        writer = csv.writer(file, 
                            lineterminator="\n",
                            quoting=csv.QUOTE_MINIMAL)
        if cols:
            writer.writerow(cols)
        writer.writerows(stream)
    print("Saved file:", path)
    return path


@print_elapsed_time
def dicts_to_csv(path, dict_stream, column_names):
     with open(path, 'w', encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=column_names,
                                      lineterminator="\n",
                                      quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for d in dict_stream: 
            print('Wrote to file:', d)
            writer.writerow(d)
