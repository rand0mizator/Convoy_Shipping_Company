import pandas as pd
import csv
import sqlite3
import re

SHEET_NAME = 'Vehicles'


def file_reader(f_name):
    if f_name.endswith('.xlsx'):
        df = pd.read_excel(f_name, sheet_name=SHEET_NAME, dtype=str)
    elif f_name.endswith('.csv'):
        df = pd.read_csv(f_name)
    else:
        print(f'Something went wrong, didnt catch file type for file: {f_name}')
    return df


def file_editor(df):
    corrected_cells_count = 0
    f_name = file_name.split('.')[0]
    for index in df.index:
        for column in df.columns:
            line1 = df.loc[index, column]
            line2 = re.sub(r'\D', '', df.loc[index, column])
            if line1 != line2:
                df.loc[index, column] = re.sub(r'\D', '', df.loc[index, column])
                corrected_cells_count += 1
    df.to_csv(fr'{f_name}[CHECKED].csv', index=None, header=True)
    print(f"{corrected_cells_count} {'cells were' if corrected_cells_count > 1 else 'cell was'} corrected"
          f" in {f_name}[CHECKED].csv")
    return df


def file_converter(df):
    f_name = file_name.split('.')[0]
    rows_count = df.shape[0]
    df.to_csv(fr'{f_name}.csv', index=None, header=True)
    print(f"{rows_count} {'lines were' if rows_count > 1 else 'line was'} imported to {f_name}.csv")


# def csv_to_db(df):
#     f_name = file_name.split('[CHECKED]')[0] + '.s3db'
#     conn = sqlite3.connect(f_name)
#     cur = conn.cursor()
#     cur.execute()


def create_table(f_name):
    db_name = f_name.split('[CHECKED]')[0] + '.s3db'
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS convoy (
                vehicle_id INTEGER PRIMARY KEY)
                """)
    conn.commit()
    conn.close()


def add_columns_to_db(f_name, df):
    db_name = f_name.split('[CHECKED]')[0] + '.s3db'
    columns = list(df.columns)
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    for column in columns:
        cur.execute("""ALTER TABLE convoy 
                       ADD COLUMN ? INTEGER""", column)
    conn.commit()
    conn.close()
"""
Prompt the user to give a name for the input file (complete with the .xlsx, .csv, or [CHECKED].csv extension).
For the prompt message, use Input file name followed by a newline.
"""
print("Input file name")
file_name = input()

file = file_reader(file_name)

if '[CHECKED]' not in file_name:
    checked = file_editor(file)
else:
    checked = file

"""
If your file is .xlsx or .csv, perform all the previous transformations in the correct order, 
until you get a file that ends with %...%[CHECKED].csv.
"""

# if file_name.endswith('.xlsx'):
#
#
#     # convert to csv
#     # save to db
# elif file_name.endswith('.csv'):
#
#
#     # save to db
# elif file_name.endswith('[CHECKED].csv'):
#
#     # save to db
