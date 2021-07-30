import pandas as pd
import re
import sqlite3

SHEET_NAME = 'Vehicles'


def get_name(file_name):
    """
    takes file name with extension and returns meaningfull part
    example: convoy.xlsx -> convoy
    """
    if '[CHECKED]' in file_name:
        return file_name.split('[CHECKED]'[0])
    else:
        return file_name.split('.')[0]


def create_table(file_name: str):
    db_name = get_name(file_name) + '.s3db'
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS convoy (
                vehicle_id INTEGER PRIMARY KEY)
                """)
    conn.commit()
    conn.close()


def add_columns_to_db(file_name: str, column_names: list):
    db_name = get_name(file_name) + '.s3db'
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    for column_name in column_names:
        cur.execute("""ALTER TABLE convoy 
                       ADD COLUMN ? INTEGER""", column_name)
    conn.commit()
    conn.close()


def editor(file_name):
    if file_name.endswith('.xlsx'):
        file_type = '.xlsx'
        my_df = pd.read_excel(file_name, sheet_name=SHEET_NAME, dtype=str)
        file_name = file_name[:-5]
    elif file_name.endswith('.csv'):
        file_type = '.csv'
        my_df = pd.read_csv(file_name, dtype=str)
        file_name = file_name[:-4]
    rows_count = my_df.shape[0]
    my_df.to_csv(fr'{file_name}.csv', index=None, header=True)
    if file_type == '.xlsx':
        print(f"{rows_count} {'lines were' if rows_count > 1 else 'line was'} imported to {file_name}.csv")
    corrected_cells_count = 0
    for index in my_df.index:
        for column in my_df.columns:
            line1 = my_df.loc[index, column]
            line2 = re.sub(r'\D', '', my_df.loc[index, column])
            if line1 != line2:
                my_df.loc[index, column] = re.sub(r'\D', '', my_df.loc[index, column])
                corrected_cells_count += 1
    my_df.to_csv(fr'{file_name}[CHECKED].csv', index=None, header=True)
    print(f"{corrected_cells_count} {'cells were' if corrected_cells_count > 1 else 'cell was'} corrected in {file_name}[CHECKED].csv")


print("Input file name")
file_name = input()
editor(file_name)
