import pandas as pd
import re

SHEET_NAME = 'Vehicles'


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
