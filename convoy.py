import pandas as pd
import sqlite3
import re
from sqlalchemy import create_engine


SHEET_NAME = 'Vehicles'

# sequences for conveyor. In which order provide converting according to input file extension
xlsx_sequence = ['to_csv', 'to_checked_csv', 'to_s3db', 'to_json', 'to_xml']
csv_sequence = ['to_checked_csv', 'to_s3db', 'to_json', 'to_xml']
checked_csv_sequence = ['to_s3db', 'to_json', 'to_xml']
s3db_sequence = ['to_json', 'to_xml']
column_names = ['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']


def convert_to_csv(f_name, df):
    name = f_name.split('.')[0]
    rows_count = df.shape[0]
    df.to_csv(fr'{name}.csv', index=None, header=True)
    print(f"{rows_count} {'lines were' if rows_count > 1 else 'line was'} imported to {name}.csv")


def convert_to_checked_csv(f_name, df):
    name = f_name.split('.')[0]
    corrected_cells_count = 0
    for index in df.index:
        for column in df.columns:
            line1 = df.loc[index, column]
            line2 = re.sub(r'\D', '', df.loc[index, column])  # \D - filters all symbols except digits
            if line1 != line2:
                df.loc[index, column] = re.sub(r'\D', '', df.loc[index, column])
                corrected_cells_count += 1
    df.to_csv(fr'{name}[CHECKED].csv', index=None, header=True)
    print(f"{corrected_cells_count} {'cells were' if corrected_cells_count > 1 else 'cell was'} corrected in {name}[CHECKED].csv")
    return df


def convert_to_s3db(f_name, df):
    """Convert dataframe to s3db using sqlite3 library"""
    if '[CHECKED]' in f_name:
        db_name = f_name.split('[CHECKED]')[0] + '.s3db'
    else:
        db_name = f_name.split('.')[0] + '.s3db'
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS convoy (
                vehicle_id INTEGER PRIMARY KEY,
                engine_capacity INTEGER NOT NULL,
                fuel_consumption INTEGER NOT NULL,
                maximum_load INTEGER NOT NULL,
                score INTEGER NOT NULL)
                """)
    con.commit()
    rows_count = df.shape[0]
    # result = [f(row[0], ..., row[n]) for row in df[['col1', ...,'coln']].to_numpy()]
    scores = [scoring(row) for row in df[column_names].to_numpy()]
    df['score'] = scores
    df.to_sql('convoy', con=con, if_exists='append', index=False)
    print(f"{rows_count} {'records were' if rows_count > 1 else 'record was'} inserted to {db_name}")
    con.commit()
    con.close()


def convert_to_json(f_name, df):
    # All entries with a score of greater than 3
    # should be exported to the JSON file
    if '[CHECKED]' in f_name:
        name = f_name.split('[CHECKED]')[0]
    else:
        name = f_name.split('.')[0]
    df = df.loc[df['score'] >= 3]
    df = df.drop('score', axis=1)
    rows_count = df.shape[0]
    json_string = df.to_json(orient='records', indent=4)  # dataframe -> json string
    with open(name + '.json', 'w') as f:
        f.write('{"convoy": ' + json_string + '}')
    print(f"{rows_count} {'vehicles were' if rows_count > 1 or rows_count == 0 else 'vehicle was'} saved to {name}.json")


def convert_to_xml(f_name, df):
    """Converts pandas dataframe to xml. Important parameters: index, root_name, row_name, xml_declaration"""
    if '[CHECKED]' in f_name:
        name = f_name.split('[CHECKED]')[0]
    else:
        name = f_name.split('.')[0]
    df = df.loc[df['score'] < 3]
    df = df.drop('score', axis=1)
    rows_count = df.shape[0]
    if rows_count > 0:
        df.to_xml(f"{name}.xml", index=False, root_name='convoy', row_name='vehicle', xml_declaration=False)
    else:
        with open(f"{name}.xml", 'w') as xml:
            xml.write('<convoy>\n</convoy>')
    print(f"{rows_count} {'vehicles were' if rows_count > 1 or rows_count == 0 else 'vehicle was'} saved to {name}.xml")


def converting_conveyor(sequence, f_name, df):
    """Iterates over provided sequence and makes needed convertings"""
    if 'to_csv' in sequence:
        convert_to_csv(f_name, df)
    if 'to_checked_csv' in sequence:
        df = convert_to_checked_csv(f_name, df)
    if 'to_s3db' in sequence:
        convert_to_s3db(f_name, df)
    if 'to_json' in sequence:
        convert_to_json(f_name, df)
    if 'to_xml' in sequence:
        convert_to_xml(f_name, df)


def main(f_name: str):
    if f_name.endswith('.xlsx'):
        df = pd.read_excel(f_name, sheet_name=SHEET_NAME, dtype=str)
        converting_conveyor(xlsx_sequence, f_name, df)
    elif f_name.endswith('.s3db'):
        # read database using SQLALCHEMY engine
        con = create_engine('sqlite:///' + f_name)
        df = pd.read_sql('convoy', con)
        converting_conveyor(s3db_sequence, f_name, df)
    elif f_name.endswith('.csv'):
        df = pd.read_csv(f_name, dtype=str)
        if '[CHECKED]' in f_name:
            converting_conveyor(checked_csv_sequence, f_name, df)
        else:
            converting_conveyor(csv_sequence, f_name, df)
    else:
        print(f"Something went wrong. {f_name}")


# column_names = ['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']
def scoring(row):
    route_length = 450
    engine_capacity = int(row[1])
    fuel_consumption = int(row[2])
    maximum_load = int(row[3])
    pit_stops = route_length // (engine_capacity / fuel_consumption * 100)
    fuel_burned = route_length / 100 * fuel_consumption
    score = 0

    # Number of pitstops. If there are two or more gas stops on the way, the object has 0 points.
    # One stop at the filling station means 1 point.
    # No stops — 2 scoring points.
    if pit_stops >= 2:
        score += 0
    elif pit_stops == 1:
        score += 1
    elif pit_stops < 1:
        score += 2

    # Fuel consumed over the entire trip.
    # If a truck burned 230 liters or less, 2 points are given.
    # If more — 1 point.
    if fuel_burned <= 230:
        score += 2
    else:
        score += 1

    # Truck capacity.
    # If the capacity is 20 tones or more, it gets 2 points.
    # If less — 0 points.
    if maximum_load >= 20:
        score += 2

    return score


print("Input file name")
file_name = input() #'d_one_xlsx.xlsx'
main(file_name)
