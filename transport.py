import sqlite3
import argparse
import sys


def clear_rows(cursor):
    cursor.execute("""DELETE FROM infoarret""")


def insert_csv_row(csv_row, cursor):
    cursor.execute("""INSERT INTO infoarret VALUES (?,?,?,?,?,?,?,?,?,?,?) """,
                   csv_row.strip().split(";"))


def load_csv(path, cursor):
    with open(path, "r") as f:
        # ignore the header
        f.readline()
        line = f.readline()
        # loop over the lines in the file
        while line:
            insert_csv_row(line, cursor)
            line = f.readline()

def remove_table(cursor):
    cursor.execute("""DROP TABLE infoarret""")

def create_schema(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS "infoarret" (
    "course"	INTEGER,
    "stop_code"	TEXT,
    "stop_id"	INTEGER,
    "stop_name"	TEXT,
    "route_short_name"	TEXT,
    "trip_headsign"	TEXT,
    "direction_id"	INTEGER,
    "is_theorical" INTEGER,
    "departure_time"	TEXT,
    "delay_sec"	INTEGER,
    "dest_arr_code"	INTEGER
    );""")


parser = argparse.ArgumentParser("Script to interact with data from the TAM API")
parser.add_argument("db_path", help="path to sqlite database")
parser.add_argument("csv_path", help="path to csv file to load into the db")


def main():
    args = parser.parse_args()
    if not args.csv_path or not args.csv_path:
        print("Error : missing command line arguments")
        return 1

    conn = sqlite3.connect(args.db_path)

    if not conn:
        print("Error : could not connect to database {}".format(args.db_path))
        return 1

    c = conn.cursor()

    remove_table(c)
    create_schema(c)

    load_csv(args.csv_path, c)

    #write changes to database
    conn.commit()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
