import sqlite3
import argparse
import sys
import urllib.request

parser = argparse.ArgumentParser("Script to interact with data from the TAM API")
parser.add_argument("-n","--now", help="path to sqlite database", action="store_true", required=False)
parser.add_argument("-d","--db_path",nargs=1,type=str, help="path to sqlite database", required=True)
parser.add_argument("-c","--csv_path",nargs=1,type=str, help="path to csv file to load into the db", required=True)
args = parser.parse_args()

def download_csv():
    url='https://data.montpellier3m.fr/sites/default/files/ressources/TAM_MMM_TpsReel.csv'
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}
    request = urllib.request.Request(url, headers=hdr)
    data = urllib.request.urlopen(request).read().decode('utf-8')
    data_str = str(data)
    file = open(args.csv_path[0], 'w')
    file.write(data_str)
    file.close()

def clear_rows(cursor):
    cursor.execute("""DELETE FROM infoarret""")


def insert_csv_row(csv_row, cursor):
    cursor.execute("""INSERT INTO infoarret VALUES (?,?,?,?,?,?,?,?,?,?,?) """,
                   csv_row.strip().split(";"))


def load_csv(cursor):
    with open(args.csv_path[0], "r") as f:
        # ignore the header
        f.readline()
        line = f.readline()
        # loop over the lines in the file
        while line:
            if line != "\n":
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


def refresh():

    conn = sqlite3.connect(args.db_path[0])
    if not conn:
        print("Error : could not connect to database {}".format(args.db_path[0]))
        return 1

    c = conn.cursor()
    if args.now:
        remove_table(c)
    create_schema(c)

    load_csv(c)

    #write changes to database
    conn.commit()
    conn.close()
    return 0

def main():
    if not args.db_path or not args.csv_path:
        print("Error : missing command line arguments")
        return 1
    if args.now:
        download_csv()
    refresh()


if __name__ == "__main__":
    sys.exit(main())