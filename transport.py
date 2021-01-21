import sqlite3
import argparse
import sys
import urllib.request

parser = argparse.ArgumentParser("Script to interact with data from the TAM API")
parser.add_argument("-n","--now", help="path to sqlite database", action="store_true", required=False)
parser.add_argument("-d","--db_path",nargs=1,type=str, help="path to sqlite database", required=True)
parser.add_argument("-c","--csv_path",nargs=1,type=str, help="path to csv file to load into the db", required=True)
subparser = parser.add_subparsers(dest='command')
timefunc = subparser.add_parser('time')
nextfunc = subparser.add_parser('next')
timefunc.add_argument('-l','--line',nargs=1, type=str)
timefunc.add_argument('-d','--dest',nargs=1, type=str)
timefunc.add_argument('-s','--station',nargs=1, type=str)
nextfunc.add_argument('-s','--station',nargs=1, type=str)
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

    if args.command == 'time':
        #print(args.station[0], args.line[0], args.dest[0])
        conn = sqlite3.connect(args.db_path[0])
        if not conn:
            print("Error : could not connect to database {}".format(args.db_path[0]))
            return 1
        req = "SELECT stop_name, route_short_name, trip_headsign, delay_sec " \
            "FROM infoarret " \
            "WHERE stop_name like 'stationname%' " \
            "AND route_short_name = 'linenumber' " \
            "AND trip_headsign like 'destname%' " \
            "ORDER BY delay_sec"
        req = req.replace('linenumber', args.line[0])
        req = req.replace('stationname', args.station[0])
        req = req.replace('destname', args.dest[0])
        print(req)
        c = conn.cursor()
        c.execute(req)

        response = c.fetchall()
        for row in response:
            #print(row)
            print('passage vers', row[2], "à l'arret", row[0],"dans", int(row[3]/60),'minutes et', int(row[3]%60),'secondes')
    
    elif args.command == 'next':
        #print(args.station[0], args.line[0], args.dest[0])
        conn = sqlite3.connect(args.db_path[0])
        if not conn:
            print("Error : could not connect to database {}".format(args.db_path[0]))
            return 1
        req = "SELECT stop_name, route_short_name, trip_headsign, delay_sec " \
            "FROM infoarret " \
            "WHERE stop_name like 'stationname' " \
            "ORDER BY delay_sec"
        req = req.replace('stationname', args.station[0])
        print(req)
        c = conn.cursor()
        c.execute(req)

        response = c.fetchall()
        for row in response:
            #print(row)
            print("passage sur la ligne", row[1], "vers", row[2], "à l'arret", row[0],"dans", int(row[3]/60),'minutes et', int(row[3]%60),'secondes')

if __name__ == "__main__":
    sys.exit(main())