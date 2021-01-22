import sqlite3
import argparse
import sys
import urllib.request
import os
import logging

logging.basicConfig(
    filename="loggroupe.log", level=logging.INFO,
    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')
sources = os.listdir("./")
logging.info("Analyse du contenu du dossier")

parser = argparse.ArgumentParser(
    "Script to use data from the TAM API")
parser.add_argument(
    "-n", "--now",
    help="dowload the TAM csv and refresh the database",
    action="store_true", required=False)
parser.add_argument(
    "-d", "--db_path", nargs=1, type=str,
    help="path to sqlite database", required=True)
parser.add_argument(
    "-c", "--csv_path", nargs=1, type=str,
    help="path to csv file to load into the db", required=True)
parser.add_argument(
    "-o", "--output_path", action="store_true",
    help="path to output file you want to edit", required=False)
subparser = parser.add_subparsers(dest='command')
timefunc = subparser.add_parser('time')
timefunc.add_argument('-l', '--line', nargs=1, type=str)
timefunc.add_argument('-d', '--dest', nargs=1, type=str)
timefunc.add_argument('-s', '--station', nargs=1, type=str)
nextfunc = subparser.add_parser('next')
nextfunc.add_argument('-s', '--station', nargs=1, type=str)
args = parser.parse_args()


def download_csv():
    """Cette fonction se charge du téléchargement du fichier CSV TAM actuel"""
    logging.info('Téléchargement du fichier CSV depuis la TAM API')
    url = 'https://data.montpellier3m.fr/sites/default/' \
          'files/ressources/TAM_MMM_TpsReel.csv'
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11'
           ' (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,'
           'application/xml;q=0.9,*/*;q=0.8',
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
    logging.info("fichier csv téléchargé")


def clear_rows(cursor):
    """Cette fonction efface le contenu de 'infoarret'"""
    logging.info('Remise à zéro de la table "infoarret"')
    cursor.execute("""DELETE FROM infoarret""")


def insert_csv_row(csv_row, cursor):
    """Cette fonction sert a ajouter une ligne dans la DB"""
    cursor.execute("""INSERT INTO infoarret VALUES (?,?,?,?,?,?,?,?,?,?,?) """,
                   csv_row.strip().split(";"))


def load_csv(cursor):
    """Cette fonction se charge de remplir la table 'infoarret'"""
    logging.info('Lecture du fichier CSV et transfert dans la DB')
    with open(args.csv_path[0], "r") as f:
        f.readline()
        line = f.readline()
        while line:
            if line != "\n":
                insert_csv_row(line, cursor)
            line = f.readline()
    logging.info('transfert terminé')


def remove_table(cursor):
    """Cette fonction supprime la table 'infoarret'"""
    logging.warning("suppression de latable 'infoarret'")
    cursor.execute("""DROP TABLE infoarret""")


def create_schema(cursor):
    """Cette fonction sert a créer la table 'infoarret' si inexistante"""
    logging.info("création de la table 'infoarret'")
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
    """Cette fonction rafraichis la DB à partir du fichier CSV"""
    logging.info('connexion à la DB')
    conn = sqlite3.connect(args.db_path[0])
    if not conn:
        logging.error('connexion à la DB échouée')
        print(
            "Error : could not connect to database {}".format(args.db_path[0]))
        return 1
    logging.info('Connexion à la DB réussie')
    c = conn.cursor()
    if args.now and args.db_path[0] in sources:
        clear_rows(c)
        logging.info("table 'infoarret' vidée")
        load_csv(c)
        logging.info("table 'infoarret' synchronisée avec le fichier CSV")
    elif args.db_path[0] not in sources:
        create_schema(c)
        logging.info("table 'infoarret' créée")
        load_csv(c)
        logging.info("table 'infoarret' synchronisée avec le fichier CSV")

    conn.commit()
    conn.close()
    logging.info('fin de connexion à la DB')
    return 0


def main():
    """Cette fonction éxecute la requete utilisateur"""
    if not args.db_path or not args.csv_path:
        logging.error("argument obligatoire manquant")
        return 1

    if args.now or args.csv_path[0] not in sources:
        logging.info("la fichier csv et la DB vont être raffraichis")
        download_csv()
        refresh()

    if args.command == 'time':
        logging.info(
            f"l'utilisateur lance la commande {args.command}"
            f" sur la ligne{args.line[0]}"
            f" allant vers {args.dest[0]}"
            f" à partir de {args.station[0]}")
        conn = sqlite3.connect(args.db_path[0])
        if not conn:
            logging.error("connexion echouée à la DB")
            print(
                "Error : could not connect"
                " to database {}".format(args.db_path[0]))
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
        c = conn.cursor()
        c.execute(req)

        response = c.fetchall()
        for row in response:
            print('passage vers', row[2], "à l'arret", row[0],
                  "dans", int(row[3]/60), 'minutes et',
                  int(row[3] % 60), 'secondes')
        logging.info('commande réussie')
    elif args.command == 'next':
        logging.info(
            f"l'utilisateur lance la commande {args.command}"
            f" sur la station {args.station[0]}")
        conn = sqlite3.connect(args.db_path[0])
        if not conn:
            logging.error("connexion echouée à la DB")
            print("Error : could not connect"
                  " to database {}".format(args.db_path[0]))
            return 1
        req = "SELECT stop_name, route_short_name, trip_headsign, delay_sec " \
            "FROM infoarret " \
            "WHERE stop_name like 'stationname' " \
            "ORDER BY delay_sec"
        req = req.replace('stationname', args.station[0])
        c = conn.cursor()
        c.execute(req)

        response = c.fetchall()
        if args.output_path:
            result = ""
            for row in response:
                result += "passage sur la ligne " + row[1] + " vers "
                result += row[2] + " à l'arret " + row[0]
                result += " dans " + str(int(row[3]/60)) + 'minutes et'
                result += str(int(row[3] % 60)) + 'secondes \n'
            with open('resultats.txt', 'w', encoding="utf-8") as fichierres:
                fichierres.write(result)

        else:
            for row in response:
                print("passage sur la ligne", row[1], "vers",
                      row[2], "à l'arret", row[0],
                      "dans", int(row[3]/60), 'minutes et',
                      int(row[3] % 60), 'secondes')
            logging.info('commande réussie')

if __name__ == "__main__":
    sys.exit(main())
