import csv
import configparser

import psycopg2


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn_str = f"dbname={dbname} user={user} password={password} host={host} port={port}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

DATA_FOLDER = "data"

tables = [
    ("addresses", ["address_id", "address", "zipcode", "state", "country"]),
    ("order_items", ["order_id", "product_id", "quantity"]),
]
for table, header  in tables:
    with open(f"{DATA_FOLDER}/{table}.csv", "wt") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        query = f"select * from {table}"
        cursor.execute(query)

        results = cursor.fetchall()
        for each in results:
            writer.writerow(each)