import configparser
import csv

import requests

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
host = parser.get("api_config", "host")
port = parser.getint("api_config", "port")

API_URL = f"http://{host}:{port}/"

DATA_FOLDER = "data"

apis = [
    ("events", "2021-02-10"),
    ("users", "2020-10-23"),
    ("orders", "2021-02-10"),
]

for name, date  in apis:
    response = requests.get(f"{API_URL}/{name}/?created_at={date}")
    data = response.json()
    with open(f"{DATA_FOLDER}/{name}.csv", "wt") as f:
        writer = csv.writer(f)
        header = data[0].keys()
        writer.writerow(header)

        for each in data:
            writer.writerow(each.values())
