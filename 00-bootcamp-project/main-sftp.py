import configparser

import pysftp


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
username = parser.get("sftp_config", "username")
password = parser.get("sftp_config", "password")
host = parser.get("sftp_config", "host")
port = parser.getint("sftp_config", "port")


# Security risk! Don't do this on production
# You lose a protection against Man-in-the-middle attacks
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

files = [
    "promos.csv",
    "products.csv",
    "addresses.csv",
    "events.csv",
    "order_items.csv",
    "orders.csv",
    "users.csv",
]
with pysftp.Connection(host, username=username, password=password, port=port, cnopts=cnopts) as sftp:
    for f in files:
        sftp.get(f, "data/"+f)
        print(f"Finished downloading: {f}")

    # sftp.put("some.txt", "uploaded.txt")