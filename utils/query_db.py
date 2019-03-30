import sqlite3
from sqlite3 import Error
from pathlib import Path
# Import config file
import sys
import os
sys.path.append('../')
from configs.appconf import conf


# Get path of db
scheduler_db_path = conf["scheduler_db_path"]

# When connect to an SQLite database file that does not exist, SQLite creates a new database
conn = sqlite3.connect(scheduler_db_path)

# Get query from argument
sql = sys.argv[1]
c = conn.cursor()

if (sql[:6].lower() == "select"):
    rows = cur.fetchall()
    for row in rows:
        print(row)
else:
    c.execute(sql)
